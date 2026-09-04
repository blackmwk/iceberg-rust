// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestFile, ManifestWriter,
    ManifestWriterBuilder,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

#[derive(Clone)]
struct FilteredManifest {
    output: ManifestFile,
    removed_files: Vec<DataFile>,
}

/// Retry-persistent exact-path filtering for one manifest content type.
pub(crate) struct ManifestFilterManager {
    content: ManifestContentType,
    requested_paths: HashSet<String>,
    loaded_manifests: HashMap<String, crate::spec::Manifest>,
    filtered_manifests: HashMap<String, FilteredManifest>,
    found_paths: HashSet<String>,
    rewrite_counter: u64,
    owned_artifacts: HashSet<String>,
}

impl ManifestFilterManager {
    pub(crate) fn new(
        content: ManifestContentType,
        requested_paths: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            content,
            requested_paths: requested_paths.into_iter().collect(),
            loaded_manifests: HashMap::new(),
            filtered_manifests: HashMap::new(),
            found_paths: HashSet::new(),
            rewrite_counter: 0,
            owned_artifacts: HashSet::new(),
        }
    }

    pub(crate) fn requested_paths(&self) -> &HashSet<String> {
        &self.requested_paths
    }

    pub(crate) fn owned_artifacts(&self) -> &HashSet<String> {
        &self.owned_artifacts
    }

    pub(crate) async fn filter(
        &mut self,
        table: &Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifests: Vec<ManifestFile>,
    ) -> Result<(Vec<ManifestFile>, Vec<DataFile>)> {
        if self.requested_paths.is_empty() {
            return Ok((manifests, Vec::new()));
        }

        let mut outputs = Vec::with_capacity(manifests.len());
        let mut removed = Vec::new();
        for source in manifests {
            if source.content != self.content {
                outputs.push(source);
                continue;
            }
            let result = if let Some(cached) = self.filtered_manifests.get(&source.manifest_path) {
                cached.clone()
            } else {
                let result = self
                    .filter_manifest(table, snapshot_id, commit_uuid, &source)
                    .await?;
                self.filtered_manifests
                    .insert(source.manifest_path.clone(), result.clone());
                result
            };
            self.found_paths.extend(
                result
                    .removed_files
                    .iter()
                    .map(|file| file.file_path().to_string()),
            );
            removed.extend(result.removed_files.iter().cloned());
            outputs.push(result.output);
        }

        let missing: Vec<_> = self
            .requested_paths
            .difference(&self.found_paths)
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok((outputs, removed))
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot remove files that are not live: {}",
                    missing.join(", ")
                ),
            ))
        }
    }

    async fn filter_manifest(
        &mut self,
        table: &Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        source: &ManifestFile,
    ) -> Result<FilteredManifest> {
        let manifest = if let Some(cached) = self.loaded_manifests.get(&source.manifest_path) {
            cached.clone()
        } else {
            let manifest = table.manifest_reader().read(source).await?;
            self.loaded_manifests
                .insert(source.manifest_path.clone(), manifest.clone());
            manifest
        };
        let removed_files: Vec<_> = manifest
            .entries()
            .iter()
            .filter(|entry| entry.is_alive() && self.requested_paths.contains(entry.file_path()))
            .map(|entry| entry.data_file().clone())
            .collect();
        if removed_files.is_empty() {
            return Ok(FilteredManifest {
                output: source.clone(),
                removed_files,
            });
        }

        let path = format!(
            "{}/{}-{}-r{}.{}",
            table.metadata().metadata_location()?,
            commit_uuid,
            match self.content {
                ManifestContentType::Data => "data",
                ManifestContentType::Deletes => "delete",
            },
            self.rewrite_counter,
            DataFileFormat::Avro
        );
        self.rewrite_counter += 1;
        let mut writer = new_writer(table, snapshot_id, source, &path)?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            if self.requested_paths.contains(entry.file_path()) {
                writer.add_delete_entry((**entry).clone())?;
            } else {
                writer.add_existing_entry((**entry).clone())?;
            }
        }
        let output = writer.write_manifest_file().await?;
        self.owned_artifacts.insert(path);
        Ok(FilteredManifest {
            output,
            removed_files,
        })
    }
}

fn new_writer(
    table: &Table,
    snapshot_id: i64,
    source: &ManifestFile,
    path: &str,
) -> Result<ManifestWriter> {
    let spec = table
        .metadata()
        .partition_spec_by_id(source.partition_spec_id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot find partition spec {} for manifest rewrite",
                    source.partition_spec_id
                ),
            )
        })?
        .as_ref()
        .clone();
    let schema = table.metadata().current_schema().clone();
    let output = table.file_io().new_output(path)?;
    let builder = match table.encryption_manager() {
        Some(manager) => ManifestWriterBuilder::new_from_encrypted(
            manager.encrypt(output),
            Some(snapshot_id),
            schema,
            spec,
        )?,
        None => ManifestWriterBuilder::new(output, Some(snapshot_id), schema, spec),
    };
    match (table.metadata().format_version(), source.content) {
        (FormatVersion::V1, ManifestContentType::Data) => Ok(builder.build_v1()),
        (FormatVersion::V1, ManifestContentType::Deletes) => Err(Error::new(
            ErrorKind::DataInvalid,
            "Delete manifests are not supported in V1",
        )),
        (FormatVersion::V2, ManifestContentType::Data) => Ok(builder.build_v2_data()),
        (FormatVersion::V2, ManifestContentType::Deletes) => Ok(builder.build_v2_deletes()),
        (FormatVersion::V3, ManifestContentType::Data) => Ok(builder.build_v3_data()),
        (FormatVersion::V3, ManifestContentType::Deletes) => Ok(builder.build_v3_deletes()),
    }
}
