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

//! Persistent producer for append-like snapshot actions.

#![allow(dead_code)] // FastAppend is migrated in the next stack layer.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestEntry, ManifestFile, ManifestWriter,
    ManifestWriterBuilder, Operation, SnapshotRef, Struct, StructType,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot_helpers::{
    generate_snapshot_id, manifest_list_path, manifest_path, write_snapshot_commit,
};
use crate::{Error, ErrorKind, Result};

#[derive(Debug)]
struct CachedSnapshot {
    snapshot: SnapshotRef,
    manifests: Vec<ManifestFile>,
}

/// A concrete snapshot producer retained by a simple action across retries.
#[derive(Default)]
pub(crate) struct SimpleSnapshotProducer {
    requested_commit_uuid: Option<Uuid>,
    commit_uuid: Option<Uuid>,
    snapshot_id: Option<i64>,
    attempt: u64,
    manifest_counter: u64,
    current_snapshot: Option<CachedSnapshot>,
    added_data_manifest: Option<ManifestFile>,
    attempted_manifest_lists: HashSet<String>,
    /// Metadata files created by this producer and safe to remove if they are not committed.
    owned_artifacts: HashSet<String>,
    #[cfg(test)]
    manifest_list_loads: usize,
    #[cfg(test)]
    added_manifest_writes: usize,
}

impl SimpleSnapshotProducer {
    pub(crate) fn new(commit_uuid: Option<Uuid>) -> Self {
        Self {
            requested_commit_uuid: commit_uuid,
            ..Self::default()
        }
    }

    fn commit_uuid(&mut self) -> Uuid {
        *self
            .commit_uuid
            .get_or_insert_with(|| self.requested_commit_uuid.unwrap_or_else(Uuid::now_v7))
    }

    fn snapshot_id(&mut self, table: &Table) -> i64 {
        *self
            .snapshot_id
            .get_or_insert_with(|| generate_snapshot_id(table))
    }

    fn next_manifest_path(&mut self, table: &Table) -> Result<String> {
        let commit_uuid = self.commit_uuid();
        let counter = self.manifest_counter;
        self.manifest_counter += 1;
        manifest_path(table, commit_uuid, counter)
    }

    fn next_manifest_list_path(&mut self, table: &Table) -> Result<String> {
        let snapshot_id = self.snapshot_id(table);
        let commit_uuid = self.commit_uuid();
        let attempt = self.attempt;
        self.attempt += 1;
        let path = manifest_list_path(table, snapshot_id, attempt, commit_uuid)?;
        self.attempted_manifest_lists.insert(path.clone());
        self.owned_artifacts.insert(path.clone());
        Ok(path)
    }

    pub(crate) fn validate_added_data_files(
        &self,
        table: &Table,
        data_files: &[DataFile],
    ) -> Result<()> {
        let metadata = table.metadata();
        for file in data_files {
            if file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            if metadata.default_partition_spec_id() != file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            validate_partition_value(file.partition(), metadata.default_partition_type())?;
        }
        Ok(())
    }

    pub(crate) async fn apply(
        &mut self,
        table: &Table,
        properties: HashMap<String, String>,
        data_files: &[DataFile],
    ) -> Result<ActionCommit> {
        if data_files.is_empty() && properties.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or snapshot properties were provided",
            ));
        }
        self.validate_added_data_files(table, data_files)?;
        let mut manifests = self.current_manifests(table).await?;
        if !data_files.is_empty() {
            manifests.push(self.added_manifest(table, data_files).await?);
        }

        let snapshot_id = self.snapshot_id(table);
        let manifest_list = self.next_manifest_list_path(table)?;
        write_snapshot_commit(
            table,
            snapshot_id,
            manifest_list,
            Operation::Append,
            properties,
            data_files,
            &[],
            manifests,
        )
        .await
    }

    async fn current_manifests(&mut self, table: &Table) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            self.current_snapshot = None;
            return Ok(Vec::new());
        };
        if let Some(cached) = &self.current_snapshot
            && cached.snapshot.as_ref() == snapshot.as_ref()
        {
            return Ok(cached.manifests.clone());
        }

        let list = table.manifest_list_reader(snapshot).load().await?;
        #[cfg(test)]
        {
            self.manifest_list_loads += 1;
        }
        let manifests = list
            .consume_entries()
            .into_iter()
            .filter(|manifest| {
                manifest.has_added_files()
                    || manifest.has_existing_files()
                    || manifest.has_deleted_files()
            })
            .collect();
        self.current_snapshot = Some(CachedSnapshot {
            snapshot: snapshot.clone(),
            manifests,
        });
        Ok(self.current_snapshot.as_ref().unwrap().manifests.clone())
    }

    async fn added_manifest(
        &mut self,
        table: &Table,
        data_files: &[DataFile],
    ) -> Result<ManifestFile> {
        if let Some(manifest) = &self.added_data_manifest {
            return Ok(manifest.clone());
        }

        let path = self.next_manifest_path(table)?;
        let output = table.file_io().new_output(path.clone())?;
        let spec = table.metadata().default_partition_spec().as_ref().clone();
        let schema = table.metadata().current_schema().clone();
        let snapshot_id = self.snapshot_id(table);
        let builder = match table.encryption_manager() {
            Some(manager) => ManifestWriterBuilder::new_from_encrypted(
                manager.encrypt(output),
                Some(snapshot_id),
                schema,
                spec,
            )?,
            None => ManifestWriterBuilder::new(output, Some(snapshot_id), schema, spec),
        };
        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => builder.build_v2_data(),
            FormatVersion::V3 => builder.build_v3_data(),
        };
        add_data_entries(
            &mut writer,
            table.metadata().format_version(),
            snapshot_id,
            data_files,
        )?;
        let manifest = writer.write_manifest_file().await?;
        self.owned_artifacts.insert(path);
        self.added_data_manifest = Some(manifest.clone());
        #[cfg(test)]
        {
            self.added_manifest_writes += 1;
        }
        Ok(manifest)
    }
}

fn add_data_entries(
    writer: &mut ManifestWriter,
    format_version: FormatVersion,
    snapshot_id: i64,
    files: &[DataFile],
) -> Result<()> {
    for file in files {
        let builder = ManifestEntry::builder()
            .status(crate::spec::ManifestStatus::Added)
            .data_file(file.clone());
        let entry = if format_version == FormatVersion::V1 {
            builder.snapshot_id(snapshot_id).build()
        } else {
            builder.build()
        };
        writer.add_entry(entry)?;
    }
    Ok(())
}

fn validate_partition_value(value: &Struct, partition_type: &StructType) -> Result<()> {
    if value.fields().len() != partition_type.fields().len() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Partition value is not compatible with partition type",
        ));
    }
    for (value, field) in value.fields().iter().zip(partition_type.fields()) {
        let primitive = field.field_type.as_primitive_type().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Partition field should only be a primitive type",
            )
        })?;
        if let Some(value) = value
            && !primitive.compatible(&value.as_primitive_literal().unwrap())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataFileBuilder, DataFileFormat, Literal, ManifestListWriter};
    use crate::transaction::tests::make_v2_table;

    fn data_file(table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    async fn write_empty_current_manifest_list(table: &Table) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let output = table
            .file_io()
            .new_output(snapshot.manifest_list())
            .unwrap();
        ManifestListWriter::v2(
            output.writer().await.unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        )
        .close()
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn keeps_identity_and_added_manifest_across_attempts() {
        let table = make_v2_table();
        write_empty_current_manifest_list(&table).await;
        let requested = Uuid::now_v7();
        let mut producer = SimpleSnapshotProducer::new(Some(requested));
        let file = data_file(&table);

        producer
            .apply(&table, HashMap::new(), std::slice::from_ref(&file))
            .await
            .unwrap();
        let snapshot_id = producer.snapshot_id.unwrap();
        producer
            .apply(&table, HashMap::new(), std::slice::from_ref(&file))
            .await
            .unwrap();

        assert_eq!(producer.commit_uuid, Some(requested));
        assert_eq!(producer.snapshot_id, Some(snapshot_id));
        assert_eq!(producer.added_manifest_writes, 1);
        assert_eq!(producer.manifest_list_loads, 1);
        assert_eq!(producer.attempted_manifest_lists.len(), 2);
    }

    #[test]
    fn rejects_non_data_content() {
        let table = make_v2_table();
        let mut file = data_file(&table);
        file.content = DataContentType::EqualityDeletes;
        let producer = SimpleSnapshotProducer::default();
        assert_eq!(
            producer
                .validate_added_data_files(&table, &[file])
                .unwrap_err()
                .kind(),
            ErrorKind::DataInvalid
        );
    }
}
