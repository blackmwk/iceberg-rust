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

//! Persistent producer for snapshot actions that merge additions and removals.

#![allow(dead_code)] // RowDelta is introduced by later stack layers.

use std::collections::{BTreeMap, HashMap, HashSet};

use uuid::Uuid;

use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation, Struct,
    StructType,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::conflict_filter::ConflictFilter;
use crate::transaction::manifest_filter::ManifestFilterManager;
use crate::transaction::snapshot_helpers::{
    delete_artifact, generate_snapshot_id, manifest_list_path, manifest_path, write_snapshot_commit,
};
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotFingerprint {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
}

#[derive(Debug)]
struct CurrentSnapshot {
    fingerprint: SnapshotFingerprint,
    manifests: Vec<ManifestFile>,
}

#[derive(Debug)]
pub(crate) struct ProcessedSnapshot {
    fingerprint: SnapshotFingerprint,
    operation: Operation,
    data_manifests: Vec<ManifestFile>,
    delete_manifests: Vec<ManifestFile>,
}

impl ProcessedSnapshot {
    pub(crate) fn operation(&self) -> &Operation {
        &self.operation
    }

    pub(crate) fn data_manifests(&self) -> &[ManifestFile] {
        &self.data_manifests
    }

    pub(crate) fn delete_manifests(&self) -> &[ManifestFile] {
        &self.delete_manifests
    }
}

/// A concrete merging producer retained by a RowDelta action across retries.
#[derive(Default)]
pub(crate) struct MergingSnapshotProducer {
    requested_commit_uuid: Option<Uuid>,
    commit_uuid: Option<Uuid>,
    snapshot_id: Option<i64>,
    attempt: u64,
    manifest_counter: u64,
    current_snapshot: Option<CurrentSnapshot>,
    processed_snapshots: HashMap<i64, ProcessedSnapshot>,
    loaded_manifests: HashMap<String, crate::spec::Manifest>,
    predicate_results: HashMap<String, (bool, bool)>,
    new_data_manifests: Option<Vec<ManifestFile>>,
    new_delete_manifests: Option<Vec<ManifestFile>>,
    data_filter: Option<ManifestFilterManager>,
    delete_filter: Option<ManifestFilterManager>,
    attempted_manifest_lists: HashSet<String>,
    owned_artifacts: HashSet<String>,
    #[cfg(test)]
    manifest_list_loads: usize,
    #[cfg(test)]
    new_manifest_writes: usize,
}

impl MergingSnapshotProducer {
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply(
        &mut self,
        table: &Table,
        operation: Operation,
        properties: HashMap<String, String>,
        added_data_files: &[DataFile],
        added_delete_files: &[DataFile],
        removed_data_files: &[DataFile],
        removed_delete_files: &[DataFile],
    ) -> Result<ActionCommit> {
        if added_data_files.is_empty()
            && added_delete_files.is_empty()
            && removed_data_files.is_empty()
            && removed_delete_files.is_empty()
            && properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No files or snapshot properties were provided",
            ));
        }
        self.validate_files(table, added_data_files, added_delete_files)?;
        validate_disjoint_changes(
            added_data_files,
            added_delete_files,
            removed_data_files,
            removed_delete_files,
        )?;

        let current = self.current_manifests(table).await?;
        let (mut manifests, removed_files) = self
            .filter_manifests(table, current, removed_data_files, removed_delete_files)
            .await?;
        manifests.extend(self.new_data_manifests(table, added_data_files).await?);
        manifests.extend(self.new_delete_manifests(table, added_delete_files).await?);
        let mut added_files = Vec::with_capacity(added_data_files.len() + added_delete_files.len());
        added_files.extend_from_slice(added_data_files);
        added_files.extend_from_slice(added_delete_files);

        let snapshot_id = self.snapshot_id(table);
        let manifest_list = self.next_manifest_list_path(table)?;
        write_snapshot_commit(
            table,
            snapshot_id,
            manifest_list,
            operation,
            properties,
            &added_files,
            &removed_files,
            manifests,
        )
        .await
    }

    pub(crate) async fn finish_commit(&mut self, table: &Table, commit_error: Option<&Error>) {
        let retained = match commit_error {
            None => self.committed_artifacts(table).await,
            Some(error) if error.kind() == ErrorKind::CatalogCommitConflicts => {
                Some(HashSet::new())
            }
            Some(_) => None,
        };
        let Some(retained) = retained else {
            return;
        };
        let obsolete: Vec<_> = self
            .owned_artifacts
            .difference(&retained)
            .cloned()
            .collect();
        for path in obsolete {
            delete_artifact(table, &path).await;
            self.owned_artifacts.remove(&path);
        }
        self.attempted_manifest_lists
            .retain(|path| retained.contains(path));
    }

    async fn committed_artifacts(&self, table: &Table) -> Option<HashSet<String>> {
        let snapshot = table.metadata().snapshot_by_id(self.snapshot_id?)?;
        let mut retained = HashSet::from([snapshot.manifest_list().to_string()]);
        let list = table.manifest_list_reader(snapshot).load().await.ok()?;
        retained.extend(
            list.entries()
                .iter()
                .map(|manifest| manifest.manifest_path.clone()),
        );
        Some(retained)
    }

    async fn filter_manifests(
        &mut self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        removed_data_files: &[DataFile],
        removed_delete_files: &[DataFile],
    ) -> Result<(Vec<ManifestFile>, Vec<DataFile>)> {
        let data_paths = removed_data_files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect::<HashSet<_>>();
        let delete_paths = removed_delete_files
            .iter()
            .map(|file| file.file_path().to_string())
            .collect::<HashSet<_>>();
        initialize_filter(&mut self.data_filter, ManifestContentType::Data, data_paths)?;
        initialize_filter(
            &mut self.delete_filter,
            ManifestContentType::Deletes,
            delete_paths,
        )?;

        let snapshot_id = self.snapshot_id(table);
        let commit_uuid = self.commit_uuid();
        let (manifests, mut removed) = self
            .data_filter
            .as_mut()
            .unwrap()
            .filter(table, snapshot_id, commit_uuid, manifests)
            .await?;
        let (manifests, removed_deletes) = self
            .delete_filter
            .as_mut()
            .unwrap()
            .filter(table, snapshot_id, commit_uuid, manifests)
            .await?;
        removed.extend(removed_deletes);
        self.owned_artifacts.extend(
            self.data_filter
                .as_ref()
                .unwrap()
                .owned_artifacts()
                .iter()
                .cloned(),
        );
        self.owned_artifacts.extend(
            self.delete_filter
                .as_ref()
                .unwrap()
                .owned_artifacts()
                .iter()
                .cloned(),
        );
        Ok((manifests, removed))
    }

    fn validate_files(
        &self,
        table: &Table,
        data_files: &[DataFile],
        delete_files: &[DataFile],
    ) -> Result<()> {
        for file in data_files {
            if file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Row data files must have data content",
                ));
            }
            validate_partition(table, file)?;
        }
        for file in delete_files {
            if file.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Delete files must have position or equality delete content",
                ));
            }
            validate_delete_version(table.metadata().format_version(), file)?;
            validate_partition(table, file)?;
        }
        Ok(())
    }

    async fn current_manifests(&mut self, table: &Table) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            self.current_snapshot = None;
            return Ok(Vec::new());
        };
        let fingerprint = SnapshotFingerprint {
            snapshot_id: snapshot.snapshot_id(),
            parent_snapshot_id: snapshot.parent_snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            manifest_list: snapshot.manifest_list().to_string(),
        };
        if let Some(cached) = &self.current_snapshot
            && cached.fingerprint == fingerprint
        {
            return Ok(cached.manifests.clone());
        }

        let list = table.manifest_list_reader(snapshot).load().await?;
        #[cfg(test)]
        {
            self.manifest_list_loads += 1;
        }
        let manifests = list.consume_entries().into_iter().collect();
        self.current_snapshot = Some(CurrentSnapshot {
            fingerprint,
            manifests,
        });
        Ok(self.current_snapshot.as_ref().unwrap().manifests.clone())
    }

    /// Process ancestry down to `starting_snapshot_id` (exclusive), loading
    /// only snapshots that this producer has not seen on an earlier attempt.
    pub(crate) async fn process_new_snapshots(
        &mut self,
        table: &Table,
        starting_snapshot_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let mut snapshot = table.metadata().current_snapshot().cloned();
        let mut visited = HashSet::new();
        let mut newly_processed = Vec::new();
        let mut reached_start = starting_snapshot_id.is_none();

        while let Some(current) = snapshot {
            let snapshot_id = current.snapshot_id();
            if Some(snapshot_id) == starting_snapshot_id {
                reached_start = true;
                break;
            }
            if !visited.insert(snapshot_id) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot ancestry contains a cycle at {snapshot_id}"),
                ));
            }
            if self.load_processed_snapshot(table, &current).await? {
                newly_processed.push(snapshot_id);
            }
            snapshot = current
                .parent_snapshot_id()
                .and_then(|parent| table.metadata().snapshot_by_id(parent).cloned());
        }

        if !reached_start {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot determine history from the current snapshot to starting snapshot {}",
                    starting_snapshot_id.unwrap()
                ),
            ));
        }
        Ok(newly_processed)
    }

    async fn load_processed_snapshot(
        &mut self,
        table: &Table,
        snapshot: &crate::spec::SnapshotRef,
    ) -> Result<bool> {
        let snapshot_id = snapshot.snapshot_id();
        let fingerprint = SnapshotFingerprint {
            snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            manifest_list: snapshot.manifest_list().to_string(),
        };
        if let Some(processed) = self.processed_snapshots.get(&snapshot_id) {
            if processed.fingerprint != fingerprint {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {snapshot_id} changed while retrying a commit"),
                ));
            }
            return Ok(false);
        }

        let manifests = if let Some(current) = &self.current_snapshot
            && current.fingerprint == fingerprint
        {
            current.manifests.clone()
        } else {
            let list = table.manifest_list_reader(snapshot).load().await?;
            #[cfg(test)]
            {
                self.manifest_list_loads += 1;
            }
            list.consume_entries().into_iter().collect()
        };
        let mut data_manifests = Vec::new();
        let mut delete_manifests = Vec::new();
        for manifest in manifests {
            match manifest.content {
                ManifestContentType::Data => data_manifests.push(manifest),
                ManifestContentType::Deletes => delete_manifests.push(manifest),
            }
        }
        self.processed_snapshots
            .insert(snapshot_id, ProcessedSnapshot {
                fingerprint,
                operation: snapshot.summary().operation.clone(),
                data_manifests,
                delete_manifests,
            });
        Ok(true)
    }

    pub(crate) fn processed_snapshot(&self, snapshot_id: i64) -> Option<&ProcessedSnapshot> {
        self.processed_snapshots.get(&snapshot_id)
    }

    async fn validation_history(
        &mut self,
        table: &Table,
        starting_snapshot_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        self.process_new_snapshots(table, starting_snapshot_id)
            .await?;
        let mut history = Vec::new();
        let mut snapshot = table.metadata().current_snapshot();
        while let Some(current) = snapshot {
            if Some(current.snapshot_id()) == starting_snapshot_id {
                break;
            }
            history.push(current.snapshot_id());
            snapshot = current
                .parent_snapshot_id()
                .and_then(|parent| table.metadata().snapshot_by_id(parent));
        }
        Ok(history)
    }

    async fn load_manifest(
        &mut self,
        table: &Table,
        manifest: &ManifestFile,
    ) -> Result<crate::spec::Manifest> {
        if let Some(cached) = self.loaded_manifests.get(&manifest.manifest_path) {
            return Ok(cached.clone());
        }
        let loaded = table.manifest_reader().read(manifest).await?;
        self.loaded_manifests
            .insert(manifest.manifest_path.clone(), loaded.clone());
        Ok(loaded)
    }

    /// Validate that referenced data paths are live in the current snapshot.
    pub(crate) async fn validate_data_files_exist(
        &mut self,
        table: &Table,
        paths: &HashSet<String>,
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let live = self.live_files(table).await?;
        let missing: Vec<_> = paths
            .iter()
            .filter(|path| !live.data.contains_key(*path))
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Required data files are no longer live: {}",
                    missing.join(", ")
                ),
            ))
        }
    }

    /// Reject referenced paths removed or rewritten since the validation boundary.
    pub(crate) async fn validate_no_rewrites(
        &mut self,
        table: &Table,
        starting_snapshot_id: Option<i64>,
        paths: &HashSet<String>,
    ) -> Result<()> {
        let history = self.validation_history(table, starting_snapshot_id).await?;
        for snapshot_id in history {
            let manifests = self
                .processed_snapshots
                .get(&snapshot_id)
                .map(|snapshot| snapshot.data_manifests.clone())
                .unwrap_or_default();
            for manifest_file in manifests
                .into_iter()
                .filter(|manifest| manifest.added_snapshot_id == snapshot_id)
            {
                let removed = self
                    .load_manifest(table, &manifest_file)
                    .await?
                    .entries()
                    .iter()
                    .any(|entry| {
                        entry.status() == ManifestStatus::Deleted
                            && paths.contains(entry.file_path())
                    });
                if removed {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "A required data file was concurrently removed or rewritten",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Reject delete files introduced since the boundary that apply to the
    /// supplied data files according to Iceberg sequence rules.
    pub(crate) async fn validate_no_new_deletes_for_data_files(
        &mut self,
        table: &Table,
        starting_snapshot_id: Option<i64>,
        data_files: &[DataFile],
        ignore_equality_deletes: bool,
    ) -> Result<()> {
        let live = self.live_files(table).await?;
        let history = self.validation_history(table, starting_snapshot_id).await?;
        for snapshot_id in history {
            let manifests = self
                .processed_snapshots
                .get(&snapshot_id)
                .map(|snapshot| snapshot.delete_manifests.clone())
                .unwrap_or_default();
            for manifest_file in manifests
                .into_iter()
                .filter(|manifest| manifest.added_snapshot_id == snapshot_id)
            {
                let manifest = self.load_manifest(table, &manifest_file).await?;
                for delete in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                    if ignore_equality_deletes
                        && delete.content_type() == DataContentType::EqualityDeletes
                    {
                        continue;
                    }
                    for data_file in data_files {
                        let data_sequence = live
                            .data
                            .get(data_file.file_path())
                            .and_then(|file| file.sequence_number)
                            .unwrap_or(0);
                        if delete_applies(
                            data_file,
                            data_sequence,
                            delete.data_file(),
                            delete.sequence_number().unwrap_or(0),
                        ) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "New delete file {} applies to data file {}",
                                    delete.file_path(),
                                    data_file.file_path()
                                ),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn live_files(&mut self, table: &Table) -> Result<LiveFiles> {
        let manifests = self.current_manifests(table).await?;
        let mut live = LiveFiles::default();
        for manifest_file in manifests {
            let content = manifest_file.content;
            for entry in self
                .load_manifest(table, &manifest_file)
                .await?
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
            {
                let destination = if content == ManifestContentType::Data {
                    &mut live.data
                } else {
                    &mut live.deletes
                };
                destination.insert(entry.file_path().to_string(), LiveFile {
                    sequence_number: entry.sequence_number(),
                });
            }
        }
        Ok(live)
    }

    /// Detect predicate-scoped additions and removals in snapshots committed
    /// after the validation boundary.
    pub(crate) async fn validate_no_conflicting_files(
        &mut self,
        table: &Table,
        starting_snapshot_id: Option<i64>,
        filter: &ConflictFilter,
        check_data_additions: bool,
        check_delete_additions: bool,
        check_removals: bool,
    ) -> Result<()> {
        let history = self.validation_history(table, starting_snapshot_id).await?;
        for snapshot_id in history {
            let manifests = self
                .processed_snapshots
                .get(&snapshot_id)
                .map(|snapshot| {
                    snapshot
                        .data_manifests
                        .iter()
                        .chain(&snapshot.delete_manifests)
                        .filter(|manifest| manifest.added_snapshot_id == snapshot_id)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for manifest_file in manifests {
                if !filter.manifest_could_match(table, &manifest_file)? {
                    continue;
                }
                let manifest = self.load_manifest(table, &manifest_file).await?;
                for entry in manifest.entries() {
                    let is_removal = entry.status() == ManifestStatus::Deleted;
                    let is_addition = entry.status() == ManifestStatus::Added;
                    let should_check = (is_removal && check_removals)
                        || (is_addition
                            && match entry.content_type() {
                                DataContentType::Data => check_data_additions,
                                DataContentType::PositionDeletes
                                | DataContentType::EqualityDeletes => check_delete_additions,
                            });
                    if !should_check {
                        continue;
                    }
                    let path = entry.file_path();
                    let (could_match, must_match) =
                        if let Some(result) = self.predicate_results.get(path) {
                            *result
                        } else {
                            let result = (
                                filter.could_match(table, entry.data_file())?,
                                filter.must_match(table, entry.data_file())?,
                            );
                            self.predicate_results.insert(path.to_string(), result);
                            result
                        };
                    if could_match || (is_removal && must_match) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Conflicting file {path} matches the validation filter"),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn new_data_manifests(
        &mut self,
        table: &Table,
        files: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        if let Some(manifests) = &self.new_data_manifests {
            return Ok(manifests.clone());
        }
        let manifests = self
            .write_manifests(table, ManifestContentType::Data, files)
            .await?;
        self.new_data_manifests = Some(manifests.clone());
        Ok(manifests)
    }

    async fn new_delete_manifests(
        &mut self,
        table: &Table,
        files: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        if let Some(manifests) = &self.new_delete_manifests {
            return Ok(manifests.clone());
        }
        let manifests = self
            .write_manifests(table, ManifestContentType::Deletes, files)
            .await?;
        self.new_delete_manifests = Some(manifests.clone());
        Ok(manifests)
    }

    async fn write_manifests(
        &mut self,
        table: &Table,
        content: ManifestContentType,
        files: &[DataFile],
    ) -> Result<Vec<ManifestFile>> {
        let mut by_spec: BTreeMap<i32, Vec<DataFile>> = BTreeMap::new();
        for file in files {
            by_spec
                .entry(file.partition_spec_id)
                .or_default()
                .push(file.clone());
        }

        let mut manifests = Vec::with_capacity(by_spec.len());
        for (spec_id, files) in by_spec {
            let path = self.next_manifest_path(table)?;
            let mut writer = self
                .new_manifest_writer(table, &path, spec_id, content)
                .await?;
            let snapshot_id = self.snapshot_id(table);
            for file in files {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(file);
                let entry = if table.metadata().format_version() == FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else {
                    builder.build()
                };
                writer.add_entry(entry)?;
            }
            let manifest = writer.write_manifest_file().await?;
            self.owned_artifacts.insert(path);
            manifests.push(manifest);
            #[cfg(test)]
            {
                self.new_manifest_writes += 1;
            }
        }
        Ok(manifests)
    }

    async fn new_manifest_writer(
        &mut self,
        table: &Table,
        path: &str,
        spec_id: i32,
        content: ManifestContentType,
    ) -> Result<ManifestWriter> {
        let spec = table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| unknown_spec(spec_id))?
            .as_ref()
            .clone();
        let schema = table.metadata().current_schema().clone();
        let output = table.file_io().new_output(path)?;
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
        match (table.metadata().format_version(), content) {
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
}

#[derive(Default)]
struct LiveFiles {
    data: HashMap<String, LiveFile>,
    deletes: HashMap<String, LiveFile>,
}

struct LiveFile {
    sequence_number: Option<i64>,
}

fn delete_applies(
    data: &DataFile,
    data_sequence: i64,
    delete: &DataFile,
    delete_sequence: i64,
) -> bool {
    if let Some(referenced) = delete.referenced_data_file()
        && referenced != data.file_path
    {
        return false;
    }
    let same_partition = delete.partition().fields().is_empty()
        || (delete.partition_spec_id == data.partition_spec_id
            && delete.partition() == data.partition());
    if !same_partition {
        return false;
    }
    match delete.content_type() {
        DataContentType::EqualityDeletes => delete_sequence > data_sequence,
        DataContentType::PositionDeletes => delete_sequence >= data_sequence,
        DataContentType::Data => false,
    }
}

fn initialize_filter(
    filter: &mut Option<ManifestFilterManager>,
    content: ManifestContentType,
    paths: HashSet<String>,
) -> Result<()> {
    match filter {
        Some(existing) if existing.requested_paths() != &paths => Err(Error::new(
            ErrorKind::DataInvalid,
            "Files to remove changed while retrying a snapshot action",
        )),
        Some(_) => Ok(()),
        None => {
            *filter = Some(ManifestFilterManager::new(content, paths));
            Ok(())
        }
    }
}

fn validate_disjoint_changes(
    added_data: &[DataFile],
    added_deletes: &[DataFile],
    removed_data: &[DataFile],
    removed_deletes: &[DataFile],
) -> Result<()> {
    let added = added_data
        .iter()
        .chain(added_deletes)
        .map(DataFile::file_path)
        .collect::<HashSet<_>>();
    let removed = removed_data
        .iter()
        .chain(removed_deletes)
        .map(DataFile::file_path)
        .collect::<HashSet<_>>();
    let overlap = added.intersection(&removed).copied().collect::<Vec<_>>();
    if overlap.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot add and remove the same files: {}",
                overlap.join(", ")
            ),
        ))
    }
}

fn validate_delete_version(version: FormatVersion, file: &DataFile) -> Result<()> {
    let deletion_vector = file.file_format() == DataFileFormat::Puffin
        || file.content_offset().is_some()
        || file.content_size_in_bytes().is_some();
    if deletion_vector {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Deletion vectors are not supported by RowDelta",
        ));
    }
    match version {
        FormatVersion::V1 => Err(Error::new(
            ErrorKind::DataInvalid,
            "Delete files are not supported in V1",
        )),
        FormatVersion::V3 if file.content_type() == DataContentType::PositionDeletes => {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "V3 position deletes require deletion vectors, which are not supported",
            ))
        }
        FormatVersion::V2 | FormatVersion::V3 => Ok(()),
    }
}

fn validate_partition(table: &Table, file: &DataFile) -> Result<()> {
    let spec = table
        .metadata()
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| unknown_spec(file.partition_spec_id))?;
    let partition_type = spec.partition_type(table.metadata().current_schema())?;
    validate_partition_value(file.partition(), &partition_type)
}

fn unknown_spec(spec_id: i32) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("Cannot write a file for unknown partition spec {spec_id}"),
    )
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
    use crate::spec::{DataFileBuilder, Literal, ManifestListWriter};
    use crate::transaction::tests::make_v2_table;

    fn file(table: &Table, content: DataContentType, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(10)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids((content == DataContentType::EqualityDeletes).then_some(vec![1]))
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

    async fn write_empty_manifest_lists(table: &Table) {
        for snapshot in table.metadata().snapshots() {
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
    }

    #[tokio::test]
    async fn processes_each_snapshot_manifest_list_once() {
        let table = make_v2_table();
        write_empty_manifest_lists(&table).await;
        let mut producer = MergingSnapshotProducer::default();

        let first = producer.process_new_snapshots(&table, None).await.unwrap();
        assert_eq!(first.len(), table.metadata().snapshots().count());
        let loads = producer.manifest_list_loads;
        let retry = producer.process_new_snapshots(&table, None).await.unwrap();
        assert!(retry.is_empty());
        assert_eq!(producer.manifest_list_loads, loads);
    }

    #[tokio::test]
    async fn rejects_non_ancestor_validation_boundary() {
        let table = make_v2_table();
        write_empty_manifest_lists(&table).await;
        let mut producer = MergingSnapshotProducer::default();
        let error = producer
            .process_new_snapshots(&table, Some(i64::MAX))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn writes_data_and_delete_manifests_once_across_attempts() {
        let table = make_v2_table();
        write_empty_current_manifest_list(&table).await;
        let data = file(&table, DataContentType::Data, "s3://bucket/data.parquet");
        let delete = file(
            &table,
            DataContentType::EqualityDeletes,
            "s3://bucket/delete.parquet",
        );
        let mut producer = MergingSnapshotProducer::default();

        for _ in 0..2 {
            producer
                .apply(
                    &table,
                    Operation::Overwrite,
                    HashMap::new(),
                    std::slice::from_ref(&data),
                    std::slice::from_ref(&delete),
                    &[],
                    &[],
                )
                .await
                .unwrap();
        }

        assert_eq!(producer.new_manifest_writes, 2);
        assert_eq!(producer.manifest_list_loads, 1);
        assert_eq!(producer.attempted_manifest_lists.len(), 2);
    }

    #[tokio::test]
    async fn reuses_filtered_manifest_across_attempts() {
        let table = make_v2_table();
        write_empty_current_manifest_list(&table).await;
        let data = file(&table, DataContentType::Data, "s3://bucket/remove.parquet");
        let mut append = MergingSnapshotProducer::default();
        let commit = append
            .apply(
                &table,
                Operation::Append,
                HashMap::new(),
                std::slice::from_ref(&data),
                &[],
                &[],
                &[],
            )
            .await
            .unwrap();
        let table =
            crate::transaction::Transaction::apply(table, commit, &mut Vec::new(), &mut Vec::new())
                .unwrap();
        let mut remove = MergingSnapshotProducer::default();
        let first = remove
            .apply(
                &table,
                Operation::Delete,
                HashMap::new(),
                &[],
                &[],
                std::slice::from_ref(&data),
                &[],
            )
            .await
            .unwrap();
        let second = remove
            .apply(
                &table,
                Operation::Delete,
                HashMap::new(),
                &[],
                &[],
                std::slice::from_ref(&data),
                &[],
            )
            .await
            .unwrap();

        fn manifest_list(mut commit: ActionCommit) -> String {
            commit
                .take_updates()
                .into_iter()
                .find_map(|update| match update {
                    crate::TableUpdate::AddSnapshot { snapshot } => {
                        Some(snapshot.manifest_list().to_string())
                    }
                    _ => None,
                })
                .unwrap()
        }
        let first_snapshot = table.file_io().new_input(manifest_list(first)).unwrap();
        let second_snapshot = table.file_io().new_input(manifest_list(second)).unwrap();
        let first_list = crate::spec::ManifestList::parse_with_version(
            &first_snapshot.read().await.unwrap(),
            FormatVersion::V2,
        )
        .unwrap();
        let second_list = crate::spec::ManifestList::parse_with_version(
            &second_snapshot.read().await.unwrap(),
            FormatVersion::V2,
        )
        .unwrap();
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );
    }

    #[test]
    fn rejects_deletion_vectors() {
        let table = make_v2_table();
        let mut dv = file(
            &table,
            DataContentType::PositionDeletes,
            "s3://bucket/dv.puffin",
        );
        dv.file_format = DataFileFormat::Puffin;
        let producer = MergingSnapshotProducer::default();
        assert_eq!(
            producer
                .validate_files(&table, &[], &[dv])
                .unwrap_err()
                .kind(),
            ErrorKind::FeatureUnsupported
        );
    }

    #[test]
    fn delete_sequence_rules_are_content_specific() {
        let table = make_v2_table();
        let data = file(&table, DataContentType::Data, "data.parquet");
        let equality = file(&table, DataContentType::EqualityDeletes, "eq.parquet");
        let position = file(&table, DataContentType::PositionDeletes, "pos.parquet");

        assert!(!delete_applies(&data, 5, &equality, 5));
        assert!(delete_applies(&data, 5, &equality, 6));
        assert!(delete_applies(&data, 5, &position, 5));
        assert!(!delete_applies(&data, 5, &position, 4));
    }
}
