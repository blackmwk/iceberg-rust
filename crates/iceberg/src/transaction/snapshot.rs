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
use std::ops::RangeFrom;

use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct, StructType, Summary,
    TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::retry::{FilteredManifest, SnapshotRetryState};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// Immutable files to apply in a snapshot-producing action.
#[derive(Default)]
pub(crate) struct SnapshotChanges {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_paths: HashSet<String>,
    removed_delete_paths: HashSet<String>,
    fail_missing_files: bool,
    data_sequence_number: Option<i64>,
}

impl SnapshotChanges {
    pub(crate) fn new(added_data_files: Vec<DataFile>) -> Self {
        Self {
            added_data_files,
            added_delete_files: Vec::new(),
            removed_data_paths: HashSet::new(),
            removed_delete_paths: HashSet::new(),
            fail_missing_files: true,
            data_sequence_number: None,
        }
    }

    #[allow(dead_code)] // Used by row-delta in a later stack layer.
    pub(crate) fn with_added_delete_files(mut self, files: Vec<DataFile>) -> Self {
        self.added_delete_files = files;
        self
    }

    #[allow(dead_code)] // Used by delete/rewrite actions in later stack layers.
    pub(crate) fn with_removed_data_files(mut self, files: &[DataFile]) -> Self {
        self.removed_data_paths
            .extend(files.iter().map(|file| file.file_path.clone()));
        self
    }

    #[allow(dead_code)] // Used by delete/overwrite actions in later stack layers.
    pub(crate) fn with_removed_data_paths(
        mut self,
        paths: impl IntoIterator<Item = String>,
    ) -> Self {
        self.removed_data_paths.extend(paths);
        self
    }

    #[allow(dead_code)] // Used by rewrite/overwrite actions in later stack layers.
    pub(crate) fn with_removed_delete_files(mut self, files: &[DataFile]) -> Self {
        self.removed_delete_paths
            .extend(files.iter().map(|file| file.file_path.clone()));
        self
    }

    #[allow(dead_code)] // Used by path-based delete actions in later stack layers.
    pub(crate) fn with_fail_missing_files(mut self, fail: bool) -> Self {
        self.fail_missing_files = fail;
        self
    }

    #[allow(dead_code)] // Used by rewrite actions in a later stack layer.
    pub(crate) fn with_data_sequence_number(mut self, sequence_number: Option<i64>) -> Self {
        self.data_sequence_number = sequence_number;
        self
    }
}

/// Retry-aware entry point for producing a snapshot from an explicit change set.
pub(crate) struct SnapshotCommitBuilder<'a> {
    table: &'a Table,
    operation: Operation,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    changes: SnapshotChanges,
    check_duplicate: bool,
}

impl<'a> SnapshotCommitBuilder<'a> {
    pub(crate) fn new(
        table: &'a Table,
        operation: Operation,
        commit_uuid: Option<Uuid>,
        snapshot_properties: HashMap<String, String>,
        changes: SnapshotChanges,
    ) -> Self {
        Self {
            table,
            operation,
            commit_uuid,
            snapshot_properties,
            changes,
            check_duplicate: true,
        }
    }

    pub(crate) fn with_check_duplicate(mut self, check_duplicate: bool) -> Self {
        self.check_duplicate = check_duplicate;
        self
    }

    pub(crate) async fn commit(self, retry: &mut SnapshotRetryState) -> Result<ActionCommit> {
        let existing_manifests = retry
            .process_current_snapshot(self.table)
            .await?
            .map(|snapshot| {
                snapshot
                    .data_manifests()
                    .iter()
                    .chain(snapshot.delete_manifests())
                    .filter(|manifest| {
                        manifest.has_added_files()
                            || manifest.has_existing_files()
                            || manifest.has_deleted_files()
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        let commit_uuid = retry.commit_uuid(self.commit_uuid)?;
        let snapshot_id = retry.snapshot_id(self.table);
        let attempt = retry.next_attempt();
        let mut writer = SnapshotWriter::new(
            self.table,
            snapshot_id,
            commit_uuid,
            self.snapshot_properties,
            self.changes,
        );
        writer.validate_added_data_files()?;
        writer.validate_added_delete_files()?;
        if self.check_duplicate {
            writer.validate_duplicate_files().await?;
        }
        let existing_manifests = writer.filter_manifests(existing_manifests, retry).await?;
        let result = writer
            .commit(
                self.operation,
                existing_manifests,
                attempt,
                retry.added_data_manifest(),
                retry.added_delete_manifest(),
            )
            .await?;
        if let Some(manifest) = result.added_data_manifest {
            retry.cache_added_data_manifest(manifest);
        }
        if let Some(manifest) = result.added_delete_manifest {
            retry.cache_added_delete_manifest(manifest);
        }
        retry.track_manifest_list(result.manifest_list_path);
        Ok(result.action_commit)
    }
}

#[cfg(test)]
mod change_set_tests {
    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, SnapshotRef, Struct,
    };
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_minimal_table;

    fn take_snapshot(mut commit: ActionCommit) -> SnapshotRef {
        commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(SnapshotRef::new(snapshot)),
                _ => None,
            })
            .unwrap()
    }

    #[tokio::test]
    async fn commits_added_data_files_from_change_set() {
        let table = make_v2_minimal_table();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/added.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let mut retry = SnapshotRetryState::default();

        let mut commit = SnapshotCommitBuilder::new(
            &table,
            Operation::Append,
            Some(Uuid::now_v7()),
            HashMap::new(),
            SnapshotChanges::new(vec![file]),
        )
        .commit(&mut retry)
        .await
        .unwrap();

        let snapshot = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Append);
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("added-data-files"),
            Some(&"1".to_string())
        );
    }

    #[tokio::test]
    async fn retries_reuse_manifest_and_cleanup_attempt_lists() {
        let table = make_v2_minimal_table();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/retry.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let mut retry = SnapshotRetryState::default();

        let first = take_snapshot(
            SnapshotCommitBuilder::new(
                &table,
                Operation::Append,
                None,
                HashMap::new(),
                SnapshotChanges::new(vec![file.clone()]),
            )
            .commit(&mut retry)
            .await
            .unwrap(),
        );
        let second = take_snapshot(
            SnapshotCommitBuilder::new(
                &table,
                Operation::Append,
                None,
                HashMap::new(),
                SnapshotChanges::new(vec![file.clone()]),
            )
            .commit(&mut retry)
            .await
            .unwrap(),
        );

        assert_eq!(first.snapshot_id(), second.snapshot_id());
        assert_ne!(first.manifest_list(), second.manifest_list());
        let first_list = table.manifest_list_reader(&first).load().await.unwrap();
        let second_list = table.manifest_list_reader(&second).load().await.unwrap();
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );

        let paths = [
            first.manifest_list(),
            second.manifest_list(),
            &first_list.entries()[0].manifest_path,
        ];
        let conflict = Error::new(ErrorKind::CatalogCommitConflicts, "commit conflict");
        retry.cleanup(&table, Some(&conflict)).await;
        for path in paths {
            assert!(!table.file_io().exists(path).await.unwrap());
        }

        let mut unknown = SnapshotRetryState::default();
        let snapshot = take_snapshot(
            SnapshotCommitBuilder::new(
                &table,
                Operation::Append,
                None,
                HashMap::new(),
                SnapshotChanges::new(vec![file]),
            )
            .commit(&mut unknown)
            .await
            .unwrap(),
        );
        let unknown_error = Error::new(ErrorKind::Unexpected, "commit state unknown");
        unknown.cleanup(&table, Some(&unknown_error)).await;
        assert!(
            table
                .file_io()
                .exists(snapshot.manifest_list())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn writes_and_reuses_delete_manifests() {
        let table = make_v2_minimal_table();
        let delete_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("test/delete.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .equality_ids(Some(vec![1]))
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let mut retry = SnapshotRetryState::default();

        let first = take_snapshot(
            SnapshotCommitBuilder::new(
                &table,
                Operation::Delete,
                None,
                HashMap::new(),
                SnapshotChanges::new(Vec::new()).with_added_delete_files(vec![delete_file.clone()]),
            )
            .commit(&mut retry)
            .await
            .unwrap(),
        );
        let second = take_snapshot(
            SnapshotCommitBuilder::new(
                &table,
                Operation::Delete,
                None,
                HashMap::new(),
                SnapshotChanges::new(Vec::new()).with_added_delete_files(vec![delete_file.clone()]),
            )
            .commit(&mut retry)
            .await
            .unwrap(),
        );
        let first_list = table.manifest_list_reader(&first).load().await.unwrap();
        let second_list = table.manifest_list_reader(&second).load().await.unwrap();

        assert_eq!(
            first_list.entries()[0].content,
            ManifestContentType::Deletes
        );
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );
        assert_eq!(
            first
                .summary()
                .additional_properties
                .get("added-delete-files")
                .map(String::as_str),
            Some("1")
        );

        let v1 = crate::transaction::tests::make_v1_table();
        let result = SnapshotCommitBuilder::new(
            &v1,
            Operation::Delete,
            None,
            HashMap::new(),
            SnapshotChanges::new(Vec::new()).with_added_delete_files(vec![delete_file]),
        )
        .commit(&mut SnapshotRetryState::default())
        .await;
        let Err(err) = result else {
            panic!("format v1 must reject delete files")
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn filters_removed_files_once_across_retries() {
        let table = make_v2_minimal_table();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/remove.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let append = SnapshotCommitBuilder::new(
            &table,
            Operation::Append,
            None,
            HashMap::new(),
            SnapshotChanges::new(vec![file.clone()]),
        )
        .commit(&mut SnapshotRetryState::default())
        .await
        .unwrap();
        let table = Transaction::apply(table, append, &mut Vec::new(), &mut Vec::new()).unwrap();
        let mut retry = SnapshotRetryState::default();

        let remove = || SnapshotChanges::new(Vec::new()).with_removed_data_files(&[file.clone()]);
        let first = take_snapshot(
            SnapshotCommitBuilder::new(&table, Operation::Delete, None, HashMap::new(), remove())
                .commit(&mut retry)
                .await
                .unwrap(),
        );
        let second = take_snapshot(
            SnapshotCommitBuilder::new(&table, Operation::Delete, None, HashMap::new(), remove())
                .commit(&mut retry)
                .await
                .unwrap(),
        );
        let first_list = table.manifest_list_reader(&first).load().await.unwrap();
        let second_list = table.manifest_list_reader(&second).load().await.unwrap();
        assert_eq!(
            first_list.entries()[0].manifest_path,
            second_list.entries()[0].manifest_path
        );
        let rewritten = table
            .manifest_reader()
            .read(&first_list.entries()[0])
            .await
            .unwrap();
        assert_eq!(
            rewritten.entries()[0].status(),
            crate::spec::ManifestStatus::Deleted
        );
        assert_eq!(
            first
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .map(String::as_str),
            Some("1")
        );

        let missing = SnapshotCommitBuilder::new(
            &table,
            Operation::Delete,
            None,
            HashMap::new(),
            SnapshotChanges::new(Vec::new())
                .with_removed_data_paths(["test/missing.parquet".to_string()]),
        )
        .commit(&mut SnapshotRetryState::default())
        .await;
        assert!(missing.is_err());
    }
}

struct SnapshotWriter<'a> {
    table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_paths: HashSet<String>,
    removed_delete_paths: HashSet<String>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    fail_missing_files: bool,
    data_sequence_number: Option<i64>,
    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // Note: This counter is limited to the range of (0..u64::MAX).
    manifest_counter: RangeFrom<u64>,
}

struct SnapshotWriteResult {
    action_commit: ActionCommit,
    manifest_list_path: String,
    added_data_manifest: Option<ManifestFile>,
    added_delete_manifest: Option<ManifestFile>,
}

impl<'a> SnapshotWriter<'a> {
    fn new(
        table: &'a Table,
        snapshot_id: i64,
        commit_uuid: Uuid,
        snapshot_properties: HashMap<String, String>,
        changes: SnapshotChanges,
    ) -> Self {
        Self {
            table,
            snapshot_id,
            commit_uuid,
            snapshot_properties,
            added_data_files: changes.added_data_files,
            added_delete_files: changes.added_delete_files,
            removed_data_paths: changes.removed_data_paths,
            removed_delete_paths: changes.removed_delete_paths,
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            fail_missing_files: changes.fail_missing_files,
            data_sequence_number: changes.data_sequence_number,
            manifest_counter: (0..),
        }
    }

    pub(crate) fn validate_added_data_files(&self) -> Result<()> {
        for data_file in &self.added_data_files {
            if data_file.content_type() != crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only data content type is allowed for fast append",
                ));
            }
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
        if !self.added_delete_files.is_empty()
            && self.table.metadata().format_version() == FormatVersion::V1
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Delete files require table format version 2 or newer",
            ));
        }

        let mut paths = self
            .added_data_files
            .iter()
            .map(|file| file.file_path.as_str())
            .collect::<HashSet<_>>();
        for delete_file in &self.added_delete_files {
            if !paths.insert(delete_file.file_path.as_str()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot add duplicate file path {}", delete_file.file_path),
                ));
            }
            if delete_file.content_type() == crate::spec::DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Only position or equality delete files can be added as delete files",
                ));
            }
            if self.table.metadata().default_partition_spec_id() != delete_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Delete file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                delete_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
            if delete_file.content_type() == crate::spec::DataContentType::EqualityDeletes {
                let ids = delete_file.equality_ids().unwrap_or_default();
                if ids.is_empty()
                    || ids.iter().any(|id| {
                        self.table
                            .metadata()
                            .current_schema()
                            .field_by_id(*id)
                            .is_none()
                    })
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Equality delete file must reference known equality field ids",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(());
        };

        let new_files: HashSet<&str> = self
            .added_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let runtime = self.table.runtime();
        let manifest_list = self
            .table
            .manifest_list_reader(current_snapshot)
            .load()
            .await?;

        let new_files_ref = &new_files;
        let referenced_files: Vec<String> = manifest_list
            .consume_entries()
            .into_iter()
            .map(|entry| {
                let reader = self.table.manifest_reader();
                runtime.io().spawn(async move { reader.read(&entry).await })
            })
            .collect::<FuturesUnordered<_>>()
            .try_fold(Vec::new(), |mut acc, manifest| async move {
                acc.extend(
                    manifest?
                        .entries()
                        .iter()
                        .filter(|e| new_files_ref.contains(e.file_path()) && e.is_alive())
                        .map(|e| e.file_path().to_string()),
                );
                Ok(acc)
            })
            .await?;

        if !referenced_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    referenced_files.join(", ")
                ),
            ));
        }

        Ok(())
    }

    async fn filter_manifests(
        &mut self,
        manifests: Vec<ManifestFile>,
        retry: &mut SnapshotRetryState,
    ) -> Result<Vec<ManifestFile>> {
        let added_paths = self
            .added_data_files
            .iter()
            .chain(&self.added_delete_files)
            .map(|file| file.file_path.as_str())
            .collect::<HashSet<_>>();
        let removed_paths = self
            .removed_data_paths
            .iter()
            .chain(&self.removed_delete_paths)
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let overlap = added_paths
            .intersection(&removed_paths)
            .copied()
            .collect::<Vec<_>>();
        if !overlap.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add and remove the same files: {}",
                    overlap.join(", ")
                ),
            ));
        }
        if removed_paths.is_empty() {
            return Ok(manifests);
        }

        let mut outputs = Vec::with_capacity(manifests.len());
        let mut found_data = HashSet::new();
        let mut found_deletes = HashSet::new();
        for source in manifests {
            let cached = retry.filtered_manifest(&source.manifest_path);
            let result = if let Some(cached) = cached {
                cached
            } else {
                let manifest = retry.load_manifest(self.table, &source).await?;
                let requested = if source.content == ManifestContentType::Data {
                    &self.removed_data_paths
                } else {
                    &self.removed_delete_paths
                };
                let removed_files = manifest
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive() && requested.contains(entry.file_path()))
                    .map(|entry| entry.data_file().clone())
                    .collect::<Vec<_>>();

                let output = if removed_files.is_empty() {
                    source.clone()
                } else {
                    let path = format!(
                        "{}/{}-r{}.{}",
                        self.table.metadata().metadata_location()?,
                        self.commit_uuid,
                        retry.next_rewrite_id(),
                        DataFileFormat::Avro
                    );
                    let mut writer = self.new_manifest_writer_for_spec(
                        source.content,
                        source.partition_spec_id,
                        path,
                    )?;
                    for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                        if requested.contains(entry.file_path()) {
                            writer.add_delete_entry((**entry).clone())?;
                        } else {
                            writer.add_existing_entry((**entry).clone())?;
                        }
                    }
                    writer.write_manifest_file().await?
                };
                let result = FilteredManifest {
                    output,
                    removed_files,
                };
                retry.cache_filtered_manifest(source.manifest_path.clone(), result.clone());
                result
            };

            for file in &result.removed_files {
                if source.content == ManifestContentType::Data {
                    found_data.insert(file.file_path.clone());
                    self.removed_data_files.push(file.clone());
                } else {
                    found_deletes.insert(file.file_path.clone());
                    self.removed_delete_files.push(file.clone());
                }
            }
            outputs.push(result.output);
        }

        if self.fail_missing_files {
            let missing = self
                .removed_data_paths
                .difference(&found_data)
                .chain(self.removed_delete_paths.difference(&found_deletes))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot remove files that are not live: {}",
                        missing.join(", ")
                    ),
                ));
            }
        }
        Ok(outputs)
    }

    fn new_manifest_writer(&mut self, content: ManifestContentType) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}-m{}.{}",
            self.table.metadata().metadata_location()?,
            self.commit_uuid,
            self.manifest_counter.next().unwrap(),
            DataFileFormat::Avro
        );
        self.new_manifest_writer_for_spec(
            content,
            self.table.metadata().default_partition_spec_id(),
            new_manifest_path,
        )
    }

    fn new_manifest_writer_for_spec(
        &self,
        content: ManifestContentType,
        spec_id: i32,
        path: String,
    ) -> Result<ManifestWriter> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find partition spec {spec_id} for manifest rewrite"),
                )
            })?
            .as_ref()
            .clone();
        let output_file = self.table.file_io().new_output(path)?;
        let schema = self.table.metadata().current_schema().clone();

        let builder = if let Some(em) = self.table.encryption_manager() {
            ManifestWriterBuilder::new_from_encrypted(
                em.encrypt(output_file),
                Some(self.snapshot_id),
                schema,
                partition_spec,
            )?
        } else {
            ManifestWriterBuilder::new(output_file, Some(self.snapshot_id), schema, partition_spec)
        };

        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Partition value is not compatible with partition type",
            ));
        }

        for (value, field) in partition_value.fields().iter().zip(partition_type.fields()) {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value
                && !field.compatible(&value.as_primitive_literal().unwrap())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Partition value is not compatible partition type",
                ));
            }
        }
        Ok(())
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(&mut self) -> Result<ManifestFile> {
        let added_data_files = std::mem::take(&mut self.added_data_files);
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let data_sequence_number = self.data_sequence_number;
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file)
                .sequence_number_opt(data_sequence_number);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_added_delete_manifest(&mut self) -> Result<ManifestFile> {
        let files = std::mem::take(&mut self.added_delete_files);
        let entries = files.into_iter().map(|file| {
            ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(file)
                .build()
        });
        let mut writer = self.new_manifest_writer(ManifestContentType::Deletes)?;
        for entry in entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    /// Creates new manifests for data files added or removed,
    /// and collects all of the manifests to be included in the new snapshot as [ManifestFile] entries.
    async fn produce_manifests(
        &mut self,
        mut existing_manifests: Vec<ManifestFile>,
        cached_added_manifest: Option<ManifestFile>,
        cached_delete_manifest: Option<ManifestFile>,
    ) -> Result<(
        Vec<ManifestFile>,
        Option<ManifestFile>,
        Option<ManifestFile>,
    )> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_files.is_empty()
            && self.removed_delete_files.is_empty()
            && self.snapshot_properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or added snapshot properties found when write a manifest file",
            ));
        }

        // Process added entries.
        let added_manifest = if self.added_data_files.is_empty() {
            None
        } else if let Some(manifest) = cached_added_manifest {
            Some(manifest)
        } else {
            Some(self.write_added_manifest().await?)
        };
        existing_manifests.extend(added_manifest.iter().cloned());
        let delete_manifest = if self.added_delete_files.is_empty() {
            None
        } else if let Some(manifest) = cached_delete_manifest {
            Some(manifest)
        } else {
            Some(self.write_added_delete_manifest().await?)
        };
        existing_manifests.extend(delete_manifest.iter().cloned());
        Ok((existing_manifests, added_manifest, delete_manifest))
    }

    // Returns a `Summary` of the current snapshot
    fn summary(&self, operation: &Operation) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }
        for delete_file in &self.added_delete_files {
            summary_collector.add_file(
                delete_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }
        for data_file in self
            .removed_data_files
            .iter()
            .chain(&self.removed_delete_files)
        {
            let partition_spec = table_metadata
                .partition_spec_by_id(data_file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot find partition spec {} for removed file",
                            data_file.partition_spec_id
                        ),
                    )
                })?
                .clone();
            summary_collector.remove_file(
                data_file,
                table_metadata.current_schema().clone(),
                partition_spec,
            );
        }

        let previous_snapshot = table_metadata.current_snapshot();

        // User-supplied snapshot properties are applied first, then the computed
        // metrics overwrite any colliding keys. This matches iceberg-java
        // (the Java `SnapshotProducer.summary`), where computed `added-*`/`total-*` values
        // are written after user properties so a user cannot shadow them with a
        // bad (or merely wrong) value that would corrupt the snapshot summary.
        let mut additional_properties = self.snapshot_properties.clone();
        additional_properties.extend(summary_collector.build());

        let summary = Summary {
            operation: operation.clone(),
            additional_properties,
        };

        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> Result<String> {
        Ok(format!(
            "{}/snap-{}-{}-{}.{}",
            self.table.metadata().metadata_location()?,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        ))
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    async fn commit(
        mut self,
        operation: Operation,
        existing_manifests: Vec<ManifestFile>,
        attempt: u64,
        cached_added_manifest: Option<ManifestFile>,
        cached_delete_manifest: Option<ManifestFile>,
    ) -> Result<SnapshotWriteResult> {
        let manifest_list_path = self.generate_manifest_list_file_path(attempt as i64)?;
        let next_seq_num = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();

        let raw_output = self
            .table
            .file_io()
            .new_output(manifest_list_path.clone())?;

        let (writer, encryption_key_id) = match self.table.encryption_manager() {
            Some(em) => {
                let encrypted_output = em.encrypt(raw_output);
                let key_id = em
                    .encrypt_manifest_list_key_metadata(encrypted_output.key_metadata())
                    .await?;
                (encrypted_output.writer().await?, Some(key_id))
            }
            None => (raw_output.writer().await?, None),
        };

        let parent_snapshot_id = self.table.metadata().current_snapshot_id();
        let mut manifest_list_writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => {
                ManifestListWriter::v1(writer, self.snapshot_id, parent_snapshot_id)
            }
            FormatVersion::V2 => {
                ManifestListWriter::v2(writer, self.snapshot_id, parent_snapshot_id, next_seq_num)
            }
            FormatVersion::V3 => ManifestListWriter::v3(
                writer,
                self.snapshot_id,
                parent_snapshot_id,
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Calling self.summary() before self.produce_manifests() is important because self.added_data_files
        // will be set to an empty vec after self.produce_manifests() returns, resulting in an empty summary
        // being generated.
        let summary = self.summary(&operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let (new_manifests, added_data_manifest, added_delete_manifest) = self
            .produce_manifests(
                existing_manifests,
                cached_added_manifest,
                cached_delete_manifest,
            )
            .await?;

        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path.clone())
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.table.metadata().current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_encryption_key_id(encryption_key_id)
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let encryption_key_updates: Vec<TableUpdate> = self
            .table
            .encryption_manager()
            .map(|em| {
                em.with_encryption_keys(|keys| {
                    keys.values()
                        .filter(|k| self.table.metadata().encryption_key(k.key_id()).is_none())
                        .map(|k| TableUpdate::AddEncryptionKey {
                            encryption_key: k.clone(),
                        })
                        .collect()
                })
            })
            .unwrap_or_default();

        let updates = [encryption_key_updates, vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ]]
        .concat();

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: self.table.metadata().current_snapshot_id(),
            },
        ];

        Ok(SnapshotWriteResult {
            action_commit: ActionCommit::new(updates, requirements),
            manifest_list_path,
            added_data_manifest,
            added_delete_manifest,
        })
    }
}
