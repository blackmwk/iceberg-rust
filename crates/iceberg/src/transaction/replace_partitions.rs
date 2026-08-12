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

use async_trait::async_trait;
use uuid::Uuid;

use crate::expr::{Predicate, Reference};
use crate::spec::{DataFile, Datum, ManifestContentType, Operation, Struct};
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;
use crate::transaction::snapshot::{SnapshotChanges, SnapshotCommitBuilder};
use crate::transaction::validate::{ConflictFilter, SnapshotValidation};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// Replaces the live contents of partitions derived from added data files.
pub struct ReplacePartitionsAction {
    added_data_files: Vec<DataFile>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    pub(crate) starting_snapshot_id: Option<i64>,
    append_only: bool,
    validate_conflicts: bool,
    case_sensitive: bool,
}

impl ReplacePartitionsAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            added_data_files: Vec::new(),
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            starting_snapshot_id,
            append_only: false,
            validate_conflicts: true,
            case_sensitive: true,
        }
    }

    /// Add replacement data files; their partitions define the targets.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Set the commit UUID used for generated metadata files.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Attach custom snapshot summary properties.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    /// Fail if any target partition already contains live data.
    pub fn append_only(mut self) -> Self {
        self.append_only = true;
        self
    }

    /// Set the snapshot boundary used for conflict validation.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }

    /// Configure case-sensitive conflict-filter binding.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Disable concurrent data/delete conflict validation.
    pub fn skip_conflict_validation(mut self) -> Self {
        self.validate_conflicts = false;
        self
    }

    fn target_filter(&self, table: &Table, targets: &HashSet<(i32, Struct)>) -> Result<Predicate> {
        let spec = table.metadata().default_partition_spec();
        let mut filter = Predicate::AlwaysFalse;
        for (_, partition) in targets {
            let mut target = Predicate::AlwaysTrue;
            for (field, value) in spec.fields().iter().zip(partition.fields()) {
                let Some(value) = value else {
                    target = target.and(Reference::new(field.name.clone()).is_null());
                    continue;
                };
                let primitive_type = value.as_primitive_literal().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Replacement partition values must be primitive",
                    )
                })?;
                let partition_type = table
                    .metadata()
                    .default_partition_type()
                    .fields()
                    .iter()
                    .find(|partition_field| partition_field.name == field.name)
                    .and_then(|partition_field| {
                        partition_field.field_type.as_primitive_type().cloned()
                    })
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Invalid replacement partition field",
                        )
                    })?;
                let datum = Datum::new(partition_type, primitive_type);
                target = target.and(Reference::new(field.name.clone()).equal_to(datum));
            }
            filter = filter.or(target);
        }
        Ok(filter)
    }

    fn targets(&self, table: &Table) -> Result<HashSet<(i32, Struct)>> {
        let default_spec = table.metadata().default_partition_spec_id();
        let mut targets = HashSet::new();
        for file in &self.added_data_files {
            if file.partition_spec_id != default_spec {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Replacement files must use the current default partition spec",
                ));
            }
            if !targets.insert((file.partition_spec_id, file.partition().clone())) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Multiple replacement files target the same partition",
                ));
            }
        }
        Ok(targets)
    }

    async fn commit_with_state(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
    ) -> Result<ActionCommit> {
        if self.added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Replace partitions requires replacement data files",
            ));
        }
        let targets = self.targets(table)?;
        let manifests = retry
            .process_current_snapshot(table)
            .await?
            .map(|snapshot| {
                snapshot
                    .data_manifests()
                    .iter()
                    .chain(snapshot.delete_manifests())
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut removed_data = Vec::new();
        let mut removed_deletes = Vec::new();
        for manifest_file in manifests {
            let destination = if manifest_file.content == ManifestContentType::Data {
                &mut removed_data
            } else {
                &mut removed_deletes
            };
            for entry in retry
                .load_manifest(table, &manifest_file)
                .await?
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
            {
                let file = entry.data_file();
                if targets.contains(&(file.partition_spec_id, file.partition().clone())) {
                    destination.push(file.clone());
                }
            }
        }

        if self.append_only && !removed_data.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Append-only replace partitions cannot overwrite an existing partition",
            ));
        }
        if self.validate_conflicts {
            let filter = ConflictFilter::new(
                table,
                self.target_filter(table, &targets)?,
                self.case_sensitive,
            )?;
            SnapshotValidation::from_snapshot(self.starting_snapshot_id)
                .validate_no_conflicting_files(table, retry, &filter, true, true, true)
                .await?;
        }

        SnapshotCommitBuilder::new(
            table,
            Operation::Overwrite,
            self.commit_uuid,
            self.snapshot_properties.clone(),
            SnapshotChanges::new(self.added_data_files.clone())
                .with_removed_data_files(&removed_data)
                .with_removed_delete_files(&removed_deletes),
        )
        .commit(retry)
        .await
    }
}

#[async_trait]
impl TransactionAction for ReplacePartitionsAction {
    type State = SnapshotRetryState;

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        self.commit_with_state(table, state).await
    }

    async fn finish_commit(
        &self,
        state: &mut Self::State,
        table: &Table,
        commit_error: Option<&Error>,
    ) {
        state.cleanup(table, commit_error).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::TableUpdate;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, SnapshotRef, Struct,
    };
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_minimal_table;

    fn file(table: &Table, path: &str, partition: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(partition))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn replaces_only_target_partitions() {
        let base = make_v2_minimal_table();
        let old_target = file(&base, "old-target.parquet", 300);
        let old_other = file(&base, "old-other.parquet", 400);
        let append = Arc::new(
            Transaction::new(&base)
                .fast_append()
                .add_data_files([old_target, old_other]),
        )
        .commit(&mut SnapshotRetryState::default(), &base)
        .await
        .unwrap();
        let table = Transaction::apply(base, append, &mut Vec::new(), &mut Vec::new()).unwrap();
        let mut commit = Arc::new(
            Transaction::new(&table)
                .replace_partitions()
                .add_data_files([file(&table, "new-target.parquet", 300)]),
        )
        .commit(&mut SnapshotRetryState::default(), &table)
        .await
        .unwrap();
        let snapshot = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(SnapshotRef::new(snapshot)),
                _ => None,
            })
            .unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let paths = futures::future::try_join_all(
            list.entries()
                .iter()
                .map(|manifest| table.manifest_reader().read(manifest)),
        )
        .await
        .unwrap()
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .map(|entry| entry.file_path().to_string())
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>();
        assert!(paths.contains("new-target.parquet"));
        assert!(paths.contains("old-other.parquet"));
        assert!(!paths.contains("old-target.parquet"));
    }

    #[tokio::test]
    async fn append_only_rejects_existing_target_partition() {
        let base = make_v2_minimal_table();
        let append = Arc::new(Transaction::new(&base).fast_append().add_data_files([file(
            &base,
            "old.parquet",
            300,
        )]))
        .commit(&mut SnapshotRetryState::default(), &base)
        .await
        .unwrap();
        let table = Transaction::apply(base, append, &mut Vec::new(), &mut Vec::new()).unwrap();
        let result = Arc::new(
            Transaction::new(&table)
                .replace_partitions()
                .append_only()
                .add_data_files([file(&table, "new.parquet", 300)]),
        )
        .commit(&mut SnapshotRetryState::default(), &table)
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rejects_non_ancestor_validation_start() {
        let table = make_v2_minimal_table();
        let result = Arc::new(
            Transaction::new(&table)
                .replace_partitions()
                .validate_from_snapshot(i64::MAX)
                .add_data_files([file(&table, "new.parquet", 300)]),
        )
        .commit(&mut SnapshotRetryState::default(), &table)
        .await;
        assert!(result.is_err());
    }
}
