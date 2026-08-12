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

use crate::expr::Predicate;
use crate::spec::{DataFile, Operation};
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;
use crate::transaction::snapshot::{SnapshotChanges, SnapshotCommitBuilder};
use crate::transaction::validate::{ConflictFilter, SnapshotValidation};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// Adds data files while removing explicit data/delete files or whole files
/// selected by a row predicate.
pub struct OverwriteFilesAction {
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    row_filter: Option<Predicate>,
    validate_added_files: bool,
    validate_conflicting_data: bool,
    validate_conflicting_deletes: bool,
    case_sensitive: bool,
    starting_snapshot_id: Option<i64>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
}

impl OverwriteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            added_data_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            row_filter: None,
            validate_added_files: false,
            validate_conflicting_data: false,
            validate_conflicting_deletes: false,
            case_sensitive: true,
            starting_snapshot_id,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Add replacement data files.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Remove exact data files.
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(files);
        self
    }

    /// Remove exact position or equality delete files.
    pub fn delete_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(files);
        self
    }

    /// Select whole data files to overwrite by row predicate.
    pub fn overwrite_by_row_filter(mut self, predicate: Predicate) -> Self {
        self.row_filter = Some(predicate);
        self
    }

    /// Require every added data file to match the overwrite filter wholly.
    pub fn validate_added_files_match_overwrite_filter(mut self) -> Self {
        self.validate_added_files = true;
        self
    }

    /// Set the snapshot boundary used for conflict validation.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }

    /// Reject concurrently added data files matching the overwrite filter.
    pub fn validate_no_conflicting_data_files(mut self) -> Self {
        self.validate_conflicting_data = true;
        self
    }

    /// Reject concurrently added delete files matching the overwrite filter.
    pub fn validate_no_conflicting_delete_files(mut self) -> Self {
        self.validate_conflicting_deletes = true;
        self
    }

    /// Configure case-sensitive filter binding.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
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

    async fn commit_with_state(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
    ) -> Result<ActionCommit> {
        if self.added_data_files.is_empty()
            && self.removed_data_files.is_empty()
            && self.removed_delete_files.is_empty()
            && self.row_filter.is_none()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Overwrite requires additions or removals",
            ));
        }

        let validation = SnapshotValidation::from_snapshot(self.starting_snapshot_id);
        let filter = self
            .row_filter
            .clone()
            .map(|predicate| ConflictFilter::new(table, predicate, self.case_sensitive))
            .transpose()?;
        let mut removed_data = self.removed_data_files.clone();
        if let Some(filter) = &filter {
            if self.validate_added_files
                && self
                    .added_data_files
                    .iter()
                    .any(|file| !filter.must_match(table, file).unwrap_or(false))
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "An added data file does not match the overwrite filter wholly",
                ));
            }
            removed_data.extend(filter.matching_live_data_files(table, retry).await?);
            if self.validate_conflicting_data || self.validate_conflicting_deletes {
                validation
                    .validate_no_conflicting_files(
                        table,
                        retry,
                        filter,
                        self.validate_conflicting_data,
                        self.validate_conflicting_deletes,
                        true,
                    )
                    .await?;
            }
        }

        let data_paths = removed_data
            .iter()
            .map(|file| file.file_path.clone())
            .collect::<HashSet<_>>();
        let delete_paths = self
            .removed_delete_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect::<HashSet<_>>();
        validation
            .validate_files_exist(table, retry, &data_paths, &delete_paths)
            .await?;
        validation
            .validate_no_rewrites(
                table,
                retry,
                &data_paths.union(&delete_paths).cloned().collect(),
            )
            .await?;

        SnapshotCommitBuilder::new(
            table,
            Operation::Overwrite,
            self.commit_uuid,
            self.snapshot_properties.clone(),
            SnapshotChanges::new(self.added_data_files.clone())
                .with_removed_data_files(&removed_data)
                .with_removed_delete_files(&self.removed_delete_files),
        )
        .commit(retry)
        .await
    }
}

#[async_trait]
impl TransactionAction for OverwriteFilesAction {
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

    fn file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn overwrites_exact_data_files() {
        let base = make_v2_minimal_table();
        let old = file(&base, "old.parquet");
        let append = Arc::new(
            Transaction::new(&base)
                .fast_append()
                .add_data_files([old.clone()]),
        )
        .commit(&mut SnapshotRetryState::default(), &base)
        .await
        .unwrap();
        let table = Transaction::apply(base, append, &mut Vec::new(), &mut Vec::new()).unwrap();
        let mut commit = Arc::new(
            Transaction::new(&table)
                .overwrite_files()
                .delete_data_files([old])
                .add_data_files([file(&table, "new.parquet")]),
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
    }
}
