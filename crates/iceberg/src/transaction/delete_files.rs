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
use crate::{Error, Result};

/// Removes whole data files using exact files, paths, or a row predicate.
pub struct DeleteFilesAction {
    data_files: Vec<DataFile>,
    paths: HashSet<String>,
    row_filter: Option<Predicate>,
    case_sensitive: bool,
    fail_missing_files: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    starting_snapshot_id: Option<i64>,
}

impl DeleteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            data_files: Vec::new(),
            paths: HashSet::new(),
            row_filter: None,
            case_sensitive: true,
            fail_missing_files: true,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            starting_snapshot_id,
        }
    }

    /// Delete exact data files.
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.data_files.extend(files);
        self
    }

    /// Delete data files by their fully qualified paths.
    pub fn delete_file_paths(mut self, paths: impl IntoIterator<Item = String>) -> Self {
        self.paths.extend(paths);
        self
    }

    /// Delete files whose rows are proven to match this predicate wholly.
    pub fn delete_from_row_filter(mut self, predicate: Predicate) -> Self {
        self.row_filter = Some(predicate);
        self
    }

    /// Configure case-sensitive predicate binding.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Configure whether requested exact files must still be live.
    pub fn fail_missing_files(mut self, fail: bool) -> Self {
        self.fail_missing_files = fail;
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
        let validation = SnapshotValidation::from_snapshot(self.starting_snapshot_id);
        let mut data_files = self.data_files.clone();
        if let Some(predicate) = self.row_filter.clone() {
            let filter = ConflictFilter::new(table, predicate, self.case_sensitive)?;
            validation
                .validate_no_conflicting_files(table, retry, &filter, true, true, true)
                .await?;
            data_files.extend(filter.matching_live_data_files(table, retry).await?);
        }

        let data_paths = data_files
            .iter()
            .map(|file| file.file_path.clone())
            .chain(self.paths.iter().cloned())
            .collect::<HashSet<_>>();
        if self.fail_missing_files {
            validation
                .validate_files_exist(table, retry, &data_paths, &HashSet::new())
                .await?;
        }
        validation
            .validate_no_rewrites(table, retry, &data_paths)
            .await?;

        let changes = SnapshotChanges::new(Vec::new())
            .with_removed_data_files(&data_files)
            .with_removed_data_paths(self.paths.iter().cloned())
            .with_fail_missing_files(self.fail_missing_files);
        SnapshotCommitBuilder::new(
            table,
            Operation::Delete,
            self.commit_uuid,
            self.snapshot_properties.clone(),
            changes,
        )
        .with_check_duplicate(false)
        .commit(retry)
        .await
    }
}

#[async_trait]
impl TransactionAction for DeleteFilesAction {
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
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestStatus, SnapshotRef,
        Struct,
    };
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_minimal_table;

    fn data_file(table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/delete-me.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn deletes_an_exact_live_file() {
        let base = make_v2_minimal_table();
        let file = data_file(&base);
        let append = Arc::new(
            Transaction::new(&base)
                .fast_append()
                .add_data_files([file.clone()]),
        )
        .commit(&mut SnapshotRetryState::default(), &base)
        .await
        .unwrap();
        let table = Transaction::apply(base, append, &mut Vec::new(), &mut Vec::new()).unwrap();

        let mut commit = Arc::new(
            Transaction::new(&table)
                .delete_files()
                .delete_data_files([file]),
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
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let manifest = table
            .manifest_reader()
            .read(&list.entries()[0])
            .await
            .unwrap();
        assert_eq!(manifest.entries()[0].status(), ManifestStatus::Deleted);
    }

    #[tokio::test]
    async fn required_missing_path_fails() {
        let table = make_v2_minimal_table();
        let result = Arc::new(
            Transaction::new(&table)
                .delete_files()
                .delete_file_paths(["test/missing.parquet".to_string()]),
        )
        .commit(&mut SnapshotRetryState::default(), &table)
        .await;
        assert!(result.is_err());
    }
}
