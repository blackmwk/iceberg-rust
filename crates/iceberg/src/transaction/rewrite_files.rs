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

use crate::spec::{DataContentType, DataFile, Operation};
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;
use crate::transaction::snapshot::{SnapshotChanges, SnapshotCommitBuilder};
use crate::transaction::validate::SnapshotValidation;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// Replaces physical files while preserving the table's logical rows.
pub struct RewriteFilesAction {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    data_sequence_number: Option<i64>,
    starting_snapshot_id: Option<i64>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
}

impl RewriteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            data_sequence_number: None,
            starting_snapshot_id,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Add replacement data or delete files, classified by content type.
    pub fn add_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }
        self
    }

    /// Remove rewritten data or delete files, classified by content type.
    pub fn delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }
        self
    }

    /// Preserve this data sequence number for replacement data files.
    pub fn set_data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.data_sequence_number = Some(sequence_number);
        self
    }

    /// Set the snapshot boundary used for conflict validation.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
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
        if self.removed_data_files.is_empty() && self.removed_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite files requires at least one input file",
            ));
        }
        if (self.removed_data_files.is_empty() != self.added_data_files.is_empty())
            || (self.removed_delete_files.is_empty() != self.added_delete_files.is_empty())
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Each rewritten content type must have input and replacement files",
            ));
        }
        let added = self
            .added_data_files
            .iter()
            .chain(&self.added_delete_files)
            .map(|file| file.file_path.as_str())
            .collect::<HashSet<_>>();
        let removed = self
            .removed_data_files
            .iter()
            .chain(&self.removed_delete_files)
            .map(|file| file.file_path.as_str())
            .collect::<HashSet<_>>();
        if added.intersection(&removed).next().is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Rewrite input and replacement file sets must be disjoint",
            ));
        }

        let validation = SnapshotValidation::from_snapshot(self.starting_snapshot_id);
        let data_paths = self
            .removed_data_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect();
        let delete_paths = self
            .removed_delete_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect();
        validation
            .validate_files_exist(table, retry, &data_paths, &delete_paths)
            .await?;
        validation
            .validate_no_rewrites(
                table,
                retry,
                &removed.iter().map(|path| (*path).to_string()).collect(),
            )
            .await?;
        validation
            .validate_no_new_deletes(
                table,
                retry,
                &self.removed_data_files,
                self.data_sequence_number.is_some(),
            )
            .await?;

        SnapshotCommitBuilder::new(
            table,
            Operation::Replace,
            self.commit_uuid,
            self.snapshot_properties.clone(),
            SnapshotChanges::new(self.added_data_files.clone())
                .with_added_delete_files(self.added_delete_files.clone())
                .with_removed_data_files(&self.removed_data_files)
                .with_removed_delete_files(&self.removed_delete_files)
                .with_data_sequence_number(self.data_sequence_number),
        )
        .commit(retry)
        .await
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
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
    use crate::spec::{DataFileBuilder, DataFileFormat, Literal, SnapshotRef, Struct};
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
    async fn replaces_a_live_data_file() {
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
                .rewrite_files()
                .delete_files([old])
                .add_files([file(&table, "new.parquet")])
                .set_data_sequence_number(0),
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
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        let list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        assert_eq!(list.entries().len(), 2);
    }

    #[tokio::test]
    async fn rejects_empty_inputs() {
        let table = make_v2_minimal_table();
        assert!(
            Arc::new(Transaction::new(&table).rewrite_files())
                .commit(&mut SnapshotRetryState::default(), &table)
                .await
                .is_err()
        );
    }
}
