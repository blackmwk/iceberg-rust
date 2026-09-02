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

use crate::Result;
use crate::spec::{DataFile, Operation};
use crate::table::Table;
use crate::transaction::merging_snapshot_producer::MergingSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};

/// Atomically adds and removes data and row-level delete files.
pub struct RowDeltaAction {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            check_duplicate: true,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Add data files to this row delta.
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Add position or equality delete files to this row delta.
    pub fn add_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(files);
        self
    }

    /// Remove live data files from this row delta.
    pub fn remove_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(files);
        self
    }

    /// Remove live position or equality delete files from this row delta.
    pub fn remove_delete_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(files);
        self
    }

    /// Configure duplicate-path checks against live table files.
    pub fn with_check_duplicate(mut self, check: bool) -> Self {
        self.check_duplicate = check;
        self
    }

    /// Set the stable UUID used for metadata files created by this action.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Attach custom properties to the new snapshot summary.
    pub fn set_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = properties;
        self
    }

    fn operation(&self) -> Operation {
        if !self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_files.is_empty()
        {
            Operation::Append
        } else if !self.added_delete_files.is_empty() && self.added_data_files.is_empty() {
            Operation::Delete
        } else {
            Operation::Overwrite
        }
    }

    fn dedupe(files: &[DataFile]) -> Vec<DataFile> {
        let mut paths = HashSet::with_capacity(files.len());
        files
            .iter()
            .filter(|file| paths.insert(file.file_path()))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    type State = MergingSnapshotProducer;

    fn new_state(&self) -> Self::State {
        MergingSnapshotProducer::new(self.commit_uuid)
    }

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        let added_data = Self::dedupe(&self.added_data_files);
        let added_deletes = Self::dedupe(&self.added_delete_files);
        let removed_data = Self::dedupe(&self.removed_data_files);
        let removed_deletes = Self::dedupe(&self.removed_delete_files);
        if self.check_duplicate {
            state
                .validate_duplicate_files(table, &added_data, &added_deletes)
                .await?;
        }
        state
            .apply(
                table,
                self.operation(),
                self.snapshot_properties.clone(),
                &added_data,
                &added_deletes,
                &removed_data,
                &removed_deletes,
            )
            .await
    }

    async fn finish_commit(
        &self,
        state: &mut Self::State,
        table: &Table,
        commit_error: Option<&crate::Error>,
    ) {
        state.finish_commit(table, commit_error).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    fn file(content: DataContentType, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .equality_ids((content == DataContentType::EqualityDeletes).then_some(vec![1]))
            .build()
            .unwrap()
    }

    #[test]
    fn operation_matches_java_base_row_delta() {
        let data = file(DataContentType::Data, "data.parquet");
        let delete = file(DataContentType::EqualityDeletes, "delete.parquet");
        assert_eq!(
            RowDeltaAction::new()
                .add_data_files([data.clone()])
                .add_delete_files([delete.clone()])
                .operation(),
            Operation::Overwrite
        );
        assert_eq!(
            RowDeltaAction::new().add_delete_files([delete]).operation(),
            Operation::Delete
        );
        assert_eq!(
            RowDeltaAction::new().remove_data_files([data]).operation(),
            Operation::Overwrite
        );
    }
}
