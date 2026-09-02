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
use crate::transaction::conflict_filter::ConflictFilter;
use crate::transaction::merging_snapshot_producer::MergingSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// Atomically adds and removes data and row-level delete files.
pub struct RowDeltaAction {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    starting_snapshot_id: Option<i64>,
    referenced_data_files: HashSet<String>,
    validate_deleted_files: bool,
    validate_conflicting_data: bool,
    validate_conflicting_deletes: bool,
    conflict_filter: Predicate,
    case_sensitive: bool,
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
            starting_snapshot_id: None,
            referenced_data_files: HashSet::new(),
            validate_deleted_files: false,
            validate_conflicting_data: false,
            validate_conflicting_deletes: false,
            conflict_filter: Predicate::AlwaysTrue,
            case_sensitive: true,
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

    /// Set the snapshot boundary used for conflict validation.
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }

    /// Require referenced data-file paths to remain live.
    pub fn validate_data_files_exist<I, S>(mut self, files: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.referenced_data_files
            .extend(files.into_iter().map(Into::into));
        self
    }

    /// Reject referenced files removed or rewritten since the validation boundary.
    pub fn validate_deleted_files(mut self) -> Self {
        self.validate_deleted_files = true;
        self
    }

    /// Set the predicate used to scope concurrent-write checks.
    pub fn conflict_detection_filter(mut self, predicate: Predicate) -> Self {
        self.conflict_filter = predicate;
        self
    }

    /// Configure case-sensitive conflict-filter binding.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Reject concurrently added data files matching the conflict filter.
    pub fn validate_no_conflicting_data_files(mut self) -> Self {
        self.validate_conflicting_data = true;
        self
    }

    /// Reject concurrently added delete files that may affect this delta.
    pub fn validate_no_conflicting_delete_files(mut self) -> Self {
        self.validate_conflicting_deletes = true;
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
        if let Some(starting_snapshot_id) = self.starting_snapshot_id {
            state.validate_starting_snapshot(table, starting_snapshot_id)?;
        }
        if !self.referenced_data_files.is_empty() {
            state
                .validate_data_files_exist(table, &self.referenced_data_files)
                .await?;
            if self.validate_deleted_files {
                state
                    .validate_no_rewrites(
                        table,
                        self.starting_snapshot_id,
                        &self.referenced_data_files,
                    )
                    .await?;
            }
        }
        let removed_paths = removed_data
            .iter()
            .map(|file| file.file_path())
            .collect::<HashSet<_>>();
        let conflicting_removals = self
            .referenced_data_files
            .iter()
            .filter(|path| removed_paths.contains(path.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !conflicting_removals.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot remove data files referenced by new delete files: {}",
                    conflicting_removals.join(", ")
                ),
            ));
        }
        if self.validate_conflicting_deletes && !removed_data.is_empty() {
            state
                .validate_no_new_deletes_for_data_files(
                    table,
                    self.starting_snapshot_id,
                    &removed_data,
                    false,
                )
                .await?;
        }
        if self.validate_conflicting_data || self.validate_conflicting_deletes {
            let filter =
                ConflictFilter::new(table, self.conflict_filter.clone(), self.case_sensitive)?;
            state
                .validate_no_conflicting_files(
                    table,
                    self.starting_snapshot_id,
                    &filter,
                    self.validate_conflicting_data,
                    self.validate_conflicting_deletes,
                    false,
                )
                .await?;
        }
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
        commit_error: Option<&Error>,
    ) {
        state.finish_commit(table, commit_error).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TableUpdate;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        ManifestListWriter, SnapshotRef, Struct,
    };
    use crate::transaction::tests::{make_v2_minimal_table, make_v2_table};

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

    #[tokio::test]
    async fn writes_data_and_delete_manifests_with_one_sequence() {
        let table = make_v2_minimal_table();
        let action = RowDeltaAction::new()
            .add_data_files([partitioned_file(
                &table,
                DataContentType::Data,
                "data.parquet",
            )])
            .add_delete_files([partitioned_file(
                &table,
                DataContentType::EqualityDeletes,
                "delete.parquet",
            )]);
        let mut state = action.new_state();
        let mut commit = action.commit(&mut state, &table).await.unwrap();
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
        assert_eq!(list.entries().len(), 2);
        assert!(
            list.entries()
                .iter()
                .all(|manifest| manifest.sequence_number == snapshot.sequence_number())
        );
        assert!(
            list.entries()
                .iter()
                .any(|manifest| manifest.content == ManifestContentType::Data)
        );
        assert!(
            list.entries()
                .iter()
                .any(|manifest| manifest.content == ManifestContentType::Deletes)
        );
    }

    fn partitioned_file(table: &Table, content: DataContentType, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids((content == DataContentType::EqualityDeletes).then_some(vec![1]))
            .build()
            .unwrap()
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
    async fn validation_retry_reuses_processed_snapshot_history() {
        let table = make_v2_table();
        write_empty_manifest_lists(&table).await;
        let action = RowDeltaAction::new()
            .validate_no_conflicting_data_files()
            .add_data_files([partitioned_file(
                &table,
                DataContentType::Data,
                "new.parquet",
            )]);
        let mut state = action.new_state();

        action.commit(&mut state, &table).await.unwrap();
        let loads = state.manifest_list_loads();
        action.commit(&mut state, &table).await.unwrap();
        assert_eq!(state.manifest_list_loads(), loads);
        assert_eq!(loads, table.metadata().snapshots().count());
    }

    #[tokio::test]
    async fn rejects_non_ancestor_validation_start() {
        let table = make_v2_table();
        let action = RowDeltaAction::new()
            .validate_from_snapshot(i64::MAX)
            .add_data_files([partitioned_file(
                &table,
                DataContentType::Data,
                "new.parquet",
            )]);
        let mut state = action.new_state();
        assert!(action.commit(&mut state, &table).await.is_err());
    }

    #[tokio::test]
    async fn merge_on_read_round_trip() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use futures::TryStreamExt;
        use parquet::file::properties::WriterProperties;

        use crate::arrow::schema_to_arrow_schema;
        use crate::memory::tests::new_memory_catalog;
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::transaction::{ApplyTransactionAction, Transaction};
        use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
        use crate::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};
        use crate::{Catalog, TableCreation, TableIdent};

        let catalog = new_memory_catalog().await;
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        let ident = TableIdent::from_strs(["row_delta", "round_trip"]).unwrap();
        catalog
            .create_namespace(ident.namespace(), HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(schema.clone())
                    .build(),
            )
            .await
            .unwrap();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let location = DefaultLocationGenerator::new(table.metadata()).unwrap();
        let data_writer = |prefix: &str| {
            DataFileWriterBuilder::new(RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(
                    WriterProperties::builder().build(),
                    table.metadata().current_schema().clone(),
                ),
                table.file_io().clone(),
                location.clone(),
                DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet),
            ))
        };

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])
        .unwrap();
        let mut writer = data_writer("base").build(None).await.unwrap();
        writer.write(batch).await.unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(writer.close().await.unwrap())
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let equality =
            EqualityDeleteWriterConfig::new(vec![1], table.metadata().current_schema().clone())
                .unwrap();
        let projected = equality.projected_arrow_schema_ref().clone();
        let projected_schema = Arc::new(crate::arrow::arrow_schema_to_schema(&projected).unwrap());
        let mut delete_writer = EqualityDeleteFileWriterBuilder::new(
            RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(WriterProperties::builder().build(), projected_schema),
                table.file_io().clone(),
                location.clone(),
                DefaultFileNameGenerator::new("delete".to_string(), None, DataFileFormat::Parquet),
            ),
            equality,
        )
        .build(None)
        .await
        .unwrap();
        delete_writer
            .write(
                RecordBatch::try_new(arrow_schema.clone(), vec![
                    Arc::new(Int64Array::from(vec![2])),
                    Arc::new(StringArray::from(vec!["b"])),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        let delete_files = delete_writer.close().await.unwrap();

        let mut writer = data_writer("replacement").build(None).await.unwrap();
        writer
            .write(
                RecordBatch::try_new(arrow_schema, vec![
                    Arc::new(Int64Array::from(vec![2])),
                    Arc::new(StringArray::from(vec!["b2"])),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .row_delta()
            .add_data_files(writer.close().await.unwrap())
            .add_delete_files(delete_files)
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let batches: Vec<RecordBatch> = table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (ids.value(row), values.value(row).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(rows, vec![
            (1, "a".to_string()),
            (2, "b2".to_string()),
            (3, "c".to_string()),
        ]);
    }
}
