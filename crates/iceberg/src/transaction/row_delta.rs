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

use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::spec::{DataFile, Operation};
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;
use crate::transaction::snapshot::{SnapshotChanges, SnapshotCommitBuilder};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, Result};

/// Atomically adds data files and row-level position or equality deletes.
pub struct RowDeltaAction {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    #[allow(dead_code)] // Used by RowDelta validation in the next stack layer.
    pub(crate) starting_snapshot_id: Option<i64>,
}

impl RowDeltaAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            check_duplicate: true,
            commit_uuid: None,
            snapshot_properties: HashMap::new(),
            starting_snapshot_id,
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

    /// Configure duplicate-path checks against live table files.
    pub fn with_check_duplicate(mut self, check: bool) -> Self {
        self.check_duplicate = check;
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

    fn operation(&self) -> Operation {
        match (
            self.added_data_files.is_empty(),
            self.added_delete_files.is_empty(),
        ) {
            (false, true) => Operation::Append,
            (true, false) => Operation::Delete,
            _ => Operation::Overwrite,
        }
    }

    pub(crate) async fn commit_with_state(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
    ) -> Result<ActionCommit> {
        let changes = SnapshotChanges::new(self.added_data_files.clone())
            .with_added_delete_files(self.added_delete_files.clone());
        SnapshotCommitBuilder::new(
            table,
            self.operation(),
            self.commit_uuid,
            self.snapshot_properties.clone(),
            changes,
        )
        .with_check_duplicate(self.check_duplicate)
        .commit(retry)
        .await
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
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
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestContentType,
        SnapshotRef, Struct,
    };
    use crate::transaction::Transaction;
    use crate::transaction::tests::make_v2_minimal_table;

    fn file(table: &Table, content: DataContentType, path: &str) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]));
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn row_delta_writes_same_sequence_data_and_deletes() {
        let table = make_v2_minimal_table();
        let mut commit = Arc::new(
            Transaction::new(&table)
                .row_delta()
                .add_data_files([file(&table, DataContentType::Data, "data.parquet")])
                .add_delete_files([file(
                    &table,
                    DataContentType::EqualityDeletes,
                    "delete.parquet",
                )]),
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
        assert_eq!(list.entries().len(), 2);
        for manifest in list.entries() {
            assert_eq!(manifest.sequence_number, snapshot.sequence_number());
        }
        assert!(
            list.entries()
                .iter()
                .any(|manifest| { manifest.content == ManifestContentType::Data })
        );
        assert!(
            list.entries()
                .iter()
                .any(|manifest| { manifest.content == ManifestContentType::Deletes })
        );
    }

    #[tokio::test]
    async fn row_delta_operation_reflects_contents() {
        let table = make_v2_minimal_table();
        let data_only = Transaction::new(&table).row_delta().add_data_files([file(
            &table,
            DataContentType::Data,
            "data.parquet",
        )]);
        assert_eq!(data_only.operation(), Operation::Append);
        let delete_only = Transaction::new(&table).row_delta().add_delete_files([file(
            &table,
            DataContentType::EqualityDeletes,
            "delete.parquet",
        )]);
        assert_eq!(delete_only.operation(), Operation::Delete);
    }

    #[tokio::test]
    async fn row_delta_merge_on_read_round_trip() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use futures::TryStreamExt;
        use parquet::file::properties::WriterProperties;

        use crate::arrow::schema_to_arrow_schema;
        use crate::memory::tests::new_memory_catalog;
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};
        use crate::transaction::ApplyTransactionAction;
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
