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

//! Stateless building blocks shared by snapshot producers.
//!
//! The producer implementations deliberately do not share mutable state. These
//! helpers only translate explicit inputs into Iceberg metadata and files.

#![allow(dead_code)] // Consumers are introduced by the next stack layers.

use std::collections::HashMap;

use uuid::Uuid;

use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestFile, ManifestListWriter,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Summary,
    TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// Generate a positive snapshot ID that is not present in the supplied table.
pub(crate) fn generate_snapshot_id(table: &Table) -> i64 {
    loop {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64 & i64::MAX;
        if table.metadata().snapshot_by_id(snapshot_id).is_none() {
            return snapshot_id;
        }
    }
}

/// Return the metadata path for a producer-owned manifest.
pub(crate) fn manifest_path(table: &Table, commit_uuid: Uuid, counter: u64) -> Result<String> {
    Ok(format!(
        "{}/{}-m{}.{}",
        table.metadata().metadata_location()?,
        commit_uuid,
        counter,
        DataFileFormat::Avro
    ))
}

/// Return the metadata path for one snapshot-production attempt's manifest list.
pub(crate) fn manifest_list_path(
    table: &Table,
    snapshot_id: i64,
    attempt: u64,
    commit_uuid: Uuid,
) -> Result<String> {
    Ok(format!(
        "{}/snap-{}-{}-{}.{}",
        table.metadata().metadata_location()?,
        snapshot_id,
        attempt,
        commit_uuid,
        DataFileFormat::Avro
    ))
}

/// Build a snapshot summary from an explicit set of file changes.
pub(crate) fn build_summary(
    table: &Table,
    operation: Operation,
    mut properties: HashMap<String, String>,
    added_files: &[DataFile],
    removed_files: &[DataFile],
) -> Result<Summary> {
    let metadata = table.metadata();
    let mut collector = SnapshotSummaryCollector::default();
    collector.set_partition_summary_limit(
        metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
            .and_then(|value| value.parse().ok())
            .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT),
    );

    for file in added_files {
        let spec = metadata
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| unknown_spec(file.partition_spec_id))?;
        collector.add_file(file, metadata.current_schema().clone(), spec.clone());
    }
    for file in removed_files {
        let spec = metadata
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| unknown_spec(file.partition_spec_id))?;
        collector.remove_file(file, metadata.current_schema().clone(), spec.clone());
    }

    // Computed metrics overwrite user properties, matching iceberg-java's
    // SnapshotProducer.summary ordering.
    properties.extend(collector.build());
    update_snapshot_summaries(
        Summary {
            operation: operation.clone(),
            additional_properties: properties,
        },
        metadata
            .current_snapshot()
            .map(|snapshot| snapshot.summary()),
        operation == Operation::Overwrite,
    )
}

fn unknown_spec(spec_id: i32) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("Cannot write a file for unknown partition spec {spec_id}"),
    )
}

/// Write one attempt's manifest list and return its metadata updates.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_snapshot_commit(
    table: &Table,
    snapshot_id: i64,
    manifest_list_path: String,
    operation: Operation,
    properties: HashMap<String, String>,
    added_files: &[DataFile],
    removed_files: &[DataFile],
    manifests: Vec<ManifestFile>,
) -> Result<ActionCommit> {
    let next_sequence_number = table.metadata().next_sequence_number();
    let first_row_id = table.metadata().next_row_id();
    let raw_output = table.file_io().new_output(manifest_list_path.clone())?;
    let (writer, encryption_key_id) = match table.encryption_manager() {
        Some(manager) => {
            let encrypted = manager.encrypt(raw_output);
            let key_id = manager
                .encrypt_manifest_list_key_metadata(encrypted.key_metadata())
                .await?;
            (encrypted.writer().await?, Some(key_id))
        }
        None => (raw_output.writer().await?, None),
    };

    let parent_snapshot_id = table.metadata().current_snapshot_id();
    let mut list_writer = match table.metadata().format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(writer, snapshot_id, parent_snapshot_id),
        FormatVersion::V2 => ManifestListWriter::v2(
            writer,
            snapshot_id,
            parent_snapshot_id,
            next_sequence_number,
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            writer,
            snapshot_id,
            parent_snapshot_id,
            next_sequence_number,
            Some(first_row_id),
        ),
    };
    list_writer.add_manifests(manifests.into_iter())?;
    let next_row_id = list_writer.next_row_id();
    list_writer.close().await?;

    let summary = build_summary(table, operation, properties, added_files, removed_files)?;
    let snapshot_builder = Snapshot::builder()
        .with_manifest_list(manifest_list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(next_sequence_number)
        .with_summary(summary)
        .with_schema_id(table.metadata().current_schema_id())
        .with_encryption_key_id(encryption_key_id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis());
    let snapshot = match next_row_id {
        Some(next) => snapshot_builder
            .with_row_range(first_row_id, next - first_row_id)
            .build(),
        None => snapshot_builder.build(),
    };

    let mut updates: Vec<TableUpdate> = table
        .encryption_manager()
        .map(|manager| {
            manager.with_encryption_keys(|keys| {
                keys.values()
                    .filter(|key| table.metadata().encryption_key(key.key_id()).is_none())
                    .map(|key| TableUpdate::AddEncryptionKey {
                        encryption_key: key.clone(),
                    })
                    .collect()
            })
        })
        .unwrap_or_default();
    updates.extend([
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ]);

    Ok(ActionCommit::new(updates, vec![
        TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: parent_snapshot_id,
        },
    ]))
}

/// Best-effort removal of a producer-owned artifact.
pub(crate) async fn delete_artifact(table: &Table, path: &str) {
    let _ = table.file_io().delete(path).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::tests::make_v2_table;

    #[test]
    fn metadata_paths_include_stable_identity_and_attempt() {
        let table = make_v2_table();
        let uuid = Uuid::nil();
        assert!(
            manifest_path(&table, uuid, 3)
                .unwrap()
                .ends_with("00000000-0000-0000-0000-000000000000-m3.avro")
        );
        assert!(
            manifest_list_path(&table, 7, 2, uuid)
                .unwrap()
                .ends_with("snap-7-2-00000000-0000-0000-0000-000000000000.avro")
        );
    }

    #[test]
    fn computed_summary_keeps_user_properties() {
        let table = make_v2_table();
        let summary = build_summary(
            &table,
            Operation::Append,
            HashMap::from([("source".to_string(), "test".to_string())]),
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(summary.operation, Operation::Append);
        assert_eq!(summary.additional_properties.get("source").unwrap(), "test");
    }

    #[test]
    fn generated_snapshot_id_is_positive_and_unused() {
        let table = make_v2_table();
        let snapshot_id = generate_snapshot_id(&table);
        assert!(snapshot_id >= 0);
        assert!(table.metadata().snapshot_by_id(snapshot_id).is_none());
    }
}
