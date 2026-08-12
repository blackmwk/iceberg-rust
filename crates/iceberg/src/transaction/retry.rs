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

use crate::spec::{ManifestContentType, ManifestFile, Operation, SnapshotRef};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotFingerprint {
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
}

impl From<&SnapshotRef> for SnapshotFingerprint {
    fn from(snapshot: &SnapshotRef) -> Self {
        Self {
            parent_snapshot_id: snapshot.parent_snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            manifest_list: snapshot.manifest_list().to_string(),
        }
    }
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

/// In-memory table history retained while an optimistic commit retries.
///
/// Snapshot metadata is immutable. Once a snapshot's manifest list has been
/// loaded, later attempts can reuse it and read only snapshots introduced by
/// concurrent commits.
#[derive(Default)]
pub(crate) struct SnapshotRetryState {
    processed_snapshots: HashMap<i64, ProcessedSnapshot>,
    #[cfg(test)]
    manifest_list_loads: usize,
}

impl SnapshotRetryState {
    async fn load_snapshot(&mut self, table: &Table, snapshot: &SnapshotRef) -> Result<bool> {
        let snapshot_id = snapshot.snapshot_id();
        let fingerprint = SnapshotFingerprint::from(snapshot);
        if let Some(processed) = self.processed_snapshots.get(&snapshot_id) {
            if processed.fingerprint != fingerprint {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Snapshot {snapshot_id} changed while retrying a commit"),
                ));
            }
            return Ok(false);
        }

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        #[cfg(test)]
        {
            self.manifest_list_loads += 1;
        }

        let mut data_manifests = Vec::new();
        let mut delete_manifests = Vec::new();
        for manifest in manifest_list.consume_entries() {
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

    pub(crate) async fn process_current_snapshot(
        &mut self,
        table: &Table,
    ) -> Result<Option<&ProcessedSnapshot>> {
        let Some(current) = table.metadata().current_snapshot().cloned() else {
            return Ok(None);
        };
        let snapshot_id = current.snapshot_id();
        self.load_snapshot(table, &current).await?;
        Ok(self.processed(snapshot_id))
    }

    /// Load history from the current snapshot back to `starting_snapshot_id`
    /// (exclusive), returning snapshot IDs whose manifest lists were loaded by
    /// this call in head-to-root order.
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

            if self.load_snapshot(table, &current).await? {
                newly_processed.push(snapshot_id);
            }

            snapshot = match current.parent_snapshot_id() {
                Some(parent_id) => table.metadata().snapshot_by_id(parent_id).cloned(),
                None => None,
            };
        }

        if !reached_start {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot determine history from current snapshot to starting snapshot {}",
                    starting_snapshot_id.unwrap()
                ),
            ));
        }

        Ok(newly_processed)
    }

    pub(crate) fn processed(&self, snapshot_id: i64) -> Option<&ProcessedSnapshot> {
        self.processed_snapshots.get(&snapshot_id)
    }

    #[cfg(test)]
    fn manifest_list_loads(&self) -> usize {
        self.manifest_list_loads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ManifestListWriter;
    use crate::transaction::tests::make_v2_table;

    async fn write_empty_manifest_lists(table: &Table) {
        for snapshot in table.metadata().snapshots() {
            let output = table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap();
            let writer = output.writer().await.unwrap();
            ManifestListWriter::v2(
                writer,
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
    async fn reuses_processed_snapshot_manifest_lists() {
        let table = make_v2_table();
        write_empty_manifest_lists(&table).await;
        let mut state = SnapshotRetryState::default();

        let first = state.process_new_snapshots(&table, None).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(state.manifest_list_loads(), 2);

        let retry = state.process_new_snapshots(&table, None).await.unwrap();
        assert!(retry.is_empty());
        assert_eq!(state.manifest_list_loads(), 2);
    }

    #[tokio::test]
    async fn rejects_non_ancestor_starting_snapshot() {
        let table = make_v2_table();
        write_empty_manifest_lists(&table).await;
        let mut state = SnapshotRetryState::default();

        let err = state
            .process_new_snapshots(&table, Some(i64::MAX))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
