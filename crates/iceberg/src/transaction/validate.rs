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
use std::sync::Arc;

use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::visitors::strict_projection::StrictProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{DataContentType, DataFile, ManifestContentType, Schema};
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;
use crate::{Error, ErrorKind, Result};

/// Retry-aware history boundary shared by snapshot conflict validations.
#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)] // Consumed by validation actions in the next stack layers.
pub(crate) struct SnapshotValidation {
    starting_snapshot_id: Option<i64>,
}

/// A bound row predicate used for conservative conflict detection and exact
/// whole-file matching.
pub(crate) struct ConflictFilter {
    bound: BoundPredicate,
    case_sensitive: bool,
}

#[allow(dead_code)] // Public snapshot actions consume this in later stack layers.
impl ConflictFilter {
    pub(crate) fn new(table: &Table, predicate: Predicate, case_sensitive: bool) -> Result<Self> {
        Ok(Self {
            bound: predicate.bind(table.metadata().current_schema().clone(), case_sensitive)?,
            case_sensitive,
        })
    }

    pub(crate) fn could_match(&self, table: &Table, file: &DataFile) -> Result<bool> {
        Ok(self.partition_matches(table, file, false)?
            && InclusiveMetricsEvaluator::eval(&self.bound, file, false)?)
    }

    pub(crate) fn must_match(&self, table: &Table, file: &DataFile) -> Result<bool> {
        Ok(self.partition_matches(table, file, true)?
            || StrictMetricsEvaluator::eval(&self.bound, file)?)
    }

    pub(crate) async fn matching_live_data_files(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
    ) -> Result<Vec<DataFile>> {
        let live = live_files(table, retry).await?;
        let mut matches = Vec::new();
        for live_file in live.data.values() {
            if self.could_match(table, &live_file.file)? {
                if !self.must_match(table, &live_file.file)? {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file {} because only some rows may match",
                            live_file.file.file_path
                        ),
                    ));
                }
                matches.push(live_file.file.clone());
            }
        }
        Ok(matches)
    }

    fn partition_matches(&self, table: &Table, file: &DataFile, strict: bool) -> Result<bool> {
        let spec = table
            .metadata()
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find partition spec {}", file.partition_spec_id),
                )
            })?
            .clone();
        if spec.fields().iter().any(|field| {
            table
                .metadata()
                .current_schema()
                .field_by_id(field.source_id)
                .is_none()
        }) {
            return Ok(!strict);
        }
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(spec.spec_id())
                .with_fields(
                    spec.partition_type(table.metadata().current_schema())?
                        .fields()
                        .to_owned(),
                )
                .build()?,
        );
        let projected = if strict {
            StrictProjection::new(spec)
                .strict_project(&self.bound)?
                .rewrite_not()
        } else {
            InclusiveProjection::new(spec)
                .project(&self.bound)?
                .rewrite_not()
        }
        .bind(partition_schema, self.case_sensitive)?;
        ExpressionEvaluator::new(projected).eval(file)
    }
}

impl SnapshotValidation {
    #[allow(dead_code)] // Consumed by validation actions in the next stack layers.
    pub(crate) fn from_snapshot(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            starting_snapshot_id,
        }
    }

    #[allow(dead_code)] // Consumed by validation actions in the next stack layers.
    pub(crate) async fn history(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
    ) -> Result<Vec<i64>> {
        retry
            .validation_history(table, self.starting_snapshot_id)
            .await
    }

    /// Validate that required paths are still live in the current snapshot.
    #[allow(dead_code)] // Used by snapshot actions in later stack layers.
    pub(crate) async fn validate_files_exist(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
        data_paths: &HashSet<String>,
        delete_paths: &HashSet<String>,
    ) -> Result<()> {
        let live = live_files(table, retry).await?;
        let missing = data_paths
            .iter()
            .filter(|path| !live.data.contains_key(*path))
            .chain(
                delete_paths
                    .iter()
                    .filter(|path| !live.deletes.contains_key(*path)),
            )
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Required files are no longer live: {}", missing.join(", ")),
            ))
        }
    }

    /// Reject data or delete files removed/replaced since the validation boundary.
    #[allow(dead_code)] // Used by snapshot actions in later stack layers.
    pub(crate) async fn validate_no_rewrites(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
        paths: &HashSet<String>,
    ) -> Result<()> {
        let history = self.history(table, retry).await?;
        for snapshot_id in history {
            let manifests = retry
                .processed(snapshot_id)
                .into_iter()
                .flat_map(|snapshot| {
                    snapshot
                        .introduced_data_manifests()
                        .iter()
                        .chain(snapshot.introduced_delete_manifests())
                        .cloned()
                })
                .collect::<Vec<_>>();
            for manifest in manifests {
                if retry
                    .load_manifest(table, &manifest)
                    .await?
                    .entries()
                    .iter()
                    .any(|entry| {
                        entry.status() == crate::spec::ManifestStatus::Deleted
                            && paths.contains(entry.file_path())
                    })
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "A required file was concurrently removed or rewritten",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Reject delete files introduced since the boundary that apply to the
    /// referenced data files, using Iceberg's data-sequence rules.
    #[allow(dead_code)] // Used by rewrite and row-delta actions in later layers.
    pub(crate) async fn validate_no_new_deletes(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
        data_files: &[DataFile],
        ignore_equality_deletes: bool,
    ) -> Result<()> {
        let live = live_files(table, retry).await?;
        let history = self.history(table, retry).await?;
        for snapshot_id in history {
            let manifests = retry
                .processed(snapshot_id)
                .map(|snapshot| snapshot.introduced_delete_manifests().to_vec())
                .unwrap_or_default();
            for manifest_file in manifests {
                let manifest = retry.load_manifest(table, &manifest_file).await?;
                for delete in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                    if ignore_equality_deletes
                        && delete.content_type() == DataContentType::EqualityDeletes
                    {
                        continue;
                    }
                    for data_file in data_files {
                        let data_sequence = live
                            .data
                            .get(data_file.file_path())
                            .and_then(|file| file.sequence_number)
                            .unwrap_or(0);
                        if delete_applies(
                            data_file,
                            data_sequence,
                            delete.data_file(),
                            delete.sequence_number().unwrap_or(0),
                        ) {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "New delete file {} applies to data file {}",
                                    delete.file_path(),
                                    data_file.file_path()
                                ),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Detect predicate-scoped data additions, delete additions, and removals
    /// in snapshots committed after the validation boundary.
    #[allow(dead_code)] // Used by delete/overwrite/row-delta actions in later layers.
    pub(crate) async fn validate_no_conflicting_files(
        &self,
        table: &Table,
        retry: &mut SnapshotRetryState,
        filter: &ConflictFilter,
        check_data_additions: bool,
        check_delete_additions: bool,
        check_removals: bool,
    ) -> Result<()> {
        let history = self.history(table, retry).await?;
        for snapshot_id in history {
            let manifests = retry
                .processed(snapshot_id)
                .into_iter()
                .flat_map(|snapshot| {
                    snapshot
                        .introduced_data_manifests()
                        .iter()
                        .chain(snapshot.introduced_delete_manifests())
                        .cloned()
                })
                .collect::<Vec<_>>();
            for manifest_file in manifests {
                let manifest = retry.load_manifest(table, &manifest_file).await?;
                for entry in manifest.entries() {
                    let is_removal = entry.status() == crate::spec::ManifestStatus::Deleted;
                    let is_addition = entry.status() == crate::spec::ManifestStatus::Added;
                    let should_check = (is_removal && check_removals)
                        || (is_addition
                            && match entry.content_type() {
                                DataContentType::Data => check_data_additions,
                                DataContentType::PositionDeletes
                                | DataContentType::EqualityDeletes => check_delete_additions,
                            });
                    if !should_check {
                        continue;
                    }
                    let path = entry.file_path();
                    let (could_match, must_match) =
                        if let Some(result) = retry.predicate_result(path) {
                            result
                        } else {
                            let result = (
                                filter.could_match(table, entry.data_file())?,
                                filter.must_match(table, entry.data_file())?,
                            );
                            retry.cache_predicate_result(path.to_string(), result);
                            result
                        };
                    if could_match || (is_removal && must_match) {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Conflicting file {} matches the validation filter", path),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct LiveFiles {
    data: HashMap<String, LiveFile>,
    deletes: HashMap<String, LiveFile>,
}

struct LiveFile {
    file: DataFile,
    sequence_number: Option<i64>,
}

async fn live_files(table: &Table, retry: &mut SnapshotRetryState) -> Result<LiveFiles> {
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
    let mut live = LiveFiles::default();
    for manifest_file in manifests {
        let destination = if manifest_file.content == ManifestContentType::Data {
            &mut live.data
        } else {
            &mut live.deletes
        };
        for entry in retry
            .load_manifest(table, &manifest_file)
            .await?
            .entries()
            .iter()
            .filter(|entry| entry.is_alive())
        {
            destination.insert(entry.file_path().to_string(), LiveFile {
                file: entry.data_file().clone(),
                sequence_number: entry.sequence_number(),
            });
        }
    }
    Ok(live)
}

fn delete_applies(
    data: &DataFile,
    data_sequence: i64,
    delete: &DataFile,
    delete_sequence: i64,
) -> bool {
    if let Some(referenced) = delete.referenced_data_file()
        && referenced != data.file_path
    {
        return false;
    }
    let same_partition = delete.partition().fields().is_empty()
        || (delete.partition_spec_id == data.partition_spec_id
            && delete.partition() == data.partition());
    if !same_partition {
        return false;
    }
    match delete.content_type() {
        DataContentType::EqualityDeletes => delete_sequence > data_sequence,
        DataContentType::PositionDeletes => delete_sequence >= data_sequence,
        DataContentType::Data => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::{DataFileBuilder, DataFileFormat, Datum, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;

    fn file(content: DataContentType, path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    #[test]
    fn delete_sequence_rules_are_content_specific() {
        let data = file(DataContentType::Data, "data.parquet");
        let equality = file(DataContentType::EqualityDeletes, "eq.parquet");
        let position = file(DataContentType::PositionDeletes, "pos.parquet");

        assert!(!delete_applies(&data, 5, &equality, 5));
        assert!(delete_applies(&data, 5, &equality, 6));
        assert!(delete_applies(&data, 5, &position, 5));
        assert!(!delete_applies(&data, 5, &position, 4));
    }

    #[test]
    fn conflict_filter_binds_case_sensitivity_and_projects_partitions() {
        let table = make_v2_minimal_table();
        assert!(
            ConflictFilter::new(&table, Reference::new("X").equal_to(Datum::long(300)), true,)
                .is_err()
        );
        let filter = ConflictFilter::new(
            &table,
            Reference::new("X").equal_to(Datum::long(300)),
            false,
        )
        .unwrap();
        let partitioned = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        assert!(filter.could_match(&table, &partitioned).unwrap());
        assert!(filter.must_match(&table, &partitioned).unwrap());
    }
}
