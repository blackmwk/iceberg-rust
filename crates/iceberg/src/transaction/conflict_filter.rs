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

use std::sync::Arc;

use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
use crate::expr::visitors::strict_projection::StrictProjection;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{DataFile, ManifestFile, PartitionSpecRef, Schema};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Bound row predicate used for conservative conflict detection.
pub(crate) struct ConflictFilter {
    bound: BoundPredicate,
    case_sensitive: bool,
}

impl ConflictFilter {
    #[allow(dead_code)] // RowDelta wires this in a later stack layer.
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

    pub(crate) fn manifest_could_match(
        &self,
        table: &Table,
        manifest: &ManifestFile,
    ) -> Result<bool> {
        let spec = self.spec(table, manifest.partition_spec_id)?;
        let projected = self.project(table, spec, false)?;
        ManifestEvaluator::builder(projected).build().eval(manifest)
    }

    fn partition_matches(&self, table: &Table, file: &DataFile, strict: bool) -> Result<bool> {
        let spec = self.spec(table, file.partition_spec_id)?;
        if spec.fields().iter().any(|field| {
            table
                .metadata()
                .current_schema()
                .field_by_id(field.source_id)
                .is_none()
        }) {
            return Ok(!strict);
        }
        ExpressionEvaluator::new(self.project(table, spec, strict)?).eval(file)
    }

    fn project(
        &self,
        table: &Table,
        spec: PartitionSpecRef,
        strict: bool,
    ) -> Result<BoundPredicate> {
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
        };
        projected.bind(partition_schema, self.case_sensitive)
    }

    fn spec(&self, table: &Table, spec_id: i32) -> Result<PartitionSpecRef> {
        table
            .metadata()
            .partition_spec_by_id(spec_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Cannot find partition spec {spec_id}"),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::{DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, Struct};
    use crate::transaction::tests::make_v2_minimal_table;

    #[test]
    fn binds_case_sensitivity_and_projects_partitions() {
        let table = make_v2_minimal_table();
        assert!(
            ConflictFilter::new(&table, Reference::new("X").equal_to(Datum::long(300)), true)
                .is_err()
        );
        let filter = ConflictFilter::new(
            &table,
            Reference::new("X").equal_to(Datum::long(300)),
            false,
        )
        .unwrap();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        assert!(filter.could_match(&table, &file).unwrap());
        assert!(filter.must_match(&table, &file).unwrap());
    }
}
