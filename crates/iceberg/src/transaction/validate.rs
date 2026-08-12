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

use crate::Result;
use crate::table::Table;
use crate::transaction::retry::SnapshotRetryState;

/// Retry-aware history boundary shared by snapshot conflict validations.
#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)] // Consumed by validation actions in the next stack layers.
pub(crate) struct SnapshotValidation {
    starting_snapshot_id: Option<i64>,
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
}
