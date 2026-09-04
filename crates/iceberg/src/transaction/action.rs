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

use std::any::Any;
use std::mem::take;
use std::sync::Arc;

use as_any::AsAny;
use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::Transaction;
use crate::{Error, Result, TableRequirement, TableUpdate};

/// A boxed, thread-safe reference to an object-safe transaction action.
pub(crate) type BoxedTransactionAction = Arc<dyn DynTransactionAction>;

/// Type-safe state retained by one action across transaction commit retries.
pub(crate) trait TransactionActionState: Default + Send + Sync + 'static {}

impl<T: Default + Send + Sync + 'static> TransactionActionState for T {}

/// Object-safe form of [`TransactionActionState`] used by [`Transaction`].
pub(crate) trait DynTransactionActionState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: TransactionActionState> DynTransactionActionState for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub(crate) struct TransactionActionEntry {
    pub(crate) action: BoxedTransactionAction,
    pub(crate) retry_state: Box<dyn DynTransactionActionState>,
}

impl TransactionActionEntry {
    fn new<T: TransactionAction + 'static>(action: T) -> Self {
        let retry_state = Box::new(action.new_state());
        Self {
            action: Arc::new(action),
            retry_state,
        }
    }

    pub(crate) fn clone_without_retry_state(&self) -> Self {
        Self {
            action: Arc::clone(&self.action),
            retry_state: self.action.new_state(),
        }
    }
}

impl std::ops::Deref for TransactionActionEntry {
    type Target = dyn DynTransactionAction;

    fn deref(&self) -> &Self::Target {
        self.action.as_ref()
    }
}

/// A trait representing an atomic action that can be part of a transaction.
///
/// Implementors of this trait define how a specific action is committed to a table.
/// Each action is responsible for generating the updates and requirements needed
/// to modify the table metadata.
#[async_trait]
pub(crate) trait TransactionAction: AsAny + Sync + Send {
    /// State retained by this action across optimistic transaction retries.
    type State: TransactionActionState;

    /// Creates the state retained by this action across commit retries.
    fn new_state(&self) -> Self::State {
        Self::State::default()
    }

    /// Commits this action against the provided table and returns the resulting updates.
    /// NOTE: This function is intended for internal use only and should not be called directly by users.
    ///
    /// # Arguments
    ///
    /// * `state` - State retained for this action across commit retries.
    /// * `table` - The current state of the table this action should apply to.
    ///
    /// # Returns
    ///
    /// An `ActionCommit` containing table updates and table requirements,
    /// or an error if the commit fails.
    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit>;

    /// Clean producer-owned artifacts after the transaction reaches a terminal outcome.
    async fn finish_commit(
        &self,
        _state: &mut Self::State,
        _table: &Table,
        _commit_error: Option<&Error>,
    ) {
    }
}

/// Object-safe adapter for storing heterogeneous transaction actions.
#[async_trait]
pub(crate) trait DynTransactionAction: AsAny + Sync + Send {
    async fn commit(
        &self,
        state: &mut dyn DynTransactionActionState,
        table: &Table,
    ) -> Result<ActionCommit>;

    async fn finish_commit(
        &self,
        state: &mut dyn DynTransactionActionState,
        table: &Table,
        commit_error: Option<&Error>,
    );

    fn new_state(&self) -> Box<dyn DynTransactionActionState>;
}

#[async_trait]
impl<T: TransactionAction> DynTransactionAction for T {
    async fn commit(
        &self,
        state: &mut dyn DynTransactionActionState,
        table: &Table,
    ) -> Result<ActionCommit> {
        let state = state
            .as_any_mut()
            .downcast_mut::<T::State>()
            .expect("an action entry must contain its action's associated state type");
        TransactionAction::commit(self, state, table).await
    }

    async fn finish_commit(
        &self,
        state: &mut dyn DynTransactionActionState,
        table: &Table,
        commit_error: Option<&Error>,
    ) {
        let state = state
            .as_any_mut()
            .downcast_mut::<T::State>()
            .expect("an action entry must contain its action's associated state type");
        TransactionAction::finish_commit(self, state, table, commit_error).await;
    }

    fn new_state(&self) -> Box<dyn DynTransactionActionState> {
        Box::new(TransactionAction::new_state(self))
    }
}

/// A helper trait for applying a `TransactionAction` to a `Transaction`.
///
/// This is implemented for all `TransactionAction` types
/// to allow easy chaining of actions into a transaction context.
pub trait ApplyTransactionAction {
    /// Adds this action to the given transaction.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to apply the action to.
    ///
    /// # Returns
    ///
    /// The modified transaction containing this action, or an error if the operation fails.
    fn apply(self, tx: Transaction) -> Result<Transaction>;
}

impl<T: TransactionAction + 'static> ApplyTransactionAction for T {
    fn apply(self, mut tx: Transaction) -> Result<Transaction>
    where Self: Sized {
        tx.actions.push(TransactionActionEntry::new(self));
        Ok(tx)
    }
}

/// The result of committing a `TransactionAction`.
///
/// This struct contains the updates to apply to the table's metadata
/// and any preconditions that must be satisfied before the update can be committed.
pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl ActionCommit {
    /// Creates a new `ActionCommit` from the given updates and requirements.
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self {
            updates,
            requirements,
        }
    }

    /// Consumes and returns the list of table updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }

    /// Consumes and returns the list of table requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use as_any::Downcast;
    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ActionCommit, ApplyTransactionAction, TransactionAction};
    use crate::transaction::tests::make_v2_table;
    use crate::{Result, TableRequirement, TableUpdate};

    struct TestAction;

    #[derive(Default)]
    struct TestActionState {
        commit_count: u32,
    }

    #[async_trait]
    impl TransactionAction for TestAction {
        type State = TestActionState;

        fn new_state(&self) -> Self::State {
            TestActionState { commit_count: 7 }
        }

        async fn commit(&self, state: &mut Self::State, _table: &Table) -> Result<ActionCommit> {
            state.commit_count += 1;
            Ok(ActionCommit::new(
                vec![TableUpdate::SetLocation {
                    location: String::from("s3://bucket/prefix/table/"),
                }],
                vec![TableRequirement::UuidMatch {
                    uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1")?,
                }],
            ))
        }
    }

    #[tokio::test]
    async fn test_commit_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;
        let mut state = TestActionState::default();

        let mut action_commit = action.commit(&mut state, &table).await.unwrap();

        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert_eq!(updates[0], TableUpdate::SetLocation {
            location: String::from("s3://bucket/prefix/table/")
        });
        assert_eq!(requirements[0], TableRequirement::UuidMatch {
            uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap()
        });
        assert_eq!(state.commit_count, 1);
    }

    #[test]
    fn test_apply_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;
        let tx = Transaction::new(&table);

        let mut updated_tx = action.apply(tx).unwrap();
        // There should be one action in the transaction now
        assert_eq!(updated_tx.actions.len(), 1);

        (*updated_tx.actions[0].action)
            .downcast_ref::<TestAction>()
            .expect("TestAction was not applied to Transaction!");

        assert_eq!(
            updated_tx.actions[0]
                .retry_state
                .as_any_mut()
                .downcast_mut::<TestActionState>()
                .unwrap()
                .commit_count,
            7
        );
        updated_tx.actions[0]
            .retry_state
            .as_any_mut()
            .downcast_mut::<TestActionState>()
            .unwrap()
            .commit_count = 2;
        let mut cloned_tx = updated_tx.clone();
        assert_eq!(
            cloned_tx.actions[0]
                .retry_state
                .as_any_mut()
                .downcast_mut::<TestActionState>()
                .unwrap()
                .commit_count,
            7
        );
    }

    #[test]
    fn test_action_commit() {
        // Create dummy updates and requirements
        let location = String::from("s3://bucket/prefix/table/");
        let uuid = Uuid::new_v4();
        let updates = vec![TableUpdate::SetLocation { location }];
        let requirements = vec![TableRequirement::UuidMatch { uuid }];

        let mut action_commit = ActionCommit::new(updates.clone(), requirements.clone());

        let taken_updates = action_commit.take_updates();
        let taken_requirements = action_commit.take_requirements();

        // Check values are returned correctly
        assert_eq!(taken_updates, updates);
        assert_eq!(taken_requirements, requirements);

        assert!(action_commit.take_updates().is_empty());
        assert!(action_commit.take_requirements().is_empty());
    }
}
