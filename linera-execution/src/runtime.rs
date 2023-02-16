// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::ExecutionStateView, CallResult, ExecutionError, ExecutionResult,
    ExecutionRuntimeContext, NewSession, QueryableStorage, ReadableStorage, SessionId,
    UserApplicationCode, UserApplicationDescription, UserApplicationId, WritableStorage,
};
use async_trait::async_trait;
use custom_debug_derive::Debug;
use linera_base::{
    data_types::{ChainId, Owner, Timestamp},
    ensure, hex_debug,
};
use linera_views::{
    common::{Batch, Context},
    key_value_store_view::KeyValueStoreView,
    register_view::RegisterView,
    views::{View, ViewError},
};
use std::{
    collections::{btree_map, BTreeMap},
    ops::DerefMut,
    sync::Arc,
};
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard, OwnedRwLockWriteGuard};

/// Runtime data tracked during the execution of a transaction.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionRuntime<'a, C, const WRITABLE: bool> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The current stack of application descriptions.
    applications: Arc<Mutex<&'a mut Vec<ApplicationStatus>>>,
    /// The storage view on the execution state.
    execution_state: Arc<Mutex<&'a mut ExecutionStateView<C>>>,
    /// All the sessions and their IDs.
    session_manager: Arc<Mutex<&'a mut SessionManager>>,
    /// Track active (i.e. locked) applications for which re-entrancy is disallowed.
    active_simple_user_states: Arc<Mutex<ActiveSimpleUserStates<C>>>,
    /// Track active (i.e. locked) applications for which re-entrancy is disallowed.
    active_view_user_states: Arc<Mutex<ActiveViewUserStates<C>>>,
    /// Track active (i.e. locked) sessions for which re-entrancy is disallowed.
    active_sessions: Arc<Mutex<ActiveSessions>>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    execution_results: Arc<Mutex<&'a mut Vec<ExecutionResult>>>,
}

/// The runtime status of an application.
#[derive(Debug, Clone)]
pub(crate) struct ApplicationStatus {
    /// The application id.
    pub(crate) id: UserApplicationId,
    /// The parameters from the application description.
    pub(crate) parameters: Vec<u8>,
    /// The authenticated signer for the execution thread, if any.
    pub(crate) signer: Option<Owner>,
}

type ActiveViewUserStates<C> =
    BTreeMap<UserApplicationId, OwnedRwLockWriteGuard<KeyValueStoreView<C>>>;
type ActiveSimpleUserStates<C> =
    BTreeMap<UserApplicationId, OwnedRwLockWriteGuard<RegisterView<C, Vec<u8>>>>;
type ActiveSessions = BTreeMap<SessionId, OwnedMutexGuard<SessionState>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionManager {
    /// Track the next session index to be used for each application.
    pub(crate) counters: BTreeMap<UserApplicationId, u64>,
    /// Track the current state (owner and data) of each session.
    pub(crate) states: BTreeMap<SessionId, Arc<Mutex<SessionState>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionState {
    /// Track which application can call into the session.
    owner: UserApplicationId,
    /// The internal state of the session.
    #[debug(with = "hex_debug")]
    data: Vec<u8>,
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) fn new(
        chain_id: ChainId,
        applications: &'a mut Vec<ApplicationStatus>,
        execution_state: &'a mut ExecutionStateView<C>,
        session_manager: &'a mut SessionManager,
        execution_results: &'a mut Vec<ExecutionResult>,
    ) -> Self {
        assert_eq!(chain_id, execution_state.context().extra().chain_id());
        Self {
            chain_id,
            applications: Arc::new(Mutex::new(applications)),
            execution_state: Arc::new(Mutex::new(execution_state)),
            session_manager: Arc::new(Mutex::new(session_manager)),
            active_simple_user_states: Arc::default(),
            active_view_user_states: Arc::default(),
            active_sessions: Arc::default(),
            execution_results: Arc::new(Mutex::new(execution_results)),
        }
    }

    fn applications_mut(&self) -> MutexGuard<'_, &'a mut Vec<ApplicationStatus>> {
        self.applications
            .try_lock()
            .expect("single-threaded execution should not lock `application_ids`")
    }

    fn execution_state_mut(&self) -> MutexGuard<'_, &'a mut ExecutionStateView<C>> {
        self.execution_state
            .try_lock()
            .expect("single-threaded execution should not lock `execution_state`")
    }

    fn session_manager_mut(&self) -> MutexGuard<'_, &'a mut SessionManager> {
        self.session_manager
            .try_lock()
            .expect("single-threaded execution should not lock `session_manager`")
    }

    fn active_simple_user_states_mut(&self) -> MutexGuard<'_, ActiveSimpleUserStates<C>> {
        self.active_simple_user_states
            .try_lock()
            .expect("single-threaded execution should not lock `active_simple_user_states`")
    }

    fn active_view_user_states_mut(&self) -> MutexGuard<'_, ActiveViewUserStates<C>> {
        self.active_view_user_states
            .try_lock()
            .expect("single-threaded execution should not lock `active_view_user_states`")
    }

    fn active_sessions_mut(&self) -> MutexGuard<'_, ActiveSessions> {
        self.active_sessions
            .try_lock()
            .expect("single-threaded execution should not lock `active_sessions`")
    }

    fn execution_results_mut(&self) -> MutexGuard<'_, &'a mut Vec<ExecutionResult>> {
        self.execution_results
            .try_lock()
            .expect("single-threaded execution should not lock `execution_results`")
    }

    async fn load_application(
        &self,
        id: UserApplicationId,
    ) -> Result<(UserApplicationCode, UserApplicationDescription), ExecutionError> {
        let description = self
            .execution_state_mut()
            .system
            .registry
            .describe_application(id)
            .await?;
        let code = self
            .execution_state_mut()
            .context()
            .extra()
            .get_user_application(&description)
            .await?;
        Ok((code, description))
    }

    fn forward_sessions(
        &self,
        session_ids: &[SessionId],
        from_id: UserApplicationId,
        to_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        let states = &self.session_manager_mut().states;
        for id in session_ids {
            let mut state = states
                .get(id)
                .ok_or(ExecutionError::InvalidSession)?
                .clone()
                .try_lock_owned()
                .map_err(|_| ExecutionError::SessionIsInUse)?;
            // Verify ownership.
            ensure!(state.owner == from_id, ExecutionError::InvalidSessionOwner);
            // Transfer the session.
            state.owner = to_id;
        }
        Ok(())
    }

    fn make_sessions(
        &self,
        new_sessions: Vec<NewSession>,
        creator_id: UserApplicationId,
        receiver_id: UserApplicationId,
    ) -> Vec<SessionId> {
        let mut manager = self.session_manager_mut();
        let manager = manager.deref_mut();
        let states = &mut manager.states;
        let counter = manager.counters.entry(creator_id).or_default();
        let mut session_ids = Vec::new();
        for session in new_sessions {
            let id = SessionId {
                application_id: creator_id,
                kind: session.kind,
                index: *counter,
            };
            *counter += 1;
            session_ids.push(id);
            let state = SessionState {
                owner: receiver_id,
                data: session.data,
            };
            states.insert(id, Arc::new(Mutex::new(state)));
        }
        session_ids
    }

    fn try_load_session(
        &self,
        session_id: SessionId,
        application_id: UserApplicationId,
    ) -> Result<Vec<u8>, ExecutionError> {
        let guard = self
            .session_manager_mut()
            .states
            .get(&session_id)
            .ok_or(ExecutionError::InvalidSession)?
            .clone()
            .try_lock_owned()
            .map_err(|_| ExecutionError::SessionIsInUse)?;
        let state = guard.data.clone();
        // Verify ownership.
        ensure!(
            guard.owner == application_id,
            ExecutionError::InvalidSessionOwner
        );
        // Remember the guard. This will prevent reentrancy.
        self.active_sessions_mut().insert(session_id, guard);
        Ok(state)
    }

    fn try_save_session(
        &self,
        session_id: SessionId,
        application_id: UserApplicationId,
        state: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        // Remove the guard.
        if let btree_map::Entry::Occupied(mut guard) = self.active_sessions_mut().entry(session_id)
        {
            // Verify ownership.
            ensure!(
                guard.get().owner == application_id,
                ExecutionError::InvalidSessionOwner
            );
            // Save the state and unlock the session for future calls.
            guard.get_mut().data = state;
            // Remove the entry from the set of active sessions.
            guard.remove();
        }
        Ok(())
    }

    fn try_close_session(
        &self,
        session_id: SessionId,
        application_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        if let btree_map::Entry::Occupied(guard) = self.active_sessions_mut().entry(session_id) {
            // Verify ownership.
            ensure!(
                guard.get().owner == application_id,
                ExecutionError::InvalidSessionOwner
            );
            // Remove the entry from the set of active sessions.
            guard.remove();
            // Delete the session entirely.
            self.session_manager_mut()
                .states
                .remove(&session_id)
                .ok_or(ExecutionError::InvalidSession)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, C, const W: bool> ReadableStorage for ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn application_id(&self) -> UserApplicationId {
        self.applications_mut()
            .last()
            .expect("at least one application description should be present in the stack")
            .id
    }

    fn application_parameters(&self) -> Vec<u8> {
        self.applications_mut()
            .last()
            .expect("at least one application description should be present in the stack")
            .parameters
            .clone()
    }

    fn read_system_balance(&self) -> crate::system::Balance {
        *self.execution_state_mut().system.balance.get()
    }

    fn read_system_timestamp(&self) -> Timestamp {
        *self.execution_state_mut().system.timestamp.get()
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        let state = self
            .execution_state_mut()
            .simple_users
            .try_load_entry_mut(&self.application_id())
            .await?
            .get()
            .to_vec();
        Ok(state)
    }

    async fn lock_view_user_state(&self) -> Result<(), ExecutionError> {
        let view = self
            .execution_state_mut()
            .view_users
            .try_load_entry_mut(&self.application_id())
            .await?;
        self.active_view_user_states_mut()
            .insert(self.application_id(), view);
        Ok(())
    }

    async fn unlock_view_user_state(&self) -> Result<(), ExecutionError> {
        // Make the view available again.
        match self
            .active_view_user_states_mut()
            .remove(&self.application_id())
        {
            Some(_) => Ok(()),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    async fn read_key_bytes(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError> {
        // read a key from the KV store
        match self
            .active_view_user_states_mut()
            .get(&self.application_id())
        {
            Some(view) => Ok(view.get(&key).await?),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        // Read keys matching a prefix. We have to collect since iterators do not pass the wit barrier
        match self
            .active_view_user_states_mut()
            .get(&self.application_id())
        {
            Some(view) => Ok(view.find_keys_by_prefix(&key_prefix).await?),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        // Read key/values matching a prefix. We have to collect since iterators do not pass the wit barrier
        match self
            .active_view_user_states_mut()
            .get(&self.application_id())
        {
            Some(view) => Ok(view.find_key_values_by_prefix(&key_prefix).await?),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }
}

#[async_trait]
impl<'a, C> QueryableStorage for ExecutionRuntime<'a, C, false>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    /// Note that queries are not available from writable contexts.
    async fn try_query_application(
        &self,
        queried_id: UserApplicationId,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        // Load the application.
        let (code, description) = self.load_application(queried_id).await?;
        // Make the call to user code.
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        self.applications_mut().push(ApplicationStatus {
            id: queried_id,
            parameters: description.parameters,
            signer: None,
        });
        let value = code
            .query_application(&query_context, self, argument)
            .await?;
        self.applications_mut().pop();
        Ok(value)
    }
}

#[async_trait]
impl<'a, C> WritableStorage for ExecutionRuntime<'a, C, true>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    fn remaining_fuel(&self) -> u64 {
        10_000_000
    }

    fn set_remaining_fuel(&self, remaining_fuel: u64) {}

    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        let view = self
            .execution_state_mut()
            .simple_users
            .try_load_entry_mut(&self.application_id())
            .await?;
        let state = view.get().to_vec();
        // Remember the view. This will prevent reentrancy.
        self.active_simple_user_states_mut()
            .insert(self.application_id(), view);
        Ok(state)
    }

    fn save_and_unlock_my_state(&self, state: Vec<u8>) -> Result<(), ExecutionError> {
        // Make the view available again.
        match self
            .active_simple_user_states_mut()
            .remove(&self.application_id())
        {
            Some(mut view) => {
                // Set the state.
                view.set(state);
                Ok(())
            }
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    fn unlock_my_state(&self) {
        self.active_simple_user_states_mut()
            .remove(&self.application_id());
    }

    async fn write_batch_and_unlock(&self, batch: Batch) -> Result<(), ExecutionError> {
        // Write the batch and make the view available again.
        match self
            .active_view_user_states_mut()
            .remove(&self.application_id())
        {
            Some(mut view) => Ok(view.write_batch(batch).await?),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let caller = self
            .applications_mut()
            .last()
            .expect("caller must exist")
            .clone();
        // Load the application.
        let (code, description) = self.load_application(callee_id).await?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, caller.id, callee_id)?;
        // Make the call to user code.
        let authenticated_signer = match caller.signer {
            Some(signer) if authenticated => Some(signer),
            _ => None,
        };
        let authenticated_caller_id = authenticated.then_some(caller.id);
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_signer,
            authenticated_caller_id,
        };
        self.applications_mut().push(ApplicationStatus {
            id: callee_id,
            parameters: description.parameters,
            // Allow further nested calls to be authenticated if this one is.
            signer: authenticated_signer,
        });
        let raw_result = code
            .call_application(&callee_context, self, argument, forwarded_sessions)
            .await?;
        self.applications_mut().pop();
        // Interpret the results of the call.
        self.execution_results_mut().push(ExecutionResult::User(
            callee_id,
            raw_result
                .execution_result
                .with_authenticated_signer(authenticated_signer),
        ));
        let sessions =
            self.make_sessions(raw_result.create_sessions, callee_id, self.application_id());
        let result = CallResult {
            value: raw_result.value,
            sessions,
        };
        Ok(result)
    }

    async fn try_call_session(
        &self,
        authenticated: bool,
        session_id: SessionId,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let callee_id = session_id.application_id;
        let caller = self
            .applications_mut()
            .last()
            .expect("caller must exist")
            .clone();
        // Load the application.
        let (code, description) = self.load_application(callee_id).await?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, caller.id, callee_id)?;
        // Load the session.
        let mut session_data = self.try_load_session(session_id, self.application_id())?;
        // Make the call to user code.
        let authenticated_signer = match caller.signer {
            Some(signer) if authenticated => Some(signer),
            _ => None,
        };
        let authenticated_caller_id = authenticated.then_some(caller.id);
        let callee_context = crate::CalleeContext {
            chain_id: self.chain_id,
            authenticated_signer,
            authenticated_caller_id,
        };
        self.applications_mut().push(ApplicationStatus {
            id: callee_id,
            parameters: description.parameters,
            // Allow further nested calls to be authenticated if this one is.
            signer: authenticated_signer,
        });
        let raw_result = code
            .call_session(
                &callee_context,
                self,
                session_id.kind,
                &mut session_data,
                argument,
                forwarded_sessions,
            )
            .await?;
        self.applications_mut().pop();
        // Interpret the results of the call.
        if raw_result.close_session {
            // Terminate the session.
            self.try_close_session(session_id, self.application_id())?;
        } else {
            // Save the session.
            self.try_save_session(session_id, self.application_id(), session_data)?;
        }
        let inner_result = raw_result.inner;
        self.execution_results_mut().push(ExecutionResult::User(
            callee_id,
            inner_result
                .execution_result
                .with_authenticated_signer(authenticated_signer),
        ));
        let sessions = self.make_sessions(
            inner_result.create_sessions,
            callee_id,
            self.application_id(),
        );
        let result = CallResult {
            value: inner_result.value,
            sessions,
        };
        Ok(result)
    }
}
