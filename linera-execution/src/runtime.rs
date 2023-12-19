// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::ExecutionStateView,
    resources::{RuntimeCounts, RuntimeLimits},
    runtime_actor::{
        ContractActorRuntime, ContractRequest, RuntimeActor, ServiceActorRuntime, ServiceRequest,
    },
    CallResult, ExecutionError, ExecutionResult, ExecutionRuntimeContext, SessionId,
    UserApplicationDescription, UserApplicationId, UserContractCode, UserServiceCode,
};
use async_lock::{Mutex, MutexGuard, MutexGuardArc};
use custom_debug_derive::Debug;
use linera_base::{
    data_types::{ArithmeticError, Timestamp},
    ensure, hex_debug,
    identifiers::{ChainId, Owner},
};
use linera_views::{
    batch::Batch,
    common::Context,
    key_value_store_view::KeyValueStoreView,
    reentrant_collection_view,
    register_view::RegisterView,
    views::{View, ViewError},
};
use std::{
    collections::{btree_map, BTreeMap},
    ops::DerefMut,
    sync::{
        atomic::{AtomicI32, AtomicU64, Ordering},
        Arc,
    },
};

/// Runtime data tracked during the execution of a transaction.
#[derive(Debug)]
pub(crate) struct ExecutionRuntime<'a, C, const WRITABLE: bool> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The current stack of application descriptions.
    applications: Mutex<&'a mut Vec<ApplicationStatus>>,
    /// The storage view on the execution state.
    execution_state: Mutex<&'a mut ExecutionStateView<C>>,
    /// All the sessions and their IDs.
    session_manager: Mutex<&'a mut SessionManager>,
    /// Track active (i.e. locked) applications for which re-entrancy is disallowed.
    active_simple_user_states: Mutex<ActiveSimpleUserStates<C>>,
    /// Track active (i.e. locked) applications for which re-entrancy is disallowed.
    active_view_user_states: Mutex<ActiveViewUserStates<C>>,
    /// Track active (i.e. locked) sessions for which re-entrancy is disallowed.
    active_sessions: Mutex<ActiveSessions>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    execution_results: Mutex<&'a mut Vec<ExecutionResult>>,

    /// The amount of fuel available for executing the application.
    remaining_fuel: AtomicU64,
    /// The number of reads
    num_reads: AtomicU64,
    /// the total size being read
    bytes_read: AtomicU64,
    /// The total size being written
    bytes_written: AtomicU64,
    /// The total size being written
    stored_size_delta: AtomicI32,
    /// The runtime limits
    runtime_limits: RuntimeLimits,
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
    BTreeMap<UserApplicationId, reentrant_collection_view::WriteGuardedView<KeyValueStoreView<C>>>;
type ActiveSimpleUserStates<C> = BTreeMap<
    UserApplicationId,
    reentrant_collection_view::WriteGuardedView<RegisterView<C, Vec<u8>>>,
>;
type ActiveSessions = BTreeMap<SessionId, MutexGuardArc<SessionState>>;

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        chain_id: ChainId,
        applications: &'a mut Vec<ApplicationStatus>,
        execution_state: &'a mut ExecutionStateView<C>,
        session_manager: &'a mut SessionManager,
        execution_results: &'a mut Vec<ExecutionResult>,
        fuel: u64,
        runtime_limits: RuntimeLimits,
    ) -> Self {
        assert_eq!(chain_id, execution_state.context().extra().chain_id());
        Self {
            applications: Mutex::new(applications),
            execution_state: Mutex::new(execution_state),
            session_manager: Mutex::new(session_manager),
            active_simple_user_states: Mutex::default(),
            active_view_user_states: Mutex::default(),
            active_sessions: Mutex::default(),
            execution_results: Mutex::new(execution_results),
            remaining_fuel: AtomicU64::new(fuel),
            num_reads: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            stored_size_delta: AtomicI32::new(0),
            runtime_limits,
            chain_id,
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

    async fn active_view_user_states_mut(&self) -> MutexGuard<'_, ActiveViewUserStates<C>> {
        self.active_view_user_states.lock().await
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

    async fn load_contract(
        &self,
        id: UserApplicationId,
    ) -> Result<(UserContractCode, UserApplicationDescription), ExecutionError> {
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
            .get_user_contract(&description)
            .await?;
        Ok((code, description))
    }

    async fn load_service(
        &self,
        id: UserApplicationId,
    ) -> Result<(UserServiceCode, UserApplicationDescription), ExecutionError> {
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
            .get_user_service(&description)
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
                .try_lock_arc()
                .ok_or(ExecutionError::SessionIsInUse)?;
            // Verify ownership.
            ensure!(state.owner == from_id, ExecutionError::InvalidSessionOwner);
            // Transfer the session.
            state.owner = to_id;
        }
        Ok(())
    }

    fn make_sessions(
        &self,
        new_sessions: Vec<Vec<u8>>,
        creator_id: UserApplicationId,
        receiver_id: UserApplicationId,
    ) -> Vec<SessionId> {
        let mut manager = self.session_manager_mut();
        let manager = manager.deref_mut();
        let states = &mut manager.states;
        let counter = manager.counters.entry(creator_id).or_default();
        let mut session_ids = Vec::new();
        for data in new_sessions {
            let id = SessionId {
                application_id: creator_id,
                index: *counter,
            };
            *counter += 1;
            session_ids.push(id);
            let state = SessionState {
                owner: receiver_id,
                data,
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
            .try_lock_arc()
            .ok_or(ExecutionError::SessionIsInUse)?;
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

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W> {
    fn increment_num_reads(&self) -> Result<(), ExecutionError> {
        self.num_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = self.num_reads.load(Ordering::Acquire);
        if bytes >= self.runtime_limits.max_budget_num_reads {
            return Err(ExecutionError::ArithmeticError(ArithmeticError::Overflow));
        }
        Ok(())
    }

    fn increment_bytes_read(&self, increment: u64) -> Result<(), ExecutionError> {
        if increment >= u64::MAX / 2 {
            return Err(ExecutionError::ExcessiveRead);
        }
        self.bytes_read.fetch_add(increment, Ordering::Relaxed);
        let bytes = self.bytes_read.load(Ordering::Acquire);
        if bytes >= self.runtime_limits.max_budget_bytes_read {
            return Err(ExecutionError::ArithmeticError(ArithmeticError::Overflow));
        }
        if bytes >= self.runtime_limits.maximum_bytes_left_to_read {
            return Err(ExecutionError::ExcessiveRead);
        }
        Ok(())
    }

    fn increment_bytes_written(&self, increment: u64) -> Result<(), ExecutionError> {
        if increment >= u64::MAX / 2 {
            return Err(ExecutionError::ExcessiveWrite);
        }
        self.bytes_written.fetch_add(increment, Ordering::Relaxed);
        let bytes = self.bytes_written.load(Ordering::Acquire);
        if bytes >= self.runtime_limits.max_budget_bytes_written {
            return Err(ExecutionError::ArithmeticError(ArithmeticError::Overflow));
        }
        if bytes >= self.runtime_limits.maximum_bytes_left_to_write {
            return Err(ExecutionError::ExcessiveWrite);
        }
        Ok(())
    }
}

impl<'a, C, const W: bool> ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub(crate) fn application_id(&self) -> UserApplicationId {
        self.applications_mut()
            .last()
            .expect("at least one application description should be present in the stack")
            .id
    }

    pub(crate) fn application_parameters(&self) -> Vec<u8> {
        self.applications_mut()
            .last()
            .expect("at least one application description should be present in the stack")
            .parameters
            .clone()
    }

    pub(crate) fn read_system_balance(&self) -> linera_base::data_types::Amount {
        *self.execution_state_mut().system.balance.get()
    }

    pub(crate) fn read_system_timestamp(&self) -> Timestamp {
        *self.execution_state_mut().system.timestamp.get()
    }

    pub(crate) async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        let state = self
            .execution_state_mut()
            .simple_users
            .try_load_entry_mut(&self.application_id())
            .await?
            .get()
            .to_vec();
        Ok(state)
    }

    pub(crate) async fn lock_view_user_state(&self) -> Result<(), ExecutionError> {
        let view = self
            .execution_state_mut()
            .view_users
            .try_load_entry_mut(&self.application_id())
            .await?;
        self.active_view_user_states_mut()
            .await
            .insert(self.application_id(), view);
        Ok(())
    }

    pub(crate) async fn unlock_view_user_state(&self) -> Result<(), ExecutionError> {
        // Make the view available again.
        match self
            .active_view_user_states_mut()
            .await
            .remove(&self.application_id())
        {
            Some(_) => Ok(()),
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    pub(crate) async fn contains_key(&self, key: Vec<u8>) -> Result<bool, ExecutionError> {
        let state = self.active_view_user_states_mut().await;
        let view = state
            .get(&self.application_id())
            .ok_or_else(|| ExecutionError::ApplicationStateNotLocked)?;
        let result = view.contains_key(&key).await?;
        self.increment_num_reads()?;
        Ok(result)
    }

    pub(crate) async fn read_value_bytes(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let state = self.active_view_user_states_mut().await;
        let view = state
            .get(&self.application_id())
            .ok_or_else(|| ExecutionError::ApplicationStateNotLocked)?;
        let result = view.get(&key).await?;
        self.increment_num_reads()?;
        if let Some(value) = &result {
            self.increment_bytes_read(value.len() as u64)?;
        }
        Ok(result)
    }

    pub(crate) async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let state = self.active_view_user_states_mut().await;
        let view = state
            .get(&self.application_id())
            .ok_or_else(|| ExecutionError::ApplicationStateNotLocked)?;
        let results = view.multi_get(keys).await?;
        self.increment_num_reads()?;
        for value in results.iter().flatten() {
            self.increment_bytes_read(value.len() as u64)?;
        }
        Ok(results)
    }

    pub(crate) async fn find_keys_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        // Read keys matching a prefix. We have to collect since iterators do not pass the wit barrier
        let state = self.active_view_user_states_mut().await;
        let view = state
            .get(&self.application_id())
            .ok_or_else(|| ExecutionError::ApplicationStateNotLocked)?;
        let keys = view.find_keys_by_prefix(&key_prefix).await?;
        self.increment_num_reads()?;
        let mut read_size = 0;
        for key in &keys {
            read_size += key.len();
        }
        self.increment_bytes_read(read_size as u64)?;
        Ok(keys)
    }

    pub(crate) async fn find_key_values_by_prefix(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        // Read key/values matching a prefix. We have to collect since iterators do not pass the wit barrier
        let state = self.active_view_user_states_mut().await;
        let view = state
            .get(&self.application_id())
            .ok_or_else(|| ExecutionError::ApplicationStateNotLocked)?;
        let key_values = view.find_key_values_by_prefix(&key_prefix).await?;
        self.increment_num_reads()?;
        let mut read_size = 0;
        for (key, value) in &key_values {
            read_size += key.len() + value.len();
        }
        self.increment_bytes_read(read_size as u64)?;
        Ok(key_values)
    }
}

impl<'a, C> ExecutionRuntime<'a, C, false>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) fn service_runtime_actor(
        &self,
    ) -> (RuntimeActor<&Self, ServiceRequest>, ServiceActorRuntime) {
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        let actor = RuntimeActor::new(self, receiver);
        let sender = ServiceActorRuntime::new(sender);
        (actor, sender)
    }

    /// Note that queries are not available from writable contexts.
    pub(crate) async fn try_query_application(
        &self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        // Load the application.
        let (code, description) = self.load_service(queried_id).await?;
        // Make the call to user code.
        let query_context = crate::QueryContext {
            chain_id: self.chain_id,
        };
        self.applications_mut().push(ApplicationStatus {
            id: queried_id,
            parameters: description.parameters,
            signer: None,
        });

        let (runtime_actor, runtime_sender) = self.service_runtime_actor();
        let mut code = code.instantiate_with_actor_runtime(runtime_sender)?;
        let value_future =
            tokio::task::spawn_blocking(move || code.handle_query(query_context, argument));
        runtime_actor.run().await?;
        let value = value_future.await??;
        self.applications_mut().pop();
        Ok(value)
    }
}

impl<'a, C> ExecutionRuntime<'a, C, true>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) fn contract_runtime_actor(
        &self,
    ) -> (
        RuntimeActor<async_lock::RwLock<&Self>, ContractRequest>,
        ContractActorRuntime,
    ) {
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        let actor = RuntimeActor::new(async_lock::RwLock::new(self), receiver);
        let sender = ContractActorRuntime::new(sender);
        (actor, sender)
    }

    pub(crate) fn remaining_fuel(&self) -> u64 {
        self.remaining_fuel.load(Ordering::Acquire)
    }

    pub(crate) fn runtime_counts(&self) -> RuntimeCounts {
        let remaining_fuel = self.remaining_fuel.load(Ordering::Acquire);
        let num_reads = self.num_reads.load(Ordering::Acquire);
        let bytes_read = self.bytes_read.load(Ordering::Acquire);
        let bytes_written = self.bytes_written.load(Ordering::Acquire);
        let stored_size_delta = self.stored_size_delta.load(Ordering::Acquire);
        RuntimeCounts {
            remaining_fuel,
            num_reads,
            bytes_read,
            bytes_written,
            stored_size_delta,
        }
    }

    pub(crate) fn set_remaining_fuel(&self, remaining_fuel: u64) {
        self.remaining_fuel.store(remaining_fuel, Ordering::Release);
    }

    pub(crate) async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
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

    pub(crate) fn save_and_unlock_my_state(&self, state: Vec<u8>) -> Result<(), ExecutionError> {
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

    pub(crate) fn unlock_my_state(&self) {
        self.active_simple_user_states_mut()
            .remove(&self.application_id());
    }

    pub(crate) async fn write_batch_and_unlock(&self, batch: Batch) -> Result<(), ExecutionError> {
        // Update the write costs
        let size = batch.size() as u64;
        self.increment_bytes_written(size)?;
        // Write the batch and make the view available again.
        match self
            .active_view_user_states_mut()
            .await
            .remove(&self.application_id())
        {
            Some(mut view) => {
                let stored_size = view.total_size().sum_i32()?;
                view.write_batch(batch).await?;
                let new_stored_size = view.total_size().sum_i32()?;
                let increment = new_stored_size - stored_size;
                self.stored_size_delta
                    .fetch_add(increment, Ordering::Relaxed);
                Ok(())
            }
            None => Err(ExecutionError::ApplicationStateNotLocked),
        }
    }

    pub(crate) async fn try_call_application(
        &self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let caller = self
            .applications_mut()
            .last()
            .expect("caller must exist")
            .clone();
        // Load the application.
        let (code, description) = self.load_contract(callee_id).await?;
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
        let (runtime_actor, runtime_sender) = self.contract_runtime_actor();
        let mut code = code.instantiate_with_actor_runtime(runtime_sender)?;
        let raw_result_future = tokio::task::spawn_blocking(move || {
            code.handle_application_call(callee_context, argument, forwarded_sessions)
        });
        runtime_actor.run().await?;
        let raw_result = raw_result_future.await??;
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

    pub(crate) async fn try_call_session(
        &self,
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let callee_id = session_id.application_id;
        let caller = self
            .applications_mut()
            .last()
            .expect("caller must exist")
            .clone();
        // Load the application.
        let (code, description) = self.load_contract(callee_id).await?;
        // Change the owners of forwarded sessions.
        self.forward_sessions(&forwarded_sessions, caller.id, callee_id)?;
        // Load the session.
        let session_state = self.try_load_session(session_id, self.application_id())?;
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
        let (runtime_actor, runtime_sender) = self.contract_runtime_actor();
        let mut code = code.instantiate_with_actor_runtime(runtime_sender)?;
        let raw_result_future = tokio::task::spawn_blocking(move || {
            code.handle_session_call(callee_context, session_state, argument, forwarded_sessions)
        });
        runtime_actor.run().await?;
        let (raw_result, session_state) = raw_result_future.await??;
        self.applications_mut().pop();

        // Interpret the results of the call.
        if raw_result.close_session {
            // Terminate the session.
            self.try_close_session(session_id, self.application_id())?;
        } else {
            // Save the session.
            self.try_save_session(session_id, self.application_id(), session_state)?;
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
