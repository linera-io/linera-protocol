// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::UserAction,
    execution_state_actor::{ExecutionStateSender, Request},
    resources::{RuntimeCounts, RuntimeLimits},
    runtime_actor::{ReceiverExt, UnboundedSenderExt},
    BaseRuntime, CallResult, ContractRuntime, ExecutionError, ExecutionResult, ServiceRuntime,
    SessionId, UserApplicationDescription, UserApplicationId, UserContractCode, UserServiceCode,
};
use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Amount, ArithmeticError, Timestamp},
    ensure,
    identifiers::{ChainId, Owner},
};
use linera_views::batch::Batch;
use oneshot::Receiver;
use std::{
    collections::BTreeMap,
    ops::DerefMut,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub struct SyncRuntime<const W: bool>(Arc<Mutex<SyncRuntimeInternal<W>>>);

pub type ContractSyncRuntime = SyncRuntime<true>;
pub type ServiceSyncRuntime = SyncRuntime<false>;

/// Runtime data tracked during the execution of a transaction on the synchronous thread.
#[derive(Debug)]
pub struct SyncRuntimeInternal<const WRITABLE: bool> {
    /// The current chain ID.
    chain_id: ChainId,

    /// How to interact with the storage view of the execution state.
    execution_state_sender: ExecutionStateSender,

    /// The current stack of application descriptions.
    applications: Vec<ApplicationStatus>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    execution_results: Vec<ExecutionResult>,

    /// All the sessions and their IDs.
    session_manager: SessionManager,
    /// Track application states (simple case).
    simple_user_states: BTreeMap<UserApplicationId, SimpleUserState>,
    /// Track application states (view case).
    view_user_states: BTreeMap<UserApplicationId, ViewUserState>,

    /// Counters to track fuel and storage consumption.
    runtime_counts: RuntimeCounts,
    /// The runtime limits.
    runtime_limits: RuntimeLimits,
}

/// The runtime status of an application.
#[derive(Debug, Clone)]
struct ApplicationStatus {
    /// The application id.
    id: UserApplicationId,
    /// The parameters from the application description.
    parameters: Vec<u8>,
    /// The authenticated signer for the execution thread, if any.
    signer: Option<Owner>,
}

#[derive(Debug, Default)]
struct SessionManager {
    /// Track the next session index to be used for each application.
    counters: BTreeMap<UserApplicationId, u64>,
    /// Track the current state (owner and data) of each session.
    states: BTreeMap<SessionId, SessionState>,
}

#[derive(Debug)]
enum Promise<T> {
    Ready(T),
    Pending(Receiver<T>),
}

impl<T> Promise<T> {
    fn force(&mut self) -> Result<(), ExecutionError> {
        if let Promise::Pending(receiver) = self {
            let value = receiver
                .recv_ref()
                .map_err(|oneshot::RecvError| ExecutionError::MissingRuntimeResponse)?;
            *self = Promise::Ready(value);
        }
        Ok(())
    }

    fn read(self) -> Result<T, ExecutionError> {
        match self {
            Promise::Pending(receiver) => {
                let value = receiver.recv_response()?;
                Ok(value)
            }
            Promise::Ready(value) => Ok(value),
        }
    }
}

#[derive(Debug, Default)]
struct SimpleUserState {
    /// Whether the application state was locked.
    locked: bool,
    /// A read query in progress on the internal state, if any.
    pending_query: Option<Receiver<Vec<u8>>>,
}

/// Manages a set of pending queries returning values of type `T`.
#[derive(Debug, Default)]
struct QueryManager<T> {
    /// The queries in progress.
    pending_queries: BTreeMap<u32, Promise<T>>,
    /// The number of queries ever registered so far. Used for the index of the next query.
    query_count: u32,
    /// The number of active queries.
    active_query_count: u32,
}

impl<T> QueryManager<T> {
    fn register(&mut self, receiver: Receiver<T>) -> Result<u32, ExecutionError> {
        let id = self.query_count;
        self.pending_queries.insert(id, Promise::Pending(receiver));
        self.query_count = self
            .query_count
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.active_query_count = self
            .active_query_count
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(id)
    }

    fn wait(&mut self, id: u32) -> Result<T, ExecutionError> {
        let promise = self
            .pending_queries
            .remove(&id)
            .ok_or(ExecutionError::PolledTwice)?;
        let value = promise.read()?;
        self.active_query_count -= 1;
        Ok(value)
    }

    fn force_all(&mut self) -> Result<(), ExecutionError> {
        for promise in self.pending_queries.values_mut() {
            promise.force()?;
        }
        Ok(())
    }
}

type Keys = Vec<Vec<u8>>;
type Value = Vec<u8>;
type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Default)]
struct ViewUserState {
    /// Whether the application state was locked.
    locked: bool,
    /// The contains-key queries in progress.
    contains_key_queries: QueryManager<bool>,
    /// The read-value queries in progress.
    read_value_queries: QueryManager<Option<Value>>,
    /// The read-multi-values queries in progress.
    read_multi_values_queries: QueryManager<Vec<Option<Value>>>,
    /// The find-keys queries in progress.
    find_keys_queries: QueryManager<Keys>,
    /// The find-key-values queries in progress.
    find_key_values_queries: QueryManager<KeyValues>,
}

impl ViewUserState {
    fn force_all_pending_queries(&mut self) -> Result<(), ExecutionError> {
        self.read_value_queries.force_all()?;
        self.find_keys_queries.force_all()?;
        self.find_key_values_queries.force_all()?;
        Ok(())
    }
}

#[derive(Debug)]
struct SessionState {
    /// Track which application can call into the session.
    owner: UserApplicationId,
    /// Whether the session is already active.
    locked: bool,
    /// Some data saved inside the session.
    data: Vec<u8>,
}

impl<const W: bool> SyncRuntimeInternal<W> {
    fn new(
        chain_id: ChainId,
        execution_state_sender: ExecutionStateSender,
        runtime_limits: RuntimeLimits,
        remaining_fuel: u64,
    ) -> Self {
        let runtime_counts = RuntimeCounts {
            remaining_fuel,
            ..Default::default()
        };
        Self {
            chain_id,
            execution_state_sender,
            applications: Vec::new(),
            execution_results: Vec::default(),
            session_manager: SessionManager::default(),
            simple_user_states: BTreeMap::default(),
            view_user_states: BTreeMap::default(),
            runtime_counts,
            runtime_limits,
        }
    }

    fn load_contract(
        &mut self,
        id: UserApplicationId,
    ) -> Result<(UserContractCode, UserApplicationDescription), ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::LoadContract { id, callback })?
            .recv_response()
    }

    fn load_service(
        &mut self,
        id: UserApplicationId,
    ) -> Result<(UserServiceCode, UserApplicationDescription), ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::LoadService { id, callback })?
            .recv_response()
    }

    fn forward_sessions(
        &mut self,
        session_ids: &[SessionId],
        from_id: UserApplicationId,
        to_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        let states = &mut self.session_manager.states;
        for id in session_ids {
            let state = states
                .get_mut(id)
                .ok_or(ExecutionError::InvalidSession(*id))?;
            // Verify ownership.
            ensure!(
                state.owner == from_id,
                ExecutionError::invalid_session_owner(*id, from_id, state.owner,)
            );
            // Transfer the session.
            state.owner = to_id;
        }
        Ok(())
    }

    fn make_sessions(
        &mut self,
        new_sessions: Vec<Vec<u8>>,
        creator_id: UserApplicationId,
        receiver_id: UserApplicationId,
    ) -> Vec<SessionId> {
        let manager = &mut self.session_manager;
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
                locked: false,
                data,
            };
            states.insert(id, state);
        }
        session_ids
    }

    fn try_load_session(
        &mut self,
        session_id: SessionId,
        application_id: UserApplicationId,
    ) -> Result<Vec<u8>, ExecutionError> {
        let state = self
            .session_manager
            .states
            .get_mut(&session_id)
            .ok_or(ExecutionError::InvalidSession(session_id))?;
        // Verify locking.
        ensure!(!state.locked, ExecutionError::SessionIsInUse(session_id));
        // Verify ownership.
        ensure!(
            state.owner == application_id,
            ExecutionError::invalid_session_owner(session_id, application_id, state.owner,)
        );
        // Lock state and return data.
        state.locked = true;
        Ok(state.data.clone())
    }

    fn try_save_session(
        &mut self,
        session_id: SessionId,
        application_id: UserApplicationId,
        data: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let state = self
            .session_manager
            .states
            .get_mut(&session_id)
            .ok_or(ExecutionError::InvalidSession(session_id))?;
        // Verify locking.
        ensure!(
            state.locked,
            ExecutionError::SessionStateNotLocked(session_id)
        );
        // Verify ownership.
        ensure!(
            state.owner == application_id,
            ExecutionError::invalid_session_owner(session_id, application_id, state.owner,)
        );
        // Save data.
        state.data = data;
        state.locked = false;
        Ok(())
    }

    fn try_close_session(
        &mut self,
        session_id: SessionId,
        application_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        let state = self
            .session_manager
            .states
            .get(&session_id)
            .ok_or(ExecutionError::InvalidSession(session_id))?;
        // Verify locking.
        ensure!(
            state.locked,
            ExecutionError::SessionStateNotLocked(session_id)
        );
        // Verify ownership.
        ensure!(
            state.owner == application_id,
            ExecutionError::invalid_session_owner(session_id, application_id, state.owner,)
        );
        // Delete the session entirely.
        self.session_manager
            .states
            .remove(&session_id)
            .ok_or(ExecutionError::InvalidSession(session_id))?;
        Ok(())
    }
}

impl<const W: bool> SyncRuntime<W> {
    fn new(runtime: SyncRuntimeInternal<W>) -> Self {
        SyncRuntime(Arc::new(Mutex::new(runtime)))
    }

    fn into_inner(self) -> Option<SyncRuntimeInternal<W>> {
        let runtime = Arc::into_inner(self.0)?
            .into_inner()
            .expect("thread should not panicked");
        Some(runtime)
    }

    fn as_inner(&mut self) -> std::sync::MutexGuard<'_, SyncRuntimeInternal<W>> {
        self.0
            .try_lock()
            .expect("Synchronous runtimes run on a single execution thread")
    }
}

impl<const W: bool> BaseRuntime for SyncRuntime<W> {
    type Read = <SyncRuntimeInternal<W> as BaseRuntime>::Read;
    type Lock = <SyncRuntimeInternal<W> as BaseRuntime>::Lock;
    type Unlock = <SyncRuntimeInternal<W> as BaseRuntime>::Unlock;
    type ReadValueBytes = <SyncRuntimeInternal<W> as BaseRuntime>::ReadValueBytes;
    type ContainsKey = <SyncRuntimeInternal<W> as BaseRuntime>::ContainsKey;
    type ReadMultiValuesBytes = <SyncRuntimeInternal<W> as BaseRuntime>::ReadMultiValuesBytes;
    type FindKeysByPrefix = <SyncRuntimeInternal<W> as BaseRuntime>::FindKeysByPrefix;
    type FindKeyValuesByPrefix = <SyncRuntimeInternal<W> as BaseRuntime>::FindKeyValuesByPrefix;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.as_inner().chain_id()
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        self.as_inner().application_id()
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.as_inner().application_parameters()
    }

    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.as_inner().read_system_balance()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.as_inner().read_system_timestamp()
    }

    fn try_read_my_state_new(&mut self) -> Result<Self::Read, ExecutionError> {
        self.as_inner().try_read_my_state_new()
    }

    fn try_read_my_state_wait(&mut self, promise: &Self::Read) -> Result<Vec<u8>, ExecutionError> {
        self.as_inner().try_read_my_state_wait(promise)
    }

    fn lock_new(&mut self) -> Result<Self::Lock, ExecutionError> {
        self.as_inner().lock_new()
    }

    fn lock_wait(&mut self, promise: &Self::Lock) -> Result<(), ExecutionError> {
        self.as_inner().lock_wait(promise)
    }

    fn unlock_new(&mut self) -> Result<Self::Unlock, ExecutionError> {
        self.as_inner().unlock_new()
    }

    fn unlock_wait(&mut self, promise: &Self::Unlock) -> Result<(), ExecutionError> {
        self.as_inner().unlock_wait(promise)
    }

    fn write_batch_and_unlock(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        self.as_inner().write_batch_and_unlock(batch)
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        self.as_inner().contains_key_new(key)
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        self.as_inner().contains_key_wait(promise)
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        self.as_inner().read_multi_values_bytes_new(keys)
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        self.as_inner().read_multi_values_bytes_wait(promise)
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        self.as_inner().read_value_bytes_new(key)
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.as_inner().read_value_bytes_wait(promise)
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        self.as_inner().find_keys_by_prefix_new(key_prefix)
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        self.as_inner().find_keys_by_prefix_wait(promise)
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
        self.as_inner().find_key_values_by_prefix_new(key_prefix)
    }

    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        self.as_inner().find_key_values_by_prefix_wait(promise)
    }
}

impl<const W: bool> BaseRuntime for SyncRuntimeInternal<W> {
    type Read = ();
    type Lock = ();
    type Unlock = ();
    type ReadValueBytes = u32;
    type ContainsKey = u32;
    type ReadMultiValuesBytes = u32;
    type FindKeysByPrefix = u32;
    type FindKeyValuesByPrefix = u32;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        Ok(self.chain_id)
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        let id = self
            .applications
            .last()
            .expect("at least one application description should be present in the stack")
            .id;
        Ok(id)
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        let parameters = self
            .applications
            .last()
            .expect("at least one application description should be present in the stack")
            .parameters
            .clone();
        Ok(parameters)
    }

    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::SystemBalance { callback })?
            .recv_response()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::SystemTimestamp { callback })?
            .recv_response()
    }

    fn try_read_my_state_new(&mut self) -> Result<Self::Read, ExecutionError> {
        let id = self.application_id()?;
        let state = self.simple_user_states.entry(id).or_default();
        ensure!(!state.locked, ExecutionError::ApplicationIsInUse(id));
        let receiver = self
            .execution_state_sender
            .send_request(|callback| Request::ReadSimpleUserState { id, callback })?;
        state.pending_query = Some(receiver);
        Ok(())
    }

    fn try_read_my_state_wait(&mut self, _promise: &Self::Read) -> Result<Vec<u8>, ExecutionError> {
        let id = self.application_id()?;
        let state = self
            .simple_user_states
            .get_mut(&id)
            .ok_or(ExecutionError::PolledTwice)?;
        let receiver =
            std::mem::take(&mut state.pending_query).ok_or(ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    // TODO(#1152): simplify away
    fn lock_new(&mut self) -> Result<Self::Lock, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(!state.locked, ExecutionError::ApplicationIsInUse(id));
        state.locked = true;
        Ok(())
    }

    fn lock_wait(&mut self, _promise: &Self::Lock) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn unlock_new(&mut self) -> Result<Self::Unlock, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        state.locked = false;
        Ok(())
    }

    fn unlock_wait(&mut self, _promise: &Self::Unlock) -> Result<(), ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        state.force_all_pending_queries()?;
        Ok(())
    }

    fn write_batch_and_unlock(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        state.force_all_pending_queries()?;
        self.execution_state_sender
            .send_request(|callback| Request::WriteBatch {
                id,
                batch,
                callback,
            })?
            .recv_response()?;
        state.locked = false;
        Ok(())
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        self.runtime_counts
            .increment_num_reads(&self.runtime_limits)?;
        let receiver = self
            .execution_state_sender
            .send_request(move |callback| Request::ContainsKey { id, key, callback })?;
        state.contains_key_queries.register(receiver)
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let value = state.contains_key_queries.wait(*promise)?;
        Ok(value)
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        self.runtime_counts
            .increment_num_reads(&self.runtime_limits)?;
        let receiver = self
            .execution_state_sender
            .send_request(move |callback| Request::ReadMultiValuesBytes { id, keys, callback })?;
        state.read_multi_values_queries.register(receiver)
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let values = state.read_multi_values_queries.wait(*promise)?;
        for value in &values {
            if let Some(value) = &value {
                self.runtime_counts
                    .increment_bytes_read(&self.runtime_limits, value.len() as u64)?;
            }
        }
        Ok(values)
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        self.runtime_counts
            .increment_num_reads(&self.runtime_limits)?;
        let receiver = self
            .execution_state_sender
            .send_request(move |callback| Request::ReadValueBytes { id, key, callback })?;
        state.read_value_queries.register(receiver)
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let value = state.read_value_queries.wait(*promise)?;
        if let Some(value) = &value {
            self.runtime_counts
                .increment_bytes_read(&self.runtime_limits, value.len() as u64)?;
        }
        Ok(value)
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        self.runtime_counts
            .increment_num_reads(&self.runtime_limits)?;
        let receiver = self.execution_state_sender.send_request(move |callback| {
            Request::FindKeysByPrefix {
                id,
                key_prefix,
                callback,
            }
        })?;
        state.find_keys_queries.register(receiver)
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let keys = state.find_keys_queries.wait(*promise)?;
        let mut read_size = 0;
        for key in &keys {
            read_size += key.len();
        }
        self.runtime_counts
            .increment_bytes_read(&self.runtime_limits, read_size as u64)?;
        Ok(keys)
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        ensure!(state.locked, ExecutionError::ApplicationStateNotLocked(id));
        self.runtime_counts
            .increment_num_reads(&self.runtime_limits)?;
        let receiver = self.execution_state_sender.send_request(move |callback| {
            Request::FindKeyValuesByPrefix {
                id,
                key_prefix,
                callback,
            }
        })?;
        state.find_key_values_queries.register(receiver)
    }

    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let key_values = state.find_key_values_queries.wait(*promise)?;
        let mut read_size = 0;
        for (key, value) in &key_values {
            read_size += key.len() + value.len();
        }
        self.runtime_counts
            .increment_bytes_read(&self.runtime_limits, read_size as u64)?;
        Ok(key_values)
    }
}

impl ContractSyncRuntime {
    /// Main entry point to start executing a user action.
    pub(crate) fn run_action(
        execution_state_sender: ExecutionStateSender,
        application_id: UserApplicationId,
        chain_id: ChainId,
        runtime_limits: RuntimeLimits,
        initial_remaining_fuel: u64,
        action: UserAction,
    ) -> Result<(Vec<ExecutionResult>, RuntimeCounts), ExecutionError> {
        let mut runtime = SyncRuntimeInternal::new(
            chain_id,
            execution_state_sender,
            runtime_limits,
            initial_remaining_fuel,
        );
        let (code, description) = runtime.load_contract(application_id)?;
        let signer = action.signer();
        runtime.applications.push(ApplicationStatus {
            id: application_id,
            parameters: description.parameters,
            signer,
        });
        let runtime = ContractSyncRuntime::new(runtime);
        let execution_result = {
            let mut code = code.instantiate_with_sync_runtime(runtime.clone())?;
            match action {
                UserAction::Initialize(context, argument) => code.initialize(context, argument)?,
                UserAction::Operation(context, operation) => {
                    code.execute_operation(context, operation)?
                }
                UserAction::Message(context, message) => code.execute_message(context, message)?,
            }
        };
        let mut runtime = runtime
            .into_inner()
            .expect("Runtime clones should have been freed by now");
        assert_eq!(runtime.applications.len(), 1);
        assert_eq!(runtime.applications[0].id, application_id);
        // Check that all sessions were properly closed.
        if let Some(session_id) = runtime.session_manager.states.keys().next() {
            return Err(ExecutionError::SessionWasNotClosed(*session_id));
        }
        // Adds the results of the last call to the execution results.
        runtime.execution_results.push(ExecutionResult::User(
            application_id,
            execution_result.with_authenticated_signer(signer),
        ));
        Ok((runtime.execution_results, runtime.runtime_counts))
    }
}

impl ContractRuntime for ContractSyncRuntime {
    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError> {
        let this = self.as_inner();
        Ok(this.runtime_counts.remaining_fuel)
    }

    fn set_remaining_fuel(&mut self, remaining_fuel: u64) -> Result<(), ExecutionError> {
        let mut this = self.as_inner();
        this.runtime_counts.remaining_fuel = remaining_fuel;
        Ok(())
    }

    fn try_read_and_lock_my_state(&mut self) -> Result<Option<Vec<u8>>, ExecutionError> {
        let mut this = self.as_inner();
        let this = this.deref_mut();
        let id = this.application_id()?;
        let state = this.simple_user_states.entry(id).or_default();
        if state.locked {
            return Ok(None);
        }
        let receiver = this
            .execution_state_sender
            .send_request(|callback| Request::ReadSimpleUserState { id, callback })?;
        let bytes = receiver.recv_response()?;
        state.locked = true;
        Ok(Some(bytes))
    }

    fn save_and_unlock_my_state(&mut self, bytes: Vec<u8>) -> Result<bool, ExecutionError> {
        let mut this = self.as_inner();
        let this = this.deref_mut();
        let id = this.application_id()?;
        let state = this.simple_user_states.entry(id).or_default();
        if !state.locked {
            return Ok(false);
        }
        let receiver =
            this.execution_state_sender
                .send_request(|callback| Request::SaveSimpleUserState {
                    id,
                    bytes,
                    callback,
                })?;
        receiver.recv_response()?;
        state.locked = false;
        Ok(true)
    }

    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let (callee_context, authenticated_signer, code) = {
            let mut this = self.as_inner();
            let caller = this.applications.last().expect("caller must exist").clone();
            // Load the application.
            let (code, description) = this.load_contract(callee_id)?;
            // Change the owners of forwarded sessions.
            this.forward_sessions(&forwarded_sessions, caller.id, callee_id)?;
            // Make the call to user code.
            let authenticated_signer = match caller.signer {
                Some(signer) if authenticated => Some(signer),
                _ => None,
            };
            let authenticated_caller_id = authenticated.then_some(caller.id);
            let callee_context = crate::CalleeContext {
                chain_id: this.chain_id,
                authenticated_signer,
                authenticated_caller_id,
            };
            this.applications.push(ApplicationStatus {
                id: callee_id,
                parameters: description.parameters,
                // Allow further nested calls to be authenticated if this one is.
                signer: authenticated_signer,
            });
            (callee_context, authenticated_signer, code)
        };
        let mut code = code.instantiate_with_sync_runtime(self.clone())?;
        let raw_result =
            code.handle_application_call(callee_context, argument, forwarded_sessions)?;
        {
            let mut this = self.as_inner();
            this.applications.pop();

            // Interpret the results of the call.
            this.execution_results.push(ExecutionResult::User(
                callee_id,
                raw_result
                    .execution_result
                    .with_authenticated_signer(authenticated_signer),
            ));
            let caller_id = this.application_id()?;
            let sessions = this.make_sessions(raw_result.create_sessions, callee_id, caller_id);
            let result = CallResult {
                value: raw_result.value,
                sessions,
            };
            Ok(result)
        }
    }

    fn try_call_session(
        &mut self,
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        let (callee_context, authenticated_signer, session_state, code) = {
            let mut this = self.as_inner();
            let callee_id = session_id.application_id;
            let caller = this.applications.last().expect("caller must exist").clone();
            // Load the application.
            let (code, description) = this.load_contract(callee_id)?;
            // Change the owners of forwarded sessions.
            this.forward_sessions(&forwarded_sessions, caller.id, callee_id)?;
            // Load the session.
            let session_state = this.try_load_session(session_id, caller.id)?;
            // Make the call to user code.
            let authenticated_signer = match caller.signer {
                Some(signer) if authenticated => Some(signer),
                _ => None,
            };
            let authenticated_caller_id = authenticated.then_some(caller.id);
            let callee_context = crate::CalleeContext {
                chain_id: this.chain_id,
                authenticated_signer,
                authenticated_caller_id,
            };
            this.applications.push(ApplicationStatus {
                id: callee_id,
                parameters: description.parameters,
                // Allow further nested calls to be authenticated if this one is.
                signer: authenticated_signer,
            });
            (callee_context, authenticated_signer, session_state, code)
        };
        let mut code = code.instantiate_with_sync_runtime(self.clone())?;
        let (raw_result, session_state) =
            code.handle_session_call(callee_context, session_state, argument, forwarded_sessions)?;
        {
            let mut this = self.as_inner();
            this.applications.pop();

            // Interpret the results of the call.
            let caller_id = this.application_id()?;
            if raw_result.close_session {
                // Terminate the session.
                this.try_close_session(session_id, caller_id)?;
            } else {
                // Save the session.
                this.try_save_session(session_id, caller_id, session_state)?;
            }
            let inner_result = raw_result.inner;
            let callee_id = session_id.application_id;
            this.execution_results.push(ExecutionResult::User(
                callee_id,
                inner_result
                    .execution_result
                    .with_authenticated_signer(authenticated_signer),
            ));
            let sessions = this.make_sessions(inner_result.create_sessions, callee_id, caller_id);
            let result = CallResult {
                value: inner_result.value,
                sessions,
            };
            Ok(result)
        }
    }
}

impl ServiceSyncRuntime {
    pub(crate) fn run_query(
        execution_state_sender: ExecutionStateSender,
        application_id: UserApplicationId,
        context: crate::QueryContext,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let runtime = SyncRuntimeInternal::new(
            context.chain_id,
            execution_state_sender,
            RuntimeLimits::default(),
            0,
        );
        ServiceSyncRuntime::new(runtime).try_query_application(application_id, query)
    }
}

impl ServiceRuntime for ServiceSyncRuntime {
    type TryQueryApplication = Vec<u8>;

    /// Note that queries are not available from writable contexts.
    // TODO(#1152): make synchronous
    fn try_query_application_new(
        &mut self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Self::TryQueryApplication, ExecutionError> {
        let (query_context, code) = {
            let mut this = self.as_inner();

            // Load the application.
            let (code, description) = this.load_service(queried_id)?;
            // Make the call to user code.
            let query_context = crate::QueryContext {
                chain_id: this.chain_id,
            };
            this.applications.push(ApplicationStatus {
                id: queried_id,
                parameters: description.parameters,
                signer: None,
            });
            (query_context, code)
        };
        let mut code = code.instantiate_with_sync_runtime(self.clone())?;
        let promise = code.handle_query(query_context, argument)?;
        {
            let mut this = self.as_inner();
            this.applications.pop();
        }
        Ok(promise)
    }

    fn try_query_application_wait(
        &mut self,
        promise: &Self::TryQueryApplication,
    ) -> Result<Vec<u8>, ExecutionError> {
        Ok(promise.to_vec())
    }
}
