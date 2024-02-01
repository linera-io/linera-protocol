// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::UserAction,
    execution_state_actor::{ExecutionStateSender, Request},
    resources::ResourceController,
    util::{ReceiverExt, UnboundedSenderExt},
    ApplicationCallOutcome, BaseRuntime, CallOutcome, CalleeContext, ContractRuntime,
    ExecutionError, ExecutionOutcome, ServiceRuntime, SessionId, UserApplicationDescription,
    UserApplicationId, UserContractCode, UserContractInstance, UserServiceInstance,
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
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
};

#[cfg(test)]
#[path = "unit_tests/runtime_tests.rs"]
mod tests;

#[derive(Debug)]
pub struct SyncRuntime<UserInstance>(Arc<Mutex<SyncRuntimeInternal<UserInstance>>>);

pub type ContractSyncRuntime = SyncRuntime<UserContractInstance>;
pub type ServiceSyncRuntime = SyncRuntime<UserServiceInstance>;

/// Runtime data tracked during the execution of a transaction on the synchronous thread.
#[derive(Debug)]
pub struct SyncRuntimeInternal<UserInstance> {
    /// The current chain ID.
    chain_id: ChainId,

    /// How to interact with the storage view of the execution state.
    execution_state_sender: ExecutionStateSender,

    /// Application instances loaded in this transaction.
    loaded_applications: HashMap<UserApplicationId, LoadedApplication<UserInstance>>,
    /// The current stack of application descriptions.
    call_stack: Vec<ApplicationStatus>,
    /// The set of the IDs of the applications that are in the `call_stack`.
    active_applications: HashSet<UserApplicationId>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    execution_outcomes: Vec<ExecutionOutcome>,

    /// All the sessions and their IDs.
    session_manager: SessionManager,
    /// Track application states (simple case).
    simple_user_states: BTreeMap<UserApplicationId, SimpleUserState>,
    /// Track application states (view case).
    view_user_states: BTreeMap<UserApplicationId, ViewUserState>,

    /// Controller to track fuel and storage consumption.
    resource_controller: ResourceController,
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

/// A loaded application instance.
#[derive(Debug)]
struct LoadedApplication<Instance> {
    instance: Arc<Mutex<Instance>>,
    parameters: Vec<u8>,
}

impl<Instance> LoadedApplication<Instance> {
    /// Creates a new [`LoadedApplication`] entry from the `instance` and its `description`.
    fn new(instance: Instance, description: UserApplicationDescription) -> Self {
        LoadedApplication {
            instance: Arc::new(Mutex::new(instance)),
            parameters: description.parameters,
        }
    }
}

impl<Instance> Clone for LoadedApplication<Instance> {
    // Manual implementation is needed to prevent the derive macro from adding an `Instance: Clone`
    // bound
    fn clone(&self) -> Self {
        LoadedApplication {
            instance: self.instance.clone(),
            parameters: self.parameters.clone(),
        }
    }
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
            .ok_or(ExecutionError::InvalidPromise)?;
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
        self.contains_key_queries.force_all()?;
        self.read_value_queries.force_all()?;
        self.read_multi_values_queries.force_all()?;
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

impl<UserInstance> SyncRuntimeInternal<UserInstance> {
    fn new(
        chain_id: ChainId,
        execution_state_sender: ExecutionStateSender,
        resource_controller: ResourceController,
    ) -> Self {
        Self {
            chain_id,
            execution_state_sender,
            loaded_applications: HashMap::new(),
            call_stack: Vec::new(),
            active_applications: HashSet::new(),
            execution_outcomes: Vec::default(),
            session_manager: SessionManager::default(),
            simple_user_states: BTreeMap::default(),
            view_user_states: BTreeMap::default(),
            resource_controller,
        }
    }

    /// Returns the [`ApplicationStatus`] of the current application.
    ///
    /// The current application is the last to be pushed to the `call_stack`.
    ///
    /// # Panics
    ///
    /// If the call stack is empty.
    fn current_application(&mut self) -> &ApplicationStatus {
        self.call_stack
            .last()
            .expect("Call stack is unexpectedly empty")
    }

    /// Inserts a new [`ApplicationStatus`] to the end of the `call_stack`.
    ///
    /// Ensures the application's ID is also tracked in the `active_applications` set.
    fn push_application(&mut self, status: ApplicationStatus) {
        self.active_applications.insert(status.id);
        self.call_stack.push(status);
    }

    /// Removes the [`current_application`][`Self::current_application`] from the `call_stack`.
    ///
    /// Ensures the application's ID is also removed from the `active_applications` set.
    ///
    /// # Panics
    ///
    /// If the call stack is empty.
    fn pop_application(&mut self) -> ApplicationStatus {
        let status = self
            .call_stack
            .pop()
            .expect("Can't remove application from empty call stack");
        assert!(self.active_applications.remove(&status.id));
        status
    }

    /// Ensures that a call to `application_id` is not-reentrant.
    ///
    /// Returns an error if there already is an entry for `application_id` in the call stack.
    fn check_for_reentrancy(
        &mut self,
        application_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        ensure!(
            !self.active_applications.contains(&application_id),
            ExecutionError::ReentrantCall(application_id)
        );
        Ok(())
    }
}

impl SyncRuntimeInternal<UserContractInstance> {
    fn load_contract(
        &mut self,
        id: UserApplicationId,
    ) -> Result<(UserContractCode, UserApplicationDescription), ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::LoadContract { id, callback })?
            .recv_response()
    }

    /// Loads a contract instance, initializing it with this runtime if needed.
    fn load_contract_instance(
        &mut self,
        this: Arc<Mutex<Self>>,
        id: UserApplicationId,
    ) -> Result<LoadedApplication<UserContractInstance>, ExecutionError> {
        match self.loaded_applications.entry(id) {
            hash_map::Entry::Vacant(entry) => {
                let (code, description) = self
                    .execution_state_sender
                    .send_request(|callback| Request::LoadContract { id, callback })?
                    .recv_response()?;

                let instance = code.instantiate(SyncRuntime(this))?;
                Ok(entry
                    .insert(LoadedApplication::new(instance, description))
                    .clone())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }

    /// Configures the runtime for executing a call to a different contract.
    fn prepare_for_call(
        &mut self,
        this: Arc<Mutex<Self>>,
        authenticated: bool,
        callee_id: UserApplicationId,
        forwarded_sessions: &[SessionId],
    ) -> Result<(Arc<Mutex<UserContractInstance>>, CalleeContext), ExecutionError> {
        self.check_for_reentrancy(callee_id)?;

        // Load the application.
        let application = self.load_contract_instance(this, callee_id)?;

        let caller = self.current_application();
        let caller_id = caller.id;
        let caller_signer = caller.signer;
        // Change the owners of forwarded sessions.
        self.forward_sessions(forwarded_sessions, caller_id, callee_id)?;
        // Make the call to user code.
        let authenticated_signer = match caller_signer {
            Some(signer) if authenticated => Some(signer),
            _ => None,
        };
        let authenticated_caller_id = authenticated.then_some(caller_id);
        let callee_context = CalleeContext {
            chain_id: self.chain_id,
            authenticated_signer,
            authenticated_caller_id,
        };
        self.push_application(ApplicationStatus {
            id: callee_id,
            parameters: application.parameters,
            // Allow further nested calls to be authenticated if this one is.
            signer: authenticated_signer,
        });
        Ok((application.instance, callee_context))
    }

    /// Cleans up the runtime after the execution of a call to a different contract.
    fn finish_call(
        &mut self,
        raw_outcome: ApplicationCallOutcome,
    ) -> Result<CallOutcome, ExecutionError> {
        let ApplicationStatus {
            id: callee_id,
            signer,
            ..
        } = self.pop_application();

        // Interpret the results of the call.
        self.execution_outcomes.push(ExecutionOutcome::User(
            callee_id,
            raw_outcome
                .execution_outcome
                .with_authenticated_signer(signer),
        ));
        let caller_id = self.application_id()?;
        let sessions = self.make_sessions(raw_outcome.create_sessions, callee_id, caller_id);
        let outcome = CallOutcome {
            value: raw_outcome.value,
            sessions,
        };
        Ok(outcome)
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

impl SyncRuntimeInternal<UserServiceInstance> {
    /// Initializes a service instance with this runtime.
    fn load_service_instance(
        &mut self,
        this: Arc<Mutex<Self>>,
        id: UserApplicationId,
    ) -> Result<LoadedApplication<UserServiceInstance>, ExecutionError> {
        match self.loaded_applications.entry(id) {
            hash_map::Entry::Vacant(entry) => {
                let (code, description) = self
                    .execution_state_sender
                    .send_request(|callback| Request::LoadService { id, callback })?
                    .recv_response()?;

                let instance = code.instantiate(SyncRuntime(this))?;
                Ok(entry
                    .insert(LoadedApplication::new(instance, description))
                    .clone())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }
}

impl<UserInstance> SyncRuntime<UserInstance> {
    fn new(runtime: SyncRuntimeInternal<UserInstance>) -> Self {
        SyncRuntime(Arc::new(Mutex::new(runtime)))
    }

    fn into_inner(self) -> Option<SyncRuntimeInternal<UserInstance>> {
        let runtime = Arc::into_inner(self.0)?
            .into_inner()
            .expect("thread should not have panicked");
        Some(runtime)
    }

    fn inner(&mut self) -> std::sync::MutexGuard<'_, SyncRuntimeInternal<UserInstance>> {
        self.0
            .try_lock()
            .expect("Synchronous runtimes run on a single execution thread")
    }
}

impl<UserInstance> BaseRuntime for SyncRuntime<UserInstance> {
    type Read = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::Read;
    type ReadValueBytes = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ReadValueBytes;
    type ContainsKey = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ContainsKey;
    type ReadMultiValuesBytes =
        <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ReadMultiValuesBytes;
    type FindKeysByPrefix = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::FindKeysByPrefix;
    type FindKeyValuesByPrefix =
        <SyncRuntimeInternal<UserInstance> as BaseRuntime>::FindKeyValuesByPrefix;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner().chain_id()
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        self.inner().application_id()
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner().application_parameters()
    }

    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner().read_system_balance()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner().read_system_timestamp()
    }

    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        self.inner().write_batch(batch)
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        self.inner().contains_key_new(key)
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        self.inner().contains_key_wait(promise)
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        self.inner().read_multi_values_bytes_new(keys)
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        self.inner().read_multi_values_bytes_wait(promise)
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        self.inner().read_value_bytes_new(key)
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.inner().read_value_bytes_wait(promise)
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        self.inner().find_keys_by_prefix_new(key_prefix)
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        self.inner().find_keys_by_prefix_wait(promise)
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
        self.inner().find_key_values_by_prefix_new(key_prefix)
    }

    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        self.inner().find_key_values_by_prefix_wait(promise)
    }
}

impl<UserInstance> BaseRuntime for SyncRuntimeInternal<UserInstance> {
    type Read = ();
    type ReadValueBytes = u32;
    type ContainsKey = u32;
    type ReadMultiValuesBytes = u32;
    type FindKeysByPrefix = u32;
    type FindKeyValuesByPrefix = u32;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        Ok(self.chain_id)
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        Ok(self.current_application().id)
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        Ok(self.current_application().parameters.clone())
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

    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        state.force_all_pending_queries()?;
        self.resource_controller.track_write_operations(
            batch
                .num_operations()
                .try_into()
                .map_err(|_| ExecutionError::from(ArithmeticError::Overflow))?,
        )?;
        self.resource_controller
            .track_bytes_written(batch.size() as u64)?;
        self.execution_state_sender
            .send_request(|callback| Request::WriteBatch {
                id,
                batch,
                callback,
            })?
            .recv_response()?;
        Ok(())
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
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
        self.resource_controller.track_read_operations(1)?;
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
                self.resource_controller
                    .track_bytes_read(value.len() as u64)?;
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
        self.resource_controller.track_read_operations(1)?;
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
            self.resource_controller
                .track_bytes_read(value.len() as u64)?;
        }
        Ok(value)
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
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
        self.resource_controller
            .track_bytes_read(read_size as u64)?;
        Ok(keys)
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
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
        self.resource_controller
            .track_bytes_read(read_size as u64)?;
        Ok(key_values)
    }
}

impl<UserInstance> Clone for SyncRuntime<UserInstance> {
    fn clone(&self) -> Self {
        SyncRuntime(self.0.clone())
    }
}

impl ContractSyncRuntime {
    /// Main entry point to start executing a user action.
    pub(crate) fn run_action(
        execution_state_sender: ExecutionStateSender,
        application_id: UserApplicationId,
        chain_id: ChainId,
        resource_controller: ResourceController,
        action: UserAction,
    ) -> Result<(Vec<ExecutionOutcome>, ResourceController), ExecutionError> {
        let mut runtime =
            SyncRuntimeInternal::new(chain_id, execution_state_sender, resource_controller);
        let (code, description) = runtime.load_contract(application_id)?;
        let signer = action.signer();
        runtime.push_application(ApplicationStatus {
            id: application_id,
            parameters: description.parameters,
            signer,
        });
        let mut runtime = ContractSyncRuntime::new(runtime);
        let execution_result = {
            let mut code = code.instantiate(runtime.clone())?;
            match action {
                UserAction::Initialize(context, argument) => code.initialize(context, argument),
                UserAction::Operation(context, operation) => {
                    code.execute_operation(context, operation)
                }
                UserAction::Message(context, message) => code.execute_message(context, message),
            }
        };
        // Ensure the `loaded_applications` are cleared to prevent circular references in the
        // `runtime`
        runtime.inner().loaded_applications.clear();
        let mut runtime = runtime
            .into_inner()
            .expect("Runtime clones should have been freed by now");
        let execution_outcome = execution_result?;
        assert_eq!(runtime.call_stack.len(), 1);
        assert_eq!(runtime.call_stack[0].id, application_id);
        assert_eq!(runtime.active_applications.len(), 1);
        assert!(runtime.active_applications.contains(&application_id));
        // Check that all sessions were properly closed.
        if let Some(session_id) = runtime.session_manager.states.keys().next() {
            return Err(ExecutionError::SessionWasNotClosed(*session_id));
        }
        // Adds the results of the last call to the execution results.
        runtime.execution_outcomes.push(ExecutionOutcome::User(
            application_id,
            execution_outcome.with_authenticated_signer(signer),
        ));
        Ok((runtime.execution_outcomes, runtime.resource_controller))
    }
}

impl ContractRuntime for ContractSyncRuntime {
    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError> {
        Ok(self.inner().resource_controller.remaining_fuel())
    }

    fn set_remaining_fuel(&mut self, remaining_fuel: u64) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let previous_fuel = this.resource_controller.remaining_fuel();
        // This is temporary. The real fix is #1599.
        if previous_fuel > remaining_fuel {
            this.resource_controller
                .track_fuel(previous_fuel - remaining_fuel)?;
        }
        Ok(())
    }

    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallOutcome, ExecutionError> {
        let cloned_self = self.clone().0;
        let (contract, callee_context) = self.inner().prepare_for_call(
            cloned_self,
            authenticated,
            callee_id,
            &forwarded_sessions,
        )?;

        let raw_outcome = contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .handle_application_call(callee_context, argument, forwarded_sessions)?;

        self.inner().finish_call(raw_outcome)
    }

    fn try_call_session(
        &mut self,
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallOutcome, ExecutionError> {
        let callee_id = session_id.application_id;

        let (contract, callee_context, session_state) = {
            let cloned_self = self.clone().0;
            let mut this = self.inner();

            // Load the session.
            let caller_id = this.application_id()?;
            let session_state = this.try_load_session(session_id, caller_id)?;

            let (contract, callee_context) =
                this.prepare_for_call(cloned_self, authenticated, callee_id, &forwarded_sessions)?;

            Ok::<_, ExecutionError>((contract, callee_context, session_state))
        }?;

        let (raw_outcome, session_state) = contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .handle_session_call(callee_context, session_state, argument, forwarded_sessions)?;

        {
            let mut this = self.inner();

            let outcome = this.finish_call(raw_outcome.inner)?;

            // Update the session.
            let caller_id = this.application_id()?;
            if raw_outcome.close_session {
                // Terminate the session.
                this.try_close_session(session_id, caller_id)?;
            } else {
                // Save the session.
                this.try_save_session(session_id, caller_id, session_state)?;
            }

            Ok(outcome)
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
        let runtime_internal = SyncRuntimeInternal::new(
            context.chain_id,
            execution_state_sender,
            ResourceController::default(),
        );
        let mut runtime = ServiceSyncRuntime::new(runtime_internal);

        let result = runtime.try_query_application(application_id, query);

        // Ensure the `loaded_applications` are cleared to remove circular references in
        // `runtime_internal`
        runtime.inner().loaded_applications.clear();
        result
    }
}

impl ServiceRuntime for ServiceSyncRuntime {
    /// Note that queries are not available from writable contexts.
    fn try_query_application(
        &mut self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (query_context, service) = {
            let cloned_self = self.clone().0;
            let mut this = self.inner();

            // Load the application.
            let application = this.load_service_instance(cloned_self, queried_id)?;
            // Make the call to user code.
            let query_context = crate::QueryContext {
                chain_id: this.chain_id,
            };
            this.push_application(ApplicationStatus {
                id: queried_id,
                parameters: application.parameters,
                signer: None,
            });
            (query_context, application.instance)
        };
        let response = service
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .handle_query(query_context, argument)?;
        self.inner().pop_application();
        Ok(response)
    }
}
