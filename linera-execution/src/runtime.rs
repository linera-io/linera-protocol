// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    mem,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    vec,
};

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, OracleRecord, OracleResponse,
        Resources, SendMessageRequest, Timestamp,
    },
    ensure,
    identifiers::{Account, ApplicationId, ChainId, ChannelName, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_views::batch::Batch;
use oneshot::Receiver;

use crate::{
    execution::UserAction,
    execution_state_actor::{ExecutionStateSender, Request},
    resources::ResourceController,
    util::{ReceiverExt, UnboundedSenderExt},
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionOutcome, FinalizeContext,
    MessageContext, OperationContext, RawExecutionOutcome, ServiceRuntime,
    UserApplicationDescription, UserApplicationId, UserContractInstance, UserServiceInstance,
};

#[cfg(test)]
#[path = "unit_tests/runtime_tests.rs"]
mod tests;

#[derive(Debug)]
pub struct SyncRuntime<UserInstance>(SyncRuntimeHandle<UserInstance>);

pub type ContractSyncRuntime = SyncRuntime<UserContractInstance>;
pub type ServiceSyncRuntime = SyncRuntime<UserServiceInstance>;

#[derive(Debug)]
pub struct SyncRuntimeHandle<UserInstance>(Arc<Mutex<SyncRuntimeInternal<UserInstance>>>);

pub type ContractSyncRuntimeHandle = SyncRuntimeHandle<UserContractInstance>;
pub type ServiceSyncRuntimeHandle = SyncRuntimeHandle<UserServiceInstance>;

/// Responses to oracle queries that are being recorded or replayed.
#[derive(Debug)]
enum OracleResponses {
    /// When executing a block _proposal_, oracles can be used and their responses are recorded.
    Record(Vec<OracleResponse>),
    /// When re-executing a validated or confirmed block, recorded responses are used.
    Replay(vec::IntoIter<OracleResponse>),
    /// In service queries, oracle responses are not recorded.
    Forget,
}

/// Runtime data tracked during the execution of a transaction on the synchronous thread.
#[derive(Debug)]
pub struct SyncRuntimeInternal<UserInstance> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The height of the next block that will be added to this chain. During operations
    /// and messages, this is the current block height.
    height: BlockHeight,
    /// The current local time.
    local_time: Timestamp,
    /// The authenticated signer of the operation or message, if any.
    authenticated_signer: Option<Owner>,
    /// The current message being executed, if there is one.
    executing_message: Option<ExecutingMessage>,
    /// The index of the next message to be created.
    next_message_index: u32,

    /// How to interact with the storage view of the execution state.
    execution_state_sender: ExecutionStateSender,

    /// If applications are being finalized.
    ///
    /// If [`true`], disables cross-application calls.
    is_finalizing: bool,
    /// Applications that need to be finalized.
    applications_to_finalize: Vec<UserApplicationId>,

    /// Application instances loaded in this transaction.
    loaded_applications: HashMap<UserApplicationId, LoadedApplication<UserInstance>>,
    /// The current stack of application descriptions.
    call_stack: Vec<ApplicationStatus>,
    /// The set of the IDs of the applications that are in the `call_stack`.
    active_applications: HashSet<UserApplicationId>,
    /// Accumulate the externally visible results (e.g. cross-chain messages) of applications.
    execution_outcomes: Vec<ExecutionOutcome>,
    /// Responses to oracle queries that are being recorded or replayed.
    oracle_responses: OracleResponses,

    /// Track application states based on views.
    view_user_states: BTreeMap<UserApplicationId, ViewUserState>,

    /// Where to send a refund for the unused part of the grant after execution, if any.
    refund_grant_to: Option<Account>,
    /// Controller to track fuel and storage consumption.
    resource_controller: ResourceController,
}

impl<UserInstance> SyncRuntimeInternal<UserInstance> {
    /// Returns the index of the next outcome's first message.
    fn next_message_index(&self) -> Result<u32, ArithmeticError> {
        let mut index = self.next_message_index;
        for outcome in &self.execution_outcomes {
            let len = match outcome {
                ExecutionOutcome::System(outcome) => outcome.messages.len(),
                ExecutionOutcome::User(_, outcome) => outcome.messages.len(),
            };
            let len = u32::try_from(len).map_err(|_| ArithmeticError::Overflow)?;
            index = index.checked_add(len).ok_or(ArithmeticError::Overflow)?;
        }
        Ok(index)
    }
}

/// The runtime status of an application.
#[derive(Debug)]
struct ApplicationStatus {
    /// The caller application ID, if forwarded during the call.
    caller_id: Option<UserApplicationId>,
    /// The application ID.
    id: UserApplicationId,
    /// The parameters from the application description.
    parameters: Vec<u8>,
    /// The authenticated signer for the execution thread, if any.
    signer: Option<Owner>,
    /// The current execution outcome of the application.
    outcome: RawExecutionOutcome<Vec<u8>>,
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

impl<UserInstance> Deref for SyncRuntime<UserInstance> {
    type Target = SyncRuntimeHandle<UserInstance>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<UserInstance> DerefMut for SyncRuntime<UserInstance> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<UserInstance> SyncRuntimeInternal<UserInstance> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        chain_id: ChainId,
        height: BlockHeight,
        local_time: Timestamp,
        authenticated_signer: Option<Owner>,
        next_message_index: u32,
        executing_message: Option<ExecutingMessage>,
        execution_state_sender: ExecutionStateSender,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        oracle_responses: OracleResponses,
    ) -> Self {
        Self {
            chain_id,
            height,
            local_time,
            authenticated_signer,
            next_message_index,
            executing_message,
            execution_state_sender,
            is_finalizing: false,
            applications_to_finalize: Vec::new(),
            loaded_applications: HashMap::new(),
            call_stack: Vec::new(),
            active_applications: HashSet::new(),
            execution_outcomes: Vec::default(),
            view_user_states: BTreeMap::default(),
            refund_grant_to,
            resource_controller,
            oracle_responses,
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

    /// Returns a mutable refernce to the [`ApplicationStatus`] of the current application.
    ///
    /// The current application is the last to be pushed to the `call_stack`.
    ///
    /// # Panics
    ///
    /// If the call stack is empty.
    fn current_application_mut(&mut self) -> &mut ApplicationStatus {
        self.call_stack
            .last_mut()
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

                let instance = code.instantiate(SyncRuntimeHandle(this))?;

                self.applications_to_finalize.push(id);
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
    ) -> Result<(Arc<Mutex<UserContractInstance>>, OperationContext), ExecutionError> {
        self.check_for_reentrancy(callee_id)?;

        ensure!(
            !self.is_finalizing,
            ExecutionError::CrossApplicationCallInFinalize {
                caller_id: Box::new(self.current_application().id),
                callee_id: Box::new(callee_id),
            }
        );

        // Load the application.
        let application = self.load_contract_instance(this, callee_id)?;

        let caller = self.current_application();
        let caller_id = caller.id;
        let caller_signer = caller.signer;
        // Make the call to user code.
        let authenticated_signer = match caller_signer {
            Some(signer) if authenticated => Some(signer),
            _ => None,
        };
        let authenticated_caller_id = authenticated.then_some(caller_id);
        let callee_context = OperationContext {
            chain_id: self.chain_id,
            authenticated_signer,
            authenticated_caller_id,
            height: self.height,
            index: None,
            next_message_index: self.next_message_index,
        };
        self.push_application(ApplicationStatus {
            caller_id: authenticated_caller_id,
            id: callee_id,
            parameters: application.parameters,
            // Allow further nested calls to be authenticated if this one is.
            signer: authenticated_signer,
            outcome: RawExecutionOutcome::default(),
        });
        Ok((application.instance, callee_context))
    }

    /// Cleans up the runtime after the execution of a call to a different contract.
    fn finish_call(&mut self) -> Result<(), ExecutionError> {
        let ApplicationStatus {
            id: callee_id,
            signer,
            outcome,
            ..
        } = self.pop_application();

        self.handle_outcome(outcome, signer, callee_id)?;

        Ok(())
    }

    /// Handles a newly produced [`RawExecutionOutcome`], conditioning and adding it to the stack
    /// of outcomes.
    ///
    /// Calculates the fees, charges for the grants and attaches the authenticated `signer` and the
    /// destination of grant refunds.
    fn handle_outcome(
        &mut self,
        raw_outcome: RawExecutionOutcome<Vec<u8>, Resources>,
        signer: Option<Owner>,
        application_id: UserApplicationId,
    ) -> Result<(), ExecutionError> {
        let outcome = raw_outcome
            .with_refund_grant_to(self.refund_grant_to)
            .with_authenticated_signer(signer)
            .into_priced(&self.resource_controller.policy)?;

        for message in &outcome.messages {
            self.resource_controller.track_grant(message.grant)?;
        }

        self.execution_outcomes
            .push(ExecutionOutcome::User(application_id, outcome));
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

                let instance = code.instantiate(SyncRuntimeHandle(this))?;
                Ok(entry
                    .insert(LoadedApplication::new(instance, description))
                    .clone())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }
}

impl<UserInstance> SyncRuntimeHandle<UserInstance> {
    fn new(runtime: SyncRuntimeInternal<UserInstance>) -> Self {
        SyncRuntimeHandle(Arc::new(Mutex::new(runtime)))
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

impl<UserInstance> BaseRuntime for SyncRuntimeHandle<UserInstance> {
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

    fn block_height(&mut self) -> Result<BlockHeight, ExecutionError> {
        self.inner().block_height()
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        self.inner().application_id()
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner().application_parameters()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner().read_system_timestamp()
    }

    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner().read_chain_balance()
    }

    fn read_owner_balance(&mut self, owner: Owner) -> Result<Amount, ExecutionError> {
        self.inner().read_owner_balance(owner)
    }

    fn read_owner_balances(&mut self) -> Result<Vec<(Owner, Amount)>, ExecutionError> {
        self.inner().read_owner_balances()
    }

    fn read_balance_owners(&mut self) -> Result<Vec<Owner>, ExecutionError> {
        self.inner().read_balance_owners()
    }

    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError> {
        self.inner().chain_ownership()
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

    fn query_service(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.inner().query_service(application_id, query)
    }

    fn http_post(
        &mut self,
        url: &str,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.inner().http_post(url, content_type, payload)
    }

    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError> {
        self.inner().assert_before(timestamp)
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

    fn block_height(&mut self) -> Result<BlockHeight, ExecutionError> {
        Ok(self.height)
    }

    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError> {
        Ok(self.current_application().id)
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        Ok(self.current_application().parameters.clone())
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::SystemTimestamp { callback })?
            .recv_response()
    }

    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::ChainBalance { callback })?
            .recv_response()
    }

    fn read_owner_balance(&mut self, owner: Owner) -> Result<Amount, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::OwnerBalance { owner, callback })?
            .recv_response()
    }

    fn read_owner_balances(&mut self) -> Result<Vec<(Owner, Amount)>, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::OwnerBalances { callback })?
            .recv_response()
    }

    fn read_balance_owners(&mut self) -> Result<Vec<Owner>, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::BalanceOwners { callback })?
            .recv_response()
    }

    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| Request::ChainOwnership { callback })?
            .recv_response()
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

    fn query_service(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        if let OracleResponses::Replay(responses) = &mut self.oracle_responses {
            return match responses.next() {
                Some(OracleResponse::Service(bytes)) => Ok(bytes),
                Some(_) => Err(ExecutionError::OracleResponseMismatch),
                None => Err(ExecutionError::MissingOracleResponse),
            };
        }
        let context = crate::QueryContext {
            chain_id: self.chain_id,
            next_block_height: self.height,
            local_time: self.local_time,
        };
        let sender = self.execution_state_sender.clone();
        let response = ServiceSyncRuntime::run_query(sender, application_id, context, query)?;
        if let OracleResponses::Record(responses) = &mut self.oracle_responses {
            responses.push(OracleResponse::Service(response.clone()));
        }
        Ok(response)
    }

    fn http_post(
        &mut self,
        url: &str,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        if let OracleResponses::Replay(responses) = &mut self.oracle_responses {
            return match responses.next() {
                Some(OracleResponse::Post(bytes)) => Ok(bytes),
                Some(_) => Err(ExecutionError::OracleResponseMismatch),
                None => Err(ExecutionError::MissingOracleResponse),
            };
        }
        let url = url.to_string();
        let bytes = self
            .execution_state_sender
            .send_request(|callback| Request::HttpPost {
                url,
                content_type,
                payload,
                callback,
            })?
            .recv_response()?;
        if let OracleResponses::Record(responses) = &mut self.oracle_responses {
            responses.push(OracleResponse::Post(bytes.clone()));
        }
        Ok(bytes)
    }

    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError> {
        if let OracleResponses::Replay(responses) = &mut self.oracle_responses {
            return match responses.next() {
                Some(OracleResponse::Assert) => Ok(()),
                Some(_) => Err(ExecutionError::OracleResponseMismatch),
                None => Err(ExecutionError::MissingOracleResponse),
            };
        }
        ensure!(
            self.local_time < timestamp,
            ExecutionError::AssertBefore {
                timestamp,
                local_time: self.local_time,
            }
        );
        if let OracleResponses::Record(responses) = &mut self.oracle_responses {
            responses.push(OracleResponse::Assert);
        }
        Ok(())
    }
}

impl<UserInstance> Clone for SyncRuntimeHandle<UserInstance> {
    fn clone(&self) -> Self {
        SyncRuntimeHandle(self.0.clone())
    }
}

impl ContractSyncRuntime {
    /// Main entry point to start executing a user action.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub(crate) fn run_action(
        execution_state_sender: ExecutionStateSender,
        application_id: UserApplicationId,
        chain_id: ChainId,
        local_time: Timestamp,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        action: UserAction,
        oracle_record: Option<OracleRecord>,
    ) -> Result<(Vec<ExecutionOutcome>, OracleRecord, ResourceController), ExecutionError> {
        let executing_message = match &action {
            UserAction::Message(context, _) => Some(context.into()),
            _ => None,
        };
        let signer = action.signer();
        let height = action.height();
        let next_message_index = action.next_message_index();
        let oracle_record = if let Some(responses) = oracle_record {
            OracleResponses::Replay(responses.responses.into_iter())
        } else {
            OracleResponses::Record(Vec::new())
        };
        let mut runtime = SyncRuntime(ContractSyncRuntimeHandle::new(SyncRuntimeInternal::new(
            chain_id,
            height,
            local_time,
            signer,
            next_message_index,
            executing_message,
            execution_state_sender,
            refund_grant_to,
            resource_controller,
            oracle_record,
        )));
        let finalize_context = FinalizeContext {
            authenticated_signer: signer,
            chain_id,
            height,
            next_message_index,
        };
        runtime.execute(application_id, signer, move |code| match action {
            UserAction::Instantiate(context, argument) => code.instantiate(context, argument),
            UserAction::Operation(context, operation) => {
                code.execute_operation(context, operation).map(|_| ())
            }
            UserAction::Message(context, message) => code.execute_message(context, message),
        })?;
        runtime.finalize(finalize_context)?;
        let runtime = runtime
            .0
            .into_inner()
            .expect("Runtime clones should have been freed by now");
        let oracle_record = if let OracleResponses::Record(responses) = runtime.oracle_responses {
            OracleRecord { responses }
        } else {
            OracleRecord::default()
        };
        Ok((
            runtime.execution_outcomes,
            oracle_record,
            runtime.resource_controller,
        ))
    }

    /// Notifies all loaded applications that execution is finalizing.
    fn finalize(&mut self, context: FinalizeContext) -> Result<(), ExecutionError> {
        let applications = mem::take(&mut self.inner().applications_to_finalize)
            .into_iter()
            .rev();

        self.inner().is_finalizing = true;

        for application in applications {
            self.execute(application, context.authenticated_signer, |contract| {
                contract.finalize(context)
            })?;
        }

        self.inner().loaded_applications.clear();

        Ok(())
    }

    /// Executes a `closure` with the contract code for the `application_id`.
    ///
    /// Automatically clears the `loaded_applications` if an error occurs, allowing for a safe
    /// early return in the caller.
    fn execute(
        &mut self,
        application_id: UserApplicationId,
        signer: Option<Owner>,
        closure: impl FnOnce(&mut UserContractInstance) -> Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        match self.try_execute(application_id, signer, closure) {
            Ok(()) => Ok(()),
            Err(error) => {
                // Ensure the `loaded_applications` are cleared to prevent circular references in
                // the `runtime`
                self.inner().loaded_applications.clear();
                Err(error)
            }
        }
    }

    /// Tries to execute a `closure` with the contract code for the `application_id`.
    ///
    /// If an error occurs, the caller *must* clear the `loaded_applications` to prevent a deadlock
    /// happening because the runtime never gets dropped due to circular references.
    fn try_execute(
        &mut self,
        application_id: UserApplicationId,
        signer: Option<Owner>,
        closure: impl FnOnce(&mut UserContractInstance) -> Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let contract = {
            let cloned_runtime = self.0 .0.clone();
            let mut runtime = self.inner();
            let application = runtime.load_contract_instance(cloned_runtime, application_id)?;

            let status = ApplicationStatus {
                caller_id: None,
                id: application_id,
                parameters: application.parameters.clone(),
                signer,
                outcome: RawExecutionOutcome::default(),
            };

            runtime.push_application(status);

            application
        };

        closure(
            &mut contract
                .instance
                .try_lock()
                .expect("Application should not be already executing"),
        )?;

        let mut runtime = self.inner();
        let application_status = runtime.pop_application();
        assert_eq!(application_status.caller_id, None);
        assert_eq!(application_status.id, application_id);
        assert_eq!(application_status.parameters, contract.parameters);
        assert_eq!(application_status.signer, signer);
        assert!(runtime.call_stack.is_empty());

        runtime.handle_outcome(application_status.outcome, signer, application_id)?;

        Ok(())
    }
}

impl ContractRuntime for ContractSyncRuntimeHandle {
    fn authenticated_signer(&mut self) -> Result<Option<Owner>, ExecutionError> {
        Ok(self.inner().authenticated_signer)
    }

    fn message_id(&mut self) -> Result<Option<MessageId>, ExecutionError> {
        Ok(self.inner().executing_message.map(|metadata| metadata.id))
    }

    fn message_is_bouncing(&mut self) -> Result<Option<bool>, ExecutionError> {
        Ok(self
            .inner()
            .executing_message
            .map(|metadata| metadata.is_bouncing))
    }

    fn authenticated_caller_id(&mut self) -> Result<Option<UserApplicationId>, ExecutionError> {
        let mut this = self.inner();
        if this.call_stack.len() <= 1 {
            return Ok(None);
        }
        Ok(this.current_application().caller_id)
    }

    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError> {
        Ok(self.inner().resource_controller.remaining_fuel())
    }

    fn consume_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        this.resource_controller.track_fuel(fuel)
    }

    fn send_message(&mut self, message: SendMessageRequest<Vec<u8>>) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application = this.current_application_mut();

        application.outcome.messages.push(message.into());

        Ok(())
    }

    fn subscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application = this.current_application_mut();

        application.outcome.subscribe.push((channel, chain));

        Ok(())
    }

    fn unsubscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application = this.current_application_mut();

        application.outcome.unsubscribe.push((channel, chain));

        Ok(())
    }

    fn transfer(
        &mut self,
        source: Option<Owner>,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let signer = self.inner().current_application().signer;
        let execution_outcome = self
            .inner()
            .execution_state_sender
            .send_request(|callback| Request::Transfer {
                source,
                destination,
                amount,
                signer,
                callback,
            })?
            .recv_response()?;
        self.inner()
            .execution_outcomes
            .push(ExecutionOutcome::System(execution_outcome));
        Ok(())
    }

    fn claim(
        &mut self,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let signer = self.inner().current_application().signer;
        let execution_outcome = self
            .inner()
            .execution_state_sender
            .send_request(|callback| Request::Claim {
                source,
                destination,
                amount,
                signer,
                callback,
            })?
            .recv_response()?
            .with_authenticated_signer(signer);
        self.inner()
            .execution_outcomes
            .push(ExecutionOutcome::System(execution_outcome));
        Ok(())
    }

    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let cloned_self = self.clone().0;
        let (contract, context) =
            self.inner()
                .prepare_for_call(cloned_self, authenticated, callee_id)?;

        let value = contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .execute_operation(context, argument)?;

        self.inner().finish_call()?;

        Ok(value)
    }

    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ChainId, ExecutionError> {
        let mut this = self.inner();
        let next_message_id = MessageId {
            chain_id: this.chain_id,
            height: this.height,
            index: this.next_message_index()?,
        };
        let chain_id = ChainId::child(next_message_id);
        let [open_chain_message, subscribe_message] = this
            .execution_state_sender
            .send_request(|callback| Request::OpenChain {
                ownership,
                balance,
                next_message_id,
                application_permissions,
                callback,
            })?
            .recv_response()?;
        let outcome = RawExecutionOutcome::default()
            .with_message(open_chain_message)
            .with_message(subscribe_message);
        this.execution_outcomes
            .push(ExecutionOutcome::System(outcome));
        Ok(chain_id)
    }

    fn close_chain(&mut self) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application_id = this.current_application().id;
        this.execution_state_sender
            .send_request(|callback| Request::CloseChain {
                application_id,
                callback,
            })?
            .recv_response()?
    }

    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let id = this.application_id()?;
        let state = this.view_user_states.entry(id).or_default();
        state.force_all_pending_queries()?;
        this.resource_controller.track_write_operations(
            batch
                .num_operations()
                .try_into()
                .map_err(|_| ExecutionError::from(ArithmeticError::Overflow))?,
        )?;
        this.resource_controller
            .track_bytes_written(batch.size() as u64)?;
        this.execution_state_sender
            .send_request(|callback| Request::WriteBatch {
                id,
                batch,
                callback,
            })?
            .recv_response()?;
        Ok(())
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
            context.next_block_height,
            context.local_time,
            None,
            0,
            None,
            execution_state_sender,
            None,
            ResourceController::default(),
            OracleResponses::Forget,
        );
        let mut runtime = ServiceSyncRuntimeHandle::new(runtime_internal);

        let result = runtime.try_query_application(application_id, query);

        // Ensure the `loaded_applications` are cleared to remove circular references in
        // `runtime_internal`
        runtime.inner().loaded_applications.clear();
        result
    }
}

impl ServiceRuntime for ServiceSyncRuntimeHandle {
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
                next_block_height: this.height,
                local_time: this.local_time,
            };
            this.push_application(ApplicationStatus {
                caller_id: None,
                id: queried_id,
                parameters: application.parameters,
                signer: None,
                outcome: RawExecutionOutcome::default(),
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

    /// Get a blob of bytes from an arbitrary URL.
    fn fetch_url(&mut self, url: &str) -> Result<Vec<u8>, ExecutionError> {
        let this = self.inner();
        let url = url.to_string();
        this.execution_state_sender
            .send_request(|callback| Request::FetchUrl { url, callback })?
            .recv_response()
    }
}

/// The origin of the execution.
#[derive(Clone, Copy, Debug)]
struct ExecutingMessage {
    id: MessageId,
    is_bouncing: bool,
}

impl From<&MessageContext> for ExecutingMessage {
    fn from(context: &MessageContext) -> Self {
        ExecutingMessage {
            id: context.message_id,
            is_bouncing: context.is_bouncing,
        }
    }
}
