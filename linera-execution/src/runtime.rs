// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    mem,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::Instant,
};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, OracleResponse,
        SendMessageRequest, Timestamp,
    },
    ensure, http,
    identifiers::{
        Account, AccountOwner, BlobId, BlobType, ChainId, EventId, GenericApplicationId, MessageId,
        StreamId, StreamName,
    },
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_views::batch::Batch;
use oneshot::Receiver;

use crate::{
    execution::UserAction,
    execution_state_actor::{ExecutionRequest, ExecutionStateSender},
    resources::ResourceController,
    system::CreateApplicationResult,
    util::{ReceiverExt, UnboundedSenderExt},
    ApplicationDescription, ApplicationId, BaseRuntime, ContractRuntime, ExecutionError,
    FinalizeContext, Message, MessageContext, MessageKind, ModuleId, Operation, OperationContext,
    OutgoingMessage, QueryContext, QueryOutcome, ServiceRuntime, TransactionTracker,
    UserContractCode, UserContractInstance, UserServiceCode, UserServiceInstance,
    MAX_STREAM_NAME_LEN,
};

#[cfg(test)]
#[path = "unit_tests/runtime_tests.rs"]
mod tests;

pub trait WithContext {
    type UserContext;
}

impl WithContext for UserContractInstance {
    type UserContext = Timestamp;
}

impl WithContext for UserServiceInstance {
    type UserContext = ();
}

#[cfg(test)]
impl WithContext for Arc<dyn std::any::Any + Send + Sync> {
    type UserContext = ();
}

#[derive(Debug)]
pub struct SyncRuntime<UserInstance: WithContext>(Option<SyncRuntimeHandle<UserInstance>>);

pub type ContractSyncRuntime = SyncRuntime<UserContractInstance>;

pub struct ServiceSyncRuntime {
    runtime: SyncRuntime<UserServiceInstance>,
    current_context: QueryContext,
}

#[derive(Debug)]
pub struct SyncRuntimeHandle<UserInstance: WithContext>(
    Arc<Mutex<SyncRuntimeInternal<UserInstance>>>,
);

pub type ContractSyncRuntimeHandle = SyncRuntimeHandle<UserContractInstance>;
pub type ServiceSyncRuntimeHandle = SyncRuntimeHandle<UserServiceInstance>;

/// Runtime data tracked during the execution of a transaction on the synchronous thread.
#[derive(Debug)]
pub struct SyncRuntimeInternal<UserInstance: WithContext> {
    /// The current chain ID.
    chain_id: ChainId,
    /// The height of the next block that will be added to this chain. During operations
    /// and messages, this is the current block height.
    height: BlockHeight,
    /// The current consensus round. Only available during block validation in multi-leader rounds.
    round: Option<u32>,
    /// The authenticated signer of the operation or message, if any.
    #[debug(skip_if = Option::is_none)]
    authenticated_signer: Option<AccountOwner>,
    /// The current message being executed, if there is one.
    #[debug(skip_if = Option::is_none)]
    executing_message: Option<ExecutingMessage>,

    /// How to interact with the storage view of the execution state.
    execution_state_sender: ExecutionStateSender,

    /// If applications are being finalized.
    ///
    /// If [`true`], disables cross-application calls.
    is_finalizing: bool,
    /// Applications that need to be finalized.
    applications_to_finalize: Vec<ApplicationId>,

    /// Application instances loaded in this transaction.
    loaded_applications: HashMap<ApplicationId, LoadedApplication<UserInstance>>,
    /// The current stack of application descriptions.
    call_stack: Vec<ApplicationStatus>,
    /// The set of the IDs of the applications that are in the `call_stack`.
    active_applications: HashSet<ApplicationId>,
    /// The tracking information for this transaction.
    transaction_tracker: TransactionTracker,
    /// The operations scheduled during this query.
    scheduled_operations: Vec<Operation>,

    /// Track application states based on views.
    view_user_states: BTreeMap<ApplicationId, ViewUserState>,

    /// The deadline this runtime should finish executing.
    ///
    /// Used to limit the execution time of services running as oracles.
    deadline: Option<Instant>,

    /// Where to send a refund for the unused part of the grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    refund_grant_to: Option<Account>,
    /// Controller to track fuel and storage consumption.
    resource_controller: ResourceController,
    /// Additional context for the runtime.
    user_context: UserInstance::UserContext,
}

/// The runtime status of an application.
#[derive(Debug)]
struct ApplicationStatus {
    /// The caller application ID, if forwarded during the call.
    caller_id: Option<ApplicationId>,
    /// The application ID.
    id: ApplicationId,
    /// The application description.
    description: ApplicationDescription,
    /// The authenticated signer for the execution thread, if any.
    signer: Option<AccountOwner>,
}

/// A loaded application instance.
#[derive(Debug)]
struct LoadedApplication<Instance> {
    instance: Arc<Mutex<Instance>>,
    description: ApplicationDescription,
}

impl<Instance> LoadedApplication<Instance> {
    /// Creates a new [`LoadedApplication`] entry from the `instance` and its `description`.
    fn new(instance: Instance, description: ApplicationDescription) -> Self {
        LoadedApplication {
            instance: Arc::new(Mutex::new(instance)),
            description,
        }
    }
}

impl<Instance> Clone for LoadedApplication<Instance> {
    // Manual implementation is needed to prevent the derive macro from adding an `Instance: Clone`
    // bound
    fn clone(&self) -> Self {
        LoadedApplication {
            instance: self.instance.clone(),
            description: self.description.clone(),
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
    /// The contains-keys queries in progress.
    contains_keys_queries: QueryManager<Vec<bool>>,
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
        self.contains_keys_queries.force_all()?;
        self.read_value_queries.force_all()?;
        self.read_multi_values_queries.force_all()?;
        self.find_keys_queries.force_all()?;
        self.find_key_values_queries.force_all()?;
        Ok(())
    }
}

impl<UserInstance: WithContext> Deref for SyncRuntime<UserInstance> {
    type Target = SyncRuntimeHandle<UserInstance>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(
            "`SyncRuntime` should not be used after its `inner` contents have been moved out",
        )
    }
}

impl<UserInstance: WithContext> DerefMut for SyncRuntime<UserInstance> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(
            "`SyncRuntime` should not be used after its `inner` contents have been moved out",
        )
    }
}

impl<UserInstance: WithContext> Drop for SyncRuntime<UserInstance> {
    fn drop(&mut self) {
        // Ensure the `loaded_applications` are cleared to prevent circular references in
        // the runtime
        if let Some(handle) = self.0.take() {
            handle.inner().loaded_applications.clear();
        }
    }
}

impl<UserInstance: WithContext> SyncRuntimeInternal<UserInstance> {
    #[expect(clippy::too_many_arguments)]
    fn new(
        chain_id: ChainId,
        height: BlockHeight,
        round: Option<u32>,
        authenticated_signer: Option<AccountOwner>,
        executing_message: Option<ExecutingMessage>,
        execution_state_sender: ExecutionStateSender,
        deadline: Option<Instant>,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        transaction_tracker: TransactionTracker,
        user_context: UserInstance::UserContext,
    ) -> Self {
        Self {
            chain_id,
            height,
            round,
            authenticated_signer,
            executing_message,
            execution_state_sender,
            is_finalizing: false,
            applications_to_finalize: Vec::new(),
            loaded_applications: HashMap::new(),
            call_stack: Vec::new(),
            active_applications: HashSet::new(),
            view_user_states: BTreeMap::new(),
            deadline,
            refund_grant_to,
            resource_controller,
            transaction_tracker,
            scheduled_operations: Vec::new(),
            user_context,
        }
    }

    /// Returns the [`ApplicationStatus`] of the current application.
    ///
    /// The current application is the last to be pushed to the `call_stack`.
    ///
    /// # Panics
    ///
    /// If the call stack is empty.
    fn current_application(&self) -> &ApplicationStatus {
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
        application_id: ApplicationId,
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
        this: SyncRuntimeHandle<UserContractInstance>,
        id: ApplicationId,
    ) -> Result<LoadedApplication<UserContractInstance>, ExecutionError> {
        match self.loaded_applications.entry(id) {
            // TODO(#2927): support dynamic loading of modules on the Web
            #[cfg(web)]
            hash_map::Entry::Vacant(_) => {
                drop(this);
                Err(ExecutionError::UnsupportedDynamicApplicationLoad(Box::new(
                    id,
                )))
            }
            #[cfg(not(web))]
            hash_map::Entry::Vacant(entry) => {
                let txn_tracker_moved = mem::take(&mut self.transaction_tracker);
                let (code, description, txn_tracker_moved) = self
                    .execution_state_sender
                    .send_request(move |callback| ExecutionRequest::LoadContract {
                        id,
                        callback,
                        txn_tracker: txn_tracker_moved,
                    })?
                    .recv_response()?;
                self.transaction_tracker = txn_tracker_moved;

                let instance = code.instantiate(this)?;

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
        this: ContractSyncRuntimeHandle,
        authenticated: bool,
        callee_id: ApplicationId,
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
        let timestamp = self.user_context;
        let callee_context = OperationContext {
            chain_id: self.chain_id,
            authenticated_signer,
            authenticated_caller_id,
            height: self.height,
            round: self.round,
            timestamp,
        };
        self.push_application(ApplicationStatus {
            caller_id: authenticated_caller_id,
            id: callee_id,
            description: application.description,
            // Allow further nested calls to be authenticated if this one is.
            signer: authenticated_signer,
        });
        Ok((application.instance, callee_context))
    }

    /// Cleans up the runtime after the execution of a call to a different contract.
    fn finish_call(&mut self) -> Result<(), ExecutionError> {
        self.pop_application();
        Ok(())
    }

    /// Runs the service in a separate thread as an oracle.
    fn run_service_oracle_query(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let context = QueryContext {
            chain_id: self.chain_id,
            next_block_height: self.height,
            local_time: self.transaction_tracker.local_time(),
        };
        let sender = self.execution_state_sender.clone();

        let txn_tracker = TransactionTracker::default()
            .with_blobs(self.transaction_tracker.created_blobs().clone());

        let timeout = self
            .resource_controller
            .remaining_service_oracle_execution_time()?;
        let execution_start = Instant::now();
        let deadline = Some(execution_start + timeout);

        let mut service_runtime =
            ServiceSyncRuntime::new_with_txn_tracker(sender, context, deadline, txn_tracker);

        let result = service_runtime.run_query(application_id, query);

        // Always track the execution time, irrespective to whether the service ran successfully or
        // timed out
        self.resource_controller
            .track_service_oracle_execution(execution_start.elapsed())?;

        let QueryOutcome {
            response,
            operations,
        } = result?;

        self.resource_controller
            .track_service_oracle_response(response.len())?;

        self.scheduled_operations.extend(operations);
        Ok(response)
    }
}

impl SyncRuntimeInternal<UserServiceInstance> {
    /// Initializes a service instance with this runtime.
    fn load_service_instance(
        &mut self,
        this: ServiceSyncRuntimeHandle,
        id: ApplicationId,
    ) -> Result<LoadedApplication<UserServiceInstance>, ExecutionError> {
        match self.loaded_applications.entry(id) {
            // TODO(#2927): support dynamic loading of modules on the Web
            #[cfg(web)]
            hash_map::Entry::Vacant(_) => {
                drop(this);
                Err(ExecutionError::UnsupportedDynamicApplicationLoad(Box::new(
                    id,
                )))
            }
            #[cfg(not(web))]
            hash_map::Entry::Vacant(entry) => {
                let txn_tracker_moved = mem::take(&mut self.transaction_tracker);
                let (code, description, txn_tracker_moved) = self
                    .execution_state_sender
                    .send_request(move |callback| ExecutionRequest::LoadService {
                        id,
                        callback,
                        txn_tracker: txn_tracker_moved,
                    })?
                    .recv_response()?;
                self.transaction_tracker = txn_tracker_moved;

                let instance = code.instantiate(this)?;
                Ok(entry
                    .insert(LoadedApplication::new(instance, description))
                    .clone())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }
}

impl<UserInstance: WithContext> SyncRuntime<UserInstance> {
    fn into_inner(mut self) -> Option<SyncRuntimeInternal<UserInstance>> {
        let handle = self.0.take().expect(
            "`SyncRuntime` should not be used after its `inner` contents have been moved out",
        );
        let runtime = Arc::into_inner(handle.0)?
            .into_inner()
            .expect("`SyncRuntime` should run in a single thread which should not panic");
        Some(runtime)
    }
}

impl<UserInstance: WithContext> From<SyncRuntimeInternal<UserInstance>>
    for SyncRuntimeHandle<UserInstance>
{
    fn from(runtime: SyncRuntimeInternal<UserInstance>) -> Self {
        SyncRuntimeHandle(Arc::new(Mutex::new(runtime)))
    }
}

impl<UserInstance: WithContext> SyncRuntimeHandle<UserInstance> {
    fn inner(&self) -> std::sync::MutexGuard<'_, SyncRuntimeInternal<UserInstance>> {
        self.0
            .try_lock()
            .expect("Synchronous runtimes run on a single execution thread")
    }
}

impl<UserInstance: WithContext> BaseRuntime for SyncRuntimeHandle<UserInstance>
where
    Self: ContractOrServiceRuntime,
{
    type Read = ();
    type ReadValueBytes = u32;
    type ContainsKey = u32;
    type ContainsKeys = u32;
    type ReadMultiValuesBytes = u32;
    type FindKeysByPrefix = u32;
    type FindKeyValuesByPrefix = u32;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        Ok(self.inner().chain_id)
    }

    fn block_height(&mut self) -> Result<BlockHeight, ExecutionError> {
        Ok(self.inner().height)
    }

    fn application_id(&mut self) -> Result<ApplicationId, ExecutionError> {
        Ok(self.inner().current_application().id)
    }

    fn application_creator_chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        Ok(self
            .inner()
            .current_application()
            .description
            .creator_chain_id)
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        Ok(self
            .inner()
            .current_application()
            .description
            .parameters
            .clone())
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::SystemTimestamp { callback })?
            .recv_response()
    }

    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::ChainBalance { callback })?
            .recv_response()
    }

    fn read_owner_balance(&mut self, owner: AccountOwner) -> Result<Amount, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::OwnerBalance { owner, callback })?
            .recv_response()
    }

    fn read_owner_balances(&mut self) -> Result<Vec<(AccountOwner, Amount)>, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::OwnerBalances { callback })?
            .recv_response()
    }

    fn read_balance_owners(&mut self) -> Result<Vec<AccountOwner>, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::BalanceOwners { callback })?
            .recv_response()
    }

    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError> {
        self.inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::ChainOwnership { callback })?
            .recv_response()
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::ContainsKey { id, key, callback })?;
        let state = this.view_user_states.entry(id).or_default();
        state.contains_key_queries.register(receiver)
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let state = this.view_user_states.entry(id).or_default();
        let value = state.contains_key_queries.wait(*promise)?;
        Ok(value)
    }

    fn contains_keys_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ContainsKeys, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::ContainsKeys { id, keys, callback })?;
        let state = this.view_user_states.entry(id).or_default();
        state.contains_keys_queries.register(receiver)
    }

    fn contains_keys_wait(
        &mut self,
        promise: &Self::ContainsKeys,
    ) -> Result<Vec<bool>, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let state = this.view_user_states.entry(id).or_default();
        let value = state.contains_keys_queries.wait(*promise)?;
        Ok(value)
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this.execution_state_sender.send_request(move |callback| {
            ExecutionRequest::ReadMultiValuesBytes { id, keys, callback }
        })?;
        let state = this.view_user_states.entry(id).or_default();
        state.read_multi_values_queries.register(receiver)
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let state = this.view_user_states.entry(id).or_default();
        let values = state.read_multi_values_queries.wait(*promise)?;
        for value in &values {
            if let Some(value) = &value {
                this.resource_controller
                    .track_bytes_read(value.len() as u64)?;
            }
        }
        Ok(values)
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::ReadValueBytes { id, key, callback })?;
        let state = this.view_user_states.entry(id).or_default();
        state.read_value_queries.register(receiver)
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let value = {
            let state = this.view_user_states.entry(id).or_default();
            state.read_value_queries.wait(*promise)?
        };
        if let Some(value) = &value {
            this.resource_controller
                .track_bytes_read(value.len() as u64)?;
        }
        Ok(value)
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this.execution_state_sender.send_request(move |callback| {
            ExecutionRequest::FindKeysByPrefix {
                id,
                key_prefix,
                callback,
            }
        })?;
        let state = this.view_user_states.entry(id).or_default();
        state.find_keys_queries.register(receiver)
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let keys = {
            let state = this.view_user_states.entry(id).or_default();
            state.find_keys_queries.wait(*promise)?
        };
        let mut read_size = 0;
        for key in &keys {
            read_size += key.len();
        }
        this.resource_controller
            .track_bytes_read(read_size as u64)?;
        Ok(keys)
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        this.resource_controller.track_read_operations(1)?;
        let receiver = this.execution_state_sender.send_request(move |callback| {
            ExecutionRequest::FindKeyValuesByPrefix {
                id,
                key_prefix,
                callback,
            }
        })?;
        let state = this.view_user_states.entry(id).or_default();
        state.find_key_values_queries.register(receiver)
    }

    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
        let state = this.view_user_states.entry(id).or_default();
        let key_values = state.find_key_values_queries.wait(*promise)?;
        let mut read_size = 0;
        for (key, value) in &key_values {
            read_size += key.len() + value.len();
        }
        this.resource_controller
            .track_bytes_read(read_size as u64)?;
        Ok(key_values)
    }

    fn perform_http_request(
        &mut self,
        request: http::Request,
    ) -> Result<http::Response, ExecutionError> {
        let mut this = self.inner();
        let app_permissions = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::GetApplicationPermissions { callback })?
            .recv_response()?;

        let app_id = this.current_application().id;
        ensure!(
            app_permissions.can_make_http_requests(&app_id),
            ExecutionError::UnauthorizedApplication(app_id)
        );

        this.resource_controller.track_http_request()?;

        let response =
            if let Some(response) = this.transaction_tracker.next_replayed_oracle_response()? {
                match response {
                    OracleResponse::Http(response) => response,
                    _ => return Err(ExecutionError::OracleResponseMismatch),
                }
            } else {
                this.execution_state_sender
                    .send_request(|callback| ExecutionRequest::PerformHttpRequest {
                        request,
                        http_responses_are_oracle_responses:
                            Self::LIMIT_HTTP_RESPONSE_SIZE_TO_ORACLE_RESPONSE_SIZE,
                        callback,
                    })?
                    .recv_response()?
            };
        this.transaction_tracker
            .add_oracle_response(OracleResponse::Http(response.clone()));
        Ok(response)
    }

    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        if !this
            .transaction_tracker
            .replay_oracle_response(OracleResponse::Assert)?
        {
            // There are no recorded oracle responses, so we check the local time.
            let local_time = this.transaction_tracker.local_time();
            ensure!(
                local_time < timestamp,
                ExecutionError::AssertBefore {
                    timestamp,
                    local_time,
                }
            );
        }
        Ok(())
    }

    fn read_data_blob(&mut self, hash: &CryptoHash) -> Result<Vec<u8>, ExecutionError> {
        let mut this = self.inner();
        let blob_id = BlobId::new(*hash, BlobType::Data);
        let (blob_content, is_new) = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::ReadBlobContent { blob_id, callback })?
            .recv_response()?;
        if is_new {
            this.transaction_tracker
                .replay_oracle_response(OracleResponse::Blob(blob_id))?;
        }
        Ok(blob_content.into_bytes().into_vec())
    }

    fn assert_data_blob_exists(&mut self, hash: &CryptoHash) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let blob_id = BlobId::new(*hash, BlobType::Data);
        let is_new = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::AssertBlobExists { blob_id, callback })?
            .recv_response()?;
        if is_new {
            this.transaction_tracker
                .replay_oracle_response(OracleResponse::Blob(blob_id))?;
        }
        Ok(())
    }
}

/// An extension trait to determine in compile time the different behaviors between contract and
/// services in the implementation of [`BaseRuntime`].
trait ContractOrServiceRuntime {
    /// Configured to `true` if the HTTP response size should be limited to the oracle response
    /// size.
    ///
    /// This is `false` for services, potentially allowing them to receive a larger HTTP response
    /// and only storing in the block a shorter oracle response.
    const LIMIT_HTTP_RESPONSE_SIZE_TO_ORACLE_RESPONSE_SIZE: bool;
}

impl ContractOrServiceRuntime for ContractSyncRuntimeHandle {
    const LIMIT_HTTP_RESPONSE_SIZE_TO_ORACLE_RESPONSE_SIZE: bool = true;
}

impl ContractOrServiceRuntime for ServiceSyncRuntimeHandle {
    const LIMIT_HTTP_RESPONSE_SIZE_TO_ORACLE_RESPONSE_SIZE: bool = false;
}

impl<UserInstance: WithContext> Clone for SyncRuntimeHandle<UserInstance> {
    fn clone(&self) -> Self {
        SyncRuntimeHandle(self.0.clone())
    }
}

impl ContractSyncRuntime {
    pub(crate) fn new(
        execution_state_sender: ExecutionStateSender,
        chain_id: ChainId,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        action: &UserAction,
        txn_tracker: TransactionTracker,
    ) -> Self {
        SyncRuntime(Some(ContractSyncRuntimeHandle::from(
            SyncRuntimeInternal::new(
                chain_id,
                action.height(),
                action.round(),
                action.signer(),
                if let UserAction::Message(context, _) = action {
                    Some(context.into())
                } else {
                    None
                },
                execution_state_sender,
                None,
                refund_grant_to,
                resource_controller,
                txn_tracker,
                action.timestamp(),
            ),
        )))
    }

    pub(crate) fn preload_contract(
        &self,
        id: ApplicationId,
        code: UserContractCode,
        description: ApplicationDescription,
    ) -> Result<(), ExecutionError> {
        let this = self
            .0
            .as_ref()
            .expect("contracts shouldn't be preloaded while the runtime is being dropped");
        let runtime_handle = this.clone();
        let mut this_guard = this.inner();

        if let hash_map::Entry::Vacant(entry) = this_guard.loaded_applications.entry(id) {
            entry.insert(LoadedApplication::new(
                code.instantiate(runtime_handle)?,
                description,
            ));
            this_guard.applications_to_finalize.push(id);
        }

        Ok(())
    }

    /// Main entry point to start executing a user action.
    pub(crate) fn run_action(
        mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        action: UserAction,
    ) -> Result<(Option<Vec<u8>>, ResourceController, TransactionTracker), ExecutionError> {
        let result = self
            .deref_mut()
            .run_action(application_id, chain_id, action)?;
        let runtime = self
            .into_inner()
            .expect("Runtime clones should have been freed by now");
        Ok((
            result,
            runtime.resource_controller,
            runtime.transaction_tracker,
        ))
    }
}

impl ContractSyncRuntimeHandle {
    fn run_action(
        &mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        action: UserAction,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let finalize_context = FinalizeContext {
            authenticated_signer: action.signer(),
            chain_id,
            height: action.height(),
            round: action.round(),
        };

        {
            let runtime = self.inner();
            assert_eq!(runtime.authenticated_signer, action.signer());
            assert_eq!(runtime.chain_id, chain_id);
            assert_eq!(runtime.height, action.height());
        }

        let signer = action.signer();
        let closure = move |code: &mut UserContractInstance| match action {
            UserAction::Instantiate(_context, argument) => {
                code.instantiate(argument).map(|()| None)
            }
            UserAction::Operation(_context, operation) => {
                code.execute_operation(operation).map(Option::Some)
            }
            UserAction::Message(_context, message) => code.execute_message(message).map(|()| None),
            UserAction::ProcessStreams(_context, updates) => {
                code.process_streams(updates).map(|()| None)
            }
        };

        let result = self.execute(application_id, signer, closure)?;
        self.finalize(finalize_context)?;
        Ok(result)
    }

    /// Notifies all loaded applications that execution is finalizing.
    fn finalize(&mut self, context: FinalizeContext) -> Result<(), ExecutionError> {
        let applications = mem::take(&mut self.inner().applications_to_finalize)
            .into_iter()
            .rev();

        self.inner().is_finalizing = true;

        for application in applications {
            self.execute(application, context.authenticated_signer, |contract| {
                contract.finalize().map(|_| None)
            })?;
            self.inner().loaded_applications.remove(&application);
        }

        Ok(())
    }

    /// Executes a `closure` with the contract code for the `application_id`.
    fn execute(
        &mut self,
        application_id: ApplicationId,
        signer: Option<AccountOwner>,
        closure: impl FnOnce(&mut UserContractInstance) -> Result<Option<Vec<u8>>, ExecutionError>,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let contract = {
            let mut runtime = self.inner();
            let application = runtime.load_contract_instance(self.clone(), application_id)?;

            let status = ApplicationStatus {
                caller_id: None,
                id: application_id,
                description: application.description.clone(),
                signer,
            };

            runtime.push_application(status);

            application
        };

        let result = closure(
            &mut contract
                .instance
                .try_lock()
                .expect("Application should not be already executing"),
        )?;

        let mut runtime = self.inner();
        let application_status = runtime.pop_application();
        assert_eq!(application_status.caller_id, None);
        assert_eq!(application_status.id, application_id);
        assert_eq!(application_status.description, contract.description);
        assert_eq!(application_status.signer, signer);
        assert!(runtime.call_stack.is_empty());

        Ok(result)
    }
}

impl ContractRuntime for ContractSyncRuntimeHandle {
    fn authenticated_signer(&mut self) -> Result<Option<AccountOwner>, ExecutionError> {
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

    fn authenticated_caller_id(&mut self) -> Result<Option<ApplicationId>, ExecutionError> {
        let this = self.inner();
        if this.call_stack.len() <= 1 {
            return Ok(None);
        }
        Ok(this.current_application().caller_id)
    }

    fn maximum_fuel_per_block(&mut self, vm_runtime: VmRuntime) -> Result<u64, ExecutionError> {
        let policy = &self.inner().resource_controller.policy;
        Ok(match vm_runtime {
            VmRuntime::Wasm => policy.maximum_wasm_fuel_per_block,
            VmRuntime::Evm => policy.maximum_evm_fuel_per_block,
        })
    }

    fn remaining_fuel(&mut self, vm_runtime: VmRuntime) -> Result<u64, ExecutionError> {
        Ok(self.inner().resource_controller.remaining_fuel(vm_runtime))
    }

    fn consume_fuel(&mut self, fuel: u64, vm_runtime: VmRuntime) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        this.resource_controller.track_fuel(fuel, vm_runtime)
    }

    fn send_message(&mut self, message: SendMessageRequest<Vec<u8>>) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application = this.current_application();
        let application_id = application.id;
        let authenticated_signer = application.signer;
        let mut refund_grant_to = this.refund_grant_to;

        let grant = this
            .resource_controller
            .policy
            .total_price(&message.grant)?;
        if grant.is_zero() {
            refund_grant_to = None;
        } else {
            this.resource_controller.track_grant(grant)?;
        }
        let kind = if message.is_tracked {
            MessageKind::Tracked
        } else {
            MessageKind::Simple
        };

        this.transaction_tracker
            .add_outgoing_message(OutgoingMessage {
                destination: message.destination,
                authenticated_signer,
                refund_grant_to,
                grant,
                kind,
                message: Message::User {
                    application_id,
                    bytes: message.message,
                },
            })?;

        Ok(())
    }

    fn transfer(
        &mut self,
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let current_application = this.current_application();
        let application_id = current_application.id;
        let signer = current_application.signer;

        let maybe_message = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::Transfer {
                source,
                destination,
                amount,
                signer,
                application_id,
                callback,
            })?
            .recv_response()?;

        this.transaction_tracker
            .add_outgoing_messages(maybe_message)?;
        Ok(())
    }

    fn claim(
        &mut self,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let current_application = this.current_application();
        let application_id = current_application.id;
        let signer = current_application.signer;

        let message = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::Claim {
                source,
                destination,
                amount,
                signer,
                application_id,
                callback,
            })?
            .recv_response()?;
        this.transaction_tracker.add_outgoing_message(message)?;
        Ok(())
    }

    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (contract, _context) =
            self.inner()
                .prepare_for_call(self.clone(), authenticated, callee_id)?;

        let value = contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .execute_operation(argument)?;

        self.inner().finish_call()?;

        Ok(value)
    }

    fn emit(&mut self, stream_name: StreamName, value: Vec<u8>) -> Result<u32, ExecutionError> {
        let mut this = self.inner();
        ensure!(
            stream_name.0.len() <= MAX_STREAM_NAME_LEN,
            ExecutionError::StreamNameTooLong
        );
        let application_id = GenericApplicationId::User(this.current_application().id);
        let stream_id = StreamId {
            stream_name,
            application_id,
        };
        let index = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::NextEventIndex {
                stream_id: stream_id.clone(),
                callback,
            })?
            .recv_response()?;
        // TODO(#365): Consider separate event fee categories.
        this.resource_controller
            .track_bytes_written(value.len() as u64)?;
        this.transaction_tracker.add_event(stream_id, index, value);
        Ok(index)
    }

    fn read_event(
        &mut self,
        chain_id: ChainId,
        stream_name: StreamName,
        index: u32,
    ) -> Result<Vec<u8>, ExecutionError> {
        let mut this = self.inner();
        ensure!(
            stream_name.0.len() <= MAX_STREAM_NAME_LEN,
            ExecutionError::StreamNameTooLong
        );
        let application_id = GenericApplicationId::User(this.current_application().id);
        let stream_id = StreamId {
            stream_name,
            application_id,
        };
        let event_id = EventId {
            stream_id,
            index,
            chain_id,
        };
        let event = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::ReadEvent {
                event_id: event_id.clone(),
                callback,
            })?
            .recv_response()?;
        // TODO(#365): Consider separate event fee categories.
        this.resource_controller
            .track_bytes_read(event.len() as u64)?;
        this.transaction_tracker
            .replay_oracle_response(OracleResponse::Event(event_id, event.clone()))?;
        Ok(event)
    }

    fn subscribe_to_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
    ) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        ensure!(
            stream_name.0.len() <= MAX_STREAM_NAME_LEN,
            ExecutionError::StreamNameTooLong
        );
        let stream_id = StreamId {
            stream_name,
            application_id: application_id.into(),
        };
        let subscriber_app_id = this.current_application().id;
        let next_index = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::SubscribeToEvents {
                chain_id,
                stream_id: stream_id.clone(),
                subscriber_app_id,
                callback,
            })?
            .recv_response()?;
        this.transaction_tracker.add_stream_to_process(
            subscriber_app_id,
            chain_id,
            stream_id,
            0,
            next_index,
        );
        Ok(())
    }

    fn unsubscribe_from_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
    ) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        ensure!(
            stream_name.0.len() <= MAX_STREAM_NAME_LEN,
            ExecutionError::StreamNameTooLong
        );
        let stream_id = StreamId {
            stream_name,
            application_id: application_id.into(),
        };
        let subscriber_app_id = this.current_application().id;
        this.execution_state_sender
            .send_request(|callback| ExecutionRequest::UnsubscribeFromEvents {
                chain_id,
                stream_id: stream_id.clone(),
                subscriber_app_id,
                callback,
            })?
            .recv_response()?;
        this.transaction_tracker
            .remove_stream_to_process(application_id, chain_id, stream_id);
        Ok(())
    }

    fn query_service(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let mut this = self.inner();

        let app_permissions = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::GetApplicationPermissions { callback })?
            .recv_response()?;

        let app_id = this.current_application().id;
        ensure!(
            app_permissions.can_call_services(&app_id),
            ExecutionError::UnauthorizedApplication(app_id)
        );

        this.resource_controller.track_service_oracle_call()?;
        let response =
            if let Some(response) = this.transaction_tracker.next_replayed_oracle_response()? {
                match response {
                    OracleResponse::Service(bytes) => bytes,
                    _ => return Err(ExecutionError::OracleResponseMismatch),
                }
            } else {
                this.run_service_oracle_query(application_id, query)?
            };

        this.transaction_tracker
            .add_oracle_response(OracleResponse::Service(response.clone()));

        Ok(response)
    }

    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<ChainId, ExecutionError> {
        let parent_id = self.inner().chain_id;
        let block_height = self.block_height()?;

        let txn_tracker_moved = mem::take(&mut self.inner().transaction_tracker);
        let timestamp = self.inner().user_context;

        let (chain_id, txn_tracker_moved) = self
            .inner()
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::OpenChain {
                ownership,
                balance,
                parent_id,
                block_height,
                timestamp,
                application_permissions,
                callback,
                txn_tracker: txn_tracker_moved,
            })?
            .recv_response()?;

        self.inner().transaction_tracker = txn_tracker_moved;

        Ok(chain_id)
    }

    fn close_chain(&mut self) -> Result<(), ExecutionError> {
        let this = self.inner();
        let application_id = this.current_application().id;
        this.execution_state_sender
            .send_request(|callback| ExecutionRequest::CloseChain {
                application_id,
                callback,
            })?
            .recv_response()?
    }

    fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<(), ExecutionError> {
        let this = self.inner();
        let application_id = this.current_application().id;
        this.execution_state_sender
            .send_request(|callback| ExecutionRequest::ChangeApplicationPermissions {
                application_id,
                application_permissions,
                callback,
            })?
            .recv_response()?
    }

    fn create_application(
        &mut self,
        module_id: ModuleId,
        parameters: Vec<u8>,
        argument: Vec<u8>,
        required_application_ids: Vec<ApplicationId>,
    ) -> Result<ApplicationId, ExecutionError> {
        let chain_id = self.inner().chain_id;
        let block_height = self.block_height()?;

        let txn_tracker_moved = mem::take(&mut self.inner().transaction_tracker);

        let CreateApplicationResult {
            app_id,
            txn_tracker: txn_tracker_moved,
        } = self
            .inner()
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::CreateApplication {
                chain_id,
                block_height,
                module_id,
                parameters,
                required_application_ids,
                callback,
                txn_tracker: txn_tracker_moved,
            })?
            .recv_response()??;

        self.inner().transaction_tracker = txn_tracker_moved;

        let (contract, _context) = self.inner().prepare_for_call(self.clone(), true, app_id)?;

        contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .instantiate(argument)?;

        self.inner().finish_call()?;

        Ok(app_id)
    }

    fn validation_round(&mut self) -> Result<Option<u32>, ExecutionError> {
        let mut this = self.inner();
        let round =
            if let Some(response) = this.transaction_tracker.next_replayed_oracle_response()? {
                match response {
                    OracleResponse::Round(round) => round,
                    _ => return Err(ExecutionError::OracleResponseMismatch),
                }
            } else {
                this.round
            };
        this.transaction_tracker
            .add_oracle_response(OracleResponse::Round(round));
        Ok(round)
    }

    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let id = this.current_application().id;
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
            .send_request(|callback| ExecutionRequest::WriteBatch {
                id,
                batch,
                callback,
            })?
            .recv_response()?;
        Ok(())
    }
}

impl ServiceSyncRuntime {
    /// Creates a new [`ServiceSyncRuntime`] ready to execute using a provided [`QueryContext`].
    pub fn new(execution_state_sender: ExecutionStateSender, context: QueryContext) -> Self {
        let mut txn_tracker = TransactionTracker::default();
        txn_tracker.set_local_time(context.local_time);
        Self::new_with_txn_tracker(execution_state_sender, context, None, txn_tracker)
    }

    /// Creates a new [`ServiceSyncRuntime`] ready to execute using a provided [`QueryContext`].
    pub fn new_with_txn_tracker(
        execution_state_sender: ExecutionStateSender,
        context: QueryContext,
        deadline: Option<Instant>,
        txn_tracker: TransactionTracker,
    ) -> Self {
        let runtime = SyncRuntime(Some(
            SyncRuntimeInternal::new(
                context.chain_id,
                context.next_block_height,
                None,
                None,
                None,
                execution_state_sender,
                deadline,
                None,
                ResourceController::default(),
                txn_tracker,
                (),
            )
            .into(),
        ));

        ServiceSyncRuntime {
            runtime,
            current_context: context,
        }
    }

    /// Loads a service into the runtime's memory.
    pub(crate) fn preload_service(
        &self,
        id: ApplicationId,
        code: UserServiceCode,
        description: ApplicationDescription,
    ) -> Result<(), ExecutionError> {
        let this = self
            .runtime
            .0
            .as_ref()
            .expect("services shouldn't be preloaded while the runtime is being dropped");
        let runtime_handle = this.clone();
        let mut this_guard = this.inner();

        if let hash_map::Entry::Vacant(entry) = this_guard.loaded_applications.entry(id) {
            entry.insert(LoadedApplication::new(
                code.instantiate(runtime_handle)?,
                description,
            ));
            this_guard.applications_to_finalize.push(id);
        }

        Ok(())
    }

    /// Runs the service runtime actor, waiting for `incoming_requests` to respond to.
    pub fn run(&mut self, incoming_requests: std::sync::mpsc::Receiver<ServiceRuntimeRequest>) {
        while let Ok(request) = incoming_requests.recv() {
            let ServiceRuntimeRequest::Query {
                application_id,
                context,
                query,
                callback,
            } = request;

            self.prepare_for_query(context);

            let _ = callback.send(self.run_query(application_id, query));
        }
    }

    /// Prepares the runtime to query an application.
    pub(crate) fn prepare_for_query(&mut self, new_context: QueryContext) {
        let expected_context = QueryContext {
            local_time: new_context.local_time,
            ..self.current_context
        };

        if new_context != expected_context {
            let execution_state_sender = self.handle_mut().inner().execution_state_sender.clone();
            *self = ServiceSyncRuntime::new(execution_state_sender, new_context);
        } else {
            self.handle_mut()
                .inner()
                .transaction_tracker
                .set_local_time(new_context.local_time);
        }
    }

    /// Queries an application specified by its [`ApplicationId`].
    pub(crate) fn run_query(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let this = self.handle_mut();
        let response = this.try_query_application(application_id, query)?;
        let operations = mem::take(&mut this.inner().scheduled_operations);

        Ok(QueryOutcome {
            response,
            operations,
        })
    }

    /// Obtains the [`SyncRuntimeHandle`] stored in this [`ServiceSyncRuntime`].
    fn handle_mut(&mut self) -> &mut ServiceSyncRuntimeHandle {
        self.runtime.0.as_mut().expect(
            "`SyncRuntimeHandle` should be available while `SyncRuntime` hasn't been dropped",
        )
    }
}

impl ServiceRuntime for ServiceSyncRuntimeHandle {
    /// Note that queries are not available from writable contexts.
    fn try_query_application(
        &mut self,
        queried_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let service = {
            let mut this = self.inner();

            // Load the application.
            let application = this.load_service_instance(self.clone(), queried_id)?;
            // Make the call to user code.
            this.push_application(ApplicationStatus {
                caller_id: None,
                id: queried_id,
                description: application.description,
                signer: None,
            });
            application.instance
        };
        let response = service
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .handle_query(argument)?;
        self.inner().pop_application();
        Ok(response)
    }

    fn schedule_operation(&mut self, operation: Vec<u8>) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application_id = this.current_application().id;

        this.scheduled_operations.push(Operation::User {
            application_id,
            bytes: operation,
        });

        Ok(())
    }

    fn check_execution_time(&mut self) -> Result<(), ExecutionError> {
        if let Some(deadline) = self.inner().deadline {
            if Instant::now() >= deadline {
                return Err(ExecutionError::MaximumServiceOracleExecutionTimeExceeded);
            }
        }
        Ok(())
    }
}

/// A request to the service runtime actor.
pub enum ServiceRuntimeRequest {
    Query {
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        callback: oneshot::Sender<Result<QueryOutcome<Vec<u8>>, ExecutionError>>,
    },
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
