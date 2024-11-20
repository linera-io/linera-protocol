// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    mem,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlockHeight, OracleResponse, Resources,
        SendMessageRequest, Timestamp,
    },
    ensure,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, BlobType, ChainId, ChannelName, MessageId,
        Owner, StreamName,
    },
    ownership::ChainOwnership,
};
use linera_views::batch::Batch;
use oneshot::Receiver;

use crate::{
    execution::UserAction,
    execution_state_actor::{ExecutionRequest, ExecutionStateSender},
    resources::ResourceController,
    util::{ReceiverExt, UnboundedSenderExt},
    BaseRuntime, ContractRuntime, ExecutionError, FinalizeContext, MessageContext,
    OperationContext, QueryContext, RawExecutionOutcome, ServiceRuntime, TransactionTracker,
    UserApplicationDescription, UserApplicationId, UserContractCode, UserContractInstance,
    UserServiceCode, UserServiceInstance, MAX_EVENT_KEY_LEN, MAX_STREAM_NAME_LEN,
};

#[cfg(test)]
#[path = "unit_tests/runtime_tests.rs"]
mod tests;

#[derive(Debug)]
pub struct SyncRuntime<UserInstance>(Option<SyncRuntimeHandle<UserInstance>>);

pub type ContractSyncRuntime = SyncRuntime<UserContractInstance>;

pub struct ServiceSyncRuntime {
    runtime: SyncRuntime<UserServiceInstance>,
    current_context: QueryContext,
}

#[derive(Debug)]
pub struct SyncRuntimeHandle<UserInstance>(Arc<Mutex<SyncRuntimeInternal<UserInstance>>>);

pub type ContractSyncRuntimeHandle = SyncRuntimeHandle<UserContractInstance>;
pub type ServiceSyncRuntimeHandle = SyncRuntimeHandle<UserServiceInstance>;

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
    #[debug(skip_if = Option::is_none)]
    authenticated_signer: Option<Owner>,
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
    applications_to_finalize: Vec<UserApplicationId>,

    /// Application instances loaded in this transaction.
    loaded_applications: HashMap<UserApplicationId, LoadedApplication<UserInstance>>,
    /// The current stack of application descriptions.
    call_stack: Vec<ApplicationStatus>,
    /// The set of the IDs of the applications that are in the `call_stack`.
    active_applications: HashSet<UserApplicationId>,
    /// The tracking information for this transaction.
    transaction_tracker: TransactionTracker,

    /// Track application states based on views.
    view_user_states: BTreeMap<UserApplicationId, ViewUserState>,

    /// Where to send a refund for the unused part of the grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    refund_grant_to: Option<Account>,
    /// Controller to track fuel and storage consumption.
    resource_controller: ResourceController,
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

impl<UserInstance> Deref for SyncRuntime<UserInstance> {
    type Target = SyncRuntimeHandle<UserInstance>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect(
            "`SyncRuntime` should not be used after its `inner` contents have been moved out",
        )
    }
}

impl<UserInstance> DerefMut for SyncRuntime<UserInstance> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect(
            "`SyncRuntime` should not be used after its `inner` contents have been moved out",
        )
    }
}

impl<UserInstance> Drop for SyncRuntime<UserInstance> {
    fn drop(&mut self) {
        // Ensure the `loaded_applications` are cleared to prevent circular references in
        // the runtime
        if let Some(handle) = self.0.take() {
            handle.inner().loaded_applications.clear();
        }
    }
}

impl<UserInstance> SyncRuntimeInternal<UserInstance> {
    #[expect(clippy::too_many_arguments)]
    fn new(
        chain_id: ChainId,
        height: BlockHeight,
        local_time: Timestamp,
        authenticated_signer: Option<Owner>,
        executing_message: Option<ExecutingMessage>,
        execution_state_sender: ExecutionStateSender,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        transaction_tracker: TransactionTracker,
    ) -> Self {
        Self {
            chain_id,
            height,
            local_time,
            authenticated_signer,
            executing_message,
            execution_state_sender,
            is_finalizing: false,
            applications_to_finalize: Vec::new(),
            loaded_applications: HashMap::new(),
            call_stack: Vec::new(),
            active_applications: HashSet::new(),
            view_user_states: BTreeMap::new(),
            refund_grant_to,
            resource_controller,
            transaction_tracker,
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
        this: SyncRuntimeHandle<UserContractInstance>,
        id: UserApplicationId,
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
                let (code, description) = self
                    .execution_state_sender
                    .send_request(|callback| ExecutionRequest::LoadContract { id, callback })?
                    .recv_response()?;

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

        self.transaction_tracker
            .add_user_outcome(application_id, outcome)?;
        Ok(())
    }
}

impl SyncRuntimeInternal<UserServiceInstance> {
    /// Initializes a service instance with this runtime.
    fn load_service_instance(
        &mut self,
        this: ServiceSyncRuntimeHandle,
        id: UserApplicationId,
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
                let (code, description) = self
                    .execution_state_sender
                    .send_request(|callback| ExecutionRequest::LoadService { id, callback })?
                    .recv_response()?;

                let instance = code.instantiate(this)?;
                Ok(entry
                    .insert(LoadedApplication::new(instance, description))
                    .clone())
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }
}

impl<UserInstance> SyncRuntime<UserInstance> {
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

impl<UserInstance> From<SyncRuntimeInternal<UserInstance>> for SyncRuntimeHandle<UserInstance> {
    fn from(runtime: SyncRuntimeInternal<UserInstance>) -> Self {
        SyncRuntimeHandle(Arc::new(Mutex::new(runtime)))
    }
}

impl<UserInstance> SyncRuntimeHandle<UserInstance> {
    fn inner(&self) -> std::sync::MutexGuard<'_, SyncRuntimeInternal<UserInstance>> {
        self.0
            .try_lock()
            .expect("Synchronous runtimes run on a single execution thread")
    }
}

impl<UserInstance> BaseRuntime for SyncRuntimeHandle<UserInstance> {
    type Read = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::Read;
    type ReadValueBytes = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ReadValueBytes;
    type ContainsKey = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ContainsKey;
    type ContainsKeys = <SyncRuntimeInternal<UserInstance> as BaseRuntime>::ContainsKeys;
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

    fn application_creator_chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner().application_creator_chain_id()
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

    fn read_owner_balance(&mut self, owner: AccountOwner) -> Result<Amount, ExecutionError> {
        self.inner().read_owner_balance(owner)
    }

    fn read_owner_balances(&mut self) -> Result<Vec<(AccountOwner, Amount)>, ExecutionError> {
        self.inner().read_owner_balances()
    }

    fn read_balance_owners(&mut self) -> Result<Vec<AccountOwner>, ExecutionError> {
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

    fn contains_keys_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ContainsKeys, ExecutionError> {
        self.inner().contains_keys_new(keys)
    }

    fn contains_keys_wait(
        &mut self,
        promise: &Self::ContainsKeys,
    ) -> Result<Vec<bool>, ExecutionError> {
        self.inner().contains_keys_wait(promise)
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

    fn read_data_blob(&mut self, hash: &CryptoHash) -> Result<Vec<u8>, ExecutionError> {
        self.inner().read_data_blob(hash)
    }

    fn assert_data_blob_exists(&mut self, hash: &CryptoHash) -> Result<(), ExecutionError> {
        self.inner().assert_data_blob_exists(hash)
    }
}

impl<UserInstance> BaseRuntime for SyncRuntimeInternal<UserInstance> {
    type Read = ();
    type ReadValueBytes = u32;
    type ContainsKey = u32;
    type ContainsKeys = u32;
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

    fn application_creator_chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        Ok(self.current_application().id.creation.chain_id)
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        Ok(self.current_application().parameters.clone())
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::SystemTimestamp { callback })?
            .recv_response()
    }

    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::ChainBalance { callback })?
            .recv_response()
    }

    fn read_owner_balance(&mut self, owner: AccountOwner) -> Result<Amount, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::OwnerBalance { owner, callback })?
            .recv_response()
    }

    fn read_owner_balances(&mut self) -> Result<Vec<(AccountOwner, Amount)>, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::OwnerBalances { callback })?
            .recv_response()
    }

    fn read_balance_owners(&mut self) -> Result<Vec<AccountOwner>, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::BalanceOwners { callback })?
            .recv_response()
    }

    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError> {
        self.execution_state_sender
            .send_request(|callback| ExecutionRequest::ChainOwnership { callback })?
            .recv_response()
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
        let receiver = self
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::ContainsKey { id, key, callback })?;
        state.contains_key_queries.register(receiver)
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let value = state.contains_key_queries.wait(*promise)?;
        Ok(value)
    }

    fn contains_keys_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ContainsKeys, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
        let receiver = self
            .execution_state_sender
            .send_request(move |callback| ExecutionRequest::ContainsKeys { id, keys, callback })?;
        state.contains_keys_queries.register(receiver)
    }

    fn contains_keys_wait(
        &mut self,
        promise: &Self::ContainsKeys,
    ) -> Result<Vec<bool>, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        let value = state.contains_keys_queries.wait(*promise)?;
        Ok(value)
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        let id = self.application_id()?;
        let state = self.view_user_states.entry(id).or_default();
        self.resource_controller.track_read_operations(1)?;
        let receiver = self.execution_state_sender.send_request(move |callback| {
            ExecutionRequest::ReadMultiValuesBytes { id, keys, callback }
        })?;
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
            .send_request(move |callback| ExecutionRequest::ReadValueBytes { id, key, callback })?;
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
            ExecutionRequest::FindKeysByPrefix {
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
            ExecutionRequest::FindKeyValuesByPrefix {
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
        ensure!(
            cfg!(feature = "unstable-oracles"),
            ExecutionError::UnstableOracle
        );
        let response =
            if let Some(response) = self.transaction_tracker.next_replayed_oracle_response()? {
                match response {
                    OracleResponse::Service(bytes) => bytes,
                    _ => return Err(ExecutionError::OracleResponseMismatch),
                }
            } else {
                let context = QueryContext {
                    chain_id: self.chain_id,
                    next_block_height: self.height,
                    local_time: self.local_time,
                };
                let sender = self.execution_state_sender.clone();
                ServiceSyncRuntime::new(sender, context).run_query(application_id, query)?
            };
        self.transaction_tracker
            .add_oracle_response(OracleResponse::Service(response.clone()));
        Ok(response)
    }

    fn http_post(
        &mut self,
        url: &str,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        ensure!(
            cfg!(feature = "unstable-oracles"),
            ExecutionError::UnstableOracle
        );
        let bytes =
            if let Some(response) = self.transaction_tracker.next_replayed_oracle_response()? {
                match response {
                    OracleResponse::Post(bytes) => bytes,
                    _ => return Err(ExecutionError::OracleResponseMismatch),
                }
            } else {
                let url = url.to_string();
                self.execution_state_sender
                    .send_request(|callback| ExecutionRequest::HttpPost {
                        url,
                        content_type,
                        payload,
                        callback,
                    })?
                    .recv_response()?
            };
        self.transaction_tracker
            .add_oracle_response(OracleResponse::Post(bytes.clone()));
        Ok(bytes)
    }

    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError> {
        if !self
            .transaction_tracker
            .replay_oracle_response(OracleResponse::Assert)?
        {
            // There are no recorded oracle responses, so we check the local time.
            ensure!(
                self.local_time < timestamp,
                ExecutionError::AssertBefore {
                    timestamp,
                    local_time: self.local_time,
                }
            );
        }
        Ok(())
    }

    fn read_data_blob(&mut self, hash: &CryptoHash) -> Result<Vec<u8>, ExecutionError> {
        let blob_id = BlobId::new(*hash, BlobType::Data);
        let (blob_content, is_new) = self
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::ReadBlobContent { blob_id, callback })?
            .recv_response()?;
        if is_new {
            self.transaction_tracker
                .replay_oracle_response(OracleResponse::Blob(blob_id))?;
        }
        Ok(blob_content.inner_bytes())
    }

    fn assert_data_blob_exists(&mut self, hash: &CryptoHash) -> Result<(), ExecutionError> {
        let blob_id = BlobId::new(*hash, BlobType::Data);
        let is_new = self
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::AssertBlobExists { blob_id, callback })?
            .recv_response()?;
        if is_new {
            self.transaction_tracker
                .replay_oracle_response(OracleResponse::Blob(blob_id))?;
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
    pub(crate) fn new(
        execution_state_sender: ExecutionStateSender,
        chain_id: ChainId,
        local_time: Timestamp,
        refund_grant_to: Option<Account>,
        resource_controller: ResourceController,
        action: &UserAction,
        txn_tracker: TransactionTracker,
    ) -> Self {
        SyncRuntime(Some(ContractSyncRuntimeHandle::from(
            SyncRuntimeInternal::new(
                chain_id,
                action.height(),
                local_time,
                action.signer(),
                if let UserAction::Message(context, _) = action {
                    Some(context.into())
                } else {
                    None
                },
                execution_state_sender,
                refund_grant_to,
                resource_controller,
                txn_tracker,
            ),
        )))
    }

    pub(crate) fn preload_contract(
        &self,
        id: UserApplicationId,
        code: UserContractCode,
        description: UserApplicationDescription,
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
        application_id: UserApplicationId,
        chain_id: ChainId,
        action: UserAction,
    ) -> Result<(ResourceController, TransactionTracker), ExecutionError> {
        let finalize_context = FinalizeContext {
            authenticated_signer: action.signer(),
            chain_id,
            height: action.height(),
        };

        {
            let runtime = self.inner();
            assert_eq!(runtime.authenticated_signer, action.signer());
            assert_eq!(runtime.chain_id, chain_id);
            assert_eq!(runtime.height, action.height());
        }
        self.execute(application_id, action.signer(), move |code| match action {
            UserAction::Instantiate(context, argument) => code.instantiate(context, argument),
            UserAction::Operation(context, operation) => {
                code.execute_operation(context, operation).map(|_| ())
            }
            UserAction::Message(context, message) => code.execute_message(context, message),
        })?;
        self.finalize(finalize_context)?;
        let runtime = self
            .into_inner()
            .expect("Runtime clones should have been freed by now");
        Ok((runtime.resource_controller, runtime.transaction_tracker))
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
            self.inner().loaded_applications.remove(&application);
        }

        Ok(())
    }

    /// Executes a `closure` with the contract code for the `application_id`.
    fn execute(
        &mut self,
        application_id: UserApplicationId,
        signer: Option<Owner>,
        closure: impl FnOnce(&mut UserContractInstance) -> Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let contract = {
            let mut runtime = self.inner();
            let application = runtime.load_contract_instance(self.clone(), application_id)?;

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
            .send_request(|callback| ExecutionRequest::Transfer {
                source,
                destination,
                amount,
                signer,
                callback,
            })?
            .recv_response()?;
        self.inner()
            .transaction_tracker
            .add_system_outcome(execution_outcome)?;
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
            .send_request(|callback| ExecutionRequest::Claim {
                source,
                destination,
                amount,
                signer,
                callback,
            })?
            .recv_response()?
            .with_authenticated_signer(signer);
        self.inner()
            .transaction_tracker
            .add_system_outcome(execution_outcome)?;
        Ok(())
    }

    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (contract, context) =
            self.inner()
                .prepare_for_call(self.clone(), authenticated, callee_id)?;

        let value = contract
            .try_lock()
            .expect("Applications should not have reentrant calls")
            .execute_operation(context, argument)?;

        self.inner().finish_call()?;

        Ok(value)
    }

    fn emit(
        &mut self,
        name: StreamName,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        ensure!(
            key.len() <= MAX_EVENT_KEY_LEN,
            ExecutionError::EventKeyTooLong
        );
        ensure!(
            name.0.len() <= MAX_STREAM_NAME_LEN,
            ExecutionError::StreamNameTooLong
        );
        let application = this.current_application_mut();
        application.outcome.events.push((name, key, value));
        Ok(())
    }

    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<(MessageId, ChainId), ExecutionError> {
        let mut this = self.inner();
        let message_id = MessageId {
            chain_id: this.chain_id,
            height: this.height,
            index: this.transaction_tracker.next_message_index(),
        };
        let chain_id = ChainId::child(message_id);
        let [open_chain_message, subscribe_message] = this
            .execution_state_sender
            .send_request(|callback| ExecutionRequest::OpenChain {
                ownership,
                balance,
                next_message_id: message_id,
                application_permissions,
                callback,
            })?
            .recv_response()?;
        let outcome = RawExecutionOutcome::default()
            .with_message(open_chain_message)
            .with_message(subscribe_message);
        this.transaction_tracker.add_system_outcome(outcome)?;
        Ok((message_id, chain_id))
    }

    fn close_chain(&mut self) -> Result<(), ExecutionError> {
        let mut this = self.inner();
        let application_id = this.current_application().id;
        this.execution_state_sender
            .send_request(|callback| ExecutionRequest::CloseChain {
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
        let runtime = SyncRuntime(Some(
            SyncRuntimeInternal::new(
                context.chain_id,
                context.next_block_height,
                context.local_time,
                None,
                None,
                execution_state_sender,
                None,
                ResourceController::default(),
                TransactionTracker::default(),
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
        id: UserApplicationId,
        code: UserServiceCode,
        description: UserApplicationDescription,
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
            self.handle_mut().inner().local_time = new_context.local_time;
        }
    }

    /// Queries an application specified by its [`UserApplicationId`].
    pub(crate) fn run_query(
        &mut self,
        application_id: UserApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.handle_mut()
            .try_query_application(application_id, query)
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
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let (query_context, service) = {
            let mut this = self.inner();

            // Load the application.
            let application = this.load_service_instance(self.clone(), queried_id)?;
            // Make the call to user code.
            let query_context = QueryContext {
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
            .send_request(|callback| ExecutionRequest::FetchUrl { url, callback })?
            .recv_response()
    }
}

/// A request to the service runtime actor.
pub enum ServiceRuntimeRequest {
    Query {
        application_id: UserApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        callback: oneshot::Sender<Result<Vec<u8>, ExecutionError>>,
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
