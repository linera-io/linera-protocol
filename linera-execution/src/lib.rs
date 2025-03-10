// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the execution of the system application and the user applications in a
//! Linera chain.

#![cfg_attr(web, feature(trait_upcasting))]
#![deny(clippy::large_futures)]

pub mod committee;
mod execution;
mod execution_state_actor;
mod graphql;
mod policy;
mod resources;
#[cfg(with_revm)]
pub mod revm;
mod runtime;
pub mod system;
#[cfg(with_testing)]
pub mod test_utils;
mod transaction_tracker;
mod util;
mod wasm;

use std::{any::Any, fmt, str::FromStr, sync::Arc};

use async_graphql::SimpleObject;
use async_trait::async_trait;
use committee::Epoch;
use custom_debug_derive::Debug;
use dashmap::DashMap;
use derive_more::Display;
#[cfg(web)]
use js_sys::wasm_bindgen::JsValue;
use linera_base::{
    abi::Abi,
    crypto::{BcsHashable, CryptoHash},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, Blob, BlockHeight, DecompressionError,
        Resources, SendMessageRequest, Timestamp, UserApplicationDescription,
    },
    doc_scalar, hex_debug, http,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, BlobType, ChainId, ChannelName, Destination,
        EventId, GenericApplicationId, MessageId, ModuleId, Owner, StreamName, UserApplicationId,
    },
    ownership::ChainOwnership,
    task,
};
use linera_views::{batch::Batch, views::ViewError};
use serde::{Deserialize, Serialize};
use system::{OpenChainConfig, SystemChannel};
use thiserror::Error;

#[cfg(with_revm)]
use crate::revm::EvmExecutionError;
use crate::runtime::ContractSyncRuntime;
#[cfg(all(with_testing, with_wasm_runtime))]
pub use crate::wasm::test as wasm_test;
#[cfg(with_wasm_runtime)]
pub use crate::wasm::{
    BaseRuntimeApi, ContractEntrypoints, ContractRuntimeApi, RuntimeApiData, ServiceEntrypoints,
    ServiceRuntimeApi, WasmContractModule, WasmExecutionError, WasmServiceModule,
};
pub use crate::{
    execution::{ExecutionStateView, ServiceRuntimeEndpoint},
    execution_state_actor::ExecutionRequest,
    policy::ResourceControlPolicy,
    resources::{ResourceController, ResourceTracker},
    runtime::{
        ContractSyncRuntimeHandle, ServiceRuntimeRequest, ServiceSyncRuntime,
        ServiceSyncRuntimeHandle,
    },
    system::{
        SystemExecutionStateView, SystemMessage, SystemOperation, SystemQuery, SystemResponse,
    },
    transaction_tracker::{TransactionOutcome, TransactionTracker},
};

/// The maximum length of an event key in bytes.
const MAX_EVENT_KEY_LEN: usize = 64;
/// The maximum length of a stream name.
const MAX_STREAM_NAME_LEN: usize = 64;

/// An implementation of [`UserContractModule`].
#[derive(Clone)]
pub struct UserContractCode(Box<dyn UserContractModule>);

/// An implementation of [`UserServiceModule`].
#[derive(Clone)]
pub struct UserServiceCode(Box<dyn UserServiceModule>);

/// An implementation of [`UserContract`].
pub type UserContractInstance = Box<dyn UserContract>;

/// An implementation of [`UserService`].
pub type UserServiceInstance = Box<dyn UserService>;

/// A factory trait to obtain a [`UserContract`] from a [`UserContractModule`]
pub trait UserContractModule: dyn_clone::DynClone + Any + task::Post + Send + Sync {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError>;
}

impl<T: UserContractModule + Send + Sync + 'static> From<T> for UserContractCode {
    fn from(module: T) -> Self {
        Self(Box::new(module))
    }
}

dyn_clone::clone_trait_object!(UserContractModule);

/// A factory trait to obtain a [`UserService`] from a [`UserServiceModule`]
pub trait UserServiceModule: dyn_clone::DynClone + Any + task::Post + Send + Sync {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError>;
}

impl<T: UserServiceModule + Send + Sync + 'static> From<T> for UserServiceCode {
    fn from(module: T) -> Self {
        Self(Box::new(module))
    }
}

dyn_clone::clone_trait_object!(UserServiceModule);

impl UserServiceCode {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        self.0.instantiate(runtime)
    }
}

impl UserContractCode {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
        self.0.instantiate(runtime)
    }
}

#[cfg(web)]
const _: () = {
    // TODO(#2775): add a vtable pointer into the JsValue rather than assuming the
    // implementor

    impl From<UserContractCode> for JsValue {
        fn from(code: UserContractCode) -> JsValue {
            let module: WasmContractModule = *(code.0 as Box<dyn Any>)
                .downcast()
                .expect("we only support Wasm modules on the Web for now");
            module.into()
        }
    }

    impl From<UserServiceCode> for JsValue {
        fn from(code: UserServiceCode) -> JsValue {
            let module: WasmServiceModule = *(code.0 as Box<dyn Any>)
                .downcast()
                .expect("we only support Wasm modules on the Web for now");
            module.into()
        }
    }

    impl TryFrom<JsValue> for UserContractCode {
        type Error = JsValue;
        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            WasmContractModule::try_from(value).map(Into::into)
        }
    }

    impl TryFrom<JsValue> for UserServiceCode {
        type Error = JsValue;
        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            WasmServiceModule::try_from(value).map(Into::into)
        }
    }
};

/// A type for errors happening during execution.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error(transparent)]
    ViewError(ViewError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error("User application reported an error: {0}")]
    UserError(String),
    #[cfg(with_wasm_runtime)]
    #[error(transparent)]
    WasmError(#[from] WasmExecutionError),
    #[cfg(with_revm)]
    #[error(transparent)]
    EvmError(#[from] EvmExecutionError),
    #[error(transparent)]
    DecompressionError(#[from] DecompressionError),
    #[error("The given promise is invalid or was polled once already")]
    InvalidPromise,

    #[error("Attempted to perform a reentrant call to application {0}")]
    ReentrantCall(UserApplicationId),
    #[error(
        "Application {caller_id} attempted to perform a cross-application to {callee_id} call \
        from `finalize`"
    )]
    CrossApplicationCallInFinalize {
        caller_id: Box<UserApplicationId>,
        callee_id: Box<UserApplicationId>,
    },
    #[error("Attempt to write to storage from a contract")]
    ServiceWriteAttempt,
    #[error("Failed to load bytecode from storage {0:?}")]
    ApplicationBytecodeNotFound(Box<UserApplicationDescription>),
    // TODO(#2927): support dynamic loading of modules on the Web
    #[error("Unsupported dynamic application load: {0:?}")]
    UnsupportedDynamicApplicationLoad(Box<UserApplicationId>),

    #[error("Excessive number of bytes read from storage")]
    ExcessiveRead,
    #[error("Excessive number of bytes written to storage")]
    ExcessiveWrite,
    #[error("Block execution required too much fuel")]
    MaximumFuelExceeded,
    #[error("Services running as oracles in block took longer than allowed")]
    MaximumServiceOracleExecutionTimeExceeded,
    #[error("Serialized size of the executed block exceeds limit")]
    ExecutedBlockTooLarge,
    #[error("HTTP response exceeds the size limit of {limit} bytes, having at least {size} bytes")]
    HttpResponseSizeLimitExceeded { limit: u64, size: u64 },
    #[error("Runtime failed to respond to application")]
    MissingRuntimeResponse,
    #[error("Module ID {0:?} is invalid")]
    InvalidModuleId(ModuleId),
    #[error("Owner is None")]
    OwnerIsNone,
    #[error("Application is not authorized to perform system operations on this chain: {0:}")]
    UnauthorizedApplication(UserApplicationId),
    #[error("Failed to make network reqwest: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Encountered I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("More recorded oracle responses than expected")]
    UnexpectedOracleResponse,
    #[error("Invalid JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
    #[error("Recorded response for oracle query has the wrong type")]
    OracleResponseMismatch,
    #[error("Assertion failed: local time {local_time} is not earlier than {timestamp}")]
    AssertBefore {
        timestamp: Timestamp,
        local_time: Timestamp,
    },

    #[error("Event keys can be at most {MAX_EVENT_KEY_LEN} bytes.")]
    EventKeyTooLong,
    #[error("Stream names can be at most {MAX_STREAM_NAME_LEN} bytes.")]
    StreamNameTooLong,
    #[error("Blob exceeds size limit")]
    BlobTooLarge,
    #[error("Bytecode exceeds size limit")]
    BytecodeTooLarge,
    #[error("Attempt to perform an HTTP request to an unauthorized host: {0:?}")]
    UnauthorizedHttpRequest(reqwest::Url),
    #[error("Attempt to perform an HTTP request to an invalid URL")]
    InvalidUrlForHttpRequest(#[from] url::ParseError),
    // TODO(#2127): Remove this error and the unstable-oracles feature once there are fees
    // and enforced limits for all oracles.
    #[error("Unstable oracles are disabled on this network.")]
    UnstableOracle,
    #[error("Failed to send contract code to worker thread: {0:?}")]
    ContractModuleSend(#[from] linera_base::task::SendError<UserContractCode>),
    #[error("Failed to send service code to worker thread: {0:?}")]
    ServiceModuleSend(#[from] linera_base::task::SendError<UserServiceCode>),
    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),

    #[error("Invalid HTTP header name used for HTTP request")]
    InvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),
    #[error("Invalid HTTP header value used for HTTP request")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("Invalid admin ID in new chain: {0}")]
    InvalidNewChainAdminId(ChainId),
    #[error("Invalid committees")]
    InvalidCommittees,
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },
    #[error("Transfer must have positive amount")]
    IncorrectTransferAmount,
    #[error("Transfer from owned account must be authenticated by the right signer")]
    UnauthenticatedTransferOwner,
    #[error("The transferred amount must not exceed the current chain balance: {balance}")]
    InsufficientFunding { balance: Amount },
    #[error("Required execution fees exceeded the total funding available: {balance}")]
    InsufficientFundingForFees { balance: Amount },
    #[error("Claim must have positive amount")]
    IncorrectClaimAmount,
    #[error("Claim must be authenticated by the right signer")]
    UnauthenticatedClaimOwner,
    #[error("Admin operations are only allowed on the admin chain.")]
    AdminOperationOnNonAdminChain,
    #[error("Failed to create new committee: expected {expected}, but got {provided}")]
    InvalidCommitteeEpoch { expected: Epoch, provided: Epoch },
    #[error("Failed to remove committee")]
    InvalidCommitteeRemoval,
    #[error("Cannot subscribe to a channel ({1}) on the same chain ({0})")]
    SelfSubscription(ChainId, SystemChannel),
    #[error("Chain {0} tried to subscribe to channel {1} but it is already subscribed")]
    AlreadySubscribedToChannel(ChainId, SystemChannel),
    #[error("Invalid unsubscription request to channel {1} on chain {0}")]
    InvalidUnsubscription(ChainId, SystemChannel),
    #[error("Amount overflow")]
    AmountOverflow,
    #[error("Amount underflow")]
    AmountUnderflow,
    #[error("Chain balance overflow")]
    BalanceOverflow,
    #[error("Chain balance underflow")]
    BalanceUnderflow,
    #[error("Cannot decrease the chain's timestamp")]
    TicksOutOfOrder,
    #[error("Application {0:?} is not registered by the chain")]
    UnknownApplicationId(Box<UserApplicationId>),
    #[error("Chain is not active yet.")]
    InactiveChain,
    #[error("No recorded response for oracle query")]
    MissingOracleResponse,
}

impl From<ViewError> for ExecutionError {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::BlobsNotFound(blob_ids) => ExecutionError::BlobsNotFound(blob_ids),
            error => ExecutionError::ViewError(error),
        }
    }
}

/// The public entry points provided by the contract part of an application.
pub trait UserContract {
    /// Instantiate the application state on the chain that owns the application.
    fn instantiate(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError>;

    /// Applies an operation from the current block.
    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Applies a message originating from a cross-chain message.
    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<(), ExecutionError>;

    /// Finishes execution of the current transaction.
    fn finalize(&mut self, context: FinalizeContext) -> Result<(), ExecutionError>;
}

/// The public entry points provided by the service part of an application.
pub trait UserService {
    /// Executes unmetered read-only queries on the state of this application.
    fn handle_query(
        &mut self,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;
}

/// Configuration options for the execution runtime available to applications.
#[derive(Clone, Copy, Default)]
pub struct ExecutionRuntimeConfig {}

/// Requirements for the `extra` field in our state views (and notably the
/// [`ExecutionStateView`]).
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
pub trait ExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId;

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig;

    fn user_contracts(&self) -> &Arc<DashMap<UserApplicationId, UserContractCode>>;

    fn user_services(&self) -> &Arc<DashMap<UserApplicationId, UserServiceCode>>;

    async fn get_user_contract(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError>;

    async fn get_user_service(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError>;

    /// Returns all requested blobs, in the specified order.
    ///
    /// If any blob cannot be found, an error is returned instead.
    async fn get_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Blob>, ViewError>;

    async fn get_event(&self, event_id: EventId) -> Result<Vec<u8>, ViewError>;

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    #[cfg(with_testing)]
    async fn add_blobs(
        &self,
        blobs: impl IntoIterator<Item = Blob> + Send,
    ) -> Result<(), ViewError>;

    #[cfg(with_testing)]
    async fn add_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError>;
}

#[derive(Clone, Copy, Debug)]
pub struct OperationContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// `None` if this is the transaction entrypoint or the caller doesn't want this particular
    /// call to be authenticated (e.g. for safety reasons).
    #[debug(skip_if = Option::is_none)]
    pub authenticated_caller_id: Option<UserApplicationId>,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
    /// The current index of the operation.
    #[debug(skip_if = Option::is_none)]
    pub index: Option<u32>,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation that created the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    pub refund_grant_to: Option<Account>,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
    /// The hash of the remote certificate that created the message.
    pub certificate_hash: CryptoHash,
    /// The ID of the message (based on the operation height and index in the remote
    /// certificate).
    pub message_id: MessageId,
}

#[derive(Clone, Copy, Debug)]
pub struct FinalizeContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The height of the next block on this chain.
    pub next_block_height: BlockHeight,
    /// The local time in the node executing the query.
    pub local_time: Timestamp,
}

pub trait BaseRuntime {
    type Read: fmt::Debug + Send + Sync;
    type ContainsKey: fmt::Debug + Send + Sync;
    type ContainsKeys: fmt::Debug + Send + Sync;
    type ReadMultiValuesBytes: fmt::Debug + Send + Sync;
    type ReadValueBytes: fmt::Debug + Send + Sync;
    type FindKeysByPrefix: fmt::Debug + Send + Sync;
    type FindKeyValuesByPrefix: fmt::Debug + Send + Sync;

    /// The current chain ID.
    fn chain_id(&mut self) -> Result<ChainId, ExecutionError>;

    /// The current block height.
    fn block_height(&mut self) -> Result<BlockHeight, ExecutionError>;

    /// The current application ID.
    fn application_id(&mut self) -> Result<UserApplicationId, ExecutionError>;

    /// The current application creator's chain ID.
    fn application_creator_chain_id(&mut self) -> Result<ChainId, ExecutionError>;

    /// The current application parameters.
    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError>;

    /// Reads the system timestamp.
    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError>;

    /// Reads the balance of the chain.
    fn read_chain_balance(&mut self) -> Result<Amount, ExecutionError>;

    /// Reads the owner balance.
    fn read_owner_balance(&mut self, owner: AccountOwner) -> Result<Amount, ExecutionError>;

    /// Reads the balances from all owners.
    fn read_owner_balances(&mut self) -> Result<Vec<(AccountOwner, Amount)>, ExecutionError>;

    /// Reads balance owners.
    fn read_balance_owners(&mut self) -> Result<Vec<AccountOwner>, ExecutionError>;

    /// Reads the current ownership configuration for this chain.
    fn chain_ownership(&mut self) -> Result<ChainOwnership, ExecutionError>;

    /// Tests whether a key exists in the key-value store
    #[cfg(feature = "test")]
    fn contains_key(&mut self, key: Vec<u8>) -> Result<bool, ExecutionError> {
        let promise = self.contains_key_new(key)?;
        self.contains_key_wait(&promise)
    }

    /// Creates the promise to test whether a key exists in the key-value store
    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError>;

    /// Resolves the promise to test whether a key exists in the key-value store
    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError>;

    /// Tests whether multiple keys exist in the key-value store
    #[cfg(feature = "test")]
    fn contains_keys(&mut self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ExecutionError> {
        let promise = self.contains_keys_new(keys)?;
        self.contains_keys_wait(&promise)
    }

    /// Creates the promise to test whether multiple keys exist in the key-value store
    fn contains_keys_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ContainsKeys, ExecutionError>;

    /// Resolves the promise to test whether multiple keys exist in the key-value store
    fn contains_keys_wait(
        &mut self,
        promise: &Self::ContainsKeys,
    ) -> Result<Vec<bool>, ExecutionError>;

    /// Reads several keys from the key-value store
    #[cfg(feature = "test")]
    fn read_multi_values_bytes(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let promise = self.read_multi_values_bytes_new(keys)?;
        self.read_multi_values_bytes_wait(&promise)
    }

    /// Creates the promise to access several keys from the key-value store
    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError>;

    /// Resolves the promise to access several keys from the key-value store
    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError>;

    /// Reads the key from the key-value store
    #[cfg(feature = "test")]
    fn read_value_bytes(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError> {
        let promise = self.read_value_bytes_new(key)?;
        self.read_value_bytes_wait(&promise)
    }

    /// Creates the promise to access a key from the key-value store
    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError>;

    /// Resolves the promise to access a key from the key-value store
    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError>;

    /// Creates the promise to access keys having a specific prefix
    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError>;

    /// Resolves the promise to access keys having a specific prefix
    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError>;

    /// Reads the data from the key/values having a specific prefix.
    #[cfg(feature = "test")]
    #[expect(clippy::type_complexity)]
    fn find_key_values_by_prefix(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let promise = self.find_key_values_by_prefix_new(key_prefix)?;
        self.find_key_values_by_prefix_wait(&promise)
    }

    /// Creates the promise to access key/values having a specific prefix
    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError>;

    /// Resolves the promise to access key/values having a specific prefix
    #[expect(clippy::type_complexity)]
    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>;

    /// Makes an HTTP request to the given URL and returns the answer, if any.
    fn perform_http_request(
        &mut self,
        request: http::Request,
    ) -> Result<http::Response, ExecutionError>;

    /// Ensures that the current time at block validation is `< timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    fn assert_before(&mut self, timestamp: Timestamp) -> Result<(), ExecutionError>;

    /// Reads a data blob specified by a given hash.
    fn read_data_blob(&mut self, hash: &CryptoHash) -> Result<Vec<u8>, ExecutionError>;

    /// Asserts the existence of a data blob with the given hash.
    fn assert_data_blob_exists(&mut self, hash: &CryptoHash) -> Result<(), ExecutionError>;
}

pub trait ServiceRuntime: BaseRuntime {
    /// Queries another application.
    fn try_query_application(
        &mut self,
        queried_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Schedules an operation to be included in the block proposed after execution.
    fn schedule_operation(&mut self, operation: Vec<u8>) -> Result<(), ExecutionError>;

    /// Checks if the service has exceeded its execution time limit.
    fn check_execution_time(&mut self) -> Result<(), ExecutionError>;
}

pub trait ContractRuntime: BaseRuntime {
    /// The authenticated signer for this execution, if there is one.
    fn authenticated_signer(&mut self) -> Result<Option<Owner>, ExecutionError>;

    /// The current message ID, if there is one.
    fn message_id(&mut self) -> Result<Option<MessageId>, ExecutionError>;

    /// If the current message (if there is one) was rejected by its destination and is now
    /// bouncing back.
    fn message_is_bouncing(&mut self) -> Result<Option<bool>, ExecutionError>;

    /// The optional authenticated caller application ID, if it was provided and if there is one
    /// based on the execution context.
    fn authenticated_caller_id(&mut self) -> Result<Option<UserApplicationId>, ExecutionError>;

    /// Returns the amount of execution fuel remaining before execution is aborted.
    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError>;

    /// Consumes some of the execution fuel.
    fn consume_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError>;

    /// Schedules a message to be sent.
    fn send_message(&mut self, message: SendMessageRequest<Vec<u8>>) -> Result<(), ExecutionError>;

    /// Schedules to subscribe to some `channel` on a `chain`.
    fn subscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError>;

    /// Schedules to unsubscribe to some `channel` on a `chain`.
    fn unsubscribe(&mut self, chain: ChainId, channel: ChannelName) -> Result<(), ExecutionError>;

    /// Transfers amount from source to destination.
    fn transfer(
        &mut self,
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError>;

    /// Claims amount from source to destination.
    fn claim(
        &mut self,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<(), ExecutionError>;

    /// Calls another application. Forwarded sessions will now be visible to
    /// `callee_id` (but not to the caller any more).
    fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Adds a new item to an event stream.
    fn emit(
        &mut self,
        name: StreamName,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ExecutionError>;

    /// Queries a service.
    fn query_service(
        &mut self,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Opens a new chain.
    fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<(MessageId, ChainId), ExecutionError>;

    /// Closes the current chain.
    fn close_chain(&mut self) -> Result<(), ExecutionError>;

    /// Changes the application permissions on the current chain.
    fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<(), ExecutionError>;

    /// Creates a new application on chain.
    fn create_application(
        &mut self,
        module_id: ModuleId,
        parameters: Vec<u8>,
        argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<UserApplicationId, ExecutionError>;

    /// Returns the round in which this block was validated.
    fn validation_round(&mut self) -> Result<Option<u32>, ExecutionError>;

    /// Writes a batch of changes.
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError>;
}

/// An operation to be executed in a block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// A system operation.
    System(SystemOperation),
    /// A user operation (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

impl<'de> BcsHashable<'de> for Operation {}

/// A message to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A system message.
    System(SystemMessage),
    /// A user message (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

/// An query to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Query {
    /// A system query.
    System(SystemQuery),
    /// A user query (in serialized form).
    User {
        application_id: UserApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

/// The outcome of the execution of a query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct QueryOutcome<Response = QueryResponse> {
    pub response: Response,
    pub operations: Vec<Operation>,
}

impl From<QueryOutcome<SystemResponse>> for QueryOutcome {
    fn from(system_outcome: QueryOutcome<SystemResponse>) -> Self {
        let QueryOutcome {
            response,
            operations,
        } = system_outcome;

        QueryOutcome {
            response: QueryResponse::System(response),
            operations,
        }
    }
}

impl From<QueryOutcome<Vec<u8>>> for QueryOutcome {
    fn from(user_service_outcome: QueryOutcome<Vec<u8>>) -> Self {
        let QueryOutcome {
            response,
            operations,
        } = user_service_outcome;

        QueryOutcome {
            response: QueryResponse::User(response),
            operations,
        }
    }
}

/// The response to a query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum QueryResponse {
    /// A system response.
    System(SystemResponse),
    /// A user response (in serialized form).
    User(
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        Vec<u8>,
    ),
}

/// A message together with routing information.
#[derive(Clone, Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct RawOutgoingMessage<Message, Grant> {
    /// The destination of the message.
    pub destination: Destination,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// The grant needed for message execution, typically specified as an `Amount` or as `Resources`.
    pub grant: Grant,
    /// The kind of outgoing message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
}

impl<Message> From<SendMessageRequest<Message>> for RawOutgoingMessage<Message, Resources> {
    fn from(request: SendMessageRequest<Message>) -> Self {
        let SendMessageRequest {
            destination,
            authenticated,
            grant,
            is_tracked,
            message,
        } = request;

        let kind = if is_tracked {
            MessageKind::Tracked
        } else {
            MessageKind::Simple
        };

        RawOutgoingMessage {
            destination,
            authenticated,
            grant,
            kind,
            message,
        }
    }
}

/// The kind of outgoing message being sent.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Copy)]
pub enum MessageKind {
    /// The message can be skipped or rejected. No receipt is requested.
    Simple,
    /// The message cannot be skipped nor rejected. No receipt is requested.
    /// This only concerns certain system messages that cannot fail.
    Protected,
    /// The message cannot be skipped but can be rejected. A receipt must be sent
    /// when the message is rejected in a block of the receiver.
    Tracked,
    /// This message is a receipt automatically created when the original message was rejected.
    Bouncing,
}

/// A posted message together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OutgoingMessage {
    /// The destination of the message.
    pub destination: Destination,
    /// The user authentication carried by the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// A grant to pay for the message execution.
    #[debug(skip_if = Amount::is_zero)]
    pub grant: Amount,
    /// Where to send a refund for the unused part of the grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    pub refund_grant_to: Option<Account>,
    /// The kind of message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
}

impl<'de> BcsHashable<'de> for OutgoingMessage {}

impl OutgoingMessage {
    /// Creates a new simple outgoing message with no grant and no authenticated signer.
    pub fn new(recipient: ChainId, message: impl Into<Message>) -> Self {
        OutgoingMessage {
            destination: Destination::Recipient(recipient),
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Simple,
            message: message.into(),
        }
    }

    /// Returns the same message, with the specified kind.
    pub fn with_kind(mut self, kind: MessageKind) -> Self {
        self.kind = kind;
        self
    }

    /// Returns the same message, with the specified authenticated signer.
    pub fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }
}

/// The identifier of a channel, relative to a particular application.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Hash, Serialize, Deserialize, SimpleObject,
)]
pub struct ChannelSubscription {
    /// The chain ID broadcasting on this channel.
    pub chain_id: ChainId,
    /// The name of the channel.
    pub name: ChannelName,
}

impl OperationContext {
    /// Returns an account for the refund.
    /// Returns `None` if there is no authenticated signer of the [`OperationContext`].
    fn refund_grant_to(&self) -> Option<Account> {
        self.authenticated_signer.map(|owner| Account {
            chain_id: self.chain_id,
            owner: AccountOwner::User(owner),
        })
    }

    fn next_message_id(&self, next_message_index: u32) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index: next_message_index,
        }
    }
}

#[cfg(with_testing)]
#[derive(Clone)]
pub struct TestExecutionRuntimeContext {
    chain_id: ChainId,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    blobs: Arc<DashMap<BlobId, Blob>>,
    events: Arc<DashMap<EventId, Vec<u8>>>,
}

#[cfg(with_testing)]
impl TestExecutionRuntimeContext {
    pub fn new(chain_id: ChainId, execution_runtime_config: ExecutionRuntimeConfig) -> Self {
        Self {
            chain_id,
            execution_runtime_config,
            user_contracts: Arc::default(),
            user_services: Arc::default(),
            blobs: Arc::default(),
            events: Arc::default(),
        }
    }
}

#[cfg(with_testing)]
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl ExecutionRuntimeContext for TestExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig {
        self.execution_runtime_config
    }

    fn user_contracts(&self) -> &Arc<DashMap<UserApplicationId, UserContractCode>> {
        &self.user_contracts
    }

    fn user_services(&self) -> &Arc<DashMap<UserApplicationId, UserServiceCode>> {
        &self.user_services
    }

    async fn get_user_contract(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError> {
        let application_id = description.into();
        Ok(self
            .user_contracts()
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }

    async fn get_user_service(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError> {
        let application_id = description.into();
        Ok(self
            .user_services()
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }

    async fn get_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Blob>, ViewError> {
        let mut missing = Vec::new();
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            match self.blobs.get(blob_id) {
                None => missing.push(*blob_id),
                Some(blob) => blobs.push(blob.clone()),
            }
        }
        linera_base::ensure!(missing.is_empty(), ViewError::BlobsNotFound(missing));
        Ok(blobs)
    }

    async fn get_event(&self, event_id: EventId) -> Result<Vec<u8>, ViewError> {
        Ok(self
            .events
            .get(&event_id)
            .ok_or_else(|| ViewError::EventsNotFound(vec![event_id]))?
            .clone())
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        Ok(self.blobs.contains_key(&blob_id))
    }

    #[cfg(with_testing)]
    async fn add_blobs(
        &self,
        blobs: impl IntoIterator<Item = Blob> + Send,
    ) -> Result<(), ViewError> {
        for blob in blobs {
            self.blobs.insert(blob.id(), blob);
        }

        Ok(())
    }

    #[cfg(with_testing)]
    async fn add_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError> {
        for (event_id, bytes) in events {
            self.events.insert(event_id, bytes);
        }

        Ok(())
    }
}

impl From<SystemOperation> for Operation {
    fn from(operation: SystemOperation) -> Self {
        Operation::System(operation)
    }
}

impl Operation {
    pub fn system(operation: SystemOperation) -> Self {
        Operation::System(operation)
    }

    /// Creates a new user application operation following the `application_id`'s [`Abi`].
    #[cfg(with_testing)]
    pub fn user<A: Abi>(
        application_id: ApplicationId<A>,
        operation: &A::Operation,
    ) -> Result<Self, bcs::Error> {
        Self::user_without_abi(application_id.forget_abi(), operation)
    }

    /// Creates a new user application operation assuming that the `operation` is valid for the
    /// `application_id`.
    #[cfg(with_testing)]
    pub fn user_without_abi(
        application_id: ApplicationId<()>,
        operation: &impl Serialize,
    ) -> Result<Self, bcs::Error> {
        Ok(Operation::User {
            application_id,
            bytes: bcs::to_bytes(&operation)?,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }

    /// Returns the IDs of all blobs published in this operation.
    pub fn published_blob_ids(&self) -> Vec<BlobId> {
        match self {
            Operation::System(SystemOperation::PublishDataBlob { blob_hash }) => {
                vec![BlobId::new(*blob_hash, BlobType::Data)]
            }
            Operation::System(SystemOperation::PublishCommitteeBlob { blob_hash }) => {
                vec![BlobId::new(*blob_hash, BlobType::Committee)]
            }
            Operation::System(SystemOperation::PublishModule { module_id }) => vec![
                BlobId::new(module_id.contract_blob_hash, BlobType::ContractBytecode),
                BlobId::new(module_id.service_blob_hash, BlobType::ServiceBytecode),
            ],
            _ => vec![],
        }
    }
}

impl From<SystemMessage> for Message {
    fn from(message: SystemMessage) -> Self {
        Message::System(message)
    }
}

impl Message {
    pub fn system(message: SystemMessage) -> Self {
        Message::System(message)
    }

    /// Creates a new user application message assuming that the `message` is valid for the
    /// `application_id`.
    pub fn user<A, M: Serialize>(
        application_id: ApplicationId<A>,
        message: &M,
    ) -> Result<Self, bcs::Error> {
        let application_id = application_id.forget_abi();
        let bytes = bcs::to_bytes(&message)?;
        Ok(Message::User {
            application_id,
            bytes,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }

    /// Returns whether this message must be added to the inbox.
    pub fn goes_to_inbox(&self) -> bool {
        !matches!(
            self,
            Message::System(SystemMessage::Subscribe { .. } | SystemMessage::Unsubscribe { .. })
        )
    }

    pub fn matches_subscribe(&self) -> Option<(&ChainId, &ChannelSubscription)> {
        match self {
            Message::System(SystemMessage::Subscribe { id, subscription }) => {
                Some((id, subscription))
            }
            _ => None,
        }
    }

    pub fn matches_unsubscribe(&self) -> Option<(&ChainId, &ChannelSubscription)> {
        match self {
            Message::System(SystemMessage::Unsubscribe { id, subscription }) => {
                Some((id, subscription))
            }
            _ => None,
        }
    }

    pub fn matches_open_chain(&self) -> Option<&OpenChainConfig> {
        match self {
            Message::System(SystemMessage::OpenChain(config)) => Some(config),
            _ => None,
        }
    }
}

impl From<SystemQuery> for Query {
    fn from(query: SystemQuery) -> Self {
        Query::System(query)
    }
}

impl Query {
    pub fn system(query: SystemQuery) -> Self {
        Query::System(query)
    }

    /// Creates a new user application query following the `application_id`'s [`Abi`].
    pub fn user<A: Abi>(
        application_id: ApplicationId<A>,
        query: &A::Query,
    ) -> Result<Self, serde_json::Error> {
        Self::user_without_abi(application_id.forget_abi(), query)
    }

    /// Creates a new user application query assuming that the `query` is valid for the
    /// `application_id`.
    pub fn user_without_abi(
        application_id: ApplicationId<()>,
        query: &impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        Ok(Query::User {
            application_id,
            bytes: serde_json::to_vec(&query)?,
        })
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }
}

impl From<SystemResponse> for QueryResponse {
    fn from(response: SystemResponse) -> Self {
        QueryResponse::System(response)
    }
}

impl From<Vec<u8>> for QueryResponse {
    fn from(response: Vec<u8>) -> Self {
        QueryResponse::User(response)
    }
}

/// The state of a blob of binary data.
#[derive(Eq, PartialEq, Debug, Hash, Clone, Serialize, Deserialize)]
pub struct BlobState {
    /// Hash of the last `Certificate` that published or used this blob.
    pub last_used_by: CryptoHash,
    /// The `ChainId` of the chain that published the change
    pub chain_id: ChainId,
    /// The `BlockHeight` of the chain that published the change
    pub block_height: BlockHeight,
    /// Epoch of the `last_used_by` certificate.
    pub epoch: Epoch,
}

/// The runtime to use for running the application.
#[derive(Clone, Copy, Display)]
#[cfg_attr(with_wasm_runtime, derive(Debug, Default))]
pub enum WasmRuntime {
    #[cfg(with_wasmer)]
    #[default]
    #[display("wasmer")]
    Wasmer,
    #[cfg(with_wasmtime)]
    #[cfg_attr(not(with_wasmer), default)]
    #[display("wasmtime")]
    Wasmtime,
    #[cfg(with_wasmer)]
    WasmerWithSanitizer,
    #[cfg(with_wasmtime)]
    WasmtimeWithSanitizer,
}

#[derive(Clone, Copy, Display)]
#[cfg_attr(with_revm, derive(Debug, Default))]
pub enum EvmRuntime {
    #[cfg(with_revm)]
    #[default]
    #[display("revm")]
    Revm,
}

/// Trait used to select a default `WasmRuntime`, if one is available.
pub trait WithWasmDefault {
    fn with_wasm_default(self) -> Self;
}

impl WasmRuntime {
    pub fn needs_sanitizer(self) -> bool {
        match self {
            #[cfg(with_wasmer)]
            WasmRuntime::WasmerWithSanitizer => true,
            #[cfg(with_wasmtime)]
            WasmRuntime::WasmtimeWithSanitizer => true,
            #[cfg(with_wasm_runtime)]
            _ => false,
        }
    }
}

impl WithWasmDefault for Option<WasmRuntime> {
    fn with_wasm_default(self) -> Self {
        #[cfg(with_wasm_runtime)]
        {
            Some(self.unwrap_or_default())
        }
        #[cfg(not(with_wasm_runtime))]
        {
            None
        }
    }
}

impl FromStr for WasmRuntime {
    type Err = InvalidWasmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            #[cfg(with_wasmer)]
            "wasmer" => Ok(WasmRuntime::Wasmer),
            #[cfg(with_wasmtime)]
            "wasmtime" => Ok(WasmRuntime::Wasmtime),
            unknown => Err(InvalidWasmRuntime(unknown.to_owned())),
        }
    }
}

/// Attempts to create an invalid [`WasmRuntime`] instance from a string.
#[derive(Clone, Debug, Error)]
#[error("{0:?} is not a valid WebAssembly runtime")]
pub struct InvalidWasmRuntime(String);

doc_scalar!(Operation, "An operation to be executed in a block");
doc_scalar!(
    Message,
    "A message to be sent and possibly executed in the receiver's block."
);
doc_scalar!(MessageKind, "The kind of outgoing message being sent");
