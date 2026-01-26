// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the execution of the system application and the user applications in a
//! Linera chain.

pub mod committee;
pub mod evm;
mod execution;
pub mod execution_state_actor;
#[cfg(with_graphql)]
mod graphql;
mod policy;
mod resources;
mod runtime;
pub mod system;
#[cfg(with_testing)]
pub mod test_utils;
mod transaction_tracker;
mod util;
mod wasm;

use std::{any::Any, collections::BTreeMap, fmt, ops::RangeInclusive, str::FromStr, sync::Arc};

use allocative::Allocative;
use async_graphql::SimpleObject;
use async_trait::async_trait;
use custom_debug_derive::Debug;
use derive_more::Display;
#[cfg(web)]
use js_sys::wasm_bindgen::JsValue;
use linera_base::{
    abi::Abi,
    crypto::{BcsHashable, CryptoHash},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, ArithmeticError, Blob, BlockHeight,
        Bytecode, DecompressionError, Epoch, NetworkDescription, SendMessageRequest, StreamUpdate,
        Timestamp,
    },
    doc_scalar, ensure, hex_debug, http,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, BlobType, ChainId, DataBlobHash, EventId,
        GenericApplicationId, ModuleId, StreamId, StreamName,
    },
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_views::{batch::Batch, ViewError};
use serde::{Deserialize, Serialize};
use system::AdminOperation;
use thiserror::Error;
pub use web_thread_pool::Pool as ThreadPool;
use web_thread_select as web_thread;

#[cfg(with_revm)]
use crate::evm::EvmExecutionError;
use crate::system::{EpochEventData, EPOCH_STREAM_NAME};
#[cfg(with_testing)]
use crate::test_utils::dummy_chain_description;
#[cfg(all(with_testing, with_wasm_runtime))]
pub use crate::wasm::test as wasm_test;
#[cfg(with_wasm_runtime)]
pub use crate::wasm::{
    BaseRuntimeApi, ContractEntrypoints, ContractRuntimeApi, RuntimeApiData, ServiceEntrypoints,
    ServiceRuntimeApi, WasmContractModule, WasmExecutionError, WasmServiceModule,
};
pub use crate::{
    committee::Committee,
    execution::{ExecutionStateView, ServiceRuntimeEndpoint},
    execution_state_actor::{ExecutionRequest, ExecutionStateActor},
    policy::ResourceControlPolicy,
    resources::{BalanceHolder, ResourceController, ResourceTracker},
    runtime::{
        ContractSyncRuntimeHandle, ServiceRuntimeRequest, ServiceSyncRuntime,
        ServiceSyncRuntimeHandle,
    },
    system::{
        SystemExecutionStateView, SystemMessage, SystemOperation, SystemQuery, SystemResponse,
    },
    transaction_tracker::{TransactionOutcome, TransactionTracker},
};

/// The `Linera.sol` library code to be included in solidity smart
/// contracts using Linera features.
pub const LINERA_SOL: &str = include_str!("../solidity/Linera.sol");
pub const LINERA_TYPES_SOL: &str = include_str!("../solidity/LineraTypes.sol");

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
pub trait UserContractModule: dyn_clone::DynClone + Any + web_thread::Post + Send + Sync {
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
pub trait UserServiceModule: dyn_clone::DynClone + Any + web_thread::Post + Send + Sync {
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

pub struct JsVec<T>(pub Vec<T>);

#[cfg(web)]
const _: () = {
    // TODO(#2775): add a vtable pointer into the JsValue rather than assuming the
    // implementor

    impl web_thread::AsJs for UserContractCode {
        fn to_js(&self) -> Result<JsValue, JsValue> {
            ((&*self.0) as &dyn Any)
                .downcast_ref::<WasmContractModule>()
                .expect("we only support Wasm modules on the Web for now")
                .to_js()
        }

        fn from_js(value: JsValue) -> Result<Self, JsValue> {
            WasmContractModule::from_js(value).map(Into::into)
        }
    }

    impl web_thread::Post for UserContractCode {
        fn transferables(&self) -> js_sys::Array {
            self.0.transferables()
        }
    }

    impl web_thread::AsJs for UserServiceCode {
        fn to_js(&self) -> Result<JsValue, JsValue> {
            ((&*self.0) as &dyn Any)
                .downcast_ref::<WasmServiceModule>()
                .expect("we only support Wasm modules on the Web for now")
                .to_js()
        }

        fn from_js(value: JsValue) -> Result<Self, JsValue> {
            WasmServiceModule::from_js(value).map(Into::into)
        }
    }

    impl web_thread::Post for UserServiceCode {
        fn transferables(&self) -> js_sys::Array {
            self.0.transferables()
        }
    }

    impl<T: web_thread::AsJs> web_thread::AsJs for JsVec<T> {
        fn to_js(&self) -> Result<JsValue, JsValue> {
            let array = self
                .0
                .iter()
                .map(T::to_js)
                .collect::<Result<js_sys::Array, _>>()?;
            Ok(array.into())
        }

        fn from_js(value: JsValue) -> Result<Self, JsValue> {
            let array = js_sys::Array::from(&value);
            let v = array
                .into_iter()
                .map(T::from_js)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(JsVec(v))
        }
    }

    impl<T: web_thread::Post> web_thread::Post for JsVec<T> {
        fn transferables(&self) -> js_sys::Array {
            let mut array = js_sys::Array::new();
            for x in &self.0 {
                array = array.concat(&x.transferables());
            }
            array
        }
    }
};

/// A type for errors happening during execution.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error(transparent)]
    ViewError(#[from] ViewError),
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
    ReentrantCall(ApplicationId),
    #[error(
        "Application {caller_id} attempted to perform a cross-application to {callee_id} call \
        from `finalize`"
    )]
    CrossApplicationCallInFinalize {
        caller_id: Box<ApplicationId>,
        callee_id: Box<ApplicationId>,
    },
    #[error("Failed to load bytecode from storage {0:?}")]
    ApplicationBytecodeNotFound(Box<ApplicationDescription>),
    // TODO(#2927): support dynamic loading of modules on the Web
    #[error("Unsupported dynamic application load: {0:?}")]
    UnsupportedDynamicApplicationLoad(Box<ApplicationId>),

    #[error("Excessive number of bytes read from storage")]
    ExcessiveRead,
    #[error("Excessive number of bytes written to storage")]
    ExcessiveWrite,
    #[error("Block execution required too much fuel for VM {0}")]
    MaximumFuelExceeded(VmRuntime),
    #[error("Services running as oracles in block took longer than allowed")]
    MaximumServiceOracleExecutionTimeExceeded,
    #[error("Service running as an oracle produced a response that's too large")]
    ServiceOracleResponseTooLarge,
    #[error("Serialized size of the block exceeds limit")]
    BlockTooLarge,
    #[error("HTTP response exceeds the size limit of {limit} bytes, having at least {size} bytes")]
    HttpResponseSizeLimitExceeded { limit: u64, size: u64 },
    #[error("Runtime failed to respond to application")]
    MissingRuntimeResponse,
    #[error("Application is not authorized to perform system operations on this chain: {0:}")]
    UnauthorizedApplication(ApplicationId),
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
    #[error("Service oracle query tried to create operations: {0:?}")]
    ServiceOracleQueryOperations(Vec<Operation>),
    #[error("Assertion failed: local time {local_time} is not earlier than {timestamp}")]
    AssertBefore {
        timestamp: Timestamp,
        local_time: Timestamp,
    },

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
    #[error("Worker thread failure: {0:?}")]
    Thread(#[from] web_thread::Error),
    #[error("The chain being queried is not active {0}")]
    InactiveChain(ChainId),
    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),
    #[error("Events not found: {0:?}")]
    EventsNotFound(Vec<EventId>),

    #[error("Invalid HTTP header name used for HTTP request")]
    InvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),
    #[error("Invalid HTTP header value used for HTTP request")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("No NetworkDescription found in storage")]
    NoNetworkDescriptionFound,
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },
    #[error("Transfer must have positive amount")]
    IncorrectTransferAmount,
    #[error("Transfer from owned account must be authenticated by the right owner")]
    UnauthenticatedTransferOwner,
    #[error("The transferred amount must not exceed the balance of the current account {account}: {balance}")]
    InsufficientBalance {
        balance: Amount,
        account: AccountOwner,
    },
    #[error("Required execution fees exceeded the total funding available. Fees {fees}, available balance: {balance}")]
    FeesExceedFunding { fees: Amount, balance: Amount },
    #[error("Claim must have positive amount")]
    IncorrectClaimAmount,
    #[error("Claim must be authenticated by the right owner")]
    UnauthenticatedClaimOwner,
    #[error("Admin operations are only allowed on the admin chain.")]
    AdminOperationOnNonAdminChain,
    #[error("Failed to create new committee: expected {expected}, but got {provided}")]
    InvalidCommitteeEpoch { expected: Epoch, provided: Epoch },
    #[error("Failed to remove committee")]
    InvalidCommitteeRemoval,
    #[error("No recorded response for oracle query")]
    MissingOracleResponse,
    #[error("process_streams was not called for all stream updates")]
    UnprocessedStreams,
    #[error("Internal error: {0}")]
    InternalError(&'static str),
    #[error("UpdateStreams is outdated")]
    OutdatedUpdateStreams,
}

impl ExecutionError {
    /// Returns whether this error is caused by an issue in the local node.
    ///
    /// Returns `false` whenever the error could be caused by a bad message from a peer.
    pub fn is_local(&self) -> bool {
        match self {
            ExecutionError::ArithmeticError(_)
            | ExecutionError::UserError(_)
            | ExecutionError::DecompressionError(_)
            | ExecutionError::InvalidPromise
            | ExecutionError::CrossApplicationCallInFinalize { .. }
            | ExecutionError::ReentrantCall(_)
            | ExecutionError::ApplicationBytecodeNotFound(_)
            | ExecutionError::UnsupportedDynamicApplicationLoad(_)
            | ExecutionError::ExcessiveRead
            | ExecutionError::ExcessiveWrite
            | ExecutionError::MaximumFuelExceeded(_)
            | ExecutionError::MaximumServiceOracleExecutionTimeExceeded
            | ExecutionError::ServiceOracleResponseTooLarge
            | ExecutionError::BlockTooLarge
            | ExecutionError::HttpResponseSizeLimitExceeded { .. }
            | ExecutionError::UnauthorizedApplication(_)
            | ExecutionError::UnexpectedOracleResponse
            | ExecutionError::JsonError(_)
            | ExecutionError::BcsError(_)
            | ExecutionError::OracleResponseMismatch
            | ExecutionError::ServiceOracleQueryOperations(_)
            | ExecutionError::AssertBefore { .. }
            | ExecutionError::StreamNameTooLong
            | ExecutionError::BlobTooLarge
            | ExecutionError::BytecodeTooLarge
            | ExecutionError::UnauthorizedHttpRequest(_)
            | ExecutionError::InvalidUrlForHttpRequest(_)
            | ExecutionError::InactiveChain(_)
            | ExecutionError::BlobsNotFound(_)
            | ExecutionError::EventsNotFound(_)
            | ExecutionError::InvalidHeaderName(_)
            | ExecutionError::InvalidHeaderValue(_)
            | ExecutionError::InvalidEpoch { .. }
            | ExecutionError::IncorrectTransferAmount
            | ExecutionError::UnauthenticatedTransferOwner
            | ExecutionError::InsufficientBalance { .. }
            | ExecutionError::FeesExceedFunding { .. }
            | ExecutionError::IncorrectClaimAmount
            | ExecutionError::UnauthenticatedClaimOwner
            | ExecutionError::AdminOperationOnNonAdminChain
            | ExecutionError::InvalidCommitteeEpoch { .. }
            | ExecutionError::InvalidCommitteeRemoval
            | ExecutionError::MissingOracleResponse
            | ExecutionError::UnprocessedStreams
            | ExecutionError::OutdatedUpdateStreams
            | ExecutionError::ViewError(ViewError::NotFound(_)) => false,
            #[cfg(with_wasm_runtime)]
            ExecutionError::WasmError(_) => false,
            #[cfg(with_revm)]
            ExecutionError::EvmError(..) => false,
            ExecutionError::MissingRuntimeResponse
            | ExecutionError::ViewError(_)
            | ExecutionError::ReqwestError(_)
            | ExecutionError::Thread(_)
            | ExecutionError::NoNetworkDescriptionFound
            | ExecutionError::InternalError(_)
            | ExecutionError::IoError(_) => true,
        }
    }

    /// Returns whether this error is caused by a per-block limit being exceeded.
    ///
    /// These are errors that might succeed in a later block if the limit was only exceeded
    /// due to accumulated transactions. Per-transaction or per-call limits are not included.
    pub fn is_limit_error(&self) -> bool {
        matches!(
            self,
            ExecutionError::ExcessiveRead
                | ExecutionError::ExcessiveWrite
                | ExecutionError::MaximumFuelExceeded(_)
                | ExecutionError::MaximumServiceOracleExecutionTimeExceeded
                | ExecutionError::BlockTooLarge
        )
    }
}

/// The public entry points provided by the contract part of an application.
pub trait UserContract {
    /// Instantiate the application state on the chain that owns the application.
    fn instantiate(&mut self, argument: Vec<u8>) -> Result<(), ExecutionError>;

    /// Applies an operation from the current block.
    fn execute_operation(&mut self, operation: Vec<u8>) -> Result<Vec<u8>, ExecutionError>;

    /// Applies a message originating from a cross-chain message.
    fn execute_message(&mut self, message: Vec<u8>) -> Result<(), ExecutionError>;

    /// Reacts to new events on streams this application subscribes to.
    fn process_streams(&mut self, updates: Vec<StreamUpdate>) -> Result<(), ExecutionError>;

    /// Finishes execution of the current transaction.
    fn finalize(&mut self) -> Result<(), ExecutionError>;
}

/// The public entry points provided by the service part of an application.
pub trait UserService {
    /// Executes unmetered read-only queries on the state of this application.
    fn handle_query(&mut self, argument: Vec<u8>) -> Result<Vec<u8>, ExecutionError>;
}

/// Configuration options for the execution runtime available to applications.
#[derive(Clone, Copy)]
pub struct ExecutionRuntimeConfig {
    /// Whether contract log messages should be output.
    /// This is typically enabled for clients but disabled for validators.
    pub allow_application_logs: bool,
}

impl Default for ExecutionRuntimeConfig {
    fn default() -> Self {
        Self {
            allow_application_logs: true,
        }
    }
}

/// Requirements for the `extra` field in our state views (and notably the
/// [`ExecutionStateView`]).
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
pub trait ExecutionRuntimeContext {
    fn chain_id(&self) -> ChainId;

    fn thread_pool(&self) -> &Arc<ThreadPool>;

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig;

    fn user_contracts(&self) -> &Arc<papaya::HashMap<ApplicationId, UserContractCode>>;

    fn user_services(&self) -> &Arc<papaya::HashMap<ApplicationId, UserServiceCode>>;

    async fn get_user_contract(
        &self,
        description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserContractCode, ExecutionError>;

    async fn get_user_service(
        &self,
        description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserServiceCode, ExecutionError>;

    async fn get_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError>;

    async fn get_event(&self, event_id: EventId) -> Result<Option<Vec<u8>>, ViewError>;

    async fn get_network_description(&self) -> Result<Option<NetworkDescription>, ViewError>;

    /// Returns the committees for the epochs in the given range.
    async fn get_committees(
        &self,
        epoch_range: RangeInclusive<Epoch>,
    ) -> Result<BTreeMap<Epoch, Committee>, ExecutionError> {
        let net_description = self
            .get_network_description()
            .await?
            .ok_or(ExecutionError::NoNetworkDescriptionFound)?;
        let committee_hashes = futures::future::join_all(
            (epoch_range.start().0..=epoch_range.end().0).map(|epoch| async move {
                if epoch == 0 {
                    // Genesis epoch is stored in NetworkDescription.
                    Ok((epoch, net_description.genesis_committee_blob_hash))
                } else {
                    let event_id = EventId {
                        chain_id: net_description.admin_chain_id,
                        stream_id: StreamId::system(EPOCH_STREAM_NAME),
                        index: epoch,
                    };
                    let event = self
                        .get_event(event_id.clone())
                        .await?
                        .ok_or_else(|| ExecutionError::EventsNotFound(vec![event_id]))?;
                    let event_data: EpochEventData = bcs::from_bytes(&event)?;
                    Ok((epoch, event_data.blob_hash))
                }
            }),
        )
        .await;
        let missing_events = committee_hashes
            .iter()
            .filter_map(|result| {
                if let Err(ExecutionError::EventsNotFound(event_ids)) = result {
                    return Some(event_ids);
                }
                None
            })
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        ensure!(
            missing_events.is_empty(),
            ExecutionError::EventsNotFound(missing_events)
        );
        let committee_hashes = committee_hashes
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let committees = futures::future::join_all(committee_hashes.into_iter().map(
            |(epoch, committee_hash)| async move {
                let blob_id = BlobId::new(committee_hash, BlobType::Committee);
                let committee_blob = self
                    .get_blob(blob_id)
                    .await?
                    .ok_or_else(|| ExecutionError::BlobsNotFound(vec![blob_id]))?;
                Ok((Epoch(epoch), bcs::from_bytes(committee_blob.bytes())?))
            },
        ))
        .await;
        let missing_blobs = committees
            .iter()
            .filter_map(|result| {
                if let Err(ExecutionError::BlobsNotFound(blob_ids)) = result {
                    return Some(blob_ids);
                }
                None
            })
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        ensure!(
            missing_blobs.is_empty(),
            ExecutionError::BlobsNotFound(missing_blobs)
        );
        committees.into_iter().collect()
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    async fn contains_event(&self, event_id: EventId) -> Result<bool, ViewError>;

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
    /// The authenticated owner of the operation, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_owner: Option<AccountOwner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
    /// The timestamp of the block containing the operation.
    pub timestamp: Timestamp,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The chain ID where the message originated from.
    pub origin: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated owner of the operation that created the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_owner: Option<AccountOwner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    pub refund_grant_to: Option<Account>,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
    /// The timestamp of the block executing the message.
    pub timestamp: Timestamp,
}

#[derive(Clone, Copy, Debug)]
pub struct ProcessStreamsContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The consensus round number, if this is a block that gets validated in a multi-leader round.
    pub round: Option<u32>,
    /// The timestamp of the current block.
    pub timestamp: Timestamp,
}

impl From<MessageContext> for ProcessStreamsContext {
    fn from(context: MessageContext) -> Self {
        Self {
            chain_id: context.chain_id,
            height: context.height,
            round: context.round,
            timestamp: context.timestamp,
        }
    }
}

impl From<OperationContext> for ProcessStreamsContext {
    fn from(context: OperationContext) -> Self {
        Self {
            chain_id: context.chain_id,
            height: context.height,
            round: context.round,
            timestamp: context.timestamp,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FinalizeContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated owner of the operation, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_owner: Option<AccountOwner>,
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
    fn application_id(&mut self) -> Result<ApplicationId, ExecutionError>;

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

    /// Reads the current application permissions for this chain.
    fn application_permissions(&mut self) -> Result<ApplicationPermissions, ExecutionError>;

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
    fn read_data_blob(&mut self, hash: DataBlobHash) -> Result<Vec<u8>, ExecutionError>;

    /// Asserts the existence of a data blob with the given hash.
    fn assert_data_blob_exists(&mut self, hash: DataBlobHash) -> Result<(), ExecutionError>;

    /// Returns true if the corresponding contract uses a zero amount of storage.
    fn has_empty_storage(&mut self, application: ApplicationId) -> Result<bool, ExecutionError>;

    /// Returns the maximum blob size from the `ResourceControlPolicy`.
    fn maximum_blob_size(&mut self) -> Result<u64, ExecutionError>;

    /// Returns whether contract log messages should be output.
    /// This is typically enabled for clients but disabled for validators.
    fn allow_application_logs(&mut self) -> Result<bool, ExecutionError>;

    /// Sends a log message (used for forwarding logs from web workers to the main thread).
    /// This is a fire-and-forget operation - errors are silently ignored.
    #[cfg(web)]
    fn send_log(&mut self, message: String, level: tracing::log::Level);
}

pub trait ServiceRuntime: BaseRuntime {
    /// Queries another application.
    fn try_query_application(
        &mut self,
        queried_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Schedules an operation to be included in the block proposed after execution.
    fn schedule_operation(&mut self, operation: Vec<u8>) -> Result<(), ExecutionError>;

    /// Checks if the service has exceeded its execution time limit.
    fn check_execution_time(&mut self) -> Result<(), ExecutionError>;
}

pub trait ContractRuntime: BaseRuntime {
    /// The authenticated owner for this execution, if there is one.
    fn authenticated_owner(&mut self) -> Result<Option<AccountOwner>, ExecutionError>;

    /// If the current message (if there is one) was rejected by its destination and is now
    /// bouncing back.
    fn message_is_bouncing(&mut self) -> Result<Option<bool>, ExecutionError>;

    /// The chain ID where the current message originated from, if there is one.
    fn message_origin_chain_id(&mut self) -> Result<Option<ChainId>, ExecutionError>;

    /// The optional authenticated caller application ID, if it was provided and if there is one
    /// based on the execution context.
    fn authenticated_caller_id(&mut self) -> Result<Option<ApplicationId>, ExecutionError>;

    /// Returns the maximum gas fuel per block.
    fn maximum_fuel_per_block(&mut self, vm_runtime: VmRuntime) -> Result<u64, ExecutionError>;

    /// Returns the amount of execution fuel remaining before execution is aborted.
    fn remaining_fuel(&mut self, vm_runtime: VmRuntime) -> Result<u64, ExecutionError>;

    /// Consumes some of the execution fuel.
    fn consume_fuel(&mut self, fuel: u64, vm_runtime: VmRuntime) -> Result<(), ExecutionError>;

    /// Schedules a message to be sent.
    fn send_message(&mut self, message: SendMessageRequest<Vec<u8>>) -> Result<(), ExecutionError>;

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
        callee_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Adds a new item to an event stream. Returns the new event's index in the stream.
    fn emit(&mut self, name: StreamName, value: Vec<u8>) -> Result<u32, ExecutionError>;

    /// Reads an event from a stream. Returns the event's value.
    ///
    /// Returns an error if the event doesn't exist.
    fn read_event(
        &mut self,
        chain_id: ChainId,
        stream_name: StreamName,
        index: u32,
    ) -> Result<Vec<u8>, ExecutionError>;

    /// Subscribes this application to an event stream.
    fn subscribe_to_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
    ) -> Result<(), ExecutionError>;

    /// Unsubscribes this application from an event stream.
    fn unsubscribe_from_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        stream_name: StreamName,
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
    ) -> Result<ChainId, ExecutionError>;

    /// Closes the current chain.
    fn close_chain(&mut self) -> Result<(), ExecutionError>;

    /// Changes the ownership of the current chain.
    fn change_ownership(&mut self, ownership: ChainOwnership) -> Result<(), ExecutionError>;

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
        required_application_ids: Vec<ApplicationId>,
    ) -> Result<ApplicationId, ExecutionError>;

    /// Returns the next application index, which is equal to the number of
    /// new applications created so far in this block.
    fn peek_application_index(&mut self) -> Result<u32, ExecutionError>;

    /// Creates a new data blob and returns its hash.
    fn create_data_blob(&mut self, bytes: Vec<u8>) -> Result<DataBlobHash, ExecutionError>;

    /// Publishes a module with contract and service bytecode and returns the module ID.
    fn publish_module(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
        vm_runtime: VmRuntime,
    ) -> Result<ModuleId, ExecutionError>;

    /// Returns the multi-leader round in which this block was validated.
    fn validation_round(&mut self) -> Result<Option<u32>, ExecutionError>;

    /// Writes a batch of changes.
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError>;
}

/// An operation to be executed in a block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub enum Operation {
    /// A system operation.
    System(Box<SystemOperation>),
    /// A user operation (in serialized form).
    User {
        application_id: ApplicationId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        bytes: Vec<u8>,
    },
}

impl BcsHashable<'_> for Operation {}

/// A message to be sent and possibly executed in the receiver's block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Allocative)]
pub enum Message {
    /// A system message.
    System(SystemMessage),
    /// A user message (in serialized form).
    User {
        application_id: ApplicationId,
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
        application_id: ApplicationId,
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

/// The kind of outgoing message being sent.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Copy, Allocative)]
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

impl Display for MessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageKind::Simple => write!(f, "Simple"),
            MessageKind::Protected => write!(f, "Protected"),
            MessageKind::Tracked => write!(f, "Tracked"),
            MessageKind::Bouncing => write!(f, "Bouncing"),
        }
    }
}

/// A posted message together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject, Allocative)]
pub struct OutgoingMessage {
    /// The destination of the message.
    pub destination: ChainId,
    /// The user authentication carried by the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_owner: Option<AccountOwner>,
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

impl BcsHashable<'_> for OutgoingMessage {}

impl OutgoingMessage {
    /// Creates a new simple outgoing message with no grant and no authenticated owner.
    pub fn new(recipient: ChainId, message: impl Into<Message>) -> Self {
        OutgoingMessage {
            destination: recipient,
            authenticated_owner: None,
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

    /// Returns the same message, with the specified authenticated owner.
    pub fn with_authenticated_owner(mut self, authenticated_owner: Option<AccountOwner>) -> Self {
        self.authenticated_owner = authenticated_owner;
        self
    }
}

impl OperationContext {
    /// Returns an account for the refund.
    /// Returns `None` if there is no authenticated owner of the [`OperationContext`].
    fn refund_grant_to(&self) -> Option<Account> {
        self.authenticated_owner.map(|owner| Account {
            chain_id: self.chain_id,
            owner,
        })
    }
}

#[cfg(with_testing)]
#[derive(Clone)]
pub struct TestExecutionRuntimeContext {
    chain_id: ChainId,
    thread_pool: Arc<ThreadPool>,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<papaya::HashMap<ApplicationId, UserContractCode>>,
    user_services: Arc<papaya::HashMap<ApplicationId, UserServiceCode>>,
    blobs: Arc<papaya::HashMap<BlobId, Blob>>,
    events: Arc<papaya::HashMap<EventId, Vec<u8>>>,
}

#[cfg(with_testing)]
impl TestExecutionRuntimeContext {
    pub fn new(chain_id: ChainId, execution_runtime_config: ExecutionRuntimeConfig) -> Self {
        Self {
            chain_id,
            thread_pool: Arc::new(ThreadPool::new(20)),
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

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.thread_pool
    }

    fn execution_runtime_config(&self) -> ExecutionRuntimeConfig {
        self.execution_runtime_config
    }

    fn user_contracts(&self) -> &Arc<papaya::HashMap<ApplicationId, UserContractCode>> {
        &self.user_contracts
    }

    fn user_services(&self) -> &Arc<papaya::HashMap<ApplicationId, UserServiceCode>> {
        &self.user_services
    }

    async fn get_user_contract(
        &self,
        description: &ApplicationDescription,
        _txn_tracker: &TransactionTracker,
    ) -> Result<UserContractCode, ExecutionError> {
        let application_id: ApplicationId = description.into();
        let pinned = self.user_contracts().pin();
        Ok(pinned
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }

    async fn get_user_service(
        &self,
        description: &ApplicationDescription,
        _txn_tracker: &TransactionTracker,
    ) -> Result<UserServiceCode, ExecutionError> {
        let application_id: ApplicationId = description.into();
        let pinned = self.user_services().pin();
        Ok(pinned
            .get(&application_id)
            .ok_or_else(|| {
                ExecutionError::ApplicationBytecodeNotFound(Box::new(description.clone()))
            })?
            .clone())
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError> {
        Ok(self.blobs.pin().get(&blob_id).cloned())
    }

    async fn get_event(&self, event_id: EventId) -> Result<Option<Vec<u8>>, ViewError> {
        Ok(self.events.pin().get(&event_id).cloned())
    }

    async fn get_network_description(&self) -> Result<Option<NetworkDescription>, ViewError> {
        let pinned = self.blobs.pin();
        let genesis_committee_blob_hash = pinned
            .iter()
            .find(|(_, blob)| blob.content().blob_type() == BlobType::Committee)
            .map_or_else(
                || CryptoHash::test_hash("genesis committee"),
                |(_, blob)| blob.id().hash,
            );
        Ok(Some(NetworkDescription {
            admin_chain_id: dummy_chain_description(0).id(),
            genesis_config_hash: CryptoHash::test_hash("genesis config"),
            genesis_timestamp: Timestamp::from(0),
            genesis_committee_blob_hash,
            name: "dummy network description".to_string(),
        }))
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        Ok(self.blobs.pin().contains_key(&blob_id))
    }

    async fn contains_event(&self, event_id: EventId) -> Result<bool, ViewError> {
        Ok(self.events.pin().contains_key(&event_id))
    }

    #[cfg(with_testing)]
    async fn add_blobs(
        &self,
        blobs: impl IntoIterator<Item = Blob> + Send,
    ) -> Result<(), ViewError> {
        let pinned = self.blobs.pin();
        for blob in blobs {
            pinned.insert(blob.id(), blob);
        }

        Ok(())
    }

    #[cfg(with_testing)]
    async fn add_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError> {
        let pinned = self.events.pin();
        for (event_id, bytes) in events {
            pinned.insert(event_id, bytes);
        }

        Ok(())
    }
}

impl From<SystemOperation> for Operation {
    fn from(operation: SystemOperation) -> Self {
        Operation::System(Box::new(operation))
    }
}

impl Operation {
    pub fn system(operation: SystemOperation) -> Self {
        Operation::System(Box::new(operation))
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
        application_id: ApplicationId,
        operation: &impl Serialize,
    ) -> Result<Self, bcs::Error> {
        Ok(Operation::User {
            application_id,
            bytes: bcs::to_bytes(&operation)?,
        })
    }

    /// Returns a reference to the [`SystemOperation`] in this [`Operation`], if this [`Operation`]
    /// is for the system application.
    pub fn as_system_operation(&self) -> Option<&SystemOperation> {
        match self {
            Operation::System(system_operation) => Some(system_operation),
            Operation::User { .. } => None,
        }
    }

    pub fn application_id(&self) -> GenericApplicationId {
        match self {
            Self::System(_) => GenericApplicationId::System,
            Self::User { application_id, .. } => GenericApplicationId::User(*application_id),
        }
    }

    /// Returns the IDs of all blobs published in this operation.
    pub fn published_blob_ids(&self) -> Vec<BlobId> {
        match self.as_system_operation() {
            Some(SystemOperation::PublishDataBlob { blob_hash }) => {
                vec![BlobId::new(*blob_hash, BlobType::Data)]
            }
            Some(SystemOperation::Admin(AdminOperation::PublishCommitteeBlob { blob_hash })) => {
                vec![BlobId::new(*blob_hash, BlobType::Committee)]
            }
            Some(SystemOperation::PublishModule { module_id }) => module_id.bytecode_blob_ids(),
            _ => vec![],
        }
    }

    /// Returns whether this operation is allowed regardless of application permissions.
    pub fn is_exempt_from_permissions(&self) -> bool {
        let Operation::System(system_op) = self else {
            return false;
        };
        matches!(
            **system_op,
            SystemOperation::ProcessNewEpoch(_)
                | SystemOperation::ProcessRemovedEpoch(_)
                | SystemOperation::UpdateStreams(_)
        )
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
        application_id: ApplicationId,
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
    /// Hash of the last `Certificate` that published or used this blob. If empty, the
    /// blob is known to be published by a confirmed certificate but we may not have fully
    /// processed this certificate just yet.
    pub last_used_by: Option<CryptoHash>,
    /// The `ChainId` of the chain that published the change
    pub chain_id: ChainId,
    /// The `BlockHeight` of the chain that published the change
    pub block_height: BlockHeight,
    /// Epoch of the `last_used_by` certificate (if any).
    pub epoch: Option<Epoch>,
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
