// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides an SDK for developing Linera applications using Rust.
//!
//! A Linera application consists of two WebAssembly binaries: a contract and a service.
//! In both binaries, there should be a shared application state. The state is a type that
//! represents what the application would like to persist in storage across blocks, and
//! must implement the [`Contract`](crate::Contract) trait in the contract binary and the
//! [`Service`](crate::Service) trait in the service binary.
//!
//! The application can select between two storage backends to use. Selecting the storage
//! backend is done by specifying both the [`Contract::Storage`](crate::Contract::Storage)
//! and the [`Service::Storage`](crate::Service::Storage) associated types.
//!
//! The [`SimpleStateStorage`](crate::SimpleStateStorage) backend stores the application's
//! state type by serializing it into binary blob. This allows the entire contents of the
//! state to be persisted and made available to the application when it is executed.
//!
//! The [`ViewStateStorage`](crate::ViewStateStorage) backend stores the application's
//! state using the
//! [`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a
//! framework that allows loading selected parts of the state. This is useful if the
//! application's state is large and doesn't need to be loaded in its entirety for every
//! execution.
//!
//! The contract binary should use the [`contract!`](crate::contract!) macro to export the application's contract
//! endpoints implemented via the [`Contract`](crate::Contract) trait implementation.
//!
//! The service binary should use the [`service!`](crate::service!) macro to export the application's service
//! endpoints implemented via the [`Service`](crate::Service) trait implementation.
//!
//! # Examples
//!
//! The [`examples`](https://github.com/linera-io/linera-protocol/tree/main/examples)
//! directory contains some example applications.

#![deny(missing_docs)]

pub mod base;
pub mod contract;
mod extensions;
pub mod graphql;
mod log;
pub mod service;
#[cfg(feature = "test")]
#[cfg_attr(not(target_arch = "wasm32"), path = "./test/integration/mod.rs")]
#[cfg_attr(target_arch = "wasm32", path = "./test/unit/mod.rs")]
pub mod test;
pub mod util;
pub mod views;

use std::{error::Error, fmt::Debug};

use async_trait::async_trait;
pub use linera_base::{
    abi,
    data_types::{OutgoingMessage, Resources},
    ensure,
};
use linera_base::{
    abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi},
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, MessageId, Owner},
};
use serde::{Deserialize, Serialize};
#[doc(hidden)]
pub use wit_bindgen_guest_rust;

use self::contract::ContractStateStorage;
pub use self::{
    contract::ContractRuntime,
    extensions::{FromBcsBytes, ToBcsBytes},
    log::{ContractLogger, ServiceLogger},
    service::{ServiceRuntime, ServiceStateStorage},
};

/// A simple state management runtime based on a single byte array.
pub struct SimpleStateStorage<A>(std::marker::PhantomData<A>);

/// A state management runtime based on [`linera_views`].
pub struct ViewStateStorage<A>(std::marker::PhantomData<A>);

/// The contract interface of a Linera application.
///
/// As opposed to the [`Service`] interface of an application, contract entry points
/// are triggered by the execution of blocks in a chain. Their execution may modify
/// storage and is gas-metered.
///
/// Below we use the word "transaction" to refer to the current operation or message being
/// executed.
#[async_trait]
pub trait Contract: WithContractAbi + ContractAbi + Send + Sized {
    /// The type used to report errors to the execution environment.
    ///
    /// Errors are not recoverable and always interrupt the current transaction. To return
    /// recoverable errors in the case of application calls, you may use the response types.
    type Error: Error + From<serde_json::Error> + From<bcs::Error> + 'static;

    /// The type used to store the persisted application state.
    type State: Sync;

    /// The desired storage backend used to store the application's state.
    ///
    /// Currently, the two supported backends are [`SimpleStateStorage`] or
    /// [`ViewStateStorage`]. Accordingly, this associated type may be defined as `type
    /// Storage = SimpleStateStorage<Self>` or `type Storage = ViewStateStorage<Self>`.
    ///
    /// The first deployment on other chains will use the [`Default`] implementation of the application
    /// state if [`SimpleStateStorage`] is used, or the [`Default`] value of all sub-views in the
    /// state if the [`ViewStateStorage`] is used.
    type Storage: ContractStateStorage<Self> + Send + 'static;

    /// Creates a in-memory instance of the contract handler from the application's `state`.
    async fn new(state: Self::State, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error>;

    /// Returns the current state of the application so that it can be persisted.
    fn state_mut(&mut self) -> &mut Self::State;

    /// Initializes the application on the chain that created it.
    ///
    /// This is only called once when the application is created and only on the microchain that
    /// created the application.
    async fn initialize(
        &mut self,
        argument: Self::InitializationArgument,
    ) -> Result<(), Self::Error>;

    /// Applies an operation from the current block.
    ///
    /// Operations are created by users and added to blocks, serving as the starting point for an
    /// application's execution.
    async fn execute_operation(&mut self, operation: Self::Operation) -> Result<(), Self::Error>;

    /// Applies a message originating from a cross-chain message.
    ///
    /// Messages are messages sent across chains. These messages are created and received by
    /// the same application. Messages can be either single-sender and single-receiver, or
    /// single-sender and multiple-receivers. The former allows sending cross-chain messages to the
    /// application on some other specific chain, while the latter uses broadcast channels to
    /// send a message to multiple other chains where the application is subscribed to a
    /// sender channel on this chain.
    ///
    /// For a message to be executed, a user must mark it to be received in a block of the receiver
    /// chain.
    async fn execute_message(&mut self, message: Self::Message) -> Result<(), Self::Error>;

    /// Handles a call from another application.
    ///
    /// Cross-application calls allow applications to interact inside a chain. An
    /// application can call any other application available on the chain the
    /// execution is taking place on.
    ///
    /// Use the `Self::call_application` method generated by the [`contract!`] macro to call
    /// another application.
    ///
    /// Returns a `Self::Response` to be forwarded to the caller.
    async fn handle_application_call(
        &mut self,
        argument: Self::ApplicationCall,
    ) -> Result<Self::Response, Self::Error>;

    /// Finishes the execution of the current transaction.
    ///
    /// This is called once before a transaction ends, to allow all applications that participated
    /// in the transaction to perform any final operations, and optionally it may also cancel the
    /// transaction if there are any pendencies.
    ///
    /// The default implementation persists the state, so if this method is overriden, care must be
    /// taken to persist the state manually.
    async fn finalize(&mut self) -> Result<(), Self::Error> {
        Self::Storage::store(self.state_mut()).await;
        Ok(())
    }
}

/// The service interface of a Linera application.
///
/// As opposed to the [`Contract`] interface of an application, service entry points
/// are triggered by JSON queries (typically GraphQL). Their execution cannot modify
/// storage and is not gas-metered.
#[allow(async_fn_in_trait)]
pub trait Service: WithServiceAbi + ServiceAbi + Sized {
    /// Type used to report errors to the execution environment.
    ///
    /// Errors are not recoverable and always interrupt the current query.
    type Error: Error + From<serde_json::Error>;

    /// The type used to store the persisted application state.
    type State;

    /// The desired storage backend used to store the application's state.
    ///
    /// Currently, the two supported backends are [`SimpleStateStorage`] or
    /// [`ViewStateStorage`]. Accordingly, this associated type may be defined as `type
    /// Storage = SimpleStateStorage<Self>` or `type Storage = ViewStateStorage<Self>`.
    type Storage: ServiceStateStorage;

    /// Creates a in-memory instance of the service handler from the application's `state`.
    async fn new(state: Self::State, runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error>;

    /// Executes a read-only query on the state of this application.
    async fn handle_query(&self, query: Self::Query) -> Result<Self::QueryResponse, Self::Error>;
}

/// The context of the execution of an application's operation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u32,
}

/// The context of the execution of an application's message.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The ID of the message (based on the operation height and index in the remote
    /// chain that created the message).
    pub message_id: MessageId,
}

/// The context of the execution of an application's cross-application call handler.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalleeContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

/// The context of the execution of an application's query.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The height of the next block on this chain.
    pub next_block_height: BlockHeight,
}
