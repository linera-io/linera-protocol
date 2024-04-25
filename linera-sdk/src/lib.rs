// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides an SDK for developing Linera applications using Rust.
//!
//! A Linera application consists of two WebAssembly binaries: a contract and a service.
//! In both binaries, there should be a shared application state. The state is a type that
//! represents what the application would like to persist in storage across blocks, and
//! must implement [`State`](crate::State) trait in order to specify how the state should be loaded
//! from and stored to the persistent key-value storage. An alternative is to use the
//! [`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a framework that
//! allows loading selected parts of the state. This is useful if the application's state is large
//! and doesn't need to be loaded in its entirety for every execution. By deriving
//! [`RootView`](views::RootView) on the state type it automatically implements the [`State`]
//! trait.
//!
//! The contract binary should create a type to implement the [`Contract`](crate::Contract) trait.
//! The type can store the [`ContractRuntime`](contract::ContractRuntime) and the state, and must
//! have its implementation exported by using the [`contract!`](crate::contract!) macro.
//!
//! The service binary should create a type to implement the [`Service`](crate::Service) trait.
//! The type can store the [`ServiceRuntime`](service::ServiceRuntime) and the state, and must have
//! its implementation exported by using the [`service!`](crate::service!) macro.
//!
//! # Examples
//!
//! The [`examples`](https://github.com/linera-io/linera-protocol/tree/main/examples)
//! directory contains some example applications.

#![deny(missing_docs)]

#[macro_use]
pub mod util;

pub mod abis;
pub mod base;
pub mod contract;
mod extensions;
pub mod graphql;
mod log;
#[cfg(not(target_arch = "wasm32"))]
pub mod mock_system_api;
pub mod service;
mod state;
#[cfg(feature = "test")]
#[cfg_attr(not(target_arch = "wasm32"), path = "./test/integration/mod.rs")]
#[cfg_attr(target_arch = "wasm32", path = "./test/unit/mod.rs")]
pub mod test;
pub mod views;

use std::{error::Error, fmt::Debug};

use linera_base::abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi};
pub use linera_base::{
    abi,
    data_types::{Resources, SendMessageRequest},
    ensure,
};
use serde::{de::DeserializeOwned, Serialize};

#[cfg(not(target_arch = "wasm32"))]
pub use self::mock_system_api::MockSystemApi;
#[doc(hidden)]
pub use self::{contract::export_contract, service::export_service};
pub use self::{
    contract::ContractRuntime,
    extensions::{FromBcsBytes, ToBcsBytes},
    log::{ContractLogger, ServiceLogger},
    service::ServiceRuntime,
    state::{EmptyState, StoreOnDrop},
};

/// The contract interface of a Linera application.
///
/// As opposed to the [`Service`] interface of an application, contract entry points
/// are triggered by the execution of blocks in a chain. Their execution may modify
/// storage and is gas-metered.
///
/// Below we use the word "transaction" to refer to the current operation or message being
/// executed.
#[allow(async_fn_in_trait)]
pub trait Contract: WithContractAbi + ContractAbi + Sized {
    /// The type used to report errors to the execution environment.
    ///
    /// Errors are not recoverable and always interrupt the current transaction. To return
    /// recoverable errors in the case of application calls, you may use the response types.
    type Error: Error + From<serde_json::Error> + From<bcs::Error>;

    /// The type used to store the persisted application state.
    type State: State;

    /// The type of message executed by the application.
    ///
    /// Messages are executed when a message created by the same application is received
    /// from another chain and accepted in a block.
    type Message: Serialize + DeserializeOwned + Debug;

    /// Immutable parameters specific to this application (e.g. the name of a token).
    type Parameters: Serialize + DeserializeOwned + Clone + Debug;

    /// Instantiation argument passed to a new application on the chain that created it
    /// (e.g. an initial amount of tokens minted).
    ///
    /// To share configuration data on every chain, use [`Contract::Parameters`]
    /// instead.
    type InstantiationArgument: Serialize + DeserializeOwned + Debug;

    /// Creates a in-memory instance of the contract handler from the application's `state`.
    async fn new(state: Self::State, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error>;

    /// Returns the current state of the application so that it can be persisted.
    fn state_mut(&mut self) -> &mut Self::State;

    /// Instantiates the application on the chain that created it.
    ///
    /// This is only called once when the application is created and only on the microchain that
    /// created the application.
    async fn instantiate(
        &mut self,
        argument: Self::InstantiationArgument,
    ) -> Result<(), Self::Error>;

    /// Applies an operation from the current block.
    ///
    /// Operations are created by users and added to blocks, serving as the starting point for an
    /// application's execution.
    async fn execute_operation(
        &mut self,
        operation: Self::Operation,
    ) -> Result<Self::Response, Self::Error>;

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
    type State: State;

    /// Immutable parameters specific to this application.
    type Parameters: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static;

    /// Creates a in-memory instance of the service handler from the application's `state`.
    async fn new(state: Self::State, runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error>;

    /// Executes a read-only query on the state of this application.
    async fn handle_query(&self, query: Self::Query) -> Result<Self::QueryResponse, Self::Error>;
}

/// The persistent state of a Linera application.
///
/// This is the state that is persisted to the database, and preserved across transactions. The
/// application's [`Contract`] is allowed to modiy the state, while the application's [`Service`]
/// can only read it.
///
/// The database can be accessed using an instance of
/// [`ViewStorageContext`](views::ViewStorageContext).
#[allow(async_fn_in_trait)]
pub trait State {
    /// Loads the state from the database.
    async fn load() -> Self;

    /// Persists the state into the database.
    async fn store(&mut self);
}
