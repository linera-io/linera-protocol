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
use self::views::{RootView, ViewStorageContext};
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

    /// Finishes the execution of the current transaction.
    ///
    /// This is called once before a transaction ends, to allow all applications that participated
    /// in the transaction to perform any final operations, and optionally it may also cancel the
    /// transaction if there are any pendencies.
    ///
    /// The default implementation persists the state, so if this method is overriden, care must be
    /// taken to persist the state manually.
    async fn finalize(&mut self) -> Result<(), Self::Error> {
        Self::State::store(self.state_mut()).await;
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
    type State: State;

    /// The desired storage backend used to store the application's state.
    ///
    /// Currently, the two supported backends are [`SimpleStateStorage`] or
    /// [`ViewStateStorage`]. Accordingly, this associated type may be defined as `type
    /// Storage = SimpleStateStorage<Self>` or `type Storage = ViewStateStorage<Self>`.
    type Storage: ServiceStateStorage;

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
/// The database can be accessed using an instance of [`ViewStorageContext`].
#[allow(async_fn_in_trait)]
pub trait State {
    /// Loads the state from the database.
    async fn load() -> Self;

    /// Persists the state into the database.
    async fn store(&mut self);
}

/// Representation of an empty persistent state.
///
/// This can be used by applications that don't need to store anything in the database.
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Serialize)]
pub struct EmptyState;

impl State for EmptyState {
    async fn load() -> Self {
        EmptyState
    }

    async fn store(&mut self) {}
}

impl<V> State for V
where
    V: RootView<ViewStorageContext>,
{
    async fn load() -> Self {
        V::load(ViewStorageContext::default())
            .await
            .expect("Failed to load application state")
    }

    async fn store(&mut self) {
        self.save()
            .await
            .expect("Failed to store application state")
    }
}
