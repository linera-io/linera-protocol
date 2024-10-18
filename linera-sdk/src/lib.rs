// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides an SDK for developing Linera applications using Rust.
//!
//! A Linera application consists of two WebAssembly binaries: a contract and a service.
//! Both binaries have access to the same application and chain specific storage. The service only
//! has read-only access, while the contract can write to it. The storage should be used to store
//! the application state, which is persisted across blocks. The state can be a custom type that
//! uses [`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a framework
//! that allows lazily loading selected parts of the state. This is useful if the application's
//! state is large and doesn't need to be loaded in its entirety for every execution.
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
#[cfg(feature = "ethereum")]
pub mod ethereum;
mod extensions;
pub mod graphql;
mod log;
pub mod service;
#[cfg(with_testing)]
pub mod test;
pub mod views;

use std::fmt::Debug;

pub use bcs;
pub use linera_base::{
    abi,
    data_types::{Resources, SendMessageRequest},
    ensure, http,
};
use linera_base::{
    abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi},
    crypto::CryptoHash,
    doc_scalar,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub use serde_json;

#[doc(hidden)]
pub use self::{contract::export_contract, service::export_service};
pub use self::{
    contract::ContractRuntime,
    extensions::{FromBcsBytes, ToBcsBytes},
    log::{ContractLogger, ServiceLogger},
    service::ServiceRuntime,
    views::{KeyValueStore, ViewStorageContext},
};

/// Hash of a data blob.
#[derive(Eq, Hash, PartialEq, Debug, Serialize, Deserialize, Clone, Copy)]
pub struct DataBlobHash(pub CryptoHash);

doc_scalar!(DataBlobHash, "Hash of a Data Blob");

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

    /// Creates a in-memory instance of the contract handler.
    async fn load(runtime: ContractRuntime<Self>) -> Self;

    /// Instantiates the application on the chain that created it.
    ///
    /// This is only called once when the application is created and only on the microchain that
    /// created the application.
    async fn instantiate(&mut self, argument: Self::InstantiationArgument);

    /// Applies an operation from the current block.
    ///
    /// Operations are created by users and added to blocks, serving as the starting point for an
    /// application's execution.
    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response;

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
    async fn execute_message(&mut self, message: Self::Message);

    /// Finishes the execution of the current transaction.
    ///
    /// This is called once at the end of the transaction, to allow all applications that
    /// participated in the transaction to perform any final operations, such as persisting their
    /// state.
    ///
    /// The application may also cancel the transaction by panicking if there are any pendencies.
    async fn store(self);
}

/// The service interface of a Linera application.
///
/// As opposed to the [`Contract`] interface of an application, service entry points
/// are triggered by JSON queries (typically GraphQL). Their execution cannot modify
/// storage and is not gas-metered.
#[allow(async_fn_in_trait)]
pub trait Service: WithServiceAbi + ServiceAbi + Sized {
    /// Immutable parameters specific to this application.
    type Parameters: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static;

    /// Creates a in-memory instance of the service handler.
    async fn new(runtime: ServiceRuntime<Self>) -> Self;

    /// Executes a read-only query on the state of this application.
    async fn handle_query(&self, query: Self::Query) -> Self::QueryResponse;
}
