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
mod base;
pub mod contract;
#[cfg(feature = "ethereum")]
pub mod ethereum;
mod extensions;
pub mod formats;
pub mod graphql;
pub mod linera_base_types;
mod log;
pub mod service;
#[cfg(with_testing)]
pub mod test;
pub mod views;

use std::fmt::Debug;

pub use bcs;
pub use linera_base::{
    abi,
    data_types::{
        CanonicalBTreeMap, CanonicalBTreeSet, NonCanonicalBTreeMap, NonCanonicalBTreeSet,
        Resources, SendMessageRequest,
    },
    ensure, http, task_processor,
};
use linera_base::{
    abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi},
    data_types::StreamUpdate,
};
use serde::{de::DeserializeOwned, Serialize};
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

/// Declares a token marker type implementing [`Token`](linera_base_types::Token), for use as
/// the brand of a [`TokenAmount`](linera_base_types::TokenAmount).
///
/// Two forms are supported:
///
/// - **Runtime precision** — the number of decimals is set once, from the application
///   parameters, via the generated `configure_decimals` associated function. Call it in
///   `Contract::load` and `Service::new`. Reading the precision before it is configured panics.
///
///   ```ignore
///   linera_sdk::branded_token!(pub struct MyToken = "MyToken");
///   // in load()/new():
///   MyToken::configure_decimals(runtime.application_parameters().decimals);
///   ```
///
/// - **Fixed precision** — the number of decimals is known at compile time; no global is used.
///
///   ```ignore
///   linera_sdk::branded_token!(pub struct MyToken = "MyToken", decimals = 6);
///   ```
///
/// The string literal is the token's [`Token::NAME`](linera_base_types::Token::NAME), used as
/// the serde/GraphQL container name.
#[macro_export]
macro_rules! branded_token {
    ($(#[$meta:meta])* $vis:vis struct $name:ident = $wire:literal $(,)?) => {
        $(#[$meta])*
        $vis struct $name;

        const _: () = {
            static DECIMALS: ::std::sync::OnceLock<u8> = ::std::sync::OnceLock::new();

            impl $crate::linera_base_types::Token for $name {
                const NAME: &'static str = $wire;

                fn decimals() -> u8 {
                    *DECIMALS.get().expect(::core::concat!(
                        ::core::stringify!($name),
                        "::decimals() called before configure_decimals()"
                    ))
                }
            }

            impl $name {
                /// Sets this token's precision. Call exactly once, in `Contract::load` and
                /// `Service::new`, from the application parameters. Subsequent calls are ignored.
                $vis fn configure_decimals(decimals: u8) {
                    DECIMALS.get_or_init(|| decimals);
                }
            }
        };
    };

    ($(#[$meta:meta])* $vis:vis struct $name:ident = $wire:literal, decimals = $decimals:expr $(,)?) => {
        $(#[$meta])*
        $vis struct $name;

        impl $crate::linera_base_types::Token for $name {
            const NAME: &'static str = $wire;

            fn decimals() -> u8 {
                $decimals
            }
        }
    };
}

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

    /// Event values for streams created by this application.
    type EventValue: Serialize + DeserializeOwned + Debug;

    /// Creates an in-memory instance of the contract handler.
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
    /// Messages are sent across chains. These messages are created and received by
    /// the same application. Messages can be either single-sender and single-receiver, or
    /// single-sender and multiple-receivers. The former allows sending cross-chain messages to the
    /// application on some other specific chain, while the latter uses broadcast channels to
    /// send a message to multiple other chains where the application is subscribed to a
    /// sender channel on this chain.
    ///
    /// For a message to be executed, a user must mark it to be received in a block of the receiver
    /// chain.
    async fn execute_message(&mut self, message: Self::Message);

    /// Reacts to new events on streams.
    ///
    /// This is called whenever there is a new event on any stream that this application
    /// subscribes to.
    async fn process_streams(&mut self, _updates: Vec<StreamUpdate>) {}

    /// Summarizes the application's own event streams at a checkpoint.
    ///
    /// This is called when a checkpoint is created, once for each of the application's own
    /// streams that published events since the previous checkpoint. The application may emit
    /// a summary event to such a stream: after the checkpoint, only the latest summary is
    /// guaranteed to remain available, while older events may eventually be dropped. An
    /// application that emits no summary lets the stream's older events lapse; a stream that
    /// is not summarized at a checkpoint is effectively closed.
    async fn summarize_events(&mut self, _updates: Vec<StreamUpdate>) {}

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

    /// Creates an in-memory instance of the service handler.
    async fn new(runtime: ServiceRuntime<Self>) -> Self;

    /// Executes a read-only query on the state of this application.
    async fn handle_query(&self, query: Self::Query) -> Self::QueryResponse;
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use crate::linera_base_types::{Token, TokenAmount};

    branded_token!(struct FixedToken = "FixedToken", decimals = 6);
    branded_token!(struct RuntimeToken = "RuntimeToken");

    #[test]
    fn fixed_precision_token() {
        assert_eq!(6, FixedToken::decimals());
        assert_eq!(
            "1.5",
            TokenAmount::<FixedToken>::from_inner(1_500_000).to_string()
        );
    }

    #[test]
    fn runtime_precision_token_configures_once() {
        RuntimeToken::configure_decimals(2);
        assert_eq!(2, RuntimeToken::decimals());
        // Subsequent calls are ignored (first wins).
        RuntimeToken::configure_decimals(9);
        assert_eq!(2, RuntimeToken::decimals());
        assert_eq!(
            "1.5",
            TokenAmount::<RuntimeToken>::from_inner(150).to_string()
        );
    }

    #[test]
    #[should_panic(expected = "configure_decimals")]
    fn runtime_precision_panics_before_configuration() {
        branded_token!(struct UnconfiguredToken = "UnconfiguredToken");
        UnconfiguredToken::decimals();
    }
}
