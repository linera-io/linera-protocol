// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
# `linera-web`

This module defines the JavaScript bindings to the client API.

It is compiled to Wasm, with a JavaScript wrapper to inject its imports, and published on
NPM as `@linera/client`.

The `signer` subdirectory contains a TypeScript interface specifying the types of objects
that can be passed as signers — cryptographic integrations used to sign transactions, as
well as a demo implementation (not recommended for production use) that stores a private
key directly in memory and uses it to sign.
*/

// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]
#![recursion_limit = "256"]

use std::rc::Rc;

use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

pub mod client;
pub use client::Client;
pub mod chain;
pub use chain::Chain;
pub mod faucet;

pub mod signer;
pub use signer::Signer;
pub mod storage;
pub use storage::Storage;
pub mod wallet;
pub use wallet::Wallet;

pub type Network = linera_rpc::node_provider::NodeProvider;
pub type Environment =
    linera_core::environment::Impl<Storage, Network, Signer, Rc<linera_core::wallet::Memory>>;

type JsResult<T> = Result<T, JsError>;

#[derive(serde::Deserialize, Default, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default, rename_all = "camelCase")]
pub struct InitializeOptions {
    /// A log filter string, in the format of [`tracing_subscriber::EnvFilter`].
    /// Defaults to `INFO`.
    pub log: String,
    /// Whether to enable performance reporting through the Performance API.
    pub profiling: bool,
}

static INITIALIZED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

#[wasm_bindgen]
pub fn initialize(options: Option<InitializeOptions>) {
    use tracing_subscriber::{
        prelude::__tracing_subscriber_SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter,
    };

    if INITIALIZED.set(()).is_err() {
        tracing::warn!("already initialized, not re-initializing");
        return;
    }

    let options = options.unwrap_or_default();
    // If no log filter is provided, disable the user application log by default, to avoid
    // overwhelming the console with logs from the client library itself.
    let log_filter = if options.log.is_empty() {
        "user_application_log=off,linera_client=info"
    } else {
        &options.log
    };

    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .parse_lossy(log_filter),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_writer(tracing_web::MakeWebConsoleWriter::new()),
        )
        .with(
            Some(
                tracing_web::performance_layer()
                    .with_details_from_fields(tracing_subscriber::fmt::format::Pretty::default()),
            )
            .filter(|_| options.profiling),
        )
        .init();
}
