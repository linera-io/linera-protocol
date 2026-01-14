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

// This crate is only for compiling to the Web.
#![cfg(target_arch = "wasm32")]
// We sometimes need functions in this module to be async in order to
// ensure the generated code will return a `Promise`.
#![allow(clippy::unused_async)]
#![recursion_limit = "256"]

use std::{rc::Rc, sync::Arc};

use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex};
use linera_base::identifiers::ChainId;
use linera_client::chain_listener::{ChainListener, ClientContext as _};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

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

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Client(
    // This use of `futures::lock::Mutex` is safe because we only
    // expose concurrency to the browser, which must always run all
    // futures on the global task queue.
    // It does nothing here in this single-threaded context, but is
    // hard-coded by `ChainListener`.
    Arc<AsyncMutex<linera_client::ClientContext<Environment>>>,
);

#[wasm_bindgen]
impl Client {
    /// Creates a new client and connects to the network.
    ///
    /// # Errors
    /// On transport or protocol error, if persistent storage is
    /// unavailable, or if `options` is incorrectly structured.
    #[wasm_bindgen(constructor)]
    pub async fn new(
        wallet: &Wallet,
        signer: Signer,
        options: Option<linera_client::Options>,
    ) -> Result<Client, JsError> {
        const BLOCK_CACHE_SIZE: usize = 5000;
        const EXECUTION_STATE_CACHE_SIZE: usize = 10000;

        let options = options.unwrap_or_default();

        let mut storage = storage::get_storage().await?;
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;

        let client = linera_client::ClientContext::new(
            storage.clone(),
            wallet.chains.clone(),
            signer,
            &options,
            wallet.default,
            wallet.genesis_config.clone(),
            BLOCK_CACHE_SIZE,
            EXECUTION_STATE_CACHE_SIZE,
        )
        .await?;
        // The `Arc` here is useless, but it is required by the `ChainListener` API.
        #[expect(clippy::arc_with_non_send_sync)]
        let client = Arc::new(AsyncMutex::new(client));
        let client_clone = client.clone();
        let chain_listener = ChainListener::new(
            options.chain_listener_config,
            client_clone,
            storage,
            tokio_util::sync::CancellationToken::new(),
            tokio::sync::mpsc::unbounded_channel().1,
            true, // Enable background sync
        )
        .run()
        .boxed_local()
        .await?
        .boxed_local();
        wasm_bindgen_futures::spawn_local(
            async move {
                if let Err(error) = chain_listener.await {
                    tracing::error!("ChainListener error: {error:?}");
                }
            }
            .boxed_local(),
        );
        log::info!("Linera Web client successfully initialized");
        Ok(Self(client))
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId) -> JsResult<Chain> {
        Ok(Chain {
            chain_client: self.0.lock().await.make_chain_client(chain).await?,
            client: self.clone(),
        })
    }
}

#[wasm_bindgen(start)]
pub fn main() {
    use tracing_subscriber::{
        prelude::__tracing_subscriber_SubscriberExt as _, util::SubscriberInitExt as _,
    };

    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_writer(tracing_web::MakeWebConsoleWriter::new()),
        )
        .with(
            tracing_web::performance_layer()
                .with_details_from_fields(tracing_subscriber::fmt::format::Pretty::default()),
        )
        .init();
}
