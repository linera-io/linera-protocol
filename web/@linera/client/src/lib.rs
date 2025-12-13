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

use std::{collections::HashMap, future::Future, rc::Rc, sync::Arc};

use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_base::identifiers::{AccountOwner, ApplicationId, ChainId};
use linera_client::{
    chain_listener::{ChainListener, ClientContext as _},
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use linera_views::store::WithError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

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
pub type Environment = linera_core::environment::Impl<Storage, Network, Signer, Rc<linera_core::wallet::Memory>>;
type JsResult<T> = Result<T, JsError>;

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Client {
    // This use of `futures::lock::Mutex` is safe because we only
    // expose concurrency to the browser, which must always run all
    // futures on the global task queue.
    // It does nothing here in this single-threaded context, but is
    // hard-coded by `ChainListener`.
    client: Arc<AsyncMutex<linera_client::ClientContext<Environment>>>,
}

fn true_() -> bool { true }

#[derive(serde::Deserialize, tsify_next::Tsify)]
#[tsify(from_wasm_abi)]
pub struct StorageOptions {
    /// Whether to output logs from running contracts.
    #[serde(default = "true_")]
    pub allow_application_logs: bool,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            allow_application_logs: true,
        }
    }
}

#[derive(Default, serde::Deserialize, tsify_next::Tsify)]
#[tsify(from_wasm_abi)]
pub struct Options {
    #[serde(flatten)]
    pub storage: StorageOptions,
    #[serde(flatten)]
    pub client: linera_client::Options,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client and connects to the network.
    ///
    /// # Errors
    /// On transport or protocol error, or if persistent storage is
    /// unavailable.
    #[wasm_bindgen(constructor)]
    pub async fn new(
        wallet: &Wallet,
        signer: Signer,
        options: Options,
    ) -> Result<Client, JsError> {
        const BLOCK_CACHE_SIZE: usize = 5000;
        const EXECUTION_STATE_CACHE_SIZE: usize = 10000;

        let mut storage = storage::get_storage()
            .await?
            // TODO(TODO) this should be moved into `linera_client`
            .with_allow_application_logs(options.storage.allow_application_logs);
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;
        let client = linera_client::ClientContext::new(
            storage.clone(),
            wallet.chains.clone(),
            signer,
            &options.client,
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
            options.client.chain_listener_config,
            client_clone,
            storage,
            tokio_util::sync::CancellationToken::new(),
            tokio::sync::mpsc::unbounded_channel().1,
        )
        .run(true) // Enable background sync
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
        Ok(Self { client })
    }

    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId) -> JsResult<Chain> {
        Ok(Chain {
            chain_client: self.client.lock().await.make_chain_client(chain).await?,
            client: self.client.clone(),
        })
    }
}
