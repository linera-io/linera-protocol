// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex};
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::chain_listener::{ChainListener, ClientContext as _};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{chain::Chain, signer::Signer, storage, wallet::Wallet, Environment, JsResult};

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
    pub(crate) client_context: Arc<AsyncMutex<linera_client::ClientContext<Environment>>>,
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default)]
/// Options for `Client.chain`.
pub struct ChainOptions {
    /// The owner to use for operations on this chain.
    owner: Option<AccountOwner>,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client and connects to the network.
    ///
    /// # Errors
    /// On transport or protocol error, if persistent storage is
    /// unavailable, or if `options` is incorrectly structured.
    #[wasm_bindgen(constructor)]
    pub async fn new(
        mut wallet: Wallet,
        signer: Signer,
        options: Option<linera_client::Options>,
    ) -> Result<Client, JsError> {
        const BLOCK_CACHE_SIZE: usize = 5000;
        const EXECUTION_STATE_CACHE_SIZE: usize = 10000;

        let options = options.unwrap_or_default();

        wallet.lock().await?;
        let mut storage = storage::get_storage(&wallet.name()).await?;
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
        Ok(Self {
            client_context: client,
        })
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> JsResult<Chain> {
        let options = options.unwrap_or_default();
        let mut chain_client = self
            .client_context
            .lock()
            .await
            .make_chain_client(chain)
            .await?;
        if let Some(owner) = options.owner {
            chain_client.set_preferred_owner(owner);
        }

        Ok(Chain {
            chain_client,
            client: self.clone(),
        })
    }
}
