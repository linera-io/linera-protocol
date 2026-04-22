// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use futures::lock::Mutex as AsyncMutex;
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::chain_listener::ClientContext as _;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{
    chain::Chain,
    listener::Listener,
    signer::Signer,
    storage::{self, Storage},
    wallet::Wallet,
    ClientContext, Error, Result,
};

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
    pub(crate) context: ClientContext,
    listener: Arc<AsyncMutex<Option<Listener>>>,
    pub(crate) chain_listener_config: linera_client::chain_listener::ChainListenerConfig,
    pub(crate) storage: Storage,
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
    ) -> Result<Client> {
        let options = options.unwrap_or_default();

        wallet.lock().await?;
        let mut storage = storage::get_storage(&wallet.name()).await?;
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;

        let default = wallet.default;
        let genesis_config = wallet.genesis_config.clone();

        let client = linera_client::ClientContext::new(
            storage.clone(),
            wallet,
            signer,
            &options,
            default,
            genesis_config,
            linera_core::worker::DEFAULT_BLOCK_CACHE_SIZE,
            linera_core::worker::DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        )
        .await?;

        Ok(Self {
            // The `Arc` here is useless, but it is required by the `ChainListener` API.
            #[expect(clippy::arc_with_non_send_sync)]
            context: Arc::new(AsyncMutex::new(client)),
            listener: Arc::default(),
            chain_listener_config: options.chain_listener_config,
            storage,
        })
    }

    /// Start listening for updates on relevant chains. If already listening, does
    /// nothing.
    ///
    /// # Errors
    ///
    /// If the chain listener could not be initialized.
    #[wasm_bindgen]
    pub async fn start(&self) -> Result<()> {
        let mut guard = self.listener.lock().await;
        if guard.is_none() {
            *guard = Some(Listener::start(self.clone()).await?);
        }
        Ok(())
    }

    /// Start listening for updates on relevant chains. If already listening, does
    /// nothing.
    ///
    /// # Errors
    ///
    /// If the chain listener could not be initialized.
    #[wasm_bindgen]
    pub async fn stop(&self) -> Result<()> {
        if let Some(listener) = self.listener.lock().await.take() {
            listener.stop().await?;
        }

        Ok(())
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
        let options = options.unwrap_or_default();
        let mut chain_client = self.context.lock().await.make_chain_client(chain).await?;
        if let Some(owner) = options.owner {
            chain_client.set_preferred_owner(owner);
        }

        Ok(Chain {
            chain_client,
            client: self.clone(),
        })
    }

    /// Cleanly shut down the client, completing when it is destroyed and all
    /// resources it owns are released.
    ///
    /// # Errors
    ///
    /// If the context is being referenced by any other objects (chains,
    /// applications…). Free these with `.free()` before disposing of this
    /// object.
    ///
    /// Propagates any errors that occurred during background execution of the
    /// client.
    #[wasm_bindgen(js_name = asyncDispose)]
    pub async fn async_dispose(self) -> Result<()> {
        self.stop().await?;

        let context = Arc::into_inner(self.context).ok_or(Error::new(
            "Client disposed while being referenced elsewhere",
        ))?;

        drop(context);

        Ok(())
    }
}
