// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, pin::Pin, rc::Rc, sync::Arc};

use futures::{
    future::{FutureExt as _, RemoteHandle},
    lock::Mutex as AsyncMutex,
};
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _};
use tokio_util::sync::CancellationToken;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{
    chain::Chain, signer::Signer, storage, wallet::Wallet, Environment, Error, Result, Storage,
};

pub(crate) type ClientContext = Arc<AsyncMutex<linera_client::ClientContext<Environment>>>;

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default)]
/// Options for `Client.chain`.
pub struct ChainOptions {
    /// The owner to use for operations on this chain.
    owner: Option<AccountOwner>,
}

/// A client that has been created but whose chain listener has not yet started.
///
/// Use `chain()` to perform pre-start operations (e.g. `addOwner`), then call
/// `start()` to begin background synchronization and obtain a [`RunningClient`].
#[wasm_bindgen]
pub struct Client {
    pub(crate) client_context: ClientContext,
    storage: Storage,
    chain_listener_config: ChainListenerConfig,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client without starting the chain listener.
    ///
    /// After creating the client, you can perform operations like `addOwner`
    /// via `chain()`. Call `start()` to begin background chain synchronization
    /// and obtain a [`RunningClient`].
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
        let chain_listener_config = options.chain_listener_config.clone();

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
        // The `Arc` here is useless, but it is required by the `ChainListener` API.
        #[expect(clippy::arc_with_non_send_sync)]
        let client = Arc::new(AsyncMutex::new(client));

        Ok(Client {
            client_context: client,
            storage,
            chain_listener_config,
        })
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
        make_chain(&self.client_context, chain, options).await
    }

    /// Cleanly shut down the client without starting the chain listener.
    ///
    /// # Errors
    ///
    /// If the context is being referenced by any other objects (chains,
    /// applications…). Free these with `.free()` before disposing of this
    /// object.
    #[wasm_bindgen(js_name = asyncDispose)]
    pub async fn async_dispose(self) -> Result<()> {
        Arc::into_inner(self.client_context).ok_or(Error::new(
            "Client disposed while being referenced elsewhere",
        ))?;
        Ok(())
    }

    /// Starts the chain listener for background synchronization, consuming this
    /// `Client` and returning a [`RunningClient`].
    ///
    /// # Errors
    /// If the chain listener fails to start.
    #[wasm_bindgen]
    pub async fn start(self) -> Result<RunningClient> {
        let cancellation_token = CancellationToken::new();
        let chain_listener_config = self.chain_listener_config;

        let chain_listener_handle = start_listener(
            chain_listener_config.clone(),
            self.client_context.clone(),
            self.storage,
            cancellation_token.clone(),
        )
        .await?;

        Ok(RunningClient {
            client_context: self.client_context,
            cancellation_token,
            chain_listener_handle: Box::pin(chain_listener_handle),
            chain_listener_config,
        })
    }
}

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request.
///
/// Obtained by calling [`Client::start()`].
#[wasm_bindgen]
pub struct RunningClient {
    pub(crate) client_context: ClientContext,
    cancellation_token: CancellationToken,
    chain_listener_handle: Pin<Box<dyn Future<Output = Result<Storage, Rc<linera_client::Error>>>>>,
    chain_listener_config: ChainListenerConfig,
}

#[wasm_bindgen]
impl RunningClient {
    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
        make_chain(&self.client_context, chain, options).await
    }

    /// Stops the chain listener and returns a [`Client`] that can be reconfigured
    /// and restarted.
    ///
    /// # Errors
    /// Propagates any errors that occurred during background execution of the
    /// chain listener.
    #[wasm_bindgen]
    pub async fn stop(self) -> Result<Client> {
        self.cancellation_token.cancel();

        let storage = self
            .chain_listener_handle
            .await
            .map_err(|e| match Rc::into_inner(e) {
                Some(e) => Error::from(e),
                None => Error::new("chain listener error (details unavailable)"),
            })?;

        Ok(Client {
            client_context: self.client_context,
            storage,
            chain_listener_config: self.chain_listener_config,
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
        self.cancellation_token.cancel();

        if let Err(e) = self.chain_listener_handle.await {
            if let Some(e) = Rc::into_inner(e) {
                return Err(e.into());
            }
        }

        Arc::into_inner(self.client_context).ok_or(Error::new(
            "Client disposed while being referenced elsewhere",
        ))?;

        Ok(())
    }
}

/// Shared implementation for creating a [`Chain`] from a [`ClientContext`].
async fn make_chain(
    client_context: &ClientContext,
    chain: ChainId,
    options: Option<ChainOptions>,
) -> Result<Chain> {
    let options = options.unwrap_or_default();
    let mut chain_client = client_context.lock().await.make_chain_client(chain).await?;
    if let Some(owner) = options.owner {
        chain_client.set_preferred_owner(owner);
    }

    Ok(Chain {
        client_context: client_context.clone(),
        chain_client,
    })
}

async fn start_listener(
    config: ChainListenerConfig,
    context: ClientContext,
    storage: Storage,
    cancellation_token: CancellationToken,
) -> Result<RemoteHandle<Result<Storage, Rc<linera_client::Error>>>> {
    tracing::debug!("starting chain listener...");
    let chain_listener = ChainListener::new(
        config,
        context,
        storage,
        cancellation_token,
        tokio::sync::mpsc::unbounded_channel().1,
        true, // Enable background sync
    )
    .run()
    .await?;

    let (run_chain_listener, chain_listener_handle) = async move {
        let result = chain_listener.await.map_err(|error| {
            tracing::error!("ChainListener error: {error:?}");
            Rc::new(error)
        });
        tracing::debug!("chain listener completed");
        result
    }
    .remote_handle();

    wasm_bindgen_futures::spawn_local(run_chain_listener);

    Ok(chain_listener_handle)
}
