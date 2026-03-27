// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{rc::Rc, sync::Arc};

use futures::{
    future::{self, FutureExt as _},
    lock::Mutex as AsyncMutex,
};
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::chain_listener::{ChainListener, ClientContext as _};
use tokio_util::sync::CancellationToken;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{
    chain::Chain,
    signer::Signer,
    storage::{self, IdbEnvironment, MemEnvironment},
    wallet::Wallet,
    Error, Result,
};

/// Internal enum dispatching between IndexedDB and in-memory storage.
// This use of `futures::lock::Mutex = AsyncMutex` is safe because we only expose
// concurrency to the browser, which must always run all futures on the global task queue.
// It does nothing here in this single-threaded context, but is hard-coded by
// `ChainListener`.
#[derive(Clone)]
pub(crate) enum ClientContextInner {
    Idb(Arc<AsyncMutex<linera_client::ClientContext<IdbEnvironment>>>),
    Mem(Arc<AsyncMutex<linera_client::ClientContext<MemEnvironment>>>),
}

/// Internal enum dispatching between IndexedDB and in-memory chain clients.
pub(crate) enum ChainClientInner {
    Idb(linera_core::client::ChainClient<IdbEnvironment>),
    Mem(linera_core::client::ChainClient<MemEnvironment>),
}

impl Clone for ChainClientInner {
    fn clone(&self) -> Self {
        match self {
            Self::Idb(client) => Self::Idb(client.clone()),
            Self::Mem(client) => Self::Mem(client.clone()),
        }
    }
}

/// The full client API, exposed to the wallet implementation. Calls
/// to this API can be trusted to have originated from the user's
/// request.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: ClientContextInner,
    cancellation_token: CancellationToken,
    chain_listener_result:
        future::Shared<future::RemoteHandle<Result<(), Rc<linera_client::Error>>>>,
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default)]
/// Options for `Client.chain`.
pub struct ChainOptions {
    /// The owner to use for operations on this chain.
    owner: Option<AccountOwner>,
}

/// Storage backend selection for the client.
#[wasm_bindgen]
pub enum StorageKind {
    /// Persistent browser storage using IndexedDB.
    IndexedDb,
    /// Ephemeral in-memory storage (not persisted across page loads).
    Memory,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client and connects to the network.
    ///
    /// `storage_kind` selects the storage backend: `StorageKind.IndexedDb` for
    /// persistent browser storage, or `StorageKind.Memory` for ephemeral in-memory
    /// storage.
    ///
    /// # Errors
    /// On transport or protocol error, if persistent storage is
    /// unavailable, or if `options` is incorrectly structured.
    #[wasm_bindgen(constructor)]
    pub async fn new(
        mut wallet: Wallet,
        signer: Signer,
        storage_kind: StorageKind,
        options: Option<linera_client::Options>,
    ) -> Result<Client> {
        let options = options.unwrap_or_default();

        tracing::debug!("acquiring wallet lock...");
        wallet.lock().await?;

        match storage_kind {
            StorageKind::IndexedDb => Self::new_idb(wallet, signer, options).await,
            StorageKind::Memory => Self::new_mem(wallet, signer, options).await,
        }
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
        let options = options.unwrap_or_default();
        let chain_client = match &self.inner {
            ClientContextInner::Idb(context) => {
                let mut chain_client = context.lock().await.make_chain_client(chain).await?;
                if let Some(owner) = options.owner {
                    chain_client.set_preferred_owner(owner);
                }
                ChainClientInner::Idb(chain_client)
            }
            ClientContextInner::Mem(context) => {
                let mut chain_client = context.lock().await.make_chain_client(chain).await?;
                if let Some(owner) = options.owner {
                    chain_client.set_preferred_owner(owner);
                }
                ChainClientInner::Mem(chain_client)
            }
        };

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
        self.cancellation_token.cancel();

        if let Err(Some(e)) = self.chain_listener_result.await.map_err(Rc::into_inner) {
            return Err(e.into());
        }

        match self.inner {
            ClientContextInner::Idb(context) => {
                Arc::into_inner(context).ok_or(Error::new(
                    "Client disposed while being referenced elsewhere",
                ))?;
            }
            ClientContextInner::Mem(context) => {
                Arc::into_inner(context).ok_or(Error::new(
                    "Client disposed while being referenced elsewhere",
                ))?;
            }
        }

        Ok(())
    }
}

impl Client {
    async fn new_idb(
        wallet: Wallet,
        signer: Signer,
        options: linera_client::Options,
    ) -> Result<Client> {
        tracing::debug!("opening IndexedDB storage...");
        let mut storage = linera_base::time::timer::timeout(
            std::time::Duration::from_secs(10),
            storage::get_idb_storage(&wallet.name()),
        )
        .await
        .map_err(|_| {
            Error::new(
                "Timed out opening IndexedDB storage. \
                 Try clearing site data and reloading.",
            )
        })??;

        tracing::debug!("initializing storage...");
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;

        let default = wallet.default;
        let genesis_config = wallet.genesis_config.clone();

        tracing::debug!("creating ClientContext...");
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
        let client_clone = client.clone();
        let cancellation_token = CancellationToken::new();

        tracing::debug!("starting chain listener...");
        let chain_listener = ChainListener::new(
            options.chain_listener_config,
            client_clone,
            storage,
            cancellation_token.clone(),
            tokio::sync::mpsc::unbounded_channel().1,
            true, // Enable background sync
        )
        .run()
        .await?;

        let (run_chain_listener, chain_listener_result) = async move {
            let result = chain_listener.await.map_err(|error| {
                tracing::error!("ChainListener error: {error:?}");
                Rc::new(error)
            });
            tracing::debug!("chain listener completed");
            result
        }
        .remote_handle();

        wasm_bindgen_futures::spawn_local(run_chain_listener);

        Ok(Self {
            inner: ClientContextInner::Idb(client),
            cancellation_token,
            chain_listener_result: chain_listener_result.shared(),
        })
    }

    async fn new_mem(
        wallet: Wallet,
        signer: Signer,
        options: linera_client::Options,
    ) -> Result<Client> {
        tracing::debug!("opening in-memory storage...");
        let mut storage = storage::get_mem_storage(&wallet.name()).await?;

        tracing::debug!("initializing storage...");
        wallet
            .genesis_config
            .initialize_storage(&mut storage)
            .await?;

        let default = wallet.default;
        let genesis_config = wallet.genesis_config.clone();

        tracing::debug!("creating ClientContext...");
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
        let client_clone = client.clone();
        let cancellation_token = CancellationToken::new();

        tracing::debug!("starting chain listener...");
        let chain_listener = ChainListener::new(
            options.chain_listener_config,
            client_clone,
            storage,
            cancellation_token.clone(),
            tokio::sync::mpsc::unbounded_channel().1,
            true, // Enable background sync
        )
        .run()
        .await?;

        let (run_chain_listener, chain_listener_result) = async move {
            let result = chain_listener.await.map_err(|error| {
                tracing::error!("ChainListener error: {error:?}");
                Rc::new(error)
            });
            tracing::debug!("chain listener completed");
            result
        }
        .remote_handle();

        wasm_bindgen_futures::spawn_local(run_chain_listener);

        Ok(Self {
            inner: ClientContextInner::Mem(client),
            cancellation_token,
            chain_listener_result: chain_listener_result.shared(),
        })
    }
}
