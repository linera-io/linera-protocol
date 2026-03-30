// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{rc::Rc, sync::Arc};

use futures::{
    future::{self, FutureExt as _},
    lock::Mutex as AsyncMutex,
};
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _};
use tokio_util::sync::CancellationToken;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{
    chain::Chain,
    signer::Signer,
    storage::{self, IdbEnvironment, IdbStorage, MemEnvironment, MemStorage},
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

/// Storage backend, dispatching between IndexedDB and in-memory.
pub(crate) enum Storage {
    Idb(IdbStorage),
    Mem(MemStorage),
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default)]
/// Options for `Client.chain` / `Client.chain`.
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

/// A client that has been created but whose chain listener has not yet started.
///
/// Use `chain()` to perform pre-start operations (e.g. `addOwner`), then call
/// `start()` to begin background synchronization and obtain a [`Client`].
#[wasm_bindgen]
pub struct Client {
    inner: ClientContextInner,
    storage: Storage,
    chain_listener_config: ChainListenerConfig,
}

#[wasm_bindgen]
impl Client {
    /// Creates a new client without starting the chain listener.
    ///
    /// After creating the client, you can perform operations like `addOwner`
    /// via `chain()`. Call `start()` to begin background chain synchronization
    /// and obtain a [`Client`].
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
            StorageKind::IndexedDb => Self::create_idb(wallet, signer, options).await,
            StorageKind::Memory => Self::create_mem(wallet, signer, options).await,
        }
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
        make_chain(&self.inner, chain, options).await
    }

    /// Starts the chain listener for background synchronization, consuming this
    /// `Client` and returning a [`Client`].
    ///
    /// # Errors
    /// If the chain listener fails to start.
    #[wasm_bindgen]
    pub async fn start(self) -> Result<RunningClient> {
        let cancellation_token = CancellationToken::new();

        let chain_listener_result = match (&self.storage, &self.inner) {
            (Storage::Idb(storage), ClientContextInner::Idb(client)) => {
                start_listener(
                    self.chain_listener_config,
                    client.clone(),
                    storage.clone(),
                    cancellation_token.clone(),
                )
                .await?
            }
            (Storage::Mem(storage), ClientContextInner::Mem(client)) => {
                start_listener(
                    self.chain_listener_config,
                    client.clone(),
                    storage.clone(),
                    cancellation_token.clone(),
                )
                .await?
            }
            _ => unreachable!("mismatched storage and client context"),
        };

        Ok(RunningClient {
            inner: self.inner,
            cancellation_token,
            chain_listener_result,
        })
    }
}

impl Client {
    async fn create_idb(
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
        let chain_listener_config = options.chain_listener_config.clone();

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

        Ok(Client {
            inner: ClientContextInner::Idb(client),
            storage: Storage::Idb(storage),
            chain_listener_config,
        })
    }

    async fn create_mem(
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
        let chain_listener_config = options.chain_listener_config.clone();

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

        Ok(Client {
            inner: ClientContextInner::Mem(client),
            storage: Storage::Mem(storage),
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
    pub(crate) inner: ClientContextInner,
    cancellation_token: CancellationToken,
    chain_listener_result:
        future::Shared<future::RemoteHandle<Result<(), Rc<linera_client::Error>>>>,
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
        make_chain(&self.inner, chain, options).await
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

/// Shared implementation for creating a [`Chain`] from a [`ClientContextInner`].
async fn make_chain(
    inner: &ClientContextInner,
    chain: ChainId,
    options: Option<ChainOptions>,
) -> Result<Chain> {
    let options = options.unwrap_or_default();
    let chain_client = match inner {
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
        inner: inner.clone(),
        chain_client,
    })
}

async fn start_listener<C: linera_client::chain_listener::ClientContext + 'static>(
    config: ChainListenerConfig,
    context: Arc<AsyncMutex<C>>,
    storage: <C::Environment as linera_core::Environment>::Storage,
    cancellation_token: CancellationToken,
) -> Result<future::Shared<future::RemoteHandle<Result<(), Rc<linera_client::Error>>>>> {
    tracing::debug!("Client: starting chain listener...");
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
    tracing::debug!("Client: chain listener started");

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

    Ok(chain_listener_result.shared())
}
