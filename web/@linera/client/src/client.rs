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

use crate::{chain::Chain, signer::Signer, storage, wallet::Wallet, Environment, Error, Result};

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
        )
        .await?;
        // The `Arc` here is useless, but it is required by the `ChainListener` API.
        #[expect(clippy::arc_with_non_send_sync)]
        let client = Arc::new(AsyncMutex::new(client));
        let client_clone = client.clone();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
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
            client_context: client,
            cancellation_token,
            chain_listener_result: chain_listener_result.shared(),
        })
    }

    /// Connect to a chain on the Linera network.
    ///
    /// # Errors
    ///
    /// If the wallet could not be read or chain synchronization fails.
    #[wasm_bindgen]
    pub async fn chain(&self, chain: ChainId, options: Option<ChainOptions>) -> Result<Chain> {
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

        let context = Arc::into_inner(self.client_context).ok_or(Error::new(
            "Client disposed while being referenced elsewhere",
        ))?;

        drop(context);

        Ok(())
    }
}
