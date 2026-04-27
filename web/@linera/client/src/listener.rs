// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::future::{self, FutureExt as _};
use linera_client::chain_listener::ChainListener;
use tokio_util::sync::CancellationToken;

use crate::Result;

pub struct Listener {
    cancellation_token: CancellationToken,
    result: future::RemoteHandle<Result<(), linera_client::Error>>,
}

impl Listener {
    pub(crate) async fn start(client: crate::Client) -> Result<Self> {
        let cancellation_token = CancellationToken::new();
        let chain_listener = ChainListener::new(
            client.chain_listener_config,
            client.context,
            client.storage,
            cancellation_token.clone(),
            tokio::sync::mpsc::unbounded_channel().1,
            true, // Enable background sync
        )
        .run()
        .await?;

        let (run_chain_listener, chain_listener_result) = async move {
            let result = chain_listener.await.map_err(|error| {
                tracing::error!("ChainListener error: {error:?}");
                error
            });
            tracing::debug!("chain listener completed");
            result
        }
        .remote_handle();

        wasm_bindgen_futures::spawn_local(run_chain_listener);

        Ok(Self {
            cancellation_token,
            result: chain_listener_result,
        })
    }
}

impl Listener {
    pub(crate) async fn stop(self) -> Result<()> {
        self.cancellation_token.cancel();
        Ok(self.result.await?)
    }
}
