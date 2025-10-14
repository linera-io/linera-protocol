// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use linera_base::data_types::Amount;
use linera_persistent::{self as persistent, Persist};
use tempfile::{tempdir, TempDir};

use super::{
    local_net::PathProvider, ClientWrapper, Faucet, LineraNet, LineraNetConfig, Network,
    OnClientDrop,
};

pub struct RemoteNetTestingConfig {
    faucet: Faucet,
    close_chains: OnClientDrop,
}

impl RemoteNetTestingConfig {
    /// Creates a new [`RemoteNetTestingConfig`] for running tests with an external Linera
    /// network.
    ///
    /// The faucet URL is obtained from the `LINERA_FAUCET_URL` environment variable.
    /// If `close_chains` is true, chains will be closed on drop, otherwise they will be left active.
    pub fn new(close_chains: OnClientDrop) -> Self {
        Self {
            faucet: Faucet::new(
                env::var("LINERA_FAUCET_URL")
                    .expect("Missing `LINERA_FAUCET_URL` environment variable"),
            ),
            close_chains,
        }
    }
}

#[async_trait]
impl LineraNetConfig for RemoteNetTestingConfig {
    type Net = RemoteNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let mut net = RemoteNet::new(None, &self.faucet, self.close_chains)
            .await
            .expect("Creating RemoteNet should not fail");

        let client = net.make_client().await;
        // The tests assume we've created a genesis config with 2
        // chains with 10 tokens each. We create the first chain here
        client.wallet_init(Some(&self.faucet)).await?;
        client.request_chain(&self.faucet, true).await?;

        // And the remaining 2 here
        for _ in 0..2 {
            client
                .open_and_assign(&client, Amount::from_tokens(10))
                .await
                .unwrap();
        }

        Ok((net, client))
    }
}

/// Remote net
#[derive(Clone)]
pub struct RemoteNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    tmp_dir: Arc<TempDir>,
    close_chains: OnClientDrop,
}

#[async_trait]
impl LineraNet for RemoteNet {
    async fn ensure_is_running(&mut self) -> Result<()> {
        // Leaving this just returning for now.
        // We would have to connect to each validator in the remote net then run
        // ensure_connected_cluster_is_running
        Ok(())
    }

    async fn make_client(&mut self) -> ClientWrapper {
        let path_provider = PathProvider::TemporaryDirectory {
            tmp_dir: self.tmp_dir.clone(),
        };
        let client = ClientWrapper::new(
            path_provider,
            self.network,
            self.testing_prng_seed,
            self.next_client_id,
            self.close_chains,
        );
        if let Some(seed) = self.testing_prng_seed {
            self.testing_prng_seed = Some(seed + 1);
        }
        self.next_client_id += 1;
        client
    }

    async fn terminate(&mut self) -> Result<()> {
        // We're not killing the remote net :)
        Ok(())
    }
}

impl RemoteNet {
    async fn new(
        testing_prng_seed: Option<u64>,
        faucet: &Faucet,
        close_chains: OnClientDrop,
    ) -> Result<Self> {
        let tmp_dir = Arc::new(tempdir()?);
        // Write json config to disk
        persistent::File::new(
            tmp_dir.path().join("genesis.json").as_path(),
            faucet.genesis_config().await?,
        )?
        .persist()
        .await?;
        Ok(Self {
            network: Network::Grpc,
            testing_prng_seed,
            next_client_id: 0,
            tmp_dir,
            close_chains,
        })
    }
}
