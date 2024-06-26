// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use linera_base::data_types::Amount;
use linera_execution::ResourceControlPolicy;
use tempfile::{tempdir, TempDir};

use crate::{
    cli_wrappers::{
        local_net::PathProvider, ClientWrapper, Faucet, FaucetOption, LineraNet, LineraNetConfig,
        Network,
    },
    persistent::{self, Persistent},
};

pub struct RemoteNetTestingConfig {
    faucet: Faucet,
}

impl RemoteNetTestingConfig {
    pub fn new(faucet_url: Option<String>) -> Self {
        Self {
            faucet: Faucet::new(
                faucet_url.unwrap_or(
                    env::var("LINERA_FAUCET_URL")
                        .unwrap_or("https://faucet.devnet.linera.net".to_string()),
                ),
            ),
        }
    }
}

#[async_trait]
impl LineraNetConfig for RemoteNetTestingConfig {
    type Net = RemoteNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let seed = 37;
        let mut net = RemoteNet::new(Some(seed), &self.faucet)
            .await
            .expect("Creating RemoteNet should not fail");

        let client = net.make_client().await;
        // The tests assume we've created a genesis config with 10
        // chains with 10 tokens each. We create the first chain here
        client
            .wallet_init(&[], FaucetOption::NewChain(&self.faucet))
            .await
            .unwrap();

        // And the remaining 9 here
        for _ in 0..9 {
            client
                .open_and_assign(&client, Amount::from_tokens(10))
                .await
                .unwrap();
        }

        Ok((net, client))
    }

    async fn policy(&self) -> ResourceControlPolicy {
        self.faucet
            .genesis_config()
            .await
            .expect("should get genesis config from faucet")
            .policy
    }
}

/// Remote net
#[derive(Clone)]
pub struct RemoteNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    tmp_dir: Arc<TempDir>,
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
    async fn new(testing_prng_seed: Option<u64>, faucet: &Faucet) -> Result<Self> {
        let tmp_dir = Arc::new(tempdir()?);
        // Write json config to disk
        Persistent::save(&mut persistent::File::new(
            tmp_dir.path().join("genesis.json").as_path(),
            faucet.genesis_config().await?,
        )?)?;
        Ok(Self {
            network: Network::Grpc,
            testing_prng_seed,
            next_client_id: 0,
            tmp_dir,
        })
    }
}
