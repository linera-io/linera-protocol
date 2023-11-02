// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cli_wrappers::{ClientWrapper, LineraNet, LineraNetConfig, Network},
    util::CommandExt,
    util::{self, current_binary_parent},
};
use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use std::{env, fs, sync::Arc};
use tempfile::{tempdir, TempDir};
use tokio::process::Command;

use super::{docker_wrapper::DockerWrapper, helm_wrapper::HelmWrapper, kind_wrapper::KindWrapper};

/// The information needed to start a [`LocalKubernetesNet`].
pub struct LocalKubernetesNetConfig {
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub binaries_dir: Option<String>,
}

/// A simplified version of [`LocalKubernetesNetConfig`]
#[cfg(any(test, feature = "test"))]
pub struct LocalKubernetesNetTestingConfig {
    pub network: Network,
}

/// A set of Linera validators running locally as native processes.
pub struct LocalKubernetesNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    num_initial_validators: usize,
    num_shards: usize,
    tmp_dir: Arc<TempDir>,
    binaries_dir: Option<String>,
}

#[cfg(any(test, feature = "test"))]
impl LocalKubernetesNetTestingConfig {
    pub fn new(network: Network) -> Self {
        Self { network }
    }
}

#[async_trait]
impl LineraNetConfig for LocalKubernetesNetConfig {
    type Net = LocalKubernetesNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let mut net = LocalKubernetesNet::new(
            self.network,
            self.testing_prng_seed,
            self.num_initial_validators,
            self.num_shards,
            self.binaries_dir,
        )?;
        let client = net.make_client();
        ensure!(
            self.num_initial_validators > 0,
            "There should be at least one initial validator"
        );
        net.run().await.unwrap();
        Ok((net, client))
    }
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl LineraNetConfig for LocalKubernetesNetTestingConfig {
    type Net = LocalKubernetesNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let seed = 37;
        let num_validators = 4;
        let num_shards = 1;
        let mut net =
            LocalKubernetesNet::new(self.network, Some(seed), num_validators, num_shards, None)?;
        let client = net.make_client();
        if num_validators > 0 {
            net.run().await.unwrap();
        }
        Ok((net, client))
    }
}

#[async_trait]
impl LineraNet for LocalKubernetesNet {
    fn ensure_is_running(&mut self) -> Result<()> {
        // neet do implement
        Ok(())
    }

    fn make_client(&mut self) -> ClientWrapper {
        let client = ClientWrapper::new(
            self.tmp_dir.clone(),
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

    async fn terminate(mut self) -> Result<()> {
        // need to implement
        Ok(())
    }
}

impl LocalKubernetesNet {
    fn new(
        network: Network,
        testing_prng_seed: Option<u64>,
        num_initial_validators: usize,
        num_shards: usize,
        binaries_dir: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            network,
            testing_prng_seed,
            next_client_id: 0,
            num_initial_validators,
            num_shards,
            tmp_dir: Arc::new(tempdir()?),
            binaries_dir,
        })
    }

    async fn run(&mut self) -> Result<()> {
        // Creating kind cluster
        KindWrapper::create().await?;

        let binary_parent =
            current_binary_parent().expect("Fetching current binaries path should not fail");
        let binaries_path = if let Some(binaries_dir) = &self.binaries_dir {
            binaries_dir.as_str()
        } else {
            binary_parent
                .to_str()
                .expect("Getting path in &str format should not fail")
        };

        // Build Docker image
        DockerWrapper::build(binaries_path).await?;

        KindWrapper::load_docker_image("linera-test:latest").await?;
        HelmWrapper::install("linera-core").await?;

        Ok(())
    }
}
