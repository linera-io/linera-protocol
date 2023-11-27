// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{
    docker::DockerImage, helm::HelmRelease, kind::KindCluster, kubectl::KubectlInstance,
    util::get_github_root,
};
use crate::{
    cli_wrappers::{ClientWrapper, LineraNet, LineraNetConfig, Network},
    util::{self, current_binary_parent, CommandExt},
};
use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use futures::{
    future::{self, join_all},
    FutureExt,
};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams},
    Client,
};
use std::{fs, path::PathBuf, sync::Arc};
use tempfile::{tempdir, TempDir};
use tokio::process::Command;

/// The information needed to start a [`LocalKubernetesNet`].
pub struct LocalKubernetesNetConfig {
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub binaries_dir: Option<PathBuf>,
}

/// A set of Linera validators running locally as native processes.
pub struct LocalKubernetesNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    tmp_dir: Arc<TempDir>,
    binaries_dir: Option<PathBuf>,
    kubectl_instance: KubectlInstance,
    kind_clusters: Vec<KindCluster>,
    num_initial_validators: usize,
    num_shards: usize,
}

#[async_trait]
impl LineraNetConfig for LocalKubernetesNetConfig {
    type Net = LocalKubernetesNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let mut net = LocalKubernetesNet::new(
            self.network,
            self.testing_prng_seed,
            self.binaries_dir,
            KubectlInstance::new(Vec::new()),
            future::ready(
                (0..self.num_initial_validators)
                    .map(|_| async { KindCluster::create().await })
                    .collect::<Vec<_>>(),
            )
            .then(join_all)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?,
            self.num_initial_validators,
            self.num_shards,
        )?;
        let client = net.make_client();
        ensure!(
            self.num_initial_validators > 0,
            "There should be at least one initial validator"
        );
        net.generate_initial_validator_config().await.unwrap();
        client.create_genesis_config().await.unwrap();
        net.run().await.unwrap();
        Ok((net, client))
    }
}

impl Drop for LocalKubernetesNet {
    fn drop(&mut self) {
        // Block the current runtime to cleanup
        let handle = tokio::runtime::Handle::current();
        let _guard = handle.enter();
        futures::executor::block_on(async move {
            self.terminate().await.unwrap();
        });
    }
}

#[async_trait]
impl LineraNet for LocalKubernetesNet {
    async fn ensure_is_running(&mut self) -> Result<()> {
        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::namespaced(client, "default");

        let list_params = ListParams::default().labels("app=proxy");
        for pod in pods.list(&list_params).await? {
            if let Some(status) = pod.status {
                if let Some(phase) = status.phase {
                    if phase != "Running" {
                        bail!(
                            "Validator {} is not Running",
                            pod.metadata
                                .name
                                .expect("Fetching pod name should not fail")
                        );
                    }
                }
            }
        }

        let list_params = ListParams::default().labels("app=shards");
        for pod in pods.list(&list_params).await? {
            if let Some(status) = pod.status {
                if let Some(phase) = status.phase {
                    if phase != "Running" {
                        bail!(
                            "Shard {} is not Running",
                            pod.metadata
                                .name
                                .expect("Fetching pod name should not fail")
                        );
                    }
                }
            }
        }

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

    async fn terminate(&mut self) -> Result<()> {
        let mut errors = Vec::new();

        for port_forward_child in &mut self.kubectl_instance.port_forward_children {
            if let Err(e) = port_forward_child.kill().await {
                errors.push(e.into());
            }
        }

        for kind_cluster in &mut self.kind_clusters {
            if let Err(e) = kind_cluster.delete().await {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            let err_str = if errors.len() > 1 {
                "Multiple errors"
            } else {
                "One error"
            };

            Err(errors
                .into_iter()
                .fold(anyhow!("{err_str} occurred"), |acc, e| acc.context(e)))
        }
    }
}

impl LocalKubernetesNet {
    fn new(
        network: Network,
        testing_prng_seed: Option<u64>,
        binaries_dir: Option<PathBuf>,
        kubectl_instance: KubectlInstance,
        kind_clusters: Vec<KindCluster>,
        num_initial_validators: usize,
        num_shards: usize,
    ) -> Result<Self> {
        Ok(Self {
            network,
            testing_prng_seed,
            next_client_id: 0,
            tmp_dir: Arc::new(tempdir()?),
            binaries_dir,
            kubectl_instance,
            kind_clusters,
            num_initial_validators,
            num_shards,
        })
    }

    async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = util::resolve_binary(name, env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command.current_dir(self.tmp_dir.path());
        Ok(command)
    }

    fn configuration_string(&self, server_number: usize) -> Result<String> {
        let n = server_number;
        let path = self.tmp_dir.path().join(format!("validator_{n}.toml"));
        let port = 19100 + server_number;
        let internal_port = 20100 + server_number;
        let metrics_port = 21100 + server_number;
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "127.0.0.1"
                port = {port}
                internal_host = "127.0.0.1"
                internal_port = {internal_port}
                metrics_host = "127.0.0.1"
                metrics_port = {metrics_port}
                [external_protocol]
                Grpc = "ClearText"
                [internal_protocol]
                Grpc = "ClearText"
            "#
        );
        for k in 0..self.num_shards {
            let shard_port = 19100 + server_number;
            let shard_metrics_port = 21100 + server_number;
            content.push_str(&format!(
                r#"

                [[shards]]
                host = "shards-{k}.shards.default.svc.cluster.local"
                port = {shard_port}
                metrics_host = "shards-{k}.shards.default.svc.cluster.local"
                metrics_port = {shard_metrics_port}
                "#
            ));
        }
        fs::write(&path, content)?;
        path.into_os_string().into_string().map_err(|error| {
            anyhow!(
                "could not parse OS string into string: {}",
                error.to_string_lossy()
            )
        })
    }

    async fn generate_initial_validator_config(&mut self) -> Result<()> {
        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("generate");
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
            self.testing_prng_seed = Some(seed + 1);
        }
        command.arg("--validators");
        for i in 0..self.num_initial_validators {
            command.arg(&self.configuration_string(i)?);
        }
        command
            .args(["--committee", "committee.json"])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let binary_parent =
            current_binary_parent().expect("Fetching current binaries path should not fail");
        let binaries_path = if let Some(binaries_dir) = &self.binaries_dir {
            binaries_dir
        } else {
            &binary_parent
        };

        let github_root = get_github_root().await?;
        // Build Docker image
        let docker_image = DockerImage::new(
            String::from("linera-test:latest"),
            binaries_path,
            &github_root,
        )
        .await?;

        let base_dir = github_root
            .join("kubernetes")
            .join("linera-validator")
            .join("working");
        fs::copy(
            self.tmp_dir.path().join("genesis.json"),
            base_dir.join("genesis.json"),
        )?;

        for i in 0..self.num_initial_validators {
            let cluster_id = self.kind_clusters[i].id();
            self.kind_clusters[i]
                .load_docker_image(docker_image.get_name())
                .await?;

            let server_config_filename = format!("server_{}.json", i);
            fs::copy(
                self.tmp_dir.path().join(&server_config_filename),
                base_dir.join(&server_config_filename),
            )?;

            HelmRelease::install(
                String::from("linera-core"),
                &base_dir,
                i,
                &github_root,
                self.num_shards,
                cluster_id,
            )
            .await?;

            // Query the cluster
            let output = self.kubectl_instance.get_pods(cluster_id).await?;
            let validator_pod_name = output
                .split_whitespace()
                .find(|&t| t.contains("proxy"))
                .expect("Getting validator pod name should not fail");

            let local_port = 19100 + i;
            self.kubectl_instance
                .port_forward(
                    validator_pod_name,
                    &format!("{local_port}:{local_port}"),
                    cluster_id,
                )
                .await?;
        }

        Ok(())
    }
}
