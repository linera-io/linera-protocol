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
    lock::Mutex,
    FutureExt,
};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, ListParams},
    Client,
};
use linera_base::data_types::Amount;
use std::{fs, path::PathBuf, sync::Arc};
use tempfile::{tempdir, TempDir};
use tokio::process::Command;
#[cfg(any(test, feature = "test"))]
use tokio::sync::OnceCell;

#[cfg(any(test, feature = "test"))]
static SHARED_LOCAL_KUBERNETES_TESTING_NET: OnceCell<(
    Arc<Mutex<LocalKubernetesNet>>,
    ClientWrapper,
)> = OnceCell::const_new();

/// The information needed to start a [`LocalKubernetesNet`].
pub struct LocalKubernetesNetConfig {
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub binaries_dir: Option<PathBuf>,
}

/// A simplified version of [`LocalKubernetesNetConfig`]
#[cfg(any(test, feature = "test"))]
pub struct SharedLocalKubernetesNetTestingConfig {
    pub network: Network,
    pub binaries_dir: Option<PathBuf>,
}

/// A set of Linera validators running locally as native processes.
#[derive(Clone)]
pub struct LocalKubernetesNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    tmp_dir: Arc<TempDir>,
    binaries_dir: Option<PathBuf>,
    kubectl_instance: Arc<Mutex<KubectlInstance>>,
    kind_clusters: Vec<KindCluster>,
    num_initial_validators: usize,
    num_shards: usize,
}

#[cfg(any(test, feature = "test"))]
impl SharedLocalKubernetesNetTestingConfig {
    pub fn new(network: Network, binaries_dir: Option<PathBuf>) -> Self {
        Self {
            network,
            binaries_dir,
        }
    }
}

#[async_trait]
impl LineraNetConfig for LocalKubernetesNetConfig {
    type Net = LocalKubernetesNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        ensure!(
            self.num_initial_validators > 0,
            "There should be at least one initial validator"
        );

        let mut net = LocalKubernetesNet::new(
            self.network,
            self.testing_prng_seed,
            self.binaries_dir,
            KubectlInstance::new(Vec::new()),
            future::ready(
                (0..self.num_initial_validators)
                    .map(|_| async {
                        KindCluster::create()
                            .await
                            .expect("Creating kind cluster should not fail")
                    })
                    .collect::<Vec<_>>(),
            )
            .then(join_all)
            .await
            .into_iter()
            .collect::<Vec<_>>(),
            self.num_initial_validators,
            self.num_shards,
        )?;

        let client = net.make_client().await;
        net.generate_initial_validator_config().await.unwrap();
        client
            .create_genesis_config(Amount::from_tokens(10))
            .await
            .unwrap();
        net.run().await.unwrap();

        Ok((net, client))
    }
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl LineraNetConfig for SharedLocalKubernetesNetTestingConfig {
    type Net = Arc<Mutex<LocalKubernetesNet>>;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let seed = 37;
        let (ref net_arc, initial_client) = SHARED_LOCAL_KUBERNETES_TESTING_NET
            .get_or_init(|| async {
                let num_validators = 4;
                let num_shards = 4;

                let mut net = LocalKubernetesNet::new(
                    self.network,
                    Some(seed),
                    self.binaries_dir,
                    KubectlInstance::new(Vec::new()),
                    future::ready(
                        (0..num_validators)
                            .map(|_| async {
                                KindCluster::create()
                                    .await
                                    .expect("Creating kind cluster should not fail")
                            })
                            .collect::<Vec<_>>(),
                    )
                    .then(join_all)
                    .await
                    .into_iter()
                    .collect::<Vec<_>>(),
                    num_validators,
                    num_shards,
                )
                .expect("Creating LocalKubernetesNet should not fail");

                let initial_client = net.make_client().await;
                if num_validators > 0 {
                    net.generate_initial_validator_config().await.unwrap();
                    initial_client
                        .create_genesis_config(Amount::from_tokens(1000))
                        .await
                        .unwrap();
                    net.run().await.unwrap();
                }

                (Arc::new(Mutex::new(net)), initial_client)
            })
            .await;

        let mut net_arc_clone = net_arc.clone();
        let client = net_arc_clone.make_client().await;
        client.wallet_init(&[], None).await.unwrap();

        for _ in 0..2 {
            initial_client
                .open_and_assign(&client, Amount::from_tokens(10))
                .await
                .unwrap();
        }

        Ok((net_arc_clone, client))
    }
}

#[async_trait]
impl LineraNet for Arc<Mutex<LocalKubernetesNet>> {
    async fn ensure_is_running(&mut self) -> Result<()> {
        let self_clone = self.clone();
        let mut self_lock = self_clone.lock().await;

        self_lock.ensure_is_running().await
    }

    async fn make_client(&mut self) -> ClientWrapper {
        let self_clone = self.clone();
        let mut self_lock = self_clone.lock().await;

        self_lock.make_client().await
    }

    async fn terminate(&mut self) -> Result<()> {
        // Users are responsible for killing the clusters if they want to
        Ok(())
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

    async fn make_client(&mut self) -> ClientWrapper {
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
        let mut kubectl_instance = self.kubectl_instance.lock().await;
        let mut errors = Vec::new();
        for port_forward_child in &mut kubectl_instance.port_forward_children {
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
                .fold(anyhow!("{err_str} occurred"), |acc, e: anyhow::Error| {
                    acc.context(e)
                }))
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
            kubectl_instance: Arc::new(Mutex::new(kubectl_instance)),
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
        let internal_port = 20100;
        let metrics_port = 21100;
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "127.0.0.1"
                port = {port}
                internal_host = "proxy-internal.default.svc.cluster.local"
                internal_port = {internal_port}
                metrics_host = "proxy-internal.default.svc.cluster.local"
                metrics_port = {metrics_port}
                [external_protocol]
                Grpc = "ClearText"
                [internal_protocol]
                Grpc = "ClearText"
            "#
        );
        for k in 0..self.num_shards {
            let shard_port = 19100;
            let shard_metrics_port = 21100;
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
        let docker_image = DockerImage::build(
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

        let kubectl_instance_clone = self.kubectl_instance.clone();
        let tmp_dir_path_clone = self.tmp_dir.path().to_path_buf();
        let num_shards = self.num_shards;

        let mut validators_initialization_futures = Vec::new();
        for (i, kind_cluster) in self.kind_clusters.iter().cloned().enumerate() {
            let docker_image_name = docker_image.name().to_string();
            let base_dir = base_dir.clone();
            let github_root = github_root.clone();

            let kubectl_instance = kubectl_instance_clone.clone();
            let tmp_dir_path = tmp_dir_path_clone.clone();

            let future = async move {
                let cluster_id = kind_cluster.id();
                kind_cluster.load_docker_image(&docker_image_name).await?;

                let server_config_filename = format!("server_{}.json", i);
                fs::copy(
                    tmp_dir_path.join(&server_config_filename),
                    base_dir.join(&server_config_filename),
                )?;

                HelmRelease::install(
                    String::from("linera-core"),
                    &base_dir,
                    i,
                    &github_root,
                    num_shards,
                    cluster_id,
                )
                .await?;

                let mut kubectl_instance = kubectl_instance.lock().await;
                let output = kubectl_instance.get_pods(cluster_id).await?;
                let validator_pod_name = output
                    .split_whitespace()
                    .find(|&t| t.contains("proxy"))
                    .expect("Getting validator pod name should not fail");

                let local_port = 19100 + i;
                kubectl_instance.port_forward(
                    validator_pod_name,
                    &format!("{local_port}:{local_port}"),
                    cluster_id,
                )?;

                Result::<(), anyhow::Error>::Ok(())
            };

            validators_initialization_futures.push(future);
        }

        join_all(validators_initialization_futures).await;
        Ok(())
    }
}
