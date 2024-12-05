// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use futures::{future, lock::Mutex};
use k8s_openapi::api::core::v1::Pod;
use kube::{api::ListParams, Api, Client};
use linera_base::{
    command::{resolve_binary, CommandExt},
    data_types::Amount,
};
use linera_execution::ResourceControlPolicy;
use tempfile::{tempdir, TempDir};
use tokio::process::Command;
#[cfg(with_testing)]
use {
    crate::cli_wrappers::wallet::FaucetOption, linera_base::command::current_binary_parent,
    tokio::sync::OnceCell,
};

use crate::cli_wrappers::{
    docker::{BuildArg, DockerImage},
    helmfile::HelmFile,
    kind::KindCluster,
    kubectl::KubectlInstance,
    local_net::PathProvider,
    util::get_github_root,
    ClientWrapper, LineraNet, LineraNetConfig, Network, OnClientDrop,
};

#[cfg(with_testing)]
static SHARED_LOCAL_KUBERNETES_TESTING_NET: OnceCell<(
    Arc<Mutex<LocalKubernetesNet>>,
    ClientWrapper,
)> = OnceCell::const_new();

/// The information needed to start a [`LocalKubernetesNet`].
pub struct LocalKubernetesNetConfig {
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub num_other_initial_chains: u32,
    pub initial_amount: Amount,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub binaries: BuildArg,
    pub no_build: bool,
    pub docker_image_name: String,
    pub policy: ResourceControlPolicy,
}

/// A wrapper of [`LocalKubernetesNetConfig`] to create a shared local Kubernetes network
/// or use an existing one.
#[cfg(with_testing)]
pub struct SharedLocalKubernetesNetTestingConfig(LocalKubernetesNetConfig);

/// A set of Linera validators running locally as native processes.
#[derive(Clone)]
pub struct LocalKubernetesNet {
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    tmp_dir: Arc<TempDir>,
    binaries: BuildArg,
    no_build: bool,
    docker_image_name: String,
    kubectl_instance: Arc<Mutex<KubectlInstance>>,
    kind_clusters: Vec<KindCluster>,
    num_initial_validators: usize,
    num_shards: usize,
}

#[cfg(with_testing)]
impl SharedLocalKubernetesNetTestingConfig {
    // The second argument is sometimes used locally to use specific binaries for tests.
    pub fn new(network: Network, mut binaries: BuildArg) -> Self {
        if std::env::var("LINERA_TRY_RELEASE_BINARIES").unwrap_or_default() == "true"
            && matches!(binaries, BuildArg::Build)
        {
            // For cargo test, current binary should be in debug mode
            let current_binary_parent =
                current_binary_parent().expect("Fetching current binaries path should not fail");
            // But binaries for cluster should be release mode
            let binaries_dir = current_binary_parent
                .parent()
                .expect("Getting parent should not fail")
                .join("release");
            if binaries_dir.exists() {
                // If release exists, use those binaries
                binaries = BuildArg::Directory(binaries_dir);
            }
        }
        Self(LocalKubernetesNetConfig {
            network,
            testing_prng_seed: Some(37),
            num_other_initial_chains: 2,
            initial_amount: Amount::from_tokens(2000),
            num_initial_validators: 4,
            num_shards: 4,
            binaries,
            no_build: false,
            docker_image_name: String::from("linera:latest"),
            policy: ResourceControlPolicy::devnet(),
        })
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

        let clusters = future::join_all((0..self.num_initial_validators).map(|_| async {
            KindCluster::create()
                .await
                .expect("Creating kind cluster should not fail")
        }))
        .await;

        let mut net = LocalKubernetesNet::new(
            self.network,
            self.testing_prng_seed,
            self.binaries,
            self.no_build,
            self.docker_image_name,
            KubectlInstance::new(Vec::new()),
            clusters,
            self.num_initial_validators,
            self.num_shards,
        )?;

        let client = net.make_client().await;
        net.generate_initial_validator_config().await.unwrap();
        client
            .create_genesis_config(
                self.num_other_initial_chains,
                self.initial_amount,
                self.policy,
            )
            .await
            .unwrap();
        net.run().await.unwrap();

        Ok((net, client))
    }

    async fn policy(&self) -> ResourceControlPolicy {
        self.policy.clone()
    }
}

#[cfg(with_testing)]
#[async_trait]
impl LineraNetConfig for SharedLocalKubernetesNetTestingConfig {
    type Net = Arc<Mutex<LocalKubernetesNet>>;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let (net, initial_client) = SHARED_LOCAL_KUBERNETES_TESTING_NET
            .get_or_init(|| async {
                let (net, initial_client) = self
                    .0
                    .instantiate()
                    .await
                    .expect("Instantiating LocalKubernetesNetConfig should not fail");
                (Arc::new(Mutex::new(net)), initial_client)
            })
            .await;

        let mut net = net.clone();
        let client = net.make_client().await;
        // The tests assume we've created a genesis config with 2
        // chains with 10 tokens each.
        client.wallet_init(&[], FaucetOption::None).await.unwrap();
        for _ in 0..2 {
            initial_client
                .open_and_assign(&client, Amount::from_tokens(10))
                .await
                .unwrap();
        }

        Ok((net, client))
    }

    async fn policy(&self) -> ResourceControlPolicy {
        self.0.policy().await
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
        let path_provider = PathProvider::TemporaryDirectory {
            tmp_dir: self.tmp_dir.clone(),
        };
        let client = ClientWrapper::new(
            path_provider,
            self.network,
            self.testing_prng_seed,
            self.next_client_id,
            OnClientDrop::LeakChains,
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
    #[allow(clippy::too_many_arguments)]
    fn new(
        network: Network,
        testing_prng_seed: Option<u64>,
        binaries: BuildArg,
        no_build: bool,
        docker_image_name: String,
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
            binaries,
            no_build,
            docker_image_name,
            kubectl_instance: Arc::new(Mutex::new(kubectl_instance)),
            kind_clusters,
            num_initial_validators,
            num_shards,
        })
    }

    async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = resolve_binary(name, env!("CARGO_PKG_NAME")).await?;
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
        fs_err::write(&path, content)?;
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
        let github_root = get_github_root().await?;
        // Build Docker image
        let docker_image_name = if self.no_build {
            self.docker_image_name.clone()
        } else {
            DockerImage::build(&self.docker_image_name, &self.binaries, &github_root).await?;
            self.docker_image_name.clone()
        };

        let base_dir = github_root
            .join("kubernetes")
            .join("linera-validator")
            .join("working");
        fs_err::copy(
            self.tmp_dir.path().join("genesis.json"),
            base_dir.join("genesis.json"),
        )?;

        let kubectl_instance_clone = self.kubectl_instance.clone();
        let tmp_dir_path_clone = self.tmp_dir.path().to_path_buf();
        let num_shards = self.num_shards;

        let mut validators_initialization_futures = Vec::new();
        for (i, kind_cluster) in self.kind_clusters.iter().cloned().enumerate() {
            let base_dir = base_dir.clone();
            let github_root = github_root.clone();

            let kubectl_instance = kubectl_instance_clone.clone();
            let tmp_dir_path = tmp_dir_path_clone.clone();

            let docker_image_name = docker_image_name.clone();
            let future = async move {
                let cluster_id = kind_cluster.id();
                kind_cluster.load_docker_image(&docker_image_name).await?;

                let server_config_filename = format!("server_{}.json", i);
                fs_err::copy(
                    tmp_dir_path.join(&server_config_filename),
                    base_dir.join(&server_config_filename),
                )?;

                HelmFile::sync(i, &github_root, num_shards, cluster_id).await?;

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

        future::join_all(validators_initialization_futures)
            .await
            .into_iter()
            .collect()
    }
}
