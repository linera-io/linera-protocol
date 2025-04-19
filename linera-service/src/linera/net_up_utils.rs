// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, str::FromStr};

use colored::Colorize as _;
use linera_base::{data_types::Amount, listen_for_shutdown_signals, time::Duration};
use linera_client::client_options::ResourceControlPolicyConfig;
use linera_rpc::config::CrossChainConfig;
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNetConfig, PathProvider, StorageConfigBuilder},
        ClientWrapper, FaucetService, LineraNet, LineraNetConfig, Network, NetworkConfig,
    },
    storage::{StorageConfig, StorageConfigNamespace},
};
#[cfg(feature = "storage-service")]
use linera_storage_service::{
    child::{StorageService, StorageServiceGuard},
    common::get_service_storage_binary,
};
use tokio_util::sync::CancellationToken;
use tracing::info;
#[cfg(feature = "kubernetes")]
use {
    linera_service::cli_wrappers::local_kubernetes_net::{BuildMode, LocalKubernetesNetConfig},
    std::path::PathBuf,
};

struct StorageConfigProvider {
    /// The `StorageConfig` and the namespace
    pub storage: StorageConfigNamespace,
    #[cfg(feature = "storage-service")]
    _service_guard: Option<StorageServiceGuard>,
}

impl StorageConfigProvider {
    pub async fn new(storage: &Option<String>) -> anyhow::Result<StorageConfigProvider> {
        match storage {
            #[cfg(feature = "storage-service")]
            None => {
                let service_endpoint = linera_base::port::get_free_endpoint().await?;
                let binary = get_service_storage_binary().await?.display().to_string();
                let service = StorageService::new(&service_endpoint, binary);
                let _service_guard = service.run().await?;
                let _service_guard = Some(_service_guard);
                let storage_config = StorageConfig::Service {
                    endpoint: service_endpoint,
                };
                let namespace = "table_default".to_string();
                let storage = StorageConfigNamespace {
                    storage_config,
                    namespace,
                };
                Ok(StorageConfigProvider {
                    storage,
                    _service_guard,
                })
            }
            #[cfg(not(feature = "storage-service"))]
            None => {
                panic!("When storage is not selected, the storage-service needs to be enabled");
            }
            #[cfg(feature = "storage-service")]
            Some(storage) => {
                let storage = StorageConfigNamespace::from_str(storage)?;
                Ok(StorageConfigProvider {
                    storage,
                    _service_guard: None,
                })
            }
            #[cfg(not(feature = "storage-service"))]
            Some(storage) => {
                let storage = StorageConfigNamespace::from_str(storage)?;
                Ok(StorageConfigProvider { storage })
            }
        }
    }

    pub fn storage_config(&self) -> StorageConfig {
        self.storage.storage_config.clone()
    }

    pub fn namespace(&self) -> String {
        self.storage.namespace.clone()
    }

    pub fn database(&self) -> anyhow::Result<Database> {
        match self.storage.storage_config {
            StorageConfig::Memory => anyhow::bail!("Not possible to work with memory"),
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { .. } => anyhow::bail!("Not possible to work with RocksDB"),
            #[cfg(feature = "storage-service")]
            StorageConfig::Service { .. } => Ok(Database::Service),
            #[cfg(feature = "dynamodb")]
            StorageConfig::DynamoDb { .. } => Ok(Database::DynamoDb),
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { .. } => Ok(Database::ScyllaDb),
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb { .. } => Ok(Database::DualRocksDbScyllaDb),
        }
    }
}

#[expect(clippy::too_many_arguments)]
#[cfg(feature = "kubernetes")]
pub async fn handle_net_up_kubernetes(
    num_other_initial_chains: u32,
    initial_amount: u128,
    num_initial_validators: usize,
    num_shards: usize,
    testing_prng_seed: Option<u64>,
    binaries: &Option<Option<PathBuf>>,
    no_build: bool,
    docker_image_name: String,
    build_mode: BuildMode,
    policy_config: ResourceControlPolicyConfig,
    with_faucet: bool,
    faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
) -> anyhow::Result<()> {
    if num_initial_validators < 1 {
        panic!("The local test network must have at least one validator.");
    }
    if num_shards < 1 {
        panic!("The local test network must have at least one shard per validator.");
    }
    if faucet_chain.is_some() {
        assert!(
            with_faucet,
            "--faucet-chain must be provided only with --with-faucet"
        );
    }

    let shutdown_notifier = CancellationToken::new();
    tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

    let config = LocalKubernetesNetConfig {
        network: Network::Grpc,
        testing_prng_seed,
        num_other_initial_chains,
        initial_amount: Amount::from_tokens(initial_amount),
        num_initial_validators,
        num_shards,
        binaries: binaries.clone().into(),
        no_build,
        docker_image_name,
        build_mode,
        policy_config,
    };
    let (mut net, client) = config.instantiate().await?;
    let faucet_service = print_messages_and_create_faucet(
        client,
        with_faucet,
        faucet_chain,
        faucet_port,
        faucet_amount,
        num_other_initial_chains,
    )
    .await?;
    wait_for_shutdown(shutdown_notifier, &mut net, faucet_service).await
}

#[expect(clippy::too_many_arguments)]
pub async fn handle_net_up_service(
    num_other_initial_chains: u32,
    initial_amount: u128,
    num_initial_validators: usize,
    num_shards: usize,
    testing_prng_seed: Option<u64>,
    policy_config: ResourceControlPolicyConfig,
    cross_chain_config: CrossChainConfig,
    path: &Option<String>,
    storage: &Option<String>,
    external_protocol: String,
    with_faucet: bool,
    faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
    num_block_exporters: u32,
) -> anyhow::Result<()> {
    if num_initial_validators < 1 {
        panic!("The local test network must have at least one validator.");
    }
    if num_shards < 1 {
        panic!("The local test network must have at least one shard per validator.");
    }

    let shutdown_notifier = CancellationToken::new();
    tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

    let storage = StorageConfigProvider::new(storage).await?;
    let storage_config = storage.storage_config();
    let namespace = storage.namespace();
    let database = storage.database()?;
    let storage_config_builder = StorageConfigBuilder::ExistingConfig { storage_config };
    let external = match external_protocol.as_str() {
        "grpc" => Network::Grpc,
        "grpcs" => Network::Grpcs,
        _ => panic!("Only allowed options are grpc and grpcs"),
    };
    let internal = Network::Grpc;
    let network = NetworkConfig { external, internal };
    let path_provider = PathProvider::new(path)?;
    let config = LocalNetConfig {
        network,
        database,
        testing_prng_seed,
        namespace,
        num_other_initial_chains,
        initial_amount: Amount::from_tokens(initial_amount),
        num_initial_validators,
        num_shards,
        policy_config,
        cross_chain_config,
        storage_config_builder,
        path_provider,
        num_block_exporters,
    };
    let (mut net, client) = config.instantiate().await?;
    let faucet_service = print_messages_and_create_faucet(
        client,
        with_faucet,
        faucet_chain,
        faucet_port,
        faucet_amount,
        num_other_initial_chains,
    )
    .await?;
    wait_for_shutdown(shutdown_notifier, &mut net, faucet_service).await
}

async fn wait_for_shutdown(
    shutdown_notifier: CancellationToken,
    net: &mut impl LineraNet,
    faucet_service: Option<FaucetService>,
) -> anyhow::Result<()> {
    shutdown_notifier.cancelled().await;
    eprintln!();
    if let Some(service) = faucet_service {
        eprintln!("Terminating the faucet service");
        service.terminate().await?;
    }
    eprintln!("Terminating the local test network");
    net.terminate().await?;
    eprintln!("Done.");

    Ok(())
}

async fn print_messages_and_create_faucet(
    client: ClientWrapper,
    with_faucet: bool,
    faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
    num_other_initial_chains: u32,
) -> Result<Option<FaucetService>, anyhow::Error> {
    // Make time to (hopefully) display the message after the tracing logs.
    linera_base::time::timer::sleep(Duration::from_secs(1)).await;

    // Create the wallet for the initial "root" chains.
    info!("Local test network successfully started.");

    eprintln!(
        "To use the initial wallet of this test network, you may set \
         the environment variables LINERA_WALLET and LINERA_STORAGE as follows.\n"
    );
    println!(
        "{}",
        format!(
            "export LINERA_WALLET=\"{}\"",
            client.wallet_path().display()
        )
        .bold()
    );
    println!(
        "{}",
        format!("export LINERA_STORAGE=\"{}\"\n", client.storage_path()).bold()
    );

    let chains = client.load_wallet()?.chain_ids();

    // Run the faucet,
    let faucet_service = if with_faucet {
        let faucet_chain_idx = faucet_chain.unwrap_or(1);
        assert!(
            num_other_initial_chains >= faucet_chain_idx,
            "num_other_initial_chains must be greater than the faucet chain index if with_faucet \
            is true"
        );
        let faucet_chain = chains[faucet_chain_idx as usize];
        let service = client
            .run_faucet(Some(faucet_port.into()), faucet_chain, faucet_amount)
            .await?;
        Some(service)
    } else {
        None
    };

    eprintln!(
        "\nREADY!\nPress ^C to terminate the local test network and clean the temporary directory."
    );

    Ok(faucet_service)
}
