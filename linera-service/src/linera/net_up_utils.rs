// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroU16, str::FromStr};

use colored::Colorize as _;
use linera_base::{data_types::Amount, identifiers::ChainId, time::Duration};
use linera_client::storage::{StorageConfig, StorageConfigNamespace};
use linera_execution::ResourceControlPolicy;
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNetConfig, PathProvider, StorageConfigBuilder},
        ClientWrapper, FaucetOption, FaucetService, LineraNet, LineraNetConfig, Network,
        NetworkConfig,
    },
    util::listen_for_shutdown_signals,
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
    linera_service::cli_wrappers::local_kubernetes_net::LocalKubernetesNetConfig,
    std::path::PathBuf,
};

struct StorageConfigProvider {
    /// The StorageConfig and the namespace
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
        }
    }
}

#[expect(clippy::too_many_arguments)]
#[cfg(feature = "kubernetes")]
pub async fn handle_net_up_kubernetes(
    extra_wallets: Option<usize>,
    num_other_initial_chains: u32,
    initial_amount: u128,
    num_initial_validators: usize,
    num_shards: usize,
    testing_prng_seed: Option<u64>,
    binaries: &Option<Option<PathBuf>>,
    no_build: bool,
    docker_image_name: String,
    policy: ResourceControlPolicy,
    with_faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
) -> anyhow::Result<()> {
    if num_initial_validators < 1 {
        panic!("The local test network must have at least one validator.");
    }
    if num_shards < 1 {
        panic!("The local test network must have at least one shard per validator.");
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
        policy,
    };
    let (mut net, client) = config.instantiate().await?;
    let faucet_service = create_wallets_and_faucets(
        extra_wallets,
        &mut net,
        client,
        with_faucet_chain,
        faucet_port,
        faucet_amount,
    )
    .await?;
    wait_for_shutdown(shutdown_notifier, &mut net, faucet_service).await
}

#[expect(clippy::too_many_arguments)]
pub async fn handle_net_up_service(
    extra_wallets: Option<usize>,
    num_other_initial_chains: u32,
    initial_amount: u128,
    num_initial_validators: usize,
    num_shards: usize,
    testing_prng_seed: Option<u64>,
    policy: ResourceControlPolicy,
    path: &Option<String>,
    storage: &Option<String>,
    external_protocol: String,
    with_faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
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
        policy,
        storage_config_builder,
        path_provider,
    };
    let (mut net, client) = config.instantiate().await?;
    let faucet_service = create_wallets_and_faucets(
        extra_wallets,
        &mut net,
        client,
        with_faucet_chain,
        faucet_port,
        faucet_amount,
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

async fn create_wallets_and_faucets(
    extra_wallets: Option<usize>,
    net: &mut impl LineraNet,
    client: ClientWrapper,
    with_faucet_chain: Option<u32>,
    faucet_port: NonZeroU16,
    faucet_amount: Amount,
) -> Result<Option<FaucetService>, anyhow::Error> {
    let default_chain = client
        .default_chain()
        .expect("Initialized clients should always have a default chain");

    // Make time to (hopefully) display the message after the tracing logs.
    linera_base::time::timer::sleep(Duration::from_secs(1)).await;

    // Create the wallet for the initial "root" chains.
    info!("Local test network successfully started.");
    let suffix = if let Some(extra_wallets) = extra_wallets {
        eprintln!(
            "To use the initial wallet and the extra wallets of this test \
        network, you may set the environment variables LINERA_WALLET_$N \
        and LINERA_STORAGE_$N (N = 0..={extra_wallets}) as printed on \
        the standard output, then use the option `--with-wallet $N` (or \
        `-w $N` for short) to select a wallet in the linera tool.\n"
        );
        "_0"
    } else {
        eprintln!(
            "To use the initial wallet of this test network, you may set \
        the environment variables LINERA_WALLET and LINERA_STORAGE as follows.\n"
        );
        ""
    };
    println!(
        "{}",
        format!(
            "export LINERA_WALLET{suffix}=\"{}\"",
            client.wallet_path().display()
        )
        .bold()
    );
    println!(
        "{}",
        format!(
            "export LINERA_STORAGE{suffix}=\"{}\"\n",
            client.storage_path()
        )
        .bold()
    );

    // Create the extra wallets.
    if let Some(extra_wallets) = extra_wallets {
        for wallet in 1..=extra_wallets {
            let extra_wallet = net.make_client().await;
            extra_wallet.wallet_init(&[], FaucetOption::None).await?;
            let unassigned_owner = extra_wallet.keygen().await?;
            let new_chain_msg_id = client
                .open_chain(default_chain, Some(unassigned_owner), Amount::ZERO)
                .await?
                .0;
            extra_wallet
                .assign(unassigned_owner, new_chain_msg_id)
                .await?;
            println!(
                "{}",
                format!(
                    "export LINERA_WALLET_{wallet}=\"{}\"",
                    extra_wallet.wallet_path().display(),
                )
                .bold()
            );
            println!(
                "{}",
                format!(
                    "export LINERA_STORAGE_{wallet}=\"{}\"\n",
                    extra_wallet.storage_path(),
                )
                .bold()
            );
        }
    }

    // Run the faucet,
    let faucet_service = if let Some(faucet_chain) = with_faucet_chain {
        let service = client
            .run_faucet(
                Some(faucet_port.into()),
                ChainId::root(faucet_chain),
                faucet_amount,
            )
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
