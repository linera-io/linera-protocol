// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Blob, BlockHeight, Epoch},
};
use linera_chain::data_types::ProposedBlock;
use linera_client::{client_context::ClientContext, config::GenesisConfig};
use linera_core::{
    client::{Client, ListeningMode, PendingProposal},
    join_set_ext::JoinSet,
    test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
    wallet,
};
use linera_rpc::{NodeOptions, NodeProvider};
use linera_service::Wallet;

pub async fn new_test_client_context(
    storage: impl linera_core::environment::Storage,
    wallet: Wallet,
    signer: impl linera_core::environment::Signer,
    block_cache_size: usize,
    execution_state_cache_size: usize,
) -> anyhow::Result<ClientContext<impl linera_core::Environment>> {
    use linera_core::{client::chain_client, node::CrossChainMessageDelivery};

    let send_recv_timeout = Duration::from_millis(4000);
    let retry_delay = Duration::from_millis(1000);
    let max_retries = 10;
    let chain_worker_ttl = Duration::from_secs(30);
    let sender_chain_worker_ttl = Duration::from_secs(1);

    let node_options = NodeOptions {
        send_timeout: send_recv_timeout,
        recv_timeout: send_recv_timeout,
        retry_delay,
        max_retries,
    };
    let chain_ids: Vec<_> = wallet.chain_ids();
    let chain_modes = chain_ids.iter().map(|id| (*id, ListeningMode::FullChain));
    let name = match chain_ids.len() {
        0 => "Client node".to_string(),
        1 => format!("Client node for {:.8}", chain_ids[0]),
        n => format!("Client node for {:.8} and {} others", chain_ids[0], n - 1),
    };

    let genesis_config = wallet.genesis_config().clone();
    Ok(ClientContext {
        default_chain: wallet.default_chain(),
        client: Client::new(
            linera_core::environment::Impl {
                storage,
                network: NodeProvider::new(node_options),
                signer,
                wallet,
            },
            genesis_config.admin_chain_id(),
            false,
            chain_modes,
            name,
            chain_worker_ttl,
            sender_chain_worker_ttl,
            chain_client::Options {
                cross_chain_message_delivery: CrossChainMessageDelivery::Blocking,
                ..chain_client::Options::test_default()
            },
            block_cache_size,
            execution_state_cache_size,
            linera_core::client::RequestsSchedulerConfig::default(),
        )
        .into(),
        genesis_config,
        send_timeout: send_recv_timeout,
        recv_timeout: send_recv_timeout,
        retry_delay,
        max_retries,
        chain_listeners: JoinSet::default(),
        client_metrics: None,
    })
}

/// Tests whether we can correctly save a wallet that contains pending blobs.
#[test_log::test(tokio::test)]
async fn test_save_wallet_with_pending_blobs() -> anyhow::Result<()> {
    let storage_builder = MemoryStorageBuilder::default();
    let mut signer = InMemorySigner::new(Some(42));
    let new_pubkey = signer.generate_new();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;
    builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id = builder.admin_chain_id();

    let genesis_config = GenesisConfig::new_testing(&builder);

    let tmp_dir = tempfile::tempdir()?;
    let mut config_dir = tmp_dir.keep();
    config_dir.push("linera");
    if !config_dir.exists() {
        tracing::debug!("{} does not exist, creating", config_dir.display());
        fs_err::create_dir(&config_dir)?;
        tracing::debug!("{} created.", config_dir.display());
    }
    let wallet_path = config_dir.join("wallet.json");
    if wallet_path.exists() {
        return Err(anyhow::anyhow!("Wallet already exists!"));
    }
    let wallet = Wallet::create(&wallet_path, genesis_config)?;
    let admin_description = builder.admin_description().unwrap().clone();
    wallet
        .insert(
            admin_description.id(),
            wallet::Chain {
                owner: Some(new_pubkey.into()),
                timestamp: clock.current_time(),
                pending_proposal: Some(PendingProposal {
                    block: ProposedBlock {
                        chain_id,
                        epoch: Epoch::ZERO,
                        transactions: vec![],
                        height: BlockHeight::ZERO,
                        timestamp: clock.current_time(),
                        authenticated_owner: None,
                        previous_block_hash: None,
                    },
                    blobs: vec![Blob::new_data(b"blob".to_vec())],
                }),
                ..admin_description.into()
            },
        )
        .expect("wallet should be empty");
    wallet.save()?;
    Ok(())
}
