// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use anyhow::anyhow;
use linera_base::{
    crypto::{AccountSecretKey, Ed25519SecretKey},
    data_types::{Amount, Blob, BlockHeight, Epoch},
    identifiers::{ChainDescription, ChainId},
};
use linera_chain::data_types::ProposedBlock;
use linera_core::{
    client::PendingProposal,
    test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
};
use linera_rpc::{NodeOptions, NodeProvider};
use rand::{rngs::StdRng, SeedableRng as _};

use super::util::make_genesis_config;
use crate::{
    client_context::ClientContext,
    config::WalletState,
    wallet::{UserChain, Wallet},
};

/// Tests whether we can correctly save a wallet that contains pending blobs.
#[test_log::test(tokio::test)]
async fn test_save_wallet_with_pending_blobs() -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(42);
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let chain_id = ChainId::root(0);
    builder.add_root_chain(0, Amount::ONE).await?;
    let storage = builder.make_storage().await?;

    let genesis_config = make_genesis_config(&builder);

    let tmp_dir = tempfile::tempdir()?;
    let mut config_dir = tmp_dir.into_path();
    config_dir.push("linera");
    if !config_dir.exists() {
        tracing::debug!("{} does not exist, creating", config_dir.display());
        fs_err::create_dir(&config_dir)?;
        tracing::debug!("{} created.", config_dir.display());
    }
    let wallet_path = config_dir.join("wallet.json");
    if wallet_path.exists() {
        return Err(anyhow!("Wallet already exists!"));
    }
    let mut wallet =
        WalletState::create_from_file(&wallet_path, Wallet::new(genesis_config, Some(37)))?;
    let key_pair = AccountSecretKey::Ed25519(Ed25519SecretKey::generate_from(&mut rng));
    wallet
        .add_chains(Some(UserChain::make_initial(
            key_pair,
            ChainDescription::Root(0),
            clock.current_time(),
        )))
        .await?;
    wallet.chains_mut().next().unwrap().pending_proposal = Some(PendingProposal {
        block: ProposedBlock {
            chain_id,
            epoch: Epoch::ZERO,
            incoming_bundles: vec![],
            operations: vec![],
            height: BlockHeight::ZERO,
            timestamp: clock.current_time(),
            authenticated_signer: None,
            previous_block_hash: None,
        },
        blobs: vec![Blob::new_data(b"blob".to_vec())],
    });

    let send_recv_timeout = Duration::from_millis(4000);
    let retry_delay = Duration::from_millis(1000);
    let max_retries = 10;

    let node_options = NodeOptions {
        send_timeout: send_recv_timeout,
        recv_timeout: send_recv_timeout,
        retry_delay,
        max_retries,
    };
    let node_provider = NodeProvider::new(node_options);

    let mut context = ClientContext::new_test_client_context(node_provider, storage, wallet);
    context.save_wallet().await?;
    Ok(())
}
