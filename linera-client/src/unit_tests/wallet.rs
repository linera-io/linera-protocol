// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use anyhow::anyhow;
use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, Blob},
    identifiers::ChainId,
};
use linera_core::test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder};
use rand::SeedableRng as _;

use super::util::make_genesis_config;
use crate::{client_context::ClientContext, config::WalletState, wallet::Wallet};

/// Tests whether we can correctly save a wallet that contains pending blobs.
#[test_log::test(tokio::test)]
async fn test_save_wallet_with_pending_blobs() -> anyhow::Result<()> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let chain_id = ChainId::root(0);
    builder.add_root_chain(0, Amount::ONE).await?;
    let storage = builder.make_storage().await?;
    let key_pair = KeyPair::generate_from(&mut rng);

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
    let wallet =
        WalletState::create_from_file(&wallet_path, Wallet::new(genesis_config, Some(37)))?;
    let mut context = ClientContext::new_test_client_context(storage, wallet);
    let blob = Blob::new_data(b"blob".to_vec());
    let mut pending_blobs = BTreeMap::new();
    pending_blobs.insert(blob.id(), blob);
    context
        .update_wallet_for_new_chain_with_pending_blobs(
            chain_id,
            Some(key_pair),
            clock.current_time(),
            pending_blobs,
        )
        .await?;
    context.save_wallet().await?;
    Ok(())
}
