// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Blob, BlockHeight, Epoch},
};
use linera_chain::data_types::ProposedBlock;
use linera_core::{
    client::PendingProposal,
    test_utils::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
};
use linera_persistent as persistent;

use super::util::make_genesis_config;
use crate::{
    client_context::ClientContext,
    wallet::{UserChain, Wallet},
};

/// Tests whether we can correctly save a wallet that contains pending blobs.
#[test_log::test(tokio::test)]
async fn test_save_wallet_with_pending_blobs() -> anyhow::Result<()> {
    let storage_builder = MemoryStorageBuilder::default();
    let mut signer = InMemorySigner::new(Some(42));
    let new_pubkey = signer.generate_new();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;
    builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id = builder.admin_id();
    let storage = builder.make_storage().await?;

    let genesis_config = make_genesis_config(&builder);

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
        return Err(anyhow!("Wallet already exists!"));
    }
    let mut wallet = persistent::File::<Wallet>::read_or_create(&wallet_path, || {
        Ok(Wallet::new(genesis_config))
    })?;
    wallet.insert(UserChain::make_initial(
        new_pubkey.into(),
        builder.admin_description().unwrap().clone(),
        clock.current_time(),
    ));
    wallet.chains_mut().next().unwrap().pending_proposal = Some(PendingProposal {
        block: ProposedBlock {
            chain_id,
            epoch: Epoch::ZERO,
            transactions: vec![],
            height: BlockHeight::ZERO,
            timestamp: clock.current_time(),
            authenticated_signer: None,
            previous_block_hash: None,
        },
        blobs: vec![Blob::new_data(b"blob".to_vec())],
    });
    let mut context = ClientContext::new_test_client_context(storage, wallet, signer);
    context.save_wallet().await?;
    Ok(())
}
