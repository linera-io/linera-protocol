// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detects committee rotation operations on the admin chain and relays
//! them to the EVM LightClient contract.

use alloy::providers::Provider;
use anyhow::{Context as _, Result};
use linera_base::{
    data_types::{BlockHeight, Epoch},
    identifiers::{BlobId, BlobType, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::{system::AdminOperation, Operation, SystemOperation};
use linera_storage::Storage;

use super::evm::EvmClient;
use crate::{evm::client::extract_validator_keys, monitor::db::BridgeDb};

/// Scans a certificate for a `CreateCommittee` operation.
/// Returns the epoch and blob hash if found.
pub fn find_create_committee(
    cert: &ConfirmedBlockCertificate,
) -> Option<(Epoch, linera_base::crypto::CryptoHash)> {
    cert.inner().block().body.operations().find_map(|op| {
        if let Operation::System(boxed) = op {
            if let SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch,
                blob_hash,
                ..
            }) = boxed.as_ref()
            {
                return Some((*epoch, *blob_hash));
            }
        }
        None
    })
}

/// Relays a single committee update to the LightClient contract.
pub async fn relay_committee<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    committee_blob_bytes: &[u8],
) -> Result<()> {
    let validator_keys = extract_validator_keys(committee_blob_bytes)?;
    let cert_bytes = bcs::to_bytes(cert).context("failed to BCS-serialize certificate")?;

    let tx_hash = evm_client
        .add_committee(&cert_bytes, committee_blob_bytes, validator_keys)
        .await?;

    tracing::info!(%tx_hash, "Relayed committee to LightClient");
    Ok(())
}

/// Catches up the LightClient with any committee updates missed while offline.
/// Scans admin chain blocks and relays committees newer than the LightClient's
/// current epoch. Must succeed before the relay enters the main serve loop.
///
/// Resumes from the height persisted in `db` on previous runs instead of
/// rescanning the admin chain from height 0. If the LightClient's
/// `current_epoch` is *lower* than the one observed when we last persisted
/// (e.g. EVM rolled back), we fall back to a full rescan from 0.
pub async fn catch_up<S, P>(
    storage: &S,
    evm_client: &EvmClient<P>,
    db: &BridgeDb,
    admin_chain_id: ChainId,
    admin_chain_height: BlockHeight,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: Provider,
{
    let current_epoch = match evm_client.get_current_epoch().await {
        Ok(epoch) => Epoch(epoch),
        Err(e) => {
            tracing::info!("LightClient not initialized yet, skipping catch-up: {e:#}");
            return Ok(());
        }
    };

    let persisted_height = db.get_last_scanned_admin_height().await?;
    let persisted_epoch = db.get_last_known_evm_epoch().await?;
    let start_height = match (persisted_height, persisted_epoch) {
        (None, None) => BlockHeight(0),
        (Some(h), Some(prev_epoch)) => {
            if current_epoch < prev_epoch {
                // Epochs are monotonic by construction (LightClient._setCommittee enforces
                // `epoch == currentEpoch + 1`). A decrease means the relay is pointing at a
                // different LightClient than before, or its scan_state DB is stale.
                anyhow::bail!(
                    "LightClient currentEpoch ({current_epoch}) is lower than the epoch \
                     previously observed by this relay ({prev_epoch}); refusing to proceed"
                );
            }
            h
        }
        // Only possible if a previous run crashed between the two writes (we don't write
        // them atomically). Resuming from `persisted_height` without a known epoch is
        // unsafe — if the EVM was meanwhile rolled back we'd skip committees we still
        // need to relay. Rescan from 0.
        (height, epoch) => {
            tracing::warn!(
                ?height,
                ?epoch,
                "Inconsistent scan_state (only one of admin height / epoch persisted); \
                 rescanning admin chain from 0"
            );
            BlockHeight(0)
        }
    };

    tracing::info!(
        %current_epoch,
        %admin_chain_id,
        %admin_chain_height,
        %start_height,
        "Checking for missed committee updates"
    );

    if start_height >= admin_chain_height {
        tracing::info!("No admin chain blocks to scan");
        // Persist both fields so the (Some, None) / (None, Some) "inconsistent" branch
        // above can never be reached from a clean run.
        db.set_last_scanned_admin_height(admin_chain_height).await?;
        db.set_last_known_evm_epoch(current_epoch).await?;
        return Ok(());
    }

    let heights: Vec<BlockHeight> = (start_height.0..admin_chain_height.0)
        .map(BlockHeight)
        .collect();

    let certs = storage
        .read_certificates_by_heights(admin_chain_id, &heights)
        .await?;

    let mut relayed = 0u32;
    for cert in certs.into_iter().flatten() {
        if let Some((epoch, blob_hash)) = find_create_committee(&cert) {
            if epoch <= current_epoch {
                continue;
            }
            let blob_id = BlobId::new(blob_hash, BlobType::Committee);
            let blob = storage
                .read_blob(blob_id)
                .await?
                .with_context(|| format!("committee blob {blob_id} not found in storage"))?;

            tracing::info!(?epoch, "Relaying missed committee update");
            relay_committee(evm_client, &cert, blob.bytes())
                .await
                .with_context(|| format!("failed to relay committee for epoch {epoch:?}"))?;
            relayed += 1;
        }
    }

    db.set_last_scanned_admin_height(admin_chain_height).await?;
    db.set_last_known_evm_epoch(current_epoch).await?;

    if relayed > 0 {
        tracing::info!(relayed, "Committee catch-up complete");
    } else {
        tracing::info!("LightClient is up to date");
    }
    Ok(())
}
