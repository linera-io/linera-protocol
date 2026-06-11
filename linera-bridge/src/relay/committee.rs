// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detects committee rotation operations on the admin chain and relays
//! them to the EVM LightClient contract.

use alloy::providers::Provider;
use anyhow::{Context as _, Result};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Epoch},
    identifiers::{BlobId, BlobType, ChainId, StreamId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::system::{EpochEventData, EPOCH_STREAM_NAME};
use linera_storage::Storage;

use super::evm::EvmClient;
use crate::evm::client::extract_validator_keys;

/// The epoch event Linera emits when a new committee is created, located within a block.
pub struct CommitteeEvent {
    /// Index of the transaction whose events contain the epoch event.
    pub tx_index: usize,
    /// Position of the epoch event within that transaction's events.
    pub position: usize,
    /// The new epoch — the event's index.
    pub epoch: Epoch,
    /// The committee blob hash, from the event's `EpochEventData` payload.
    pub blob_hash: CryptoHash,
}

/// Locates the epoch event Linera emits when a new committee is created. Returns its position in the
/// block (so callers can build an inclusion proof) plus the new epoch and committee blob hash.
pub fn find_committee_event(cert: &ConfirmedBlockCertificate) -> Option<CommitteeEvent> {
    let epoch_stream = StreamId::system(EPOCH_STREAM_NAME);
    for (tx_index, tx_events) in cert.inner().block().body.events.iter().enumerate() {
        for (position, event) in tx_events.iter().enumerate() {
            if event.stream_id != epoch_stream {
                continue;
            }
            let data: EpochEventData = bcs::from_bytes(&event.value).ok()?;
            return Some(CommitteeEvent {
                tx_index,
                position,
                epoch: Epoch(event.index),
                blob_hash: data.blob_hash,
            });
        }
    }
    None
}

/// Relays a single committee update to the LightClient contract.
pub async fn relay_committee<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    committee_blob_bytes: &[u8],
) -> Result<()> {
    let validator_keys = extract_validator_keys(committee_blob_bytes)?;

    let tx_hash = evm_client
        .add_committee(cert, committee_blob_bytes, validator_keys)
        .await?;

    tracing::info!(%tx_hash, "Relayed committee to LightClient");
    Ok(())
}

/// Catches up the LightClient with any committee updates missed while offline.
/// Scans admin chain blocks and relays committees newer than the LightClient's
/// current epoch. Must succeed before the relay enters the main serve loop.
pub async fn catch_up<S, P>(
    storage: &S,
    evm_client: &EvmClient<P>,
    admin_chain_id: ChainId,
    admin_chain_height: BlockHeight,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: Provider,
{
    let current_epoch = match evm_client.get_current_epoch().await {
        Ok(epoch) => epoch,
        Err(error) => {
            tracing::info!(?error, "LightClient not initialized yet, skipping catch-up");
            return Ok(());
        }
    };
    tracing::info!(
        %current_epoch,
        %admin_chain_id,
        %admin_chain_height,
        "Checking for missed committee updates"
    );

    let heights: Vec<BlockHeight> = (0..admin_chain_height.0).map(BlockHeight).collect();

    if heights.is_empty() {
        tracing::info!("No admin chain blocks to scan");
        return Ok(());
    }

    let certs = storage
        .read_certificates_by_heights(admin_chain_id, &heights)
        .await?;

    let mut relayed = 0u32;
    for cert in certs.into_iter().flatten() {
        if let Some(committee_event) = find_committee_event(&cert) {
            let epoch = committee_event.epoch;
            if epoch <= current_epoch {
                continue;
            }
            let blob_id = BlobId::new(committee_event.blob_hash, BlobType::Committee);
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

    if relayed > 0 {
        tracing::info!(relayed, "Committee catch-up complete");
    } else {
        tracing::info!("LightClient is up to date");
    }
    Ok(())
}
