// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Detects committee rotation operations on the admin chain and relays
//! them to the EVM LightClient contract.

use std::time::Duration;

use alloy::providers::Provider;
use anyhow::{Context as _, Result};
use linera_base::{
    data_types::{BlockHeight, Epoch},
    identifiers::{BlobId, BlobType, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::{system::AdminOperation, Operation, SystemOperation};
use linera_storage::Storage;
use tokio::time::sleep;

use super::evm::EvmClient;

/// Base delay between committee-relay attempts; scaled by the attempt number
/// (capped) so worst-case inline blocking stays bounded regardless of the
/// configured retry count.
const COMMITTEE_RELAY_BACKOFF: Duration = Duration::from_secs(2);

/// Relays every committee the LightClient is missing, scanning admin-chain
/// blocks up to `admin_chain_height` (exclusive). Run once at startup (must
/// succeed before entering the serve loop) and again on each admin-chain block,
/// so a missed or failed relay self-heals instead of stranding the LightClient
/// at a stale epoch.
pub async fn catch_up<S, P>(
    storage: &S,
    evm_client: &EvmClient<P>,
    admin_chain_id: ChainId,
    admin_chain_height: BlockHeight,
    max_retries: u32,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: Provider,
{
    tracing::info!(
        %admin_chain_id,
        %admin_chain_height,
        "Checking for missed committee updates"
    );
    reconcile_committees(
        storage,
        evm_client,
        admin_chain_id,
        admin_chain_height,
        max_retries,
    )
    .await
}

/// Relays every committee newer than the LightClient's current epoch by scanning
/// admin-chain blocks below `scan_upto`. Idempotent and gap-filling: each missing
/// epoch is relayed in ascending order with bounded retry. Used both at startup
/// (via [`catch_up`]) and on every live admin-chain block, so a transient relay
/// failure self-heals on a later block instead of stranding the LightClient at a
/// stale epoch — which would make it reject all subsequent burn certificates
/// signed by the newer committee.
///
/// Scans only the admin-chain blocks above the height that installed
/// `current_epoch`'s committee (queried from the LightClient via
/// [`EvmClient::committee_height`]), so the work per call is bounded by the
/// number of admin blocks since the last relayed committee rather than the full
/// admin-chain height. Falls back to a full scan from height 0 when that height
/// is unknown (genesis epoch).
async fn reconcile_committees<S, P>(
    storage: &S,
    evm_client: &EvmClient<P>,
    admin_chain_id: ChainId,
    scan_upto: BlockHeight,
    max_retries: u32,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: Provider,
{
    let current_epoch = match evm_client.get_current_epoch().await {
        Ok(epoch) => epoch,
        Err(error) => {
            tracing::info!(
                ?error,
                "LightClient not initialized yet, skipping committee reconcile"
            );
            return Ok(());
        }
    };

    // Resume scanning from the admin-chain height that installed `current_epoch`'s
    // committee: every committee at or below that height has epoch <=
    // `current_epoch` and would be skipped anyway, and committees newer than
    // `current_epoch` are created at strictly higher heights. The genesis
    // committee (and any unknown epoch) reports height 0, so this degrades to a
    // full scan. Because `current_epoch` is the LightClient's own state, a relay
    // that failed leaves `current_epoch` — and thus this origin — unchanged, so
    // the missed committee is re-scanned on the next admin block (self-healing).
    let scan_from = evm_client.committee_height(current_epoch).await?;

    let heights: Vec<BlockHeight> = (scan_from.0..scan_upto.0).map(BlockHeight).collect();
    if heights.is_empty() {
        return Ok(());
    }

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

            tracing::info!(?epoch, "Relaying committee update");
            relay_committee_with_retry(evm_client, &cert, blob.bytes(), epoch, max_retries).await?;
            relayed += 1;
        }
    }

    if relayed > 0 {
        tracing::info!(relayed, "Committee reconcile complete");
    }
    Ok(())
}

/// Scans a certificate for a `CreateCommittee` operation, returning the first
/// one's epoch and blob hash if found.
///
/// Linera emits at most one `CreateCommittee` per admin-chain block, and both
/// this relayer and the on-chain `addCommittee` (which requires the block's
/// epoch to equal the LightClient's current epoch) rely on that — only the
/// first is ever relayed. If that invariant is ever violated, only the first
/// committee would be installed and the LightClient would stall one epoch
/// behind the newer ones, so log loudly rather than fail silently.
fn find_create_committee(
    cert: &ConfirmedBlockCertificate,
) -> Option<(Epoch, linera_base::crypto::CryptoHash)> {
    let mut first = None;
    let mut count = 0u32;
    for op in cert.inner().block().body.operations() {
        if let Operation::System(boxed) = op {
            if let SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch,
                blob_hash,
                ..
            }) = boxed.as_ref()
            {
                count += 1;
                if first.is_none() {
                    first = Some((*epoch, *blob_hash));
                }
            }
        }
    }
    if count > 1 {
        tracing::error!(
            count,
            "admin block contains multiple CreateCommittee operations; only the first \
             will be relayed, so the LightClient will stall behind the newer epochs. This \
             violates the assumed at-most-one-CreateCommittee-per-admin-block invariant."
        );
    }
    first
}

/// Relays a committee with bounded backoff, retrying transient failures (RPC
/// blips, nonce races, gas spikes). Returns the last error if every attempt
/// fails, so the caller can defer the remainder to the next reconcile.
async fn relay_committee_with_retry<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    committee_blob_bytes: &[u8],
    epoch: Epoch,
    max_retries: u32,
) -> Result<()> {
    let mut attempt = 1u32;
    loop {
        match relay_committee(evm_client, cert, committee_blob_bytes).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if attempt >= max_retries {
                    return Err(error).with_context(|| {
                        format!(
                            "committee relay for epoch {epoch:?} failed after {attempt} attempts"
                        )
                    });
                }
                // Cap the multiplier so the total inline blocking stays bounded
                // even when `max_retries` is large.
                let backoff = COMMITTEE_RELAY_BACKOFF * attempt.min(4);
                tracing::warn!(?epoch, attempt, ?backoff, %error, "Committee relay failed, retrying");
                sleep(backoff).await;
                attempt += 1;
            }
        }
    }
}

/// Relays a single committee update to the LightClient contract.
async fn relay_committee<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    committee_blob_bytes: &[u8],
) -> Result<()> {
    let tx_hash = evm_client.add_committee(cert, committee_blob_bytes).await?;

    tracing::info!(%tx_hash, "Relayed committee to LightClient");
    Ok(())
}
