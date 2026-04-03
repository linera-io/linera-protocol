// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera-side monitoring: scans for BurnEvent stream events (auto-burns),
//! forwards certificates to EVM, checks EVM for completion via ERC-20
//! Transfer events, and retries unforwarded burns.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{primitives::Address, providers::Provider};
use tokio::sync::RwLock;

use super::{MonitorState, PendingBurn};
use crate::relay::{
    self,
    evm::EvmClient,
    linera::{find_burn_events, LineraClient},
};

/// Background task that scans Linera block history for BurnEvent stream
/// events and checks EVM for completion.
pub async fn linera_scan_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    evm_client: Arc<EvmClient<impl Provider + 'static>>,
    linera_client: Arc<LineraClient<E>>,
    pending_burn_tx: tokio::sync::mpsc::Sender<PendingBurn>,
    scan_interval: Duration,
    max_retries: u32,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            linera_scan_iteration(&monitor, &linera_client, &pending_burn_tx),
            check_burn_completion(&monitor, &evm_client),
        );

        if let Err(error) = scan_result {
            tracing::warn!(?error, "Linera scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(?error, "Burn completion check failed");
        }

        // Re-enqueue burns that are eligible for retry.
        {
            let state = monitor.read().await;
            for b in state.burns_ready_for_retry(max_retries) {
                let _ = pending_burn_tx.try_send(PendingBurn {
                    linera_height: b.value.linera_height,
                    burn_index: b.value.burn_index,
                    evm_recipient: b.value.evm_recipient.clone(),
                    amount: b.value.amount.clone(),
                });
            }
        }

        let summary = monitor.read().await.status_summary();
        tracing::trace!(
            pending = summary.burns_pending,
            completed = summary.burns_forwarded,
            last_height = summary.last_scanned_linera_height,
            "Linera burn scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Receives pending burns, reads the certificate containing the auto-burn,
/// and forwards it to EVM.
pub(crate) async fn retry_pending_burns<E: linera_core::environment::Environment + 'static>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
    linera_client: &LineraClient<E>,
    mut pending_rx: tokio::sync::mpsc::Receiver<PendingBurn>,
) -> anyhow::Result<()> {
    while let Some(pending) = pending_rx.recv().await {
        let credit_height = pending.linera_height;
        let burn_index = pending.burn_index;
        {
            let mut state = monitor.write().await;
            if let Some(b) = state.burns.get_mut(&(credit_height, burn_index)) {
                if b.forwarded {
                    tracing::trace!(
                        credit_height,
                        burn_index,
                        "Burn already completed, skipping"
                    );
                    continue;
                }
                b.last_retry_at = Some(Instant::now());
            } else {
                state.track_burn(pending).await;
            }
        }

        tracing::info!(credit_height, burn_index, "Processing burn...");

        // Read the certificate at the burn's block height (already contains the auto-burn).
        let cert = match async {
            linera_client.sync().await?;
            let info = linera_client.chain_info().await?;
            let mut hash = info.block_hash;
            loop {
                let Some(h) = hash else {
                    anyhow::bail!("Block at height {} not found", credit_height);
                };
                let c = linera_client.read_certificate(h).await?;
                if c.block().header.height.0 == credit_height {
                    break Ok(c);
                }
                hash = c.block().header.previous_block_hash;
            }
        }
        .await
        {
            Ok(cert) => cert,
            Err(e) => {
                tracing::warn!(
                    credit_height,
                    burn_index,
                    "Failed to read certificate: {e:#}"
                );
                monitor
                    .write()
                    .await
                    .mark_burn_retried(credit_height, burn_index);
                continue;
            }
        };

        // Persist raw BCS cert bytes so burns can be replayed without the relayer.
        let cert_bytes =
            bcs::to_bytes(&cert).expect("failed to BCS-serialize ConfirmedBlockCertificate");
        if let Some(db) = monitor.read().await.db() {
            if let Err(e) = db
                .store_burn_raw(credit_height, burn_index, &cert_bytes)
                .await
            {
                tracing::warn!(
                    credit_height,
                    burn_index,
                    "Failed to store burn raw bytes: {e:#}"
                );
            }
        }

        // Forward cert to EVM.
        let completed = match evm_client.forward_cert(&cert).await {
            Ok(()) => {
                tracing::info!(credit_height, burn_index, "Burn forwarded to EVM");
                true
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if msg.contains("already verified") {
                    tracing::trace!(credit_height, burn_index, "Block already verified on EVM");
                    true
                } else {
                    tracing::warn!(credit_height, burn_index, "EVM forwarding failed: {e:#}");
                    monitor
                        .write()
                        .await
                        .mark_burn_retried(credit_height, burn_index);
                    false
                }
            }
        };

        if completed {
            monitor
                .write()
                .await
                .complete_burn(credit_height, burn_index)
                .await;
            relay::update_balance_metrics(evm_client, linera_client).await;
        }
    }

    anyhow::bail!("Pending burn channel closed");
}

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
    pending_burn_tx: &tokio::sync::mpsc::Sender<PendingBurn>,
) -> anyhow::Result<()> {
    let last_height = monitor.read().await.last_scanned_linera_height;

    linera_client.sync().await?;
    let info = linera_client.chain_info().await?;
    let current_height = info.next_block_height.0;
    if current_height == 0 || current_height <= last_height {
        return Ok(());
    }

    let fungible_app_id = linera_client.fungible_app_id();

    let mut blocks = Vec::new();
    let mut hash = info.block_hash;
    while let Some(h) = hash {
        let block = linera_client.read_confirmed_block(h).await?;
        let height = block.block().header.height.0;
        if height < last_height {
            break;
        }
        hash = block.block().header.previous_block_hash;
        blocks.push(block);
    }
    blocks.reverse();

    let mut new_burns = Vec::new();
    for block in &blocks {
        let height = block.block().header.height.0;
        let burn_events = find_burn_events(&block.block().body.events, fungible_app_id);
        for (burn_index, burn_event) in burn_events.into_iter().enumerate() {
            let recipient = format!("0x{}", hex::encode(burn_event.target));
            new_burns.push((height, burn_index, recipient, burn_event.amount.to_string()));
        }
    }

    for (height, burn_index, recipient, amount) in &new_burns {
        tracing::info!(height, burn_index, recipient, amount, "Discovered burn");
        let _ = pending_burn_tx.try_send(PendingBurn {
            linera_height: *height,
            burn_index: *burn_index,
            evm_recipient: recipient.clone(),
            amount: amount.clone(),
        });
    }

    let mut state = monitor.write().await;
    state.last_scanned_linera_height = current_height;
    crate::relay::metrics::set_last_scanned_linera_height(current_height);
    Ok(())
}

async fn check_burn_completion(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
) -> anyhow::Result<()> {
    let pending: Vec<(u64, usize, Address)> = {
        let state = monitor.read().await;
        state
            .pending_burns()
            .into_iter()
            .filter_map(|b| {
                let addr: Address = b.value.evm_recipient.parse().ok()?;
                Some((b.value.linera_height, b.value.burn_index, addr))
            })
            .collect()
    };

    if pending.is_empty() {
        return Ok(());
    }

    for (height, burn_index, recipient) in pending {
        let logs = evm_client.get_transfer_logs(recipient).await?;
        if !logs.is_empty() {
            monitor
                .write()
                .await
                .complete_burn(height, burn_index)
                .await;
        }
    }

    Ok(())
}
