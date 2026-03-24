// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera-side monitoring: scans for Credit-to-Address20 messages (burns),
//! checks EVM for completion via ERC-20 Transfer events, and retries
//! unforwarded burns.

use std::{sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, B256},
    providers::Provider,
    rpc::types::Filter,
};
use linera_base::identifiers::ApplicationId;
use tokio::sync::RwLock;

use super::{MonitorState, PendingBurn};
use crate::relay::linera::find_address20_credits;

/// Background task that scans Linera block history for Credit messages
/// to Address20 owners and checks EVM for completion.
pub async fn linera_scan_loop<E: linera_core::environment::Environment>(
    monitor: Arc<RwLock<MonitorState>>,
    chain_client: linera_core::client::ChainClient<E>,
    fungible_app_id: ApplicationId,
    provider: impl Provider + Clone + 'static,
    bridge_addr: Address,
    pending_burn_tx: tokio::sync::mpsc::Sender<PendingBurn>,
    scan_interval: Duration,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            linera_scan_iteration(&monitor, &chain_client, fungible_app_id, &pending_burn_tx),
            check_burn_completion(&monitor, &provider, bridge_addr),
        );

        if let Err(error) = scan_result {
            tracing::warn!(?error, "Linera scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(?error, "Burn completion check failed");
        }

        let summary = monitor.read().await.status_summary();
        tracing::info!(
            pending = summary.burns_pending,
            completed = summary.burns_forwarded,
            last_height = summary.last_scanned_linera_height,
            "Linera burn scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Receives pending burns from the scanner and retries them.
pub(crate) async fn retry_pending_burns<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    chain_client: &linera_core::client::ChainClient<E>,
    fungible_app_id: ApplicationId,
    bridge_addr: Address,
    provider: &impl Provider,
    mut pending_rx: tokio::sync::mpsc::Receiver<PendingBurn>,
    max_retries: u32,
) -> anyhow::Result<()> {
    while let Some(pending) = pending_rx.recv().await {
        let credit_height = pending.linera_height;
        let burn_index = pending.burn_index;

        // Track in MonitorState for HTTP visibility + completion detection.
        monitor.write().await.track_burn(pending);

        tracing::info!(credit_height, burn_index, "Retrying unforwarded burn...");

        // Sync chain state to ensure we have the latest blocks.
        chain_client.synchronize_from_validators().await?;
        let chain_info = chain_client.chain_info().await?;

        // Walk blocks backward from tip to find the burn execution block.
        let mut hash = chain_info.block_hash;
        let mut forwarded = false;
        while let Some(h) = hash {
            let block = chain_client.read_confirmed_block(h).await?;
            let height = block.block().header.height.0;
            if height <= credit_height {
                break;
            }
            hash = block.block().header.previous_block_hash;

            // Check if this block has burn operations for the fungible app.
            let has_burn = block.block().body.transactions.iter().any(|txn| {
                if let linera_chain::data_types::Transaction::ExecuteOperation(op) = txn {
                    if let linera_execution::Operation::User { application_id, .. } = op {
                        *application_id == fungible_app_id
                    } else {
                        false
                    }
                } else {
                    false
                }
            });

            if has_burn {
                match crate::relay::evm::forward_cert_to_evm(&block, bridge_addr, provider).await {
                    Ok(()) => {
                        tracing::info!(height, "Burn cert re-forwarded to EVM");
                        forwarded = true;
                        break;
                    }
                    Err(e) => {
                        let msg = format!("{e:#}");
                        if msg.contains("already verified") {
                            tracing::debug!(height, "Block already verified on EVM, skipping");
                        } else {
                            tracing::warn!(height, "Failed to re-forward burn cert: {e:#}");
                        }
                    }
                }
            }
        }

        let mut state = monitor.write().await;
        if forwarded {
            state.complete_burn(credit_height, burn_index);
        } else if state
            .burns
            .get(&(credit_height, burn_index))
            .is_some_and(|b| b.retry_count + 1 >= max_retries)
        {
            state.mark_burn_failed(credit_height, burn_index);
            tracing::error!(
                credit_height,
                burn_index,
                "Burn marked as failed after max retries"
            );
        } else {
            state.mark_burn_retried(credit_height, burn_index);
        }
    }

    anyhow::bail!("Pending burn channel closed");
}

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    chain_client: &linera_core::client::ChainClient<E>,
    fungible_app_id: ApplicationId,
    pending_burn_tx: &tokio::sync::mpsc::Sender<PendingBurn>,
) -> anyhow::Result<()> {
    let last_height = monitor.read().await.last_scanned_linera_height;

    // Sync and get current chain info.
    chain_client.synchronize_from_validators().await?;
    let chain_info = chain_client.chain_info().await?;
    let current_height = chain_info.next_block_height.0;
    if current_height == 0 || current_height <= last_height {
        return Ok(());
    }

    // Walk backward from the tip to collect blocks we haven't scanned.
    let mut blocks = Vec::new();
    let mut hash = chain_info.block_hash;
    while let Some(h) = hash {
        let block = chain_client.read_confirmed_block(h).await?;
        let height = block.block().header.height.0;
        if height < last_height {
            break;
        }
        hash = block.block().header.previous_block_hash;
        blocks.push(block);
    }
    // Process oldest-first.
    blocks.reverse();

    // Collect burns into a local Vec first, then send on channel.
    let mut new_burns = Vec::new();
    for block in &blocks {
        let height = block.block().header.height.0;
        let credits = find_address20_credits(&block.block().body.transactions, fungible_app_id);
        for (burn_index, (owner, amount)) in credits.into_iter().enumerate() {
            new_burns.push((height, burn_index, format!("{owner}"), amount.to_string()));
        }
    }

    for (height, burn_index, recipient, amount) in &new_burns {
        let _ = pending_burn_tx.try_send(PendingBurn {
            linera_height: *height,
            burn_index: *burn_index,
            evm_recipient: recipient.clone(),
            amount: amount.clone(),
        });
    }

    let mut state = monitor.write().await;
    state.last_scanned_linera_height = current_height;
    Ok(())
}

/// ERC-20 Transfer(address indexed from, address indexed to, uint256 value) event signature.
fn transfer_event_signature() -> B256 {
    alloy::primitives::keccak256("Transfer(address,address,uint256)")
}

/// For each pending burn, scan EVM for a matching ERC-20 Transfer event
/// from the FungibleBridge address to the burn recipient.
async fn check_burn_completion(
    monitor: &RwLock<MonitorState>,
    provider: &impl Provider,
    bridge_addr: Address,
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

    let transfer_sig = transfer_event_signature();

    for (height, burn_index, recipient) in pending {
        // Look for Transfer events from the bridge to the recipient.
        let filter = Filter::new()
            .address(bridge_addr)
            .event_signature(transfer_sig)
            .topic2(B256::left_padding_from(recipient.as_slice()));
        let logs = provider.get_logs(&filter).await?;
        if !logs.is_empty() {
            monitor.write().await.complete_burn(height, burn_index);
        }
    }

    Ok(())
}
