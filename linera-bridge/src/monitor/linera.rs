// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera-side monitoring: scans for BurnEvent stream events (auto-burns),
//! forwards certificates to EVM, checks EVM for completion via ERC-20
//! Transfer events, and retries unforwarded burns.

use std::{sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::Provider};
use linera_base::data_types::BlockHeight;
use tokio::sync::{Notify, RwLock};

use super::{MonitorState, PendingBurn};
use crate::relay::{
    self,
    evm::EvmClient,
    linera::{find_burn_events, LineraClient},
    settlement::estimate_fits,
};

/// Background task that scans Linera block history for BurnEvent stream
/// events and checks EVM for completion. Newly-discovered burns are written
/// to `MonitorState` (and SQLite) and the consumer is woken via `notify`.
pub async fn linera_scan_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    evm_client: Arc<EvmClient<impl Provider + 'static>>,
    linera_client: Arc<LineraClient<E>>,
    burn_notify: Arc<Notify>,
    scan_interval: Duration,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            linera_scan_iteration(&monitor, &linera_client, &burn_notify),
            check_burn_completion(&monitor, &evm_client),
        );

        if let Err(error) = scan_result {
            tracing::warn!(?error, "Linera scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(?error, "Burn completion check failed");
        }

        let summary = monitor.read().await.status_summary();
        tracing::trace!(
            pending = summary.burns_pending,
            completed = summary.burns_forwarded,
            last_height = %summary.last_scanned_linera_height,
            "Linera burn scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Batches pending burns per height, dry-runs `addBlock(cert)`, and falls back
/// to per-tx chunked `processBurns(cert, tx_index, positions)` when it doesn't
/// fit. Sleeps on `notify` (woken by the scanner) or on `poll_interval`
/// (whichever comes first) when nothing is ready.
pub(crate) async fn process_pending_burns<E: linera_core::environment::Environment + 'static>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
    linera_client: &LineraClient<E>,
    notify: &Notify,
    poll_interval: Duration,
    max_retries: u32,
) -> anyhow::Result<()> {
    loop {
        let groups = monitor
            .read()
            .await
            .pending_burns_by_height_and_tx(max_retries);
        if groups.is_empty() {
            tracing::trace!(
                ?poll_interval,
                "Linera burns processor sleeping until notified or poll interval elapses"
            );
            tokio::select! {
                _ = notify.notified() => {
                    tracing::trace!("Linera burns processor notified about new pending item");
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
            continue;
        }
        for super::PendingBurnsAtHeight {
            height,
            block_hash,
            event_indices,
            by_tx,
        } in groups
        {
            // Infrastructure-level failures (cert fetch, gas estimate)
            // skip the height without consuming any burn's retry budget.
            let cert = match linera_client.read_certificate(block_hash).await {
                Ok(cert) => cert,
                Err(error) => {
                    tracing::warn!(?height, ?block_hash, ?error, "Failed to read certificate");
                    continue;
                }
            };

            persist_cert_bytes(monitor, height, &event_indices, &cert).await;

            match estimate_fits(evm_client.estimate_add_block_gas(&cert).await) {
                Ok(true) => {
                    submit_addblock(monitor, evm_client, &cert, height, &event_indices).await;
                    relay::update_balance_metrics(evm_client, linera_client).await;
                }
                Ok(false) => {
                    submit_chunked(monitor, evm_client, &cert, height, &by_tx).await;
                    relay::update_balance_metrics(evm_client, linera_client).await;
                }
                Err(error) => {
                    tracing::warn!(?height, ?error, "estimate_add_block_gas failed");
                }
            }
        }
    }
}

/// Submits the cert via `addBlock`. On success, leaves retry counts alone
/// (completion is observed asynchronously by `check_burn_completion`). On
/// failure, bumps retry once for every burn at the height — addBlock
/// attempted all of them.
async fn submit_addblock<P: Provider>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<P>,
    cert: &linera_chain::types::ConfirmedBlockCertificate,
    height: BlockHeight,
    event_indices: &[u32],
) {
    match evm_client.forward_cert(cert).await {
        Ok(()) => {
            tracing::info!(
                ?height,
                count = event_indices.len(),
                "Burns forwarded via addBlock"
            );
        }
        Err(e) => {
            tracing::warn!(?height, "addBlock submission failed: {e:#}");
            let mut state = monitor.write().await;
            for ei in event_indices {
                state.mark_burn_retried(height, *ei);
            }
        }
    }
}

/// Per-tx chunked fallback: split each tx group's positions to fit under
/// the block gas limit, mark any individually-oversized burn as `failed`,
/// then submit each fitting chunk as an independent `processBurns` tx.
async fn submit_chunked<P: Provider>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<P>,
    cert: &linera_chain::types::ConfirmedBlockCertificate,
    height: BlockHeight,
    by_tx: &[(u32, Vec<u32>)],
) {
    for (tx_index, positions) in by_tx {
        let (chunks, oversized) = split_to_fit(evm_client, cert, *tx_index, positions).await;
        mark_oversized_failed(monitor, height, *tx_index, &oversized).await;
        submit_chunks_with_retry(monitor, evm_client, cert, height, *tx_index, chunks).await;
    }
}

/// Iterative LIFO binary search: keep halving slices that don't fit until
/// each one either fits as a chunk or shrinks to a single oversized
/// position. Inlined as a free fn rather than factored behind an async
/// predicate so it doesn't need an `AsyncFn` closure capturing
/// `&EvmClient` across awaits (which trips an HRTB Send bound under
/// `tokio::spawn`).
///
/// Returns `(chunks, oversized)`. Chunks are in input order because each
/// split pushes right then left.
async fn split_to_fit<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &linera_chain::types::ConfirmedBlockCertificate,
    tx_index: u32,
    positions: &[u32],
) -> (Vec<Vec<u32>>, Vec<u32>) {
    let mut stack: Vec<Vec<u32>> = vec![positions.to_vec()];
    let mut chunks: Vec<Vec<u32>> = Vec::new();
    let mut oversized: Vec<u32> = Vec::new();
    while let Some(slice) = stack.pop() {
        let est = evm_client
            .estimate_process_burns_gas(cert, tx_index, &slice)
            .await;
        let fits = estimate_fits(est).unwrap_or(false);
        if fits {
            chunks.push(slice);
            continue;
        }
        if slice.len() == 1 {
            oversized.push(slice[0]);
            continue;
        }
        let mid = slice.len() / 2;
        let (left, right) = slice.split_at(mid);
        stack.push(right.to_vec());
        stack.push(left.to_vec());
    }
    (chunks, oversized)
}

/// Marks oversized positions `failed` so they drop out of subsequent
/// `pending_burns_by_height_and_tx` snapshots instead of poisoning their
/// tx group on every retry pass.
async fn mark_oversized_failed(
    monitor: &RwLock<MonitorState>,
    height: BlockHeight,
    tx_index: u32,
    oversized: &[u32],
) {
    if oversized.is_empty() {
        return;
    }
    let to_fail: Vec<u32> = {
        let state = monitor.read().await;
        oversized
            .iter()
            .filter_map(|&pos| state.event_index_for_pos(height, tx_index, pos))
            .collect()
    };
    let mut state = monitor.write().await;
    for ei in to_fail {
        tracing::error!(
            ?height,
            tx_index,
            event_index = ei,
            "single burn does not fit under the EVM block gas limit; marking failed"
        );
        state.mark_burn_failed(height, ei).await;
    }
}

/// Submits each chunk as its own `processBurns` tx. A chunk's failure only
/// consumes that chunk's retry budget — remaining chunks still attempt
/// submission, because each is independent on-chain.
async fn submit_chunks_with_retry<P: Provider>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<P>,
    cert: &linera_chain::types::ConfirmedBlockCertificate,
    height: BlockHeight,
    tx_index: u32,
    events_chunks: Vec<Vec<u32>>,
) {
    for events_chunk in events_chunks {
        if let Err(error) = evm_client
            .process_burns(cert, tx_index, &events_chunk)
            .await
        {
            tracing::warn!(
                tx_index,
                ?events_chunk,
                ?error,
                "processBurns submission failed"
            );
            let to_bump: Vec<u32> = {
                let state = monitor.read().await;
                events_chunk
                    .iter()
                    .filter_map(|&pos| state.event_index_for_pos(height, tx_index, pos))
                    .collect()
            };
            let mut state = monitor.write().await;
            for ei in to_bump {
                state.mark_burn_retried(height, ei);
            }
        }
    }
}

/// Stores BCS cert bytes for every pending burn at `height`.
async fn persist_cert_bytes(
    monitor: &RwLock<MonitorState>,
    height: BlockHeight,
    event_indices: &[u32],
    cert: &linera_chain::types::ConfirmedBlockCertificate,
) {
    let cert_bytes = bcs::to_bytes(cert).expect("BCS-serialize cert");
    let state = monitor.read().await;
    let Some(db) = state.db() else { return };
    for ei in event_indices {
        if let Err(e) = db.store_burn_raw(height, *ei, &cert_bytes).await {
            tracing::warn!(
                ?height,
                event_index = ei,
                "Failed to store burn raw bytes: {e:#}"
            );
        }
    }
}

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
    notify: &Notify,
) -> anyhow::Result<()> {
    let last_height = monitor.read().await.last_scanned_linera_height;

    linera_client.sync().await?;
    let info = linera_client.chain_info().await?;
    let current_height = info.next_block_height;
    if current_height.0 == 0 || current_height <= last_height {
        return Ok(());
    }

    let fungible_app_id = linera_client.fungible_app_id();

    let mut blocks = Vec::new();
    let mut hash = info.block_hash;
    while let Some(h) = hash {
        let block = linera_client.read_confirmed_block(h).await?;
        let height = block.block().header.height;
        if height < last_height {
            break;
        }
        hash = block.block().header.previous_block_hash;
        blocks.push((h, block));
    }
    blocks.reverse();

    let mut new_burns = Vec::new();
    for (block_hash, block) in &blocks {
        let height = block.block().header.height;
        let burn_events = find_burn_events(&block.block().body.events, fungible_app_id);
        for (tx_index, event_pos_in_tx, event_index, burn_event) in burn_events {
            new_burns.push((
                height,
                *block_hash,
                tx_index,
                event_pos_in_tx,
                event_index,
                Address::from(burn_event.target),
                burn_event.amount,
            ));
        }
    }

    let mut tracked_any = false;
    for (height, block_hash, tx_index, event_pos_in_tx, event_index, recipient, amount) in
        &new_burns
    {
        tracing::info!(?height, ?block_hash, tx_index, event_pos_in_tx, event_index, %recipient, %amount, "Discovered burn");
        let was_new = monitor
            .write()
            .await
            .track_burn(PendingBurn {
                height: *height,
                block_hash: *block_hash,
                tx_index: *tx_index,
                event_pos_in_tx: *event_pos_in_tx,
                event_index: *event_index,
                evm_recipient: *recipient,
                amount: *amount,
            })
            .await;
        tracked_any |= was_new;
    }

    {
        let mut state = monitor.write().await;
        state.last_scanned_linera_height = current_height;
    }
    crate::relay::metrics::set_last_scanned_linera_height(current_height.0);

    if tracked_any {
        notify.notify_one();
    }
    Ok(())
}

/// Polls the FungibleBridge per pending burn and marks any that the EVM
/// has already released.
async fn check_burn_completion(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
) -> anyhow::Result<()> {
    let pending: Vec<(BlockHeight, u32)> = {
        let state = monitor.read().await;
        state
            .pending_burns()
            .into_iter()
            .map(|b| (b.value.height, b.value.event_index))
            .collect()
    };

    if pending.is_empty() {
        return Ok(());
    }

    for (height, event_index) in pending {
        match evm_client.is_burn_processed(height, event_index).await {
            Ok(true) => {
                monitor
                    .write()
                    .await
                    .complete_burn(height, event_index)
                    .await;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    ?height,
                    event_index,
                    "is_burn_processed query failed: {e:#}"
                );
            }
        }
    }

    Ok(())
}
