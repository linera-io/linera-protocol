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
            event_indices: event_indices_at_height,
            by_tx,
        } in groups
        {
            // `event_indices_at_height` are the stream-index values for every
            // pending burn at this height under the same `max_retries`
            // snapshot as `by_tx`. `mark_burn_retried` / `mark_burn_failed`
            // key on event_index; the per-tx chunking driven below uses
            // (tx_index, pos_in_tx) and maps back through
            // `MonitorState::event_index_for_pos` only on failure paths.

            let cert = match fetch_cert_at_height(linera_client, height).await {
                Ok(cert) => cert,
                Err(e) => {
                    // Cert fetch is an infrastructure-level failure (e.g. node
                    // sync). Skip the height — no burn was attempted, so don't
                    // consume any retry budget.
                    tracing::warn!(?height, "Failed to read certificate: {e:#}");
                    continue;
                }
            };

            persist_cert_bytes(monitor, height, &event_indices_at_height, &cert).await;

            // Dry-run addBlock. If it fits, one tx settles everything in the block.
            match estimate_fits(evm_client.estimate_add_block_gas(&cert).await) {
                Ok(true) => match evm_client.forward_cert(&cert).await {
                    Ok(()) => {
                        // Completion is async via `check_burn_completion`, so
                        // we don't mark the burns completed here — and don't
                        // bump retry either, because the call succeeded.
                        tracing::info!(
                            ?height,
                            count = event_indices_at_height.len(),
                            "Burns forwarded via addBlock"
                        );
                        relay::update_balance_metrics(evm_client, linera_client).await;
                    }
                    Err(e) => {
                        // addBlock attempted every burn at this height — all of
                        // them just took a failed attempt, so bump retry once
                        // per burn.
                        tracing::warn!(?height, "addBlock submission failed: {e:#}");
                        let mut state = monitor.write().await;
                        for ei in &event_indices_at_height {
                            state.mark_burn_retried(height, *ei);
                        }
                    }
                },
                Ok(false) => {
                    for (tx_index, positions) in &by_tx {
                        // Iterative LIFO split-to-fit per tx group. The
                        // algorithm is inlined (rather than factored into a
                        // pure helper that takes an async predicate) because
                        // an `AsyncFn` closure capturing `&EvmClient` across
                        // awaits trips an HRTB Send bound under `tokio::spawn`.
                        let mut stack: Vec<Vec<u32>> = vec![positions.clone()];
                        let mut chunks: Vec<Vec<u32>> = Vec::new();
                        // Positions that cannot fit even on their own — these
                        // get marked `failed` so they drop out of subsequent
                        // `pending_burns_by_height_and_tx` snapshots instead
                        // of poisoning their tx group forever.
                        let mut oversized: Vec<u32> = Vec::new();
                        while let Some(slice) = stack.pop() {
                            let est = evm_client
                                .estimate_process_burns_gas(&cert, *tx_index, &slice)
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

                        if !oversized.is_empty() {
                            let to_fail: Vec<u32> = {
                                let state = monitor.read().await;
                                oversized
                                    .iter()
                                    .filter_map(|&pos| {
                                        state.event_index_for_pos(height, *tx_index, pos)
                                    })
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

                        // Pop-order yields input order because we push right then left.
                        // Each chunk is an independent processBurns tx, so a failure
                        // on one does not block the rest — only that chunk's burns
                        // consume retry budget.
                        for chunk in chunks {
                            if let Err(e) = evm_client.process_burns(&cert, *tx_index, &chunk).await
                            {
                                tracing::warn!(
                                    tx_index,
                                    ?chunk,
                                    "processBurns submission failed: {e:#}"
                                );
                                let to_bump: Vec<u32> = {
                                    let state = monitor.read().await;
                                    chunk
                                        .iter()
                                        .filter_map(|&pos| {
                                            state.event_index_for_pos(height, *tx_index, pos)
                                        })
                                        .collect()
                                };
                                let mut state = monitor.write().await;
                                for ei in to_bump {
                                    state.mark_burn_retried(height, ei);
                                }
                            }
                        }
                    }
                    relay::update_balance_metrics(evm_client, linera_client).await;
                }
                Err(e) => {
                    // Like cert-fetch, estimate-gas is infrastructure-level —
                    // no burn was attempted, so don't bump retry.
                    tracing::warn!(?height, "estimate_add_block_gas failed: {e:#}");
                }
            }
        }
    }
}

/// Walks the chain history backwards from the head until the certificate at
/// `target_height` is found.
async fn fetch_cert_at_height<E: linera_core::environment::Environment>(
    linera_client: &LineraClient<E>,
    target_height: BlockHeight,
) -> anyhow::Result<Arc<linera_chain::types::ConfirmedBlockCertificate>> {
    linera_client.sync().await?;
    let info = linera_client.chain_info().await?;
    let mut hash = info.block_hash;
    loop {
        let Some(h) = hash else {
            anyhow::bail!("Block at height {} not found", target_height);
        };
        let c = linera_client.read_certificate(h).await?;
        if c.block().header.height == target_height {
            return Ok(c);
        }
        hash = c.block().header.previous_block_hash;
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
        blocks.push(block);
    }
    blocks.reverse();

    let mut new_burns = Vec::new();
    for block in &blocks {
        let height = block.block().header.height;
        let burn_events = find_burn_events(&block.block().body.events, fungible_app_id);
        for (tx_index, event_pos_in_tx, event_index, burn_event) in burn_events {
            new_burns.push((
                height,
                tx_index,
                event_pos_in_tx,
                event_index,
                Address::from(burn_event.target),
                burn_event.amount,
            ));
        }
    }

    let mut tracked_any = false;
    for (height, tx_index, event_pos_in_tx, event_index, recipient, amount) in &new_burns {
        tracing::info!(?height, tx_index, event_pos_in_tx, event_index, %recipient, %amount, "Discovered burn");
        let was_new = monitor
            .write()
            .await
            .track_burn(PendingBurn {
                height: *height,
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
