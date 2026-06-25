// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera-side monitoring: scans for BurnEvent stream events,
//! forwards certificates to EVM, checks EVM for completion via ERC-20
//! Transfer events, and retries unforwarded burns.

use std::{sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::Provider};
use linera_base::data_types::BlockHeight;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::environment::Environment;
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
pub async fn linera_scan_loop<E: Environment + 'static>(
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

/// Batches pending burns per height and settles each via per-tx chunked
/// `processBurns(cert, tx_index, positions)`: it registers the block once, then
/// proves and releases each chunk's events independently. Sleeps on `notify`
/// (woken by the scanner) or on `poll_interval` (whichever comes first) when
/// nothing is ready.
pub(crate) async fn process_pending_burns<E: Environment + 'static>(
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

            // Every block settles through the per-tx chunked `processBurns` path: it registers the
            // block once, estimates each chunk independently, submits the ones that fit, and marks
            // any single burn that still can't fit as `failed` (so an invalid cert terminates
            // instead of being re-polled forever).
            submit_chunked(monitor, evm_client, &cert, height, &by_tx, max_retries).await;
            relay::update_balance_metrics(evm_client, linera_client).await;
        }
    }
}

/// Per-tx chunked settlement: register the block, split each tx group's
/// positions to fit under the block gas limit, mark any
/// individually-oversized burn as `failed`, then submit each fitting chunk
/// as an independent `processBurns` tx.
async fn submit_chunked<P: Provider>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    height: BlockHeight,
    by_tx: &[(u32, Vec<u32>)],
    max_retries: u32,
) {
    // Chunked settlement proves each burn against the block's registered `events_hash`, so the
    // block must be registered before any gas estimate or `processBurns` — both revert otherwise.
    if let Err(error) = evm_client.register_block(cert).await {
        tracing::warn!(?height, ?error, "registerBlock submission failed");
        let to_bump: Vec<u32> = {
            let state = monitor.read().await;
            let mut event_indices = Vec::new();
            for (tx_index, positions) in by_tx {
                for &pos in positions {
                    if let Some(ei) = state.event_index_for_pos(height, *tx_index, pos) {
                        event_indices.push(ei);
                    }
                }
            }
            event_indices
        };
        let mut state = monitor.write().await;
        for event_index in to_bump {
            state
                .mark_burn_retried(height, event_index, max_retries)
                .await;
        }
        return;
    }

    for (tx_index, positions) in by_tx {
        let (chunks, oversized, errored) =
            split_to_fit(evm_client, cert, *tx_index, positions).await;
        mark_oversized_failed(monitor, height, *tx_index, &oversized).await;
        mark_estimate_errored_retry(monitor, height, *tx_index, &errored, max_retries).await;
        submit_chunks_with_retry(
            monitor,
            evm_client,
            cert,
            height,
            *tx_index,
            chunks,
            max_retries,
        )
        .await;
    }
}

/// Iterative LIFO binary search: keep halving slices that don't fit until
/// each one either fits as a chunk or shrinks to a single oversized
/// position. Inlined as a free fn rather than factored behind an async
/// predicate so it doesn't need an `AsyncFn` closure capturing
/// `&EvmClient` across awaits (which trips an HRTB Send bound under
/// `tokio::spawn`).
///
/// Returns `(chunks, oversized, errored)`:
/// - `chunks`: slices that fit, in input order (each split pushes right then
///   left).
/// - `oversized`: single positions whose gas estimate *succeeded* and reported
///   the burn cannot fit under the EVM block gas limit — genuinely
///   un-relayable, hence terminal.
/// - `errored`: positions whose estimate *failed* (transient transport/RPC
///   error, or a revert) rather than returning a verdict. These must NOT be
///   treated as "doesn't fit": a single node hiccup must not permanently fail
///   a burn that would otherwise settle, so the caller routes them to bounded
///   retry (`max_retries`) instead of immediate failure.
async fn split_to_fit<P: Provider>(
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    tx_index: u32,
    positions: &[u32],
) -> (Vec<Vec<u32>>, Vec<u32>, Vec<u32>) {
    let mut stack: Vec<Vec<u32>> = vec![positions.to_vec()];
    let mut chunks: Vec<Vec<u32>> = Vec::new();
    let mut oversized: Vec<u32> = Vec::new();
    let mut errored: Vec<u32> = Vec::new();
    while let Some(slice) = stack.pop() {
        let fits = estimate_fits(
            evm_client
                .estimate_process_burns_gas(cert, tx_index, &slice)
                .await,
        );
        match classify_slice(&fits, slice.len()) {
            SliceVerdict::Fits => chunks.push(slice),
            SliceVerdict::Oversized => oversized.push(slice[0]),
            SliceVerdict::Bisect => {
                let mid = slice.len() / 2;
                let (left, right) = slice.split_at(mid);
                stack.push(right.to_vec());
                stack.push(left.to_vec());
            }
            SliceVerdict::Errored => {
                tracing::warn!(
                    tx_index,
                    ?slice,
                    error = ?fits,
                    "estimate_process_burns_gas failed; scheduling retry instead of failing"
                );
                errored.extend_from_slice(&slice);
            }
        }
    }
    (chunks, oversized, errored)
}

/// How `split_to_fit` should dispose of one slice, decided purely from the
/// gas-estimate verdict and the slice length. Factored out so the
/// transient-error vs. genuinely-oversized distinction — the property that
/// keeps a single RPC hiccup from permanently failing a relayable burn — is
/// unit-testable without a live EVM provider.
#[derive(Debug, PartialEq, Eq)]
enum SliceVerdict {
    /// Estimate succeeded and the slice fits — submit it as a chunk.
    Fits,
    /// Estimate succeeded and a single burn exceeds the block gas limit —
    /// genuinely un-relayable, fail it.
    Oversized,
    /// Estimate succeeded, the slice doesn't fit but holds more than one burn —
    /// bisect and re-test the halves.
    Bisect,
    /// The estimate itself failed (transient transport/RPC error or a revert)
    /// rather than returning a verdict — must NOT be read as "doesn't fit";
    /// route to bounded retry instead.
    Errored,
}

fn classify_slice(fits: &anyhow::Result<bool>, slice_len: usize) -> SliceVerdict {
    match fits {
        Ok(true) => SliceVerdict::Fits,
        Ok(false) if slice_len == 1 => SliceVerdict::Oversized,
        Ok(false) => SliceVerdict::Bisect,
        Err(_) => SliceVerdict::Errored,
    }
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
    let to_fail = {
        let state = monitor.read().await;
        oversized
            .iter()
            .filter_map(|&pos| state.event_index_for_pos(height, tx_index, pos))
            .collect::<Vec<u32>>()
    };
    let mut state = monitor.write().await;
    for event_index in to_fail {
        tracing::error!(
            ?height,
            tx_index,
            event_index,
            "single burn does not fit under the EVM block gas limit; marking failed"
        );
        state.mark_burn_failed(height, event_index).await;
    }
}

/// Bumps the retry budget for positions whose chunk-gas estimate hit a
/// transient/infra error (a *failed* estimate, not a successful over-limit
/// verdict). Routing these through `mark_burn_retried` — the same path a failed
/// `processBurns` submission uses — keeps a single RPC hiccup from permanently
/// failing a relayable burn: a persistent failure still terminates once
/// `max_retries` is exhausted, while a transient one is re-estimated and
/// settles on a later poll.
async fn mark_estimate_errored_retry(
    monitor: &RwLock<MonitorState>,
    height: BlockHeight,
    tx_index: u32,
    errored: &[u32],
    max_retries: u32,
) {
    if errored.is_empty() {
        return;
    }
    let to_retry = {
        let state = monitor.read().await;
        errored
            .iter()
            .filter_map(|&pos| state.event_index_for_pos(height, tx_index, pos))
            .collect::<Vec<u32>>()
    };
    let mut state = monitor.write().await;
    for event_index in to_retry {
        state
            .mark_burn_retried(height, event_index, max_retries)
            .await;
    }
}

/// Submits each chunk as its own `processBurns` tx. A chunk's failure only
/// consumes that chunk's retry budget — remaining chunks still attempt
/// submission, because each is independent on-chain.
async fn submit_chunks_with_retry<P: Provider>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<P>,
    cert: &ConfirmedBlockCertificate,
    height: BlockHeight,
    tx_index: u32,
    events_chunks: Vec<Vec<u32>>,
    max_retries: u32,
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
            let to_bump = {
                let state = monitor.read().await;
                events_chunk
                    .iter()
                    .filter_map(|&pos| state.event_index_for_pos(height, tx_index, pos))
                    .collect::<Vec<u32>>()
            };
            let mut state = monitor.write().await;
            for event_index in to_bump {
                state
                    .mark_burn_retried(height, event_index, max_retries)
                    .await;
            }
        }
    }
}

/// Stores BCS cert bytes for every pending burn at `height`.
async fn persist_cert_bytes(
    monitor: &RwLock<MonitorState>,
    height: BlockHeight,
    event_indices: &[u32],
    cert: &ConfirmedBlockCertificate,
) {
    let cert_bytes = bcs::to_bytes(cert).expect("BCS-serialize cert");
    let state = monitor.read().await;
    let Some(db) = state.db() else { return };
    for event_index in event_indices {
        if let Err(error) = db.store_burn_raw(height, *event_index, &cert_bytes).await {
            tracing::warn!(
                ?height,
                ?event_index,
                ?error,
                "Failed to store burn raw bytes"
            );
        }
    }
}

async fn linera_scan_iteration<E: Environment>(
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

    // Burns are now driven by the bridge application, which emits the
    // `BurnEvent` on its own "burns" stream — so scan the bridge app's events,
    // not the wrapped-fungible app's.
    let bridge_app_id = linera_client.bridge_app_id();

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
        let burn_events = find_burn_events(&block.block().body.events, bridge_app_id);
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
        state.advance_linera_cursor(current_height).await;
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
            Err(error) => {
                tracing::warn!(
                    ?height,
                    event_index,
                    ?error,
                    "is_burn_processed query failed"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{classify_slice, SliceVerdict};

    #[test]
    fn fitting_slice_is_a_chunk() {
        assert_eq!(classify_slice(&Ok(true), 1), SliceVerdict::Fits);
        assert_eq!(classify_slice(&Ok(true), 8), SliceVerdict::Fits);
    }

    #[test]
    fn single_over_limit_burn_is_oversized() {
        assert_eq!(classify_slice(&Ok(false), 1), SliceVerdict::Oversized);
    }

    #[test]
    fn multi_burn_non_fit_bisects() {
        assert_eq!(classify_slice(&Ok(false), 2), SliceVerdict::Bisect);
        assert_eq!(classify_slice(&Ok(false), 9), SliceVerdict::Bisect);
    }

    #[test]
    fn estimate_error_routes_to_retry_not_failure() {
        // A failed estimate (transient transport/RPC error, or a revert) must
        // NOT be classified `Oversized` — not even for a single-position slice.
        // Otherwise one node hiccup would permanently fail a burn whose escrow
        // is already gone on Linera. It must route to bounded retry instead.
        assert_eq!(
            classify_slice(&Err(anyhow::anyhow!("connection reset")), 1),
            SliceVerdict::Errored
        );
        assert_eq!(
            classify_slice(&Err(anyhow::anyhow!("execution reverted")), 5),
            SliceVerdict::Errored
        );
    }
}
