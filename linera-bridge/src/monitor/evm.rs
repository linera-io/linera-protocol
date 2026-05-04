// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM-side monitoring: scans for `DepositInitiated` events, checks Linera for
//! completion, and retries pending deposits.

use std::{sync::Arc, time::Duration};

use alloy::providers::Provider;
use tokio::sync::{Notify, RwLock};

use super::{MonitorState, PendingDeposit};
use crate::{
    proof::{parse_deposit_event, DepositKey, ReceiptLog},
    relay::{self, evm::EvmClient, linera::LineraClient},
};

/// Background task that polls EVM for `DepositInitiated` events and checks
/// Linera for completion. Newly-discovered deposits are written to
/// `MonitorState` (and SQLite) and the consumer is woken via `notify`.
pub async fn evm_scan_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    evm_client: Arc<EvmClient<impl Provider + 'static>>,
    linera_client: Arc<LineraClient<E>>,
    deposit_notify: Arc<Notify>,
    scan_interval: Duration,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            evm_scan_iteration(&monitor, &evm_client, &deposit_notify),
            check_deposit_completion(&monitor, &linera_client),
        );

        if let Err(error) = scan_result {
            tracing::warn!(error = ?error, "EVM scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(error = ?error, "Deposit completion check failed");
        }

        let summary = monitor.read().await.status_summary();
        tracing::trace!(
            pending = summary.deposits_pending,
            completed = summary.deposits_completed,
            last_block = summary.last_scanned_evm_block,
            "EVM deposit scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Drains `MonitorState.deposits` for items ready for retry, processing one at
/// a time. Sleeps on `notify` (woken by the scanner) or on `poll_interval`
/// (whichever comes first) when nothing is ready.
pub(crate) async fn process_pending_deposits<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
    proof_client: &crate::proof::gen::HttpDepositProofClient,
    notify: &Notify,
    poll_interval: Duration,
    max_retries: u32,
) -> anyhow::Result<()> {
    use crate::proof::gen::DepositProofClient as _;

    loop {
        let pending = monitor.read().await.next_deposit_for_retry(max_retries);
        let Some(pending) = pending else {
            tracing::trace!(
                ?poll_interval,
                "EVM deposits processor sleeping until notified or poll interval elapses"
            );
            tokio::select! {
                _ = notify.notified() => {
                    tracing::trace!("EVM deposits processor notified about new pending item");
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
            continue;
        };

        let tx_hash = pending.tx_hash;
        tracing::info!(%tx_hash, "Processing pending deposit...");
        match proof_client.generate_deposit_proof(tx_hash).await {
            Ok(proof) => {
                // Persist raw BCS operation bytes so deposits can be replayed without the relayer.
                if let Some(db) = monitor.read().await.db() {
                    for &log_index in &proof.log_indices {
                        let op = crate::abi::BridgeOperation::ProcessDeposit {
                            block_header_rlp: proof.block_header_rlp.clone(),
                            receipt_rlp: proof.receipt_rlp.clone(),
                            proof_nodes: proof.proof_nodes.clone(),
                            tx_index: proof.tx_index,
                            log_index,
                        };
                        if let Ok(raw) = bcs::to_bytes(&op) {
                            if let Err(e) = db.store_deposit_raw(&pending.key, &raw).await {
                                tracing::warn!(%tx_hash, "Failed to store deposit raw bytes: {e:#}");
                            }
                        }
                    }
                }

                match linera_client.process_deposit(proof).await {
                    Ok(()) => {
                        tracing::info!(%tx_hash, "Deposit processed successfully");
                        relay::update_linera_balance_metric(linera_client).await;
                    }
                    Err(e) => {
                        tracing::warn!(%tx_hash, "Deposit processing failed: {e}");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%tx_hash, "Proof generation failed: {e:#}");
            }
        }

        monitor.write().await.mark_deposit_retried(&pending.key);
    }
}

async fn evm_scan_iteration(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
    notify: &Notify,
) -> anyhow::Result<()> {
    let last_block = monitor.read().await.last_scanned_evm_block;

    let current_block = evm_client.get_block_number().await?;
    if current_block <= last_block {
        return Ok(());
    }

    let logs = evm_client
        .get_deposit_logs(last_block + 1, current_block)
        .await?;
    let bridge_addr = evm_client.bridge_addr();

    let mut tracked_any = false;
    for log in &logs {
        let block_hash = match log.block_hash {
            Some(h) => h,
            None => continue,
        };
        let tx_hash = match log.transaction_hash {
            Some(h) => h,
            None => continue,
        };
        let tx_index = match log.transaction_index {
            Some(i) => i,
            None => continue,
        };
        let log_index = match log.log_index {
            Some(i) => i,
            None => continue,
        };

        let receipt_log = ReceiptLog {
            address: log.address(),
            topics: log.data().topics().to_vec(),
            data: log.data().data.to_vec(),
        };
        let deposit = match parse_deposit_event(&receipt_log, bridge_addr) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(%tx_hash, "Failed to parse DepositInitiated log: {e:#}");
                continue;
            }
        };

        let key = DepositKey {
            source_chain_id: deposit.source_chain_id.to::<u64>(),
            block_hash,
            tx_index,
            log_index,
        };

        let was_new = monitor
            .write()
            .await
            .track_deposit(PendingDeposit {
                key,
                tx_hash,
                depositor: deposit.depositor,
                amount: deposit.amount,
                nonce: deposit.nonce,
            })
            .await;
        tracked_any |= was_new;
    }

    let mut state = monitor.write().await;
    state.last_scanned_evm_block = current_block;
    crate::relay::metrics::set_last_scanned_evm_block(current_block);

    if tracked_any {
        notify.notify_one();
    }
    Ok(())
}

async fn check_deposit_completion<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
) -> anyhow::Result<()> {
    let pending: Vec<DepositKey> = {
        let state = monitor.read().await;
        state
            .pending_deposits()
            .into_iter()
            .map(|d| d.value.key.clone())
            .collect()
    };

    for key in pending {
        if linera_client.query_deposit_processed(&key).await? {
            monitor.write().await.complete_deposit(&key).await;
        }
    }

    Ok(())
}
