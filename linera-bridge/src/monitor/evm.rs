// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM-side monitoring: scans for `DepositInitiated` events, checks Linera for
//! completion, and retries pending deposits.

use std::{sync::Arc, time::Duration};

use alloy::providers::Provider;
use tokio::sync::RwLock;

use super::{MonitorState, PendingDeposit};
use crate::{
    proof::{parse_deposit_event, DepositKey, ReceiptLog},
    relay::{self, evm::EvmClient, linera::LineraClient},
};

/// Background task that polls EVM for `DepositInitiated` events and checks
/// Linera for completion.
pub async fn evm_scan_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    evm_client: Arc<EvmClient<impl Provider + 'static>>,
    linera_client: Arc<LineraClient<E>>,
    pending_deposit_tx: tokio::sync::mpsc::Sender<PendingDeposit>,
    scan_interval: Duration,
    max_retries: u32,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            evm_scan_iteration(&monitor, &evm_client, &pending_deposit_tx),
            check_deposit_completion(&monitor, &linera_client),
        );

        if let Err(error) = scan_result {
            tracing::warn!(error = ?error, "EVM scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(error = ?error, "Deposit completion check failed");
        }

        // Re-enqueue deposits that are eligible for retry.
        {
            let state = monitor.read().await;
            for d in state.deposits_ready_for_retry(max_retries) {
                if let Err(error) = pending_deposit_tx.try_send(PendingDeposit {
                    key: d.value.key.clone(),
                    tx_hash: d.value.tx_hash,
                    depositor: d.value.depositor,
                    amount: d.value.amount,
                    nonce: d.value.nonce,
                }) {
                    tracing::warn!(?error, "Failed to enqueue deposit for retry");
                }
            }
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

/// Receives pending deposits from the scanner and retries them.
pub(crate) async fn retry_pending_deposits<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
    proof_client: &crate::proof::gen::HttpDepositProofClient,
    mut pending_rx: tokio::sync::mpsc::Receiver<PendingDeposit>,
) -> anyhow::Result<()> {
    use crate::proof::gen::DepositProofClient as _;

    while let Some(pending) = pending_rx.recv().await {
        let tx_hash = pending.tx_hash;
        {
            let mut state = monitor.write().await;
            if let Some(d) = state.deposits.get_mut(&pending.key) {
                if d.forwarded {
                    tracing::trace!(%tx_hash, "Deposit already completed, skipping");
                    continue;
                }
                d.last_retry_at = Some(std::time::Instant::now());
            } else {
                state.track_deposit(pending.clone()).await;
            }
        }

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

    anyhow::bail!("Pending deposit channel closed");
}

async fn evm_scan_iteration(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
    pending_tx: &tokio::sync::mpsc::Sender<PendingDeposit>,
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

        if let Err(error) = pending_tx.try_send(PendingDeposit {
            key,
            tx_hash,
            depositor: deposit.depositor,
            amount: deposit.amount,
            nonce: deposit.nonce,
        }) {
            tracing::warn!(?error, %tx_hash, "Failed to enqueue discovered deposit");
        }
    }

    let mut state = monitor.write().await;
    state.last_scanned_evm_block = current_block;
    crate::relay::metrics::set_last_scanned_evm_block(current_block);

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
