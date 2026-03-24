// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM-side monitoring: scans for `DepositInitiated` events, checks Linera for
//! completion, and retries pending deposits.

use std::{sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, B256},
    providers::Provider,
    rpc::types::Filter,
};
use linera_base::identifiers::ApplicationId;
use tokio::sync::RwLock;

use super::{query_deposit_processed, MonitorState, PendingDeposit};
use crate::proof::{deposit_event_signature, parse_deposit_event, DepositKey, ReceiptLog};

/// Background task that polls EVM for `DepositInitiated` events and checks
/// Linera for completion.
pub async fn evm_scan_loop<E: linera_core::environment::Environment>(
    monitor: Arc<RwLock<MonitorState>>,
    provider: impl Provider + Clone + 'static,
    bridge_addr: Address,
    chain_client: linera_core::client::ChainClient<E>,
    bridge_app_id: ApplicationId,
    pending_deposit_tx: tokio::sync::mpsc::Sender<PendingDeposit>,
    scan_interval: Duration,
) {
    let event_sig = deposit_event_signature();

    loop {
        let (scan_result, completion_result) = tokio::join!(
            evm_scan_iteration(
                &monitor,
                &provider,
                bridge_addr,
                event_sig,
                &pending_deposit_tx
            ),
            check_deposit_completion(&monitor, &chain_client, bridge_app_id),
        );

        if let Err(error) = scan_result {
            tracing::warn!(error = ?error, "EVM scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(error = ?error, "Deposit completion check failed");
        }

        let summary = monitor.read().await.status_summary();
        tracing::debug!(
            pending = summary.deposits_pending,
            completed = summary.deposits_completed,
            last_block = summary.last_scanned_evm_block,
            "EVM deposit scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Receives pending deposits from the scanner and retries them.
pub(crate) async fn retry_pending_deposits(
    monitor: &RwLock<MonitorState>,
    deposit_tx: &tokio::sync::mpsc::Sender<crate::relay::DepositRequest>,
    proof_client: &crate::proof::gen::HttpDepositProofClient,
    mut pending_rx: tokio::sync::mpsc::Receiver<PendingDeposit>,
    max_retries: u32,
) -> anyhow::Result<()> {
    use crate::proof::gen::DepositProofClient as _;

    while let Some(pending) = pending_rx.recv().await {
        // Track in MonitorState for HTTP visibility + completion detection.
        let tx_hash = pending.tx_hash;
        monitor.write().await.track_deposit(pending.clone());
        tracing::info!(%tx_hash, "Retrying pending deposit...");
        match proof_client.generate_deposit_proof(tx_hash).await {
            Ok(proof) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                let req = crate::relay::DepositRequest {
                    proof,
                    response: resp_tx,
                };
                if deposit_tx.send(req).await.is_err() {
                    anyhow::bail!("Deposit channel closed during retry");
                }
                match resp_rx.await {
                    Ok(Ok(())) => {
                        tracing::info!(%tx_hash, "Deposit retry succeeded");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(%tx_hash, "Deposit retry failed: {e}");
                    }
                    Err(_) => {
                        tracing::warn!(%tx_hash, "Deposit retry response channel closed");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%tx_hash, "Deposit proof generation failed during retry: {e:#}");
            }
        }

        let mut state = monitor.write().await;
        if state
            .deposits
            .get(&pending.key)
            .is_some_and(|d| d.retry_count + 1 >= max_retries)
        {
            state.mark_deposit_failed(&pending.key);
            tracing::error!(%tx_hash, "Deposit marked as failed after max retries");
        } else {
            state.mark_deposit_retried(&pending.key);
        }
    }

    anyhow::bail!("Pending deposit channel closed");
}

/// Maximum block range per `eth_getLogs` query. Most RPC providers reject
/// ranges that are too large or return too many results.
const MAX_LOG_BLOCK_RANGE: u64 = 10_000;

/// Chunked log fetcher that queries `eth_getLogs` in bounded block ranges.
///
/// Each call to [`next_batch`] returns the next chunk of logs and advances
/// the cursor. Returns `Ok(None)` when the entire range has been scanned.
struct ChunkedLogFetcher<'a, P> {
    provider: &'a P,
    filter_base: Filter,
    from: u64,
    to: u64,
}

impl<'a, P: Provider> ChunkedLogFetcher<'a, P> {
    fn new(provider: &'a P, filter_base: Filter, from: u64, to: u64) -> Self {
        Self {
            provider,
            filter_base,
            from,
            to,
        }
    }

    async fn next_batch(&mut self) -> anyhow::Result<Option<Vec<alloy::rpc::types::Log>>> {
        if self.from > self.to {
            return Ok(None);
        }

        let chunk_end = (self.from + MAX_LOG_BLOCK_RANGE - 1).min(self.to);
        let filter = self
            .filter_base
            .clone()
            .from_block(self.from)
            .to_block(chunk_end);

        let logs = self.provider.get_logs(&filter).await?;
        self.from = chunk_end + 1;
        Ok(Some(logs))
    }
}

async fn evm_scan_iteration(
    monitor: &RwLock<MonitorState>,
    provider: &impl Provider,
    bridge_addr: Address,
    event_sig: B256,
    pending_tx: &tokio::sync::mpsc::Sender<PendingDeposit>,
) -> anyhow::Result<()> {
    let last_block = monitor.read().await.last_scanned_evm_block;

    let current_block = provider.get_block_number().await?;
    if current_block <= last_block {
        return Ok(());
    }

    let filter_base = Filter::new()
        .address(bridge_addr)
        .event_signature(event_sig);

    let mut fetcher = ChunkedLogFetcher::new(provider, filter_base, last_block + 1, current_block);

    while let Some(logs) = fetcher.next_batch().await? {
        let mut state = monitor.write().await;
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
                block_hash: block_hash.0,
                tx_index,
                log_index,
            };

            let _ = pending_tx.try_send(PendingDeposit {
                key,
                tx_hash,
                depositor: deposit.depositor,
                amount: deposit.amount,
                nonce: deposit.nonce,
            });
        }

        // Save progress per chunk so we don't rescan on failure.
        state.last_scanned_evm_block = current_block.min(fetcher.from.saturating_sub(1));
        crate::relay::metrics::set_last_scanned_evm_block(state.last_scanned_evm_block);
    }

    Ok(())
}

/// For each pending deposit, query the evm-bridge app to check if it has been processed.
async fn check_deposit_completion<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    chain_client: &linera_core::client::ChainClient<E>,
    bridge_app_id: ApplicationId,
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
        if query_deposit_processed(chain_client, bridge_app_id, &key).await? {
            monitor.write().await.complete_deposit(&key);
        }
    }

    Ok(())
}
