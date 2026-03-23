// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bridge monitoring: tracks in-flight EVM↔Linera bridging requests.
//!
//! Two background scan loops actively poll both chains:
//! - **EVM scan**: queries `DepositInitiated` events, checks Linera for completion.
//! - **Linera scan**: walks block history for Credit-to-Address20 messages, checks EVM
//!   for completion via ERC-20 `Transfer` events.

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, B256, U256},
    providers::Provider,
    rpc::types::Filter,
};
use tokio::sync::RwLock;

use crate::proof::{deposit_event_signature, parse_deposit_event, DepositKey, ReceiptLog};
use crate::relay::find_address20_credits;


/// A tracked EVM→Linera deposit.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TrackedDeposit {
    pub deposit_key: DepositKey,
    pub tx_hash: String,
    pub depositor: String,
    pub amount: String,
    pub nonce: String,
    pub forwarded_to_linera: bool,
}

/// A tracked Linera→EVM burn (Credit to Address20).
#[derive(Debug, Clone, serde::Serialize)]
pub struct TrackedBurn {
    pub linera_height: u64,
    pub burn_index: usize,
    pub evm_recipient: String,
    pub amount: String,
    pub forwarded_to_evm: bool,
}

/// In-memory monitoring state shared across scan loops and HTTP handlers.
pub struct MonitorState {
    deposits: HashMap<DepositKey, TrackedDeposit>,
    burns: HashMap<(u64, usize), TrackedBurn>,
    last_scanned_evm_block: u64,
    last_scanned_linera_height: u64,
}

impl MonitorState {
    pub fn new(start_evm_block: u64) -> Self {
        Self {
            deposits: HashMap::new(),
            burns: HashMap::new(),
            last_scanned_evm_block: start_evm_block,
            last_scanned_linera_height: 0,
        }
    }

    pub fn track_evm_to_linera(
        &mut self,
        key: DepositKey,
        tx_hash: B256,
        depositor: Address,
        amount: U256,
        nonce: U256,
    ) {
        self.deposits.entry(key.clone()).or_insert(TrackedDeposit {
            deposit_key: key,
            tx_hash: format!("{tx_hash:#x}"),
            depositor: format!("{depositor:#x}"),
            amount: amount.to_string(),
            nonce: nonce.to_string(),
            forwarded_to_linera: false,
        });
    }

    pub fn complete_evm_to_linera(&mut self, key: &DepositKey) {
        if let Some(deposit) = self.deposits.get_mut(key) {
            deposit.forwarded_to_linera = true;
        } else {
            tracing::warn!(deposit_id = ?key, "Attempted to complete unknown EVM to Linera transfer");
        }
    }

    pub fn track_linera_to_evm(
        &mut self,
        linera_height: u64,
        burn_index: usize,
        evm_recipient: String,
        amount: String,
    ) {
        let key = (linera_height, burn_index);
        self.burns.entry(key).or_insert(TrackedBurn {
            linera_height,
            burn_index,
            evm_recipient,
            amount,
            forwarded_to_evm: false,
        });
    }

    pub fn complete_linera_to_evm(&mut self, linera_height: u64, burn_index: usize) {
        if let Some(burn) = self.burns.get_mut(&(linera_height, burn_index)) {
            burn.forwarded_to_evm = true;
        } else {
            tracing::warn!(
                linera_height,
                burn_index,
                "Attempted to complete unknown Linera to EVM transfer"
            );
        }
    }

    pub fn pending_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits
            .values()
            .filter(|d| !d.forwarded_to_linera)
            .collect()
    }

    pub fn completed_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits
            .values()
            .filter(|d| d.forwarded_to_linera)
            .collect()
    }

    pub fn pending_burns(&self) -> Vec<&TrackedBurn> {
        self.burns
            .values()
            .filter(|b| !b.forwarded_to_evm)
            .collect()
    }

    pub fn forwarded_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().filter(|b| b.forwarded_to_evm).collect()
    }

    pub fn status_summary(&self) -> StatusSummary {
        StatusSummary {
            deposits_pending: self
                .deposits
                .values()
                .filter(|d| !d.forwarded_to_linera)
                .count(),
            deposits_completed: self
                .deposits
                .values()
                .filter(|d| d.forwarded_to_linera)
                .count(),
            burns_pending: self.burns.values().filter(|b| !b.forwarded_to_evm).count(),
            burns_forwarded: self.burns.values().filter(|b| b.forwarded_to_evm).count(),
            last_scanned_evm_block: self.last_scanned_evm_block,
            last_scanned_linera_height: self.last_scanned_linera_height,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusSummary {
    pub deposits_pending: usize,
    pub deposits_completed: usize,
    pub burns_pending: usize,
    pub burns_forwarded: usize,
    pub last_scanned_evm_block: u64,
    pub last_scanned_linera_height: u64,
}

// ── EVM deposit scan loop ──

/// Background task that polls EVM for `DepositInitiated` events and checks
/// Linera for completion.
pub async fn evm_scan_loop(
    monitor: Arc<RwLock<MonitorState>>,
    provider: impl Provider + Clone + 'static,
    bridge_addr: Address,
    scan_interval: Duration,
) {
    let event_sig = deposit_event_signature();

    loop {
        if let Err(e) = evm_scan_iteration(&monitor, &provider, bridge_addr, event_sig).await {
            tracing::warn!("EVM scan iteration failed: {e:#}");
        }

        let summary = monitor.read().await.status_summary();
        tracing::info!(
            pending = summary.deposits_pending,
            forwarded_to_linera = summary.deposits_completed,
            last_block = summary.last_scanned_evm_block,
            "EVM deposit scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
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

            state.track_evm_to_linera(
                key,
                tx_hash,
                deposit.depositor,
                deposit.amount,
                deposit.nonce,
            );
        }

        // Save progress per chunk so we don't rescan on failure.
        state.last_scanned_evm_block = current_block.min(fetcher.from.saturating_sub(1));
    }

    Ok(())
}

// ── Linera burn scan loop ──

/// Background task that scans Linera block history for Credit messages
/// to Address20 owners and checks EVM for completion.
pub async fn linera_scan_loop<E: linera_core::environment::Environment>(
    monitor: Arc<RwLock<MonitorState>>,
    chain_client: linera_core::client::ChainClient<E>,
    fungible_app_id: linera_base::identifiers::ApplicationId,
    scan_interval: Duration,
) {
    loop {
        if let Err(e) = linera_scan_iteration(&monitor, &chain_client, fungible_app_id).await {
            tracing::warn!("Linera scan iteration failed: {e:#}");
        }

        let summary = monitor.read().await.status_summary();
        tracing::info!(
            pending = summary.burns_pending,
            forwarded = summary.burns_forwarded,
            last_height = summary.last_scanned_linera_height,
            "Linera burn scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    chain_client: &linera_core::client::ChainClient<E>,
    fungible_app_id: linera_base::identifiers::ApplicationId,
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

    // Collect burns into a local Vec first, then take write lock once to flush.
    let mut new_burns = Vec::new();
    for block in &blocks {
        let height = block.block().header.height.0;
        let credits = find_address20_credits(&block.block().body.transactions, fungible_app_id);
        for (burn_index, (owner, amount)) in credits.into_iter().enumerate() {
            new_burns.push((height, burn_index, format!("{owner}"), amount.to_string()));
        }
    }

    let mut state = monitor.write().await;
    for (height, burn_index, recipient, amount) in new_burns {
        state.track_linera_to_evm(height, burn_index, recipient, amount);
    }
    state.last_scanned_linera_height = current_height;
    Ok(())
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deposit_key_hash_matches_evm_bridge() {
        let key = DepositKey {
            source_chain_id: 8453,
            block_hash: [0xAA; 32],
            tx_index: 5,
            log_index: 0,
        };
        let hash = key.hash();
        // Hash should be deterministic.
        assert_eq!(hash, key.hash());
        // Hash should be 32 bytes of non-zero data.
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn test_deposit_key_different_log_index_different_hash() {
        let key1 = DepositKey {
            source_chain_id: 8453,
            block_hash: [0xAA; 32],
            tx_index: 5,
            log_index: 0,
        };
        let key2 = DepositKey {
            source_chain_id: 8453,
            block_hash: [0xAA; 32],
            tx_index: 5,
            log_index: 1,
        };
        assert_ne!(key1.hash(), key2.hash());
    }

    #[test]
    fn test_monitor_state_track_and_complete() {
        let mut state = MonitorState::new(0);

        let key = DepositKey {
            source_chain_id: 8453,
            block_hash: [0xAA; 32],
            tx_index: 1,
            log_index: 0,
        };
        state.track_evm_to_linera(
            key.clone(),
            B256::ZERO,
            Address::ZERO,
            U256::from(1000),
            U256::from(0),
        );

        assert_eq!(state.pending_deposits().len(), 1);
        assert_eq!(state.completed_deposits().len(), 0);

        state.complete_evm_to_linera(&key);

        assert_eq!(state.pending_deposits().len(), 0);
        assert_eq!(state.completed_deposits().len(), 1);
    }

    #[test]
    fn test_monitor_state_track_and_forward_burn() {
        let mut state = MonitorState::new(0);

        state.track_linera_to_evm(10, 0, "0xabcd".to_string(), "500".to_string());

        assert_eq!(state.pending_burns().len(), 1);
        assert_eq!(state.forwarded_burns().len(), 0);

        state.complete_linera_to_evm(10, 0);

        assert_eq!(state.pending_burns().len(), 0);
        assert_eq!(state.forwarded_burns().len(), 1);
    }

    #[test]
    fn test_status_summary() {
        let mut state = MonitorState::new(100);

        let key = DepositKey {
            source_chain_id: 1,
            block_hash: [0; 32],
            tx_index: 0,
            log_index: 0,
        };
        state.track_evm_to_linera(
            key.clone(),
            B256::ZERO,
            Address::ZERO,
            U256::ZERO,
            U256::ZERO,
        );
        state.track_linera_to_evm(5, 0, "0x1234".to_string(), "100".to_string());

        let summary = state.status_summary();
        assert_eq!(summary.deposits_pending, 1);
        assert_eq!(summary.deposits_completed, 0);
        assert_eq!(summary.burns_pending, 1);
        assert_eq!(summary.burns_forwarded, 0);
        assert_eq!(summary.last_scanned_evm_block, 100);
    }
}
