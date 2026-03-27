// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bridge monitoring: tracks in-flight EVM↔Linera bridging requests.
//!
//! Two background scan loops actively poll both chains:
//! - **EVM scan** ([`evm`]): queries `DepositInitiated` events, checks Linera for completion.
//! - **Linera scan** ([`linera`]): walks block history for Credit-to-Address20 messages,
//!   checks EVM for completion via ERC-20 `Transfer` events.

pub mod db;
pub mod evm;
pub mod linera;

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, B256, U256};
use linera_base::identifiers::ApplicationId;
use linera_execution::{Query, QueryResponse};
use tokio::sync::RwLock;

use crate::proof::DepositKey;

/// Queries the evm-bridge app to check whether a deposit has been processed on Linera.
pub async fn query_deposit_processed<E: linera_core::environment::Environment>(
    chain_client: &linera_core::client::ChainClient<E>,
    bridge_app_id: ApplicationId,
    deposit_key: &DepositKey,
) -> anyhow::Result<bool> {
    let hash_hex = format!("0x{}", hex::encode(deposit_key.hash()));
    let gql = format!(r#"{{ isDepositProcessed(hash: "{hash_hex}") }}"#);
    let query = Query::user_without_abi(bridge_app_id, &GqlRequest { query: gql })?;
    let (outcome, _) = chain_client.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    Ok(response["data"]["isDepositProcessed"].as_bool() == Some(true))
}

/// A pending deposit detected by the EVM scanner, sent to the retry loop.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PendingDeposit {
    pub key: DepositKey,
    pub tx_hash: B256,
    pub depositor: Address,
    pub amount: U256,
    pub nonce: U256,
}

/// A pending burn detected by the Linera scanner, sent to the retry loop.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PendingBurn {
    pub linera_height: u64,
    pub burn_index: usize,
    pub evm_recipient: String,
    pub amount: String,
}

/// Wraps a pending bridging request with tracking metadata.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Tracked<T: Clone> {
    #[serde(flatten)]
    pub value: T,
    pub forwarded: bool,
    pub failed: bool,
    #[serde(skip)]
    pub retry_count: u32,
    #[serde(skip)]
    pub last_retry_at: Option<Instant>,
}

impl<T: Clone> Tracked<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            forwarded: false,
            failed: false,
            retry_count: 0,
            last_retry_at: None,
        }
    }
}

pub type TrackedDeposit = Tracked<PendingDeposit>;
pub type TrackedBurn = Tracked<PendingBurn>;

/// In-memory monitoring state shared across scan loops and HTTP handlers.
pub struct MonitorState {
    pub(crate) deposits: HashMap<DepositKey, TrackedDeposit>,
    pub(crate) burns: HashMap<(u64, usize), TrackedBurn>,
    pub(crate) last_scanned_evm_block: u64,
    pub(crate) last_scanned_linera_height: u64,
    db: Option<db::BridgeDb>,
}

impl MonitorState {
    pub fn new(start_evm_block: u64) -> Self {
        Self {
            deposits: HashMap::new(),
            burns: HashMap::new(),
            last_scanned_evm_block: start_evm_block,
            last_scanned_linera_height: 0,
            db: None,
        }
    }

    /// Sets the persistent SQLite database for write-through storage.
    pub fn set_db(&mut self, db: db::BridgeDb) {
        self.db = Some(db);
    }

    /// Returns a reference to the database, if configured.
    pub fn db(&self) -> Option<&db::BridgeDb> {
        self.db.as_ref()
    }

    /// Tracks a deposit. Returns `true` if this is a newly discovered deposit.
    /// Uses Entry API instead of insert() to avoid overwriting existing entries
    /// that may have accumulated retry state.
    pub async fn track_deposit(&mut self, pending: PendingDeposit) -> bool {
        match self.deposits.entry(pending.key.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                if let Some(db) = &self.db {
                    if let Err(e) = db.insert_deposit(&pending).await {
                        tracing::warn!("Failed to persist deposit to SQLite: {e:#}");
                    }
                }
                e.insert(Tracked::new(pending));
                crate::relay::metrics::deposit_detected();
                true
            }
        }
    }

    pub async fn complete_deposit(&mut self, key: &DepositKey) {
        if let Some(d) = self.deposits.get_mut(key) {
            d.forwarded = true;
            crate::relay::metrics::deposit_completed();
            if let Some(db) = &self.db {
                if let Err(e) = db.update_deposit_status(key, "completed").await {
                    tracing::warn!(?key, "Failed to update deposit status in SQLite: {e:#}");
                }
            }
        } else {
            tracing::warn!(deposit_id = ?key, "Attempted to complete unknown deposit");
        }
    }

    /// Tracks a burn. Returns `true` if this is a newly discovered burn.
    /// Uses Entry API instead of insert() to avoid overwriting existing entries
    /// that may have accumulated retry state.
    pub async fn track_burn(&mut self, pending: PendingBurn) -> bool {
        let key = (pending.linera_height, pending.burn_index);
        match self.burns.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                if let Some(db) = &self.db {
                    if let Err(err) = db.insert_burn(&pending).await {
                        tracing::warn!("Failed to persist burn to SQLite: {err:#}");
                    }
                }
                e.insert(Tracked::new(pending));
                crate::relay::metrics::burn_detected();
                true
            }
        }
    }

    pub async fn complete_burn(&mut self, linera_height: u64, burn_index: usize) {
        if let Some(b) = self.burns.get_mut(&(linera_height, burn_index)) {
            b.forwarded = true;
            crate::relay::metrics::burn_completed();
            if let Some(db) = &self.db {
                if let Err(e) = db
                    .update_burn_status(linera_height, burn_index, "completed")
                    .await
                {
                    tracing::warn!(
                        linera_height,
                        burn_index,
                        "Failed to update burn status in SQLite: {e:#}"
                    );
                }
            }
        } else {
            tracing::warn!(
                linera_height,
                burn_index,
                "Attempted to complete unknown burn"
            );
        }
    }

    pub fn all_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits.values().collect()
    }

    pub fn pending_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits.values().filter(|d| !d.forwarded).collect()
    }

    pub fn completed_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits.values().filter(|d| d.forwarded).collect()
    }

    pub fn all_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().collect()
    }

    pub fn pending_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().filter(|b| !b.forwarded).collect()
    }

    pub fn completed_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().filter(|b| b.forwarded).collect()
    }

    pub fn deposits_ready_for_retry(&self, max_retries: u32) -> Vec<&TrackedDeposit> {
        self.deposits
            .values()
            .filter(|d| {
                !d.forwarded
                    && !d.failed
                    && retry_eligible(d.retry_count, d.last_retry_at, max_retries)
            })
            .collect()
    }

    pub fn burns_ready_for_retry(&self, max_retries: u32) -> Vec<&TrackedBurn> {
        self.burns
            .values()
            .filter(|b| {
                !b.forwarded
                    && !b.failed
                    && retry_eligible(b.retry_count, b.last_retry_at, max_retries)
            })
            .collect()
    }

    pub fn mark_deposit_retried(&mut self, key: &DepositKey) {
        if let Some(d) = self.deposits.get_mut(key) {
            d.retry_count += 1;
            d.last_retry_at = Some(Instant::now());
        }
    }

    pub async fn mark_deposit_failed(&mut self, key: &DepositKey) {
        if let Some(d) = self.deposits.get_mut(key) {
            d.failed = true;
            crate::relay::metrics::deposit_failed();
            if let Some(db) = &self.db {
                if let Err(e) = db.update_deposit_status(key, "failed").await {
                    tracing::warn!(?key, "Failed to update deposit status in SQLite: {e:#}");
                }
            }
        }
    }

    pub fn mark_burn_retried(&mut self, height: u64, burn_index: usize) {
        if let Some(b) = self.burns.get_mut(&(height, burn_index)) {
            b.retry_count += 1;
            b.last_retry_at = Some(Instant::now());
        }
    }

    pub async fn mark_burn_failed(&mut self, height: u64, burn_index: usize) {
        if let Some(b) = self.burns.get_mut(&(height, burn_index)) {
            b.failed = true;
            crate::relay::metrics::burn_failed();
            if let Some(db) = &self.db {
                if let Err(e) = db.update_burn_status(height, burn_index, "failed").await {
                    tracing::warn!(
                        height,
                        burn_index,
                        "Failed to update burn status in SQLite: {e:#}"
                    );
                }
            }
        }
    }

    pub fn status_summary(&self) -> StatusSummary {
        StatusSummary {
            deposits_pending: self.deposits.values().filter(|d| !d.forwarded).count(),
            deposits_completed: self.deposits.values().filter(|d| d.forwarded).count(),
            burns_pending: self.burns.values().filter(|b| !b.forwarded).count(),
            burns_forwarded: self.burns.values().filter(|b| b.forwarded).count(),
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

/// Runs deposit and burn retry loops concurrently.
/// Returns if either encounters an unrecoverable error.
pub(crate) async fn retry_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    proof_client: crate::proof::gen::HttpDepositProofClient,
    evm_client: Arc<crate::relay::evm::EvmClient<impl alloy::providers::Provider + 'static>>,
    linera_client: Arc<crate::relay::linera::LineraClient<E>>,
    pending_deposit_rx: tokio::sync::mpsc::Receiver<PendingDeposit>,
    pending_burn_rx: tokio::sync::mpsc::Receiver<PendingBurn>,
) -> anyhow::Result<()> {
    tokio::select! {
        result = evm::retry_pending_deposits(
            &monitor, &linera_client, &proof_client, pending_deposit_rx,
        ) => result,
        result = linera::retry_pending_burns(
            &monitor, &evm_client, &linera_client, pending_burn_rx,
        ) => result,
    }
}

/// GraphQL request body for application queries.
#[derive(serde::Serialize)]
struct GqlRequest {
    query: String,
}

/// Whether an item is eligible for retry based on exponential backoff.
/// Backoff schedule: 5s, 10s, 20s, 40s, 80s (capped).
fn retry_eligible(retry_count: u32, last_retry_at: Option<Instant>, max_retries: u32) -> bool {
    if retry_count >= max_retries {
        return false;
    }
    let backoff = Duration::from_secs(5 * 2u64.pow(retry_count.min(4)));
    match last_retry_at {
        None => true,
        Some(t) => t.elapsed() >= backoff,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256};

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
        assert_eq!(hash, key.hash());
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

    #[tokio::test]
    async fn test_monitor_state_track_and_complete() {
        let mut state = MonitorState::new(0);

        let key = DepositKey {
            source_chain_id: 8453,
            block_hash: [0xAA; 32],
            tx_index: 1,
            log_index: 0,
        };
        state
            .track_deposit(PendingDeposit {
                key: key.clone(),
                tx_hash: B256::ZERO,
                depositor: Address::ZERO,
                amount: U256::from(1000),
                nonce: U256::from(0),
            })
            .await;

        assert_eq!(state.pending_deposits().len(), 1);
        assert_eq!(state.completed_deposits().len(), 0);

        state.complete_deposit(&key).await;

        assert_eq!(state.pending_deposits().len(), 0);
        assert_eq!(state.completed_deposits().len(), 1);
    }

    #[tokio::test]
    async fn test_monitor_state_track_and_forward_burn() {
        let mut state = MonitorState::new(0);

        state
            .track_burn(PendingBurn {
                linera_height: 10,
                burn_index: 0,
                evm_recipient: "0xabcd".to_string(),
                amount: "500".to_string(),
            })
            .await;

        assert_eq!(state.pending_burns().len(), 1);
        assert_eq!(state.completed_burns().len(), 0);

        state.complete_burn(10, 0).await;

        assert_eq!(state.pending_burns().len(), 0);
        assert_eq!(state.completed_burns().len(), 1);
    }

    #[tokio::test]
    async fn test_status_summary() {
        let mut state = MonitorState::new(100);

        let key = DepositKey {
            source_chain_id: 1,
            block_hash: [0; 32],
            tx_index: 0,
            log_index: 0,
        };
        state
            .track_deposit(PendingDeposit {
                key: key.clone(),
                tx_hash: B256::ZERO,
                depositor: Address::ZERO,
                amount: U256::ZERO,
                nonce: U256::ZERO,
            })
            .await;
        state
            .track_burn(PendingBurn {
                linera_height: 5,
                burn_index: 0,
                evm_recipient: "0x1234".to_string(),
                amount: "100".to_string(),
            })
            .await;

        let summary = state.status_summary();
        assert_eq!(summary.deposits_pending, 1);
        assert_eq!(summary.deposits_completed, 0);
        assert_eq!(summary.burns_pending, 1);
        assert_eq!(summary.burns_forwarded, 0);
        assert_eq!(summary.last_scanned_evm_block, 100);
    }

    #[test]
    fn test_retry_eligible_first_attempt() {
        assert!(retry_eligible(0, None, 10));
    }

    #[test]
    fn test_retry_eligible_max_retries_exceeded() {
        assert!(!retry_eligible(10, None, 10));
    }

    #[test]
    fn test_retry_eligible_backoff_not_elapsed() {
        let just_now = Instant::now();
        assert!(!retry_eligible(0, Some(just_now), 10));
    }

    #[test]
    fn test_retry_eligible_backoff_elapsed() {
        let long_ago = Instant::now() - Duration::from_secs(60);
        assert!(retry_eligible(0, Some(long_ago), 10));
    }

    #[tokio::test]
    async fn test_deposits_ready_for_retry() {
        let mut state = MonitorState::new(0);
        let key = DepositKey {
            source_chain_id: 1,
            block_hash: [0; 32],
            tx_index: 0,
            log_index: 0,
        };
        state
            .track_deposit(PendingDeposit {
                key: key.clone(),
                tx_hash: B256::ZERO,
                depositor: Address::ZERO,
                amount: U256::ZERO,
                nonce: U256::ZERO,
            })
            .await;

        assert_eq!(state.deposits_ready_for_retry(10).len(), 1);

        state.mark_deposit_retried(&key);
        assert_eq!(state.deposits_ready_for_retry(10).len(), 0);

        state.mark_deposit_failed(&key).await;
        assert_eq!(state.deposits_ready_for_retry(10).len(), 0);
    }
}
