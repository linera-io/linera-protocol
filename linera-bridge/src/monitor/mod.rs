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
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, B256, U256};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight},
    identifiers::ApplicationId,
};
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
///
/// `event_index` is the underlying Linera `Event.index` — the position of
/// the burn event within its stream ("burns" on the configured fungible
/// application). Used both as the dedup key off-chain and as the value
/// the `FungibleBridge.isBurnProcessed(height, eventIndex)` view consumes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PendingBurn {
    pub height: BlockHeight,
    /// Hash of the Linera block that produced this burn. Lets the relayer
    /// fetch the certificate via a direct `linera_client.read_certificate`
    /// call instead of walking the chain backwards from head.
    pub block_hash: CryptoHash,
    /// Position of this burn's transaction within `body.events`.
    /// Used by `processBurns(cert, tx_index, ...)`.
    pub tx_index: u32,
    /// Position of this burn within `body.events[tx_index]`.
    /// Used by `processBurns(cert, tx_index, [event_pos_in_tx, ...])`.
    pub event_pos_in_tx: u32,
    /// `Event.index` — sequential position of this burn within its stream
    /// (the "burns" stream of the configured fungible app on the configured
    /// Linera chain). Unique for the lifetime of that stream, so unique
    /// across all heights within the relayer's scope. Off-chain and on-chain
    /// dedup key.
    pub event_index: u32,
    pub evm_recipient: Address,
    pub amount: Amount,
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

/// One height's slice of `pending_burns_by_height_and_tx`. The two views
/// (`event_indices` and `by_tx`) describe the same set of burns under one
/// retry-filter snapshot.
///
/// `Ord` is keyed solely on `height`, so a `BTreeSet<PendingBurnsAtHeight>`
/// is naturally height-sorted and structurally rejects a second entry for
/// the same height (which never happens in practice — there is at most one
/// entry per height).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBurnsAtHeight {
    pub height: BlockHeight,
    /// Hash of the Linera block at `height` — lets `process_pending_burns`
    /// pull the certificate via a direct `read_certificate` call.
    pub block_hash: CryptoHash,
    /// Stream indices (`Event.index`) of every pending burn at this height,
    /// sorted ascending. Used for retry accounting and cert persistence.
    pub event_indices: Vec<u32>,
    /// Pending burns grouped by `tx_index`, in ascending `tx_index` order;
    /// the `Vec<u32>` inside each entry is the sorted `event_pos_in_tx`
    /// positions for that tx — input to the chunked `processBurns`
    /// fallback when `addBlock` would not fit.
    pub by_tx: Vec<(u32, Vec<u32>)>,
}

impl Ord for PendingBurnsAtHeight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.height.cmp(&other.height)
    }
}

impl PartialOrd for PendingBurnsAtHeight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// In-memory monitoring state shared across scan loops and HTTP handlers.
pub struct MonitorState {
    pub(crate) deposits: HashMap<DepositKey, TrackedDeposit>,
    pub(crate) burns: HashMap<(BlockHeight, u32), TrackedBurn>,
    pub(crate) last_scanned_evm_block: u64,
    pub(crate) last_scanned_linera_height: BlockHeight,
    db: Option<db::BridgeDb>,
}

impl MonitorState {
    pub fn new(start_evm_block: u64) -> Self {
        Self {
            deposits: HashMap::new(),
            burns: HashMap::new(),
            last_scanned_evm_block: start_evm_block,
            last_scanned_linera_height: BlockHeight(0),
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
                    if let Err(error) = db.insert_deposit(&pending).await {
                        tracing::warn!(?error, "Failed to persist deposit to SQLite");
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
                if let Err(error) = db.update_deposit_status(key, "completed").await {
                    tracing::warn!(?key, ?error, "Failed to update deposit status in SQLite");
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
        let key = (pending.height, pending.event_index);
        match self.burns.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                if let Some(db) = &self.db {
                    if let Err(error) = db.insert_burn(&pending).await {
                        tracing::warn!(?error, "Failed to persist burn to SQLite");
                    }
                }
                e.insert(Tracked::new(pending));
                crate::relay::metrics::burn_detected();
                true
            }
        }
    }

    pub async fn complete_burn(&mut self, height: BlockHeight, event_index: u32) {
        if let Some(b) = self.burns.get_mut(&(height, event_index)) {
            b.forwarded = true;
            crate::relay::metrics::burn_completed();
            if let Some(db) = &self.db {
                if let Err(error) = db
                    .update_burn_status(height, event_index, "completed")
                    .await
                {
                    tracing::warn!(
                        ?height,
                        event_index,
                        ?error,
                        "Failed to update burn status in SQLite"
                    );
                }
            }
        } else {
            tracing::warn!(?height, event_index, "Attempted to complete unknown burn");
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

    /// Returns one `PendingBurnsAtHeight` per height with pending burns,
    /// in ascending height order. Burns are skipped if they are `failed`
    /// (e.g. permanently oversized) or have exceeded `max_retries`.
    pub fn pending_burns_by_height_and_tx(
        &self,
        max_retries: u32,
    ) -> BTreeSet<PendingBurnsAtHeight> {
        struct HeightAccum {
            block_hash: CryptoHash,
            by_tx: BTreeMap<u32, Vec<u32>>,
            event_indices: Vec<u32>,
        }
        let mut tree: BTreeMap<BlockHeight, HeightAccum> = BTreeMap::new();
        for tracked in self.pending_burns() {
            // Failed burns (e.g. permanently oversized) and burns past the
            // retry budget are no longer eligible for processing.
            if tracked.failed || tracked.retry_count >= max_retries {
                continue;
            }
            // All burns at a given height share the same block (cert) hash,
            // so the first one populates it and the rest just append.
            let entry = tree.entry(tracked.value.height).or_insert(HeightAccum {
                block_hash: tracked.value.block_hash,
                by_tx: BTreeMap::new(),
                event_indices: Vec::new(),
            });
            entry
                .by_tx
                .entry(tracked.value.tx_index)
                .or_default()
                .push(tracked.value.event_pos_in_tx);
            entry.event_indices.push(tracked.value.event_index);
        }
        tree.into_iter()
            .map(|(h, mut accum)| {
                for positions in accum.by_tx.values_mut() {
                    positions.sort_unstable();
                }
                accum.event_indices.sort_unstable();
                PendingBurnsAtHeight {
                    height: h,
                    block_hash: accum.block_hash,
                    event_indices: accum.event_indices,
                    by_tx: accum.by_tx.into_iter().collect(),
                }
            })
            .collect()
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

    /// Returns one pending deposit whose backoff has elapsed, cloned so the
    /// caller can drop the read lock before doing slow work. Used by the
    /// processing loop in place of an mpsc receiver.
    pub fn next_deposit_for_retry(&self, max_retries: u32) -> Option<PendingDeposit> {
        self.deposits
            .values()
            .find(|d| {
                !d.forwarded
                    && !d.failed
                    && retry_eligible(d.retry_count, d.last_retry_at, max_retries)
            })
            .map(|d| d.value.clone())
    }

    /// Returns one pending burn whose backoff has elapsed, cloned so the
    /// caller can drop the read lock before doing slow work.
    pub fn next_burn_for_retry(&self, max_retries: u32) -> Option<PendingBurn> {
        self.burns
            .values()
            .find(|b| {
                !b.forwarded
                    && !b.failed
                    && retry_eligible(b.retry_count, b.last_retry_at, max_retries)
            })
            .map(|b| b.value.clone())
    }

    /// Repopulates `deposits` and `burns` from the SQLite WAL. Called once at
    /// relay startup so that requests in flight at the previous shutdown are
    /// recovered instead of silently orphaned.
    pub async fn load_from_db(&mut self) -> anyhow::Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        let deposits = db.load_pending_deposits().await?;
        let burns = db.load_pending_burns().await?;
        let n_deposits = deposits.len();
        let n_burns = burns.len();
        for d in deposits {
            self.deposits.insert(d.key.clone(), Tracked::new(d));
        }
        for b in burns {
            self.burns
                .insert((b.height, b.event_index), Tracked::new(b));
        }
        tracing::info!(
            deposits = n_deposits,
            burns = n_burns,
            "Recovered pending bridge requests from SQLite WAL"
        );
        Ok(())
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
                if let Err(error) = db.update_deposit_status(key, "failed").await {
                    tracing::warn!(?key, ?error, "Failed to update deposit status in SQLite");
                }
            }
        }
    }

    /// Looks up the `event_index` of the pending burn at
    /// `(height, tx_index, pos_in_tx)`. Used by `process_pending_burns`
    /// to map per-chunk positions back to the stream-index keys that
    /// `mark_burn_retried` / `mark_burn_failed` expect.
    pub fn event_index_for_pos(
        &self,
        height: BlockHeight,
        tx_index: u32,
        pos_in_tx: u32,
    ) -> Option<u32> {
        self.burns.values().find_map(|b| {
            (b.value.height == height
                && b.value.tx_index == tx_index
                && b.value.event_pos_in_tx == pos_in_tx)
                .then_some(b.value.event_index)
        })
    }

    pub fn mark_burn_retried(&mut self, height: BlockHeight, event_index: u32) {
        if let Some(b) = self.burns.get_mut(&(height, event_index)) {
            b.retry_count += 1;
            b.last_retry_at = Some(Instant::now());
        }
    }

    pub async fn mark_burn_failed(&mut self, height: BlockHeight, event_index: u32) {
        if let Some(b) = self.burns.get_mut(&(height, event_index)) {
            b.failed = true;
            crate::relay::metrics::burn_failed();
            if let Some(db) = &self.db {
                if let Err(error) = db.update_burn_status(height, event_index, "failed").await {
                    tracing::warn!(
                        ?height,
                        event_index,
                        ?error,
                        "Failed to update burn status in SQLite"
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
    pub last_scanned_linera_height: BlockHeight,
}

/// Runs the deposit and burn processing loops concurrently. Each loop reads
/// pending work from `MonitorState` (the SQLite WAL is the source of truth)
/// and is woken either by a `Notify` signal from the corresponding scanner or
/// by a periodic poll for items whose retry backoff has just elapsed.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn retry_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    proof_client: crate::proof::gen::HttpDepositProofClient,
    evm_client: Arc<crate::relay::evm::EvmClient<impl alloy::providers::Provider + 'static>>,
    linera_client: Arc<crate::relay::linera::LineraClient<E>>,
    deposit_notify: Arc<tokio::sync::Notify>,
    burn_notify: Arc<tokio::sync::Notify>,
    poll_interval: Duration,
    max_retries: u32,
) -> anyhow::Result<()> {
    tokio::select! {
        result = evm::process_pending_deposits(
            &monitor, &linera_client, &proof_client, &deposit_notify, poll_interval, max_retries,
        ) => result,
        result = linera::process_pending_burns(
            &monitor, &evm_client, &linera_client, &burn_notify, poll_interval, max_retries,
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
    use linera_base::data_types::{Amount, BlockHeight};

    use super::*;

    #[test]
    fn test_deposit_key_hash_matches_evm_bridge() {
        let key = DepositKey {
            source_chain_id: 8453,
            block_hash: B256::from([0xAA; 32]),
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
            block_hash: B256::from([0xAA; 32]),
            tx_index: 5,
            log_index: 0,
        };
        let key2 = DepositKey {
            source_chain_id: 8453,
            block_hash: B256::from([0xAA; 32]),
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
            block_hash: B256::from([0xAA; 32]),
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
                height: BlockHeight(10),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 0,
                evm_recipient: Address::from([0xab; 20]),
                amount: Amount::from_attos(500),
            })
            .await;

        assert_eq!(state.pending_burns().len(), 1);
        assert_eq!(state.completed_burns().len(), 0);

        state.complete_burn(BlockHeight(10), 0).await;

        assert_eq!(state.pending_burns().len(), 0);
        assert_eq!(state.completed_burns().len(), 1);
    }

    #[tokio::test]
    async fn test_status_summary() {
        let mut state = MonitorState::new(100);

        let key = DepositKey {
            source_chain_id: 1,
            block_hash: B256::ZERO,
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
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 0,
                evm_recipient: Address::from([0x12; 20]),
                amount: Amount::from_attos(100),
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
            block_hash: B256::ZERO,
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

    /// `next_deposit_for_retry` is the API the processor uses to drain pending
    /// work from the WAL. Verifies it returns the tracked deposit while the
    /// item is fresh, returns `None` once backoff is set, and skips items
    /// that have been completed.
    #[tokio::test]
    async fn next_deposit_for_retry_returns_pending_then_respects_backoff() {
        let mut state = MonitorState::new(0);
        let key = DepositKey {
            source_chain_id: 1,
            block_hash: B256::ZERO,
            tx_index: 0,
            log_index: 0,
        };
        state
            .track_deposit(PendingDeposit {
                key: key.clone(),
                tx_hash: B256::ZERO,
                depositor: Address::ZERO,
                amount: U256::from(1_000_000u64),
                nonce: U256::ZERO,
            })
            .await;

        let next = state.next_deposit_for_retry(10);
        assert!(matches!(next, Some(p) if p.key == key));

        state.mark_deposit_retried(&key);
        assert!(state.next_deposit_for_retry(10).is_none());

        state.complete_deposit(&key).await;
        assert!(state.next_deposit_for_retry(10).is_none());
    }

    /// Regression for the silent-drop bug: a saturated mpsc channel used to
    /// drop newly-scanned deposits, and the scanner's watermark advance made
    /// the loss unrecoverable. With storage as the queue, the scanner writes
    /// directly to `MonitorState` so the processor will always see the item
    /// on its next poll regardless of backpressure.
    #[tokio::test]
    async fn scanner_writes_directly_to_state_so_processor_sees_them() {
        let mut state = MonitorState::new(100);

        // Simulate the scanner discovering many deposits in a single iteration.
        for i in 0..128u64 {
            state
                .track_deposit(PendingDeposit {
                    key: DepositKey {
                        source_chain_id: 1,
                        block_hash: B256::ZERO,
                        tx_index: i,
                        log_index: 0,
                    },
                    tx_hash: B256::ZERO,
                    depositor: Address::ZERO,
                    amount: U256::ZERO,
                    nonce: U256::from(i),
                })
                .await;
        }
        state.last_scanned_evm_block = 200;

        // Every deposit is recoverable by the processor — there is no "channel
        // full" code path that could lose them.
        assert_eq!(state.deposits_ready_for_retry(10).len(), 128);
        assert!(state.next_deposit_for_retry(10).is_some());
    }

    /// Bug #2 fix: pending requests recovered from SQLite at startup.
    #[tokio::test]
    async fn load_from_db_recovers_pending_items_on_startup() {
        let db = db::BridgeDb::open_in_memory().await.unwrap();
        let key = DepositKey {
            source_chain_id: 1,
            block_hash: B256::from([0xAA; 32]),
            tx_index: 7,
            log_index: 0,
        };
        db.insert_deposit(&PendingDeposit {
            key: key.clone(),
            tx_hash: B256::from([0xBB; 32]),
            depositor: Address::from([0xCC; 20]),
            amount: U256::from(42u64),
            nonce: U256::from(3u64),
        })
        .await
        .unwrap();
        db.insert_burn(&PendingBurn {
            height: BlockHeight(99),
            block_hash: CryptoHash::from([0u8; 32]),
            tx_index: 0,
            event_pos_in_tx: 0,
            event_index: 2,
            evm_recipient: Address::from([0xDD; 20]),
            amount: Amount::from_attos(7),
        })
        .await
        .unwrap();

        let mut state = MonitorState::new(0);
        state.set_db(db);
        state.load_from_db().await.unwrap();

        assert_eq!(state.pending_deposits().len(), 1);
        assert_eq!(state.pending_burns().len(), 1);
        let recovered = state.next_deposit_for_retry(10).unwrap();
        assert_eq!(recovered.key, key);
        assert_eq!(recovered.nonce, U256::from(3u64));
    }

    /// Same as the deposit version, but for the burn pipeline.
    #[tokio::test]
    async fn next_burn_for_retry_returns_pending_then_respects_backoff() {
        let mut state = MonitorState::new(0);
        let height = BlockHeight(101);
        state
            .track_burn(PendingBurn {
                height,
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 0,
                evm_recipient: Address::from([0xab; 20]),
                amount: Amount::from_attos(500),
            })
            .await;

        assert!(state.next_burn_for_retry(10).is_some());
        state.mark_burn_retried(height, 0);
        assert!(state.next_burn_for_retry(10).is_none());

        // Once forwarded, the item is no longer offered for retry.
        state.complete_burn(height, 0).await;
        assert!(state.next_burn_for_retry(10).is_none());
    }

    #[tokio::test]
    async fn pending_burns_by_height_and_tx_groups_and_sorts() {
        let mut state = MonitorState::new(0);
        let burns = [
            // Two burns at height 5: tx 0 has positions 1 then 0 (out of
            // order so the helper's sort is tested); tx 1 has one burn.
            PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 1,
                event_index: 11,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            },
            PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 10,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            },
            PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 1,
                event_pos_in_tx: 0,
                event_index: 12,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            },
            // One burn at a later height.
            PendingBurn {
                height: BlockHeight(7),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 0,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            },
        ];
        for b in burns {
            state.track_burn(b).await;
        }

        let groups = state.pending_burns_by_height_and_tx(/* max_retries */ 10);
        // Same retry filter applies to both views — event_indices is the
        // sorted list of `event_index` values at each height, used by
        // `process_pending_burns` for retry accounting and cert persistence.
        let expected: BTreeSet<PendingBurnsAtHeight> = [
            PendingBurnsAtHeight {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                event_indices: vec![10u32, 11, 12],
                by_tx: vec![(0u32, vec![0u32, 1]), (1u32, vec![0u32])],
            },
            PendingBurnsAtHeight {
                height: BlockHeight(7),
                block_hash: CryptoHash::from([0u8; 32]),
                event_indices: vec![0u32],
                by_tx: vec![(0u32, vec![0u32])],
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(groups, expected);
    }

    #[tokio::test]
    async fn pending_burns_by_height_and_tx_excludes_failed_burns() {
        // After a burn is marked `failed` (e.g. oversized in the chunked
        // `processBurns` path), it must not reappear in subsequent retry
        // snapshots — otherwise the chunking loop would keep re-discovering
        // it as oversized and burn estimate-RPC budget on every pass.
        let mut state = MonitorState::new(0);
        state
            .track_burn(PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 0,
                event_index: 10,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            })
            .await;
        state
            .track_burn(PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 0,
                event_pos_in_tx: 1,
                event_index: 11,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            })
            .await;

        state.mark_burn_failed(BlockHeight(5), 10).await;

        let groups = state.pending_burns_by_height_and_tx(/* max_retries */ 10);
        let expected: BTreeSet<PendingBurnsAtHeight> = [PendingBurnsAtHeight {
            height: BlockHeight(5),
            block_hash: CryptoHash::from([0u8; 32]),
            event_indices: vec![11u32],
            by_tx: vec![(0u32, vec![1u32])],
        }]
        .into_iter()
        .collect();
        assert_eq!(groups, expected);
    }

    #[tokio::test]
    async fn event_index_for_pos_matches_tracked_burn() {
        let mut state = MonitorState::new(0);
        state
            .track_burn(PendingBurn {
                height: BlockHeight(5),
                block_hash: CryptoHash::from([0u8; 32]),
                tx_index: 2,
                event_pos_in_tx: 1,
                event_index: 42,
                evm_recipient: Address::ZERO,
                amount: Amount::ZERO,
            })
            .await;

        assert_eq!(state.event_index_for_pos(BlockHeight(5), 2, 1), Some(42));
        assert_eq!(state.event_index_for_pos(BlockHeight(5), 2, 0), None);
        assert_eq!(state.event_index_for_pos(BlockHeight(5), 0, 1), None);
        assert_eq!(state.event_index_for_pos(BlockHeight(6), 2, 1), None);
    }
}
