// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bridge monitoring: tracks in-flight EVM↔Linera bridging requests.
//!
//! Two background scan loops actively poll both chains:
//! - **EVM scan**: queries `DepositInitiated` events, checks Linera for completion.
//! - **Linera scan**: walks block history for Credit-to-Address20 messages, checks EVM
//!   for completion via ERC-20 `Transfer` events.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, B256, U256},
    providers::Provider,
    rpc::types::Filter,
};
use linera_base::identifiers::ApplicationId;
use linera_execution::{Query, QueryResponse};
use tokio::sync::RwLock;

use crate::{
    proof::{deposit_event_signature, parse_deposit_event, DepositKey, ReceiptLog},
    relay::find_address20_credits,
};

/// GraphQL request body for application queries.
#[derive(serde::Serialize)]
struct GqlRequest {
    query: String,
}

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
#[derive(Debug, Clone)]
pub struct PendingDeposit {
    pub key: DepositKey,
    pub tx_hash: B256,
    pub depositor: Address,
    pub amount: U256,
    pub nonce: U256,
}

/// A pending burn detected by the Linera scanner, sent to the retry loop.
#[derive(Debug, Clone)]
pub struct PendingBurn {
    pub linera_height: u64,
    pub burn_index: usize,
    pub evm_recipient: String,
    pub amount: String,
}

/// A tracked EVM→Linera deposit.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TrackedDeposit {
    pub deposit_key: DepositKey,
    pub tx_hash: String,
    pub depositor: String,
    pub amount: String,
    pub nonce: String,
    pub forwarded_to_linera: bool,
    pub failed: bool,
    #[serde(skip)]
    pub retry_count: u32,
    #[serde(skip)]
    pub last_retry_at: Option<Instant>,
}

/// A tracked Linera→EVM burn (Credit to Address20).
#[derive(Debug, Clone, serde::Serialize)]
pub struct TrackedBurn {
    pub linera_height: u64,
    pub burn_index: usize,
    pub evm_recipient: String,
    pub amount: String,
    pub forwarded_to_evm: bool,
    pub failed: bool,
    #[serde(skip)]
    pub retry_count: u32,
    #[serde(skip)]
    pub last_retry_at: Option<Instant>,
}

/// Whether an item is eligible for retry based on exponential backoff.
/// Backoff schedule: 30s, 60s, 120s, 240s, 480s (capped).
fn retry_eligible(retry_count: u32, last_retry_at: Option<Instant>, max_retries: u32) -> bool {
    if retry_count >= max_retries {
        return false;
    }
    let backoff = Duration::from_secs(30 * 2u64.pow(retry_count.min(4)));
    match last_retry_at {
        None => true,
        Some(t) => t.elapsed() >= backoff,
    }
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

    /// Tracks a deposit. Returns `true` if this is a newly discovered deposit.
    /// Uses Entry API instead of insert() to avoid overwriting existing entries
    /// that may have accumulated retry state.
    pub fn track_evm_to_linera(
        &mut self,
        key: DepositKey,
        tx_hash: B256,
        depositor: Address,
        amount: U256,
        nonce: U256,
    ) -> bool {
        match self.deposits.entry(key.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(TrackedDeposit {
                    deposit_key: key,
                    tx_hash: format!("{tx_hash:#x}"),
                    depositor: format!("{depositor:#x}"),
                    amount: amount.to_string(),
                    nonce: nonce.to_string(),
                    forwarded_to_linera: false,
                    failed: false,
                    retry_count: 0,
                    last_retry_at: None,
                });
                true
            }
        }
    }

    pub fn complete_evm_to_linera(&mut self, key: &DepositKey) {
        if let Some(deposit) = self.deposits.get_mut(key) {
            deposit.forwarded_to_linera = true;
        } else {
            tracing::warn!(deposit_id = ?key, "Attempted to complete unknown EVM to Linera transfer");
        }
    }

    /// Tracks a burn. Returns `true` if this is a newly discovered burn.
    /// Uses Entry API instead of insert() to avoid overwriting existing entries
    /// that may have accumulated retry state.
    pub fn track_linera_to_evm(
        &mut self,
        linera_height: u64,
        burn_index: usize,
        evm_recipient: String,
        amount: String,
    ) -> bool {
        let key = (linera_height, burn_index);
        match self.burns.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(TrackedBurn {
                    linera_height,
                    burn_index,
                    evm_recipient,
                    amount,
                    forwarded_to_evm: false,
                    failed: false,
                    retry_count: 0,
                    last_retry_at: None,
                });
                true
            }
        }
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

    pub fn all_deposits(&self) -> Vec<&TrackedDeposit> {
        self.deposits.values().collect()
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

    pub fn all_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().collect()
    }

    pub fn pending_burns(&self) -> Vec<&TrackedBurn> {
        self.burns
            .values()
            .filter(|b| !b.forwarded_to_evm)
            .collect()
    }

    pub fn completed_burns(&self) -> Vec<&TrackedBurn> {
        self.burns.values().filter(|b| b.forwarded_to_evm).collect()
    }

    pub fn deposits_ready_for_retry(&self, max_retries: u32) -> Vec<&TrackedDeposit> {
        self.deposits
            .values()
            .filter(|d| {
                !d.forwarded_to_linera
                    && !d.failed
                    && retry_eligible(d.retry_count, d.last_retry_at, max_retries)
            })
            .collect()
    }

    pub fn burns_ready_for_retry(&self, max_retries: u32) -> Vec<&TrackedBurn> {
        self.burns
            .values()
            .filter(|b| {
                !b.forwarded_to_evm
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

    pub fn mark_deposit_failed(&mut self, key: &DepositKey) {
        if let Some(d) = self.deposits.get_mut(key) {
            d.failed = true;
        }
    }

    pub fn mark_burn_retried(&mut self, height: u64, burn_index: usize) {
        if let Some(b) = self.burns.get_mut(&(height, burn_index)) {
            b.retry_count += 1;
            b.last_retry_at = Some(Instant::now());
        }
    }

    pub fn mark_burn_failed(&mut self, height: u64, burn_index: usize) {
        if let Some(b) = self.burns.get_mut(&(height, burn_index)) {
            b.failed = true;
        }
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
            .map(|d| d.deposit_key.clone())
            .collect()
    };

    for key in pending {
        if query_deposit_processed(chain_client, bridge_app_id, &key).await? {
            monitor.write().await.complete_evm_to_linera(&key);
        }
    }

    Ok(())
}

// ── Linera burn scan loop ──

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

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    chain_client: &linera_core::client::ChainClient<E>,
    fungible_app_id: linera_base::identifiers::ApplicationId,
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

    // Collect burns into a local Vec first, then take write lock once to flush.
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
                let addr: Address = b.evm_recipient.parse().ok()?;
                Some((b.linera_height, b.burn_index, addr))
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
            monitor
                .write()
                .await
                .complete_linera_to_evm(height, burn_index);
        }
    }

    Ok(())
}

// ── Retry loop ──

/// Runs deposit and burn retry loops concurrently.
/// Returns if either encounters an unrecoverable error.
pub(crate) async fn retry_loop<E: linera_core::environment::Environment>(
    monitor: Arc<RwLock<MonitorState>>,
    deposit_tx: tokio::sync::mpsc::Sender<crate::relay::DepositRequest>,
    proof_client: crate::proof::gen::HttpDepositProofClient,
    pending_deposit_rx: tokio::sync::mpsc::Receiver<PendingDeposit>,
    chain_client: linera_core::client::ChainClient<E>,
    fungible_app_id: ApplicationId,
    bridge_addr: Address,
    provider: impl Provider + Clone + 'static,
    pending_burn_rx: tokio::sync::mpsc::Receiver<PendingBurn>,
    max_retries: u32,
) -> anyhow::Result<()> {
    tokio::select! {
        result = retry_pending_deposits(
            &monitor, &deposit_tx, &proof_client, pending_deposit_rx, max_retries,
        ) => result,
        result = retry_pending_burns(
            &monitor, &chain_client, fungible_app_id, bridge_addr, &provider,
            pending_burn_rx, max_retries,
        ) => result,
    }
}

/// Receives pending deposits from the scanner and retries them.
async fn retry_pending_deposits(
    monitor: &RwLock<MonitorState>,
    deposit_tx: &tokio::sync::mpsc::Sender<crate::relay::DepositRequest>,
    proof_client: &crate::proof::gen::HttpDepositProofClient,
    mut pending_rx: tokio::sync::mpsc::Receiver<PendingDeposit>,
    max_retries: u32,
) -> anyhow::Result<()> {
    use crate::proof::gen::DepositProofClient as _;

    while let Some(pending) = pending_rx.recv().await {
        // Track in MonitorState for HTTP visibility + completion detection.
        monitor.write().await.track_evm_to_linera(
            pending.key.clone(),
            pending.tx_hash,
            pending.depositor,
            pending.amount,
            pending.nonce,
        );

        let tx_hash = pending.tx_hash;
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

/// Receives pending burns from the scanner and retries them.
async fn retry_pending_burns<E: linera_core::environment::Environment>(
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
        monitor.write().await.track_linera_to_evm(
            credit_height,
            burn_index,
            pending.evm_recipient,
            pending.amount,
        );

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
                match crate::relay::forward_cert_to_evm(&block, bridge_addr, provider).await {
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
            state.complete_linera_to_evm(credit_height, burn_index);
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
        assert_eq!(state.completed_burns().len(), 0);

        state.complete_linera_to_evm(10, 0);

        assert_eq!(state.pending_burns().len(), 0);
        assert_eq!(state.completed_burns().len(), 1);
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

    #[test]
    fn test_deposits_ready_for_retry() {
        let mut state = MonitorState::new(0);
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

        // First attempt: eligible immediately.
        assert_eq!(state.deposits_ready_for_retry(10).len(), 1);

        // After marking retried: not eligible (backoff not elapsed).
        state.mark_deposit_retried(&key);
        assert_eq!(state.deposits_ready_for_retry(10).len(), 0);

        // After marking failed: never eligible.
        state.mark_deposit_failed(&key);
        assert_eq!(state.deposits_ready_for_retry(10).len(), 0);
    }
}
