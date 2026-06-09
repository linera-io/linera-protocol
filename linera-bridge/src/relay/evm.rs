// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized EVM client for all bridge EVM interactions.

// `processBurns` carries the inclusion-proof components as separate arguments, so its
// `sol!`-generated binding exceeds clippy's argument-count threshold.
#![allow(clippy::too_many_arguments)]

use alloy::{
    primitives::{Address, Bytes, B256, U256},
    providers::Provider,
    rpc::types::{Filter, Log},
    sol,
};
use alloy_sol_types::SolCall;
use anyhow::{Context as _, Result};
use linera_base::data_types::{BlockHeight, Epoch};

use crate::{
    block_proof::{BlockProof, EventInclusionProof},
    proof::deposit_event_signature,
};

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata blockProof, bytes[] calldata eventBcs, uint32[] calldata eventsPerTx) external;
        function processBurns(
            bytes32 blockHash,
            bytes[] calldata eventBcs,
            uint32 txIndex,
            uint32 numTxs,
            uint32 numEventsInTx,
            uint32[] calldata positions,
            bytes32[] calldata innerSiblings,
            bytes32[] calldata outerSiblings
        ) external;
        function lightClient() external view returns (address);
        function token() external view returns (address);
        function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool);
    }

    #[sol(rpc)]
    interface IERC20Decimals {
        function decimals() external view returns (uint8);
    }
}

sol! {
    function addCommittee(
        bytes calldata blockProof,
        bytes[] calldata transactionBcs,
        bytes calldata committeeBlob,
        bytes[] calldata validators
    ) external;

    function registerBlock(bytes calldata blockProof) external returns (bytes32);

    function currentEpoch() external view returns (uint32);
}

/// Maximum block range per `eth_getLogs` query.
const MAX_LOG_BLOCK_RANGE: u64 = 10_000;

/// Arguments for a `processBurns` call: a chunk of events within one transaction plus the
/// inclusion proof binding them to a registered block's `events_hash`.
struct ProcessBurnsArgs {
    block_hash: B256,
    event_bcs: Vec<Bytes>,
    tx_index: u32,
    num_txs: u32,
    num_events_in_tx: u32,
    positions: Vec<u32>,
    inner_siblings: Vec<B256>,
    outer_siblings: Vec<B256>,
}

/// Centralized client for all EVM interactions. Safe to share via `Arc`.
pub struct EvmClient<P> {
    provider: P,
    bridge_addr: Address,
    relayer_addr: Address,
    deposit_event_sig: B256,
    light_client_addr: tokio::sync::OnceCell<Address>,
}

impl<P: Provider> EvmClient<P> {
    pub fn new(
        provider: P,
        bridge_addr: Address,
        relayer_addr: Address,
        light_client_override: Option<Address>,
    ) -> Self {
        let light_client_addr = tokio::sync::OnceCell::new();
        if let Some(addr) = light_client_override {
            light_client_addr.set(addr).ok();
        }
        Self {
            provider,
            bridge_addr,
            relayer_addr,
            deposit_event_sig: deposit_event_signature(),
            light_client_addr,
        }
    }

    pub fn bridge_addr(&self) -> Address {
        self.bridge_addr
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        Ok(self.provider.get_block_number().await?)
    }

    /// Returns the relayer's ETH balance in wei.
    pub async fn get_relayer_balance(&self) -> Result<U256> {
        Ok(self.provider.get_balance(self.relayer_addr).await?)
    }

    /// Returns the decimals of the ERC-20 bridged by the configured `FungibleBridge`.
    pub async fn token_decimals(&self) -> Result<u8> {
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        let token_addr = bridge
            .token()
            .call()
            .await
            .context("failed to query FungibleBridge.token()")?;
        let token = IERC20Decimals::new(token_addr, &self.provider);
        let decimals = token
            .decimals()
            .call()
            .await
            .context("failed to query ERC-20 decimals()")?;
        Ok(decimals)
    }

    /// Queries `DepositInitiated` events in chunked ranges.
    pub async fn get_deposit_logs(&self, from: u64, to: u64) -> Result<Vec<Log>> {
        let filter_base = Filter::new()
            .address(self.bridge_addr)
            .event_signature(self.deposit_event_sig);

        let mut all_logs = Vec::new();
        let mut cursor = from;
        while cursor <= to {
            let chunk_end = (cursor + MAX_LOG_BLOCK_RANGE - 1).min(to);
            let filter = filter_base.clone().from_block(cursor).to_block(chunk_end);
            let logs = self.provider.get_logs(&filter).await?;
            all_logs.extend(logs);
            cursor = chunk_end + 1;
        }
        Ok(all_logs)
    }

    /// Returns whether the FungibleBridge has already released the burn
    /// at `(height, event_index)` — i.e. whether the corresponding
    /// `_onBlock` loop iteration ran to completion in some prior `addBlock`
    /// transaction. `event_index` is the underlying Linera `Event.index`.
    /// Per-burn (not per-block, not per-recipient).
    pub async fn is_burn_processed(&self, height: BlockHeight, event_index: u32) -> Result<bool> {
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        Ok(bridge.isBurnProcessed(height.0, event_index).call().await?)
    }

    /// BCS-serialize and forward a certified block to FungibleBridge on EVM.
    /// The arguments for an `addBlock` call from a certificate: the lean block proof, the per-event
    /// BCS encodings (flattened across transactions), and the number of events in each transaction.
    fn add_block_args(
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> (Bytes, Vec<Bytes>, Vec<u32>) {
        let proof = Bytes::from(
            bcs::to_bytes(&BlockProof::from_certificate(cert)).expect("BCS-serialize block proof"),
        );
        let events = &cert.block().body.events;
        let event_bcs = events
            .iter()
            .flatten()
            .map(|event| Bytes::from(bcs::to_bytes(event).expect("BCS-serialize event")))
            .collect();
        let events_per_tx = events
            .iter()
            .map(|tx_events| u32::try_from(tx_events.len()).expect("event count exceeds u32"))
            .collect();
        (proof, event_bcs, events_per_tx)
    }

    pub async fn forward_cert(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> Result<()> {
        let (proof, event_bcs, events_per_tx) = Self::add_block_args(cert);

        tracing::info!(
            size = proof.len(),
            events = event_bcs.len(),
            "Calling addBlock on FungibleBridge..."
        );

        let bridge_contract = IFungibleBridge::new(self.bridge_addr, &self.provider);
        let pending_tx = bridge_contract
            .addBlock(proof, event_bcs, events_per_tx)
            .send()
            .await
            .context("addBlock send failed")?;
        let receipt = pending_tx
            .get_receipt()
            .await
            .context("addBlock receipt failed")?;

        tracing::info!(
            tx = ?receipt.transaction_hash,
            "addBlock transaction confirmed"
        );
        Ok(())
    }

    /// Dry-runs `addBlock(cert)` against the EVM. `Ok(_)` means the call
    /// fits under the node's current block gas limit; any error covers
    /// both a real contract revert and the over-block-gas-limit case
    /// (some nodes return identical empty-data reverts for both). The
    /// caller treats any error as "route to chunked `processBurns`".
    pub async fn estimate_add_block_gas(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> alloy::contract::Result<u64> {
        let (proof, event_bcs, events_per_tx) = Self::add_block_args(cert);
        tracing::trace!(events = event_bcs.len(), "Estimating gas for addBlock");
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        bridge
            .addBlock(proof, event_bcs, events_per_tx)
            .estimate_gas()
            .await
    }

    /// Registers a block on the LightClient from its header and signatures alone, so its events can
    /// later be settled in chunks. Must succeed before `estimate_process_burns_gas` or
    /// `process_burns`, both of which prove events against the registered block.
    pub async fn register_block(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> Result<()> {
        let lc_addr = self.get_light_client_address().await?;
        let proof_bytes = bcs::to_bytes(&BlockProof::from_certificate(cert))
            .context("failed to BCS-serialize block proof")?;
        let call = registerBlockCall {
            blockProof: proof_bytes.into(),
        };
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(lc_addr)
            .input(call.abi_encode().into());
        let receipt = self
            .provider
            .send_transaction(tx)
            .await
            .context("registerBlock send failed")?
            .get_receipt()
            .await
            .context("registerBlock receipt failed")?;
        tracing::info!(tx = ?receipt.transaction_hash, "registerBlock transaction confirmed");
        Ok(())
    }

    /// Builds the `processBurns` arguments for the events at `positions` within transaction
    /// `tx_index` of `cert`: the chunk's BCS-encoded events plus the inclusion proof binding them to
    /// the block's registered `events_hash`. The block must already be registered.
    fn process_burns_args(
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        tx_index: u32,
        positions: &[u32],
    ) -> ProcessBurnsArgs {
        let events = &cert.block().body.events;
        let proof = EventInclusionProof::new(events, tx_index as usize, positions);
        let event_bcs = positions
            .iter()
            .map(|p| {
                Bytes::from(
                    bcs::to_bytes(&events[tx_index as usize][*p as usize])
                        .expect("BCS-serialize event"),
                )
            })
            .collect();
        let to_b256 = |h: &linera_base::crypto::CryptoHash| B256::from(*h.as_bytes());
        ProcessBurnsArgs {
            block_hash: B256::from(*cert.hash().as_bytes()),
            event_bcs,
            tx_index,
            num_txs: proof.num_txs,
            num_events_in_tx: proof.num_events_in_tx,
            positions: positions.to_vec(),
            inner_siblings: proof.inner_siblings.iter().map(to_b256).collect(),
            outer_siblings: proof.outer_siblings.iter().map(to_b256).collect(),
        }
    }

    /// Same as `estimate_add_block_gas` but for the chunked `processBurns(cert, tx_index,
    /// positions_in_tx)` path.
    pub async fn estimate_process_burns_gas(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> alloy::contract::Result<u64> {
        let args = Self::process_burns_args(cert, tx_index, positions_in_tx);
        tracing::trace!(
            tx_index,
            count = positions_in_tx.len(),
            "Estimating gas for processBurns"
        );
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        bridge
            .processBurns(
                args.block_hash,
                args.event_bcs,
                args.tx_index,
                args.num_txs,
                args.num_events_in_tx,
                args.positions,
                args.inner_siblings,
                args.outer_siblings,
            )
            .estimate_gas()
            .await
    }

    /// Submits `processBurns(cert, tx_index, positions_in_tx)` and waits for the receipt. Used after
    /// `split_to_fit` returns a chunk.
    pub async fn process_burns(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> Result<()> {
        let args = Self::process_burns_args(cert, tx_index, positions_in_tx);
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        tracing::info!(
            tx_index,
            count = positions_in_tx.len(),
            "Calling processBurns on FungibleBridge..."
        );
        let pending_tx = bridge
            .processBurns(
                args.block_hash,
                args.event_bcs,
                args.tx_index,
                args.num_txs,
                args.num_events_in_tx,
                args.positions,
                args.inner_siblings,
                args.outer_siblings,
            )
            .send()
            .await
            .context("processBurns send failed")?;
        let receipt = pending_tx
            .get_receipt()
            .await
            .context("processBurns receipt failed")?;
        tracing::info!(tx = ?receipt.transaction_hash, "processBurns transaction confirmed");
        Ok(())
    }

    /// Discovers the LightClient contract address from the FungibleBridge.
    pub async fn get_light_client_address(&self) -> Result<Address> {
        self.light_client_addr
            .get_or_try_init(|| async {
                let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
                let addr = bridge
                    .lightClient()
                    .call()
                    .await
                    .context("failed to query FungibleBridge.lightClient()")?;
                Ok(addr)
            })
            .await
            .copied()
    }

    /// Queries the LightClient's current epoch.
    pub async fn get_current_epoch(&self) -> Result<Epoch> {
        let lc_addr = self.get_light_client_address().await?;
        let call = currentEpochCall {};
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(lc_addr)
            .input(call.abi_encode().into());
        let result = self
            .provider
            .call(tx)
            .await
            .context("failed to query LightClient.currentEpoch()")?;
        let epoch = currentEpochCall::abi_decode_returns(&result)
            .context("failed to decode currentEpoch response")?;
        Ok(Epoch(epoch))
    }

    /// Relays a committee update to the LightClient contract.
    pub async fn add_committee(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        committee_blob: &[u8],
        validator_keys: Vec<Vec<u8>>,
    ) -> Result<alloy::primitives::TxHash> {
        let lc_addr = self.get_light_client_address().await?;
        let proof_bytes = bcs::to_bytes(&BlockProof::from_certificate(cert))
            .context("failed to BCS-serialize block proof")?;
        let transaction_bcs = cert
            .block()
            .body
            .transactions
            .iter()
            .map(|txn| Bytes::from(bcs::to_bytes(txn).expect("BCS-serialize transaction")))
            .collect();
        let call = addCommitteeCall {
            blockProof: Bytes::copy_from_slice(&proof_bytes),
            transactionBcs: transaction_bcs,
            committeeBlob: Bytes::copy_from_slice(committee_blob),
            validators: validator_keys.into_iter().map(Bytes::from).collect(),
        };
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(lc_addr)
            .input(call.abi_encode().into());
        let receipt = self
            .provider
            .send_transaction(tx)
            .await
            .context("addCommittee send failed")?
            .get_receipt()
            .await
            .context("addCommittee receipt failed")?;
        Ok(receipt.transaction_hash)
    }
}
