// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized EVM client for all bridge EVM interactions.

use alloy::{
    primitives::{Address, Bytes, B256, U256},
    providers::Provider,
    rpc::types::{Filter, Log},
};
use anyhow::{Context as _, Result};
use linera_base::data_types::{BlockHeight, Epoch};
use linera_chain::types::ConfirmedBlockCertificate;

use crate::{
    block_proof::{BlockProof, ProvenEvents},
    contracts::{IFungibleBridge, ILightClient, IERC20},
    proof::deposit_event_signature,
};

/// Maximum block range per `eth_getLogs` query.
const MAX_LOG_BLOCK_RANGE: u64 = 10_000;

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
        let token = IERC20::new(token_addr, &self.provider);
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
    /// release ran to completion in some prior `processBurns` transaction.
    /// `event_index` is the underlying Linera `Event.index`.
    /// Per-burn (not per-block, not per-recipient).
    pub async fn is_burn_processed(&self, height: BlockHeight, event_index: u32) -> Result<bool> {
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        Ok(bridge.isBurnProcessed(height.0, event_index).call().await?)
    }

    /// Registers a block on the LightClient from its header and signatures alone, so its events can
    /// later be settled in chunks. Must succeed before `estimate_process_burns_gas` or
    /// `process_burns`, both of which prove events against the registered block.
    pub async fn register_block(&self, cert: &ConfirmedBlockCertificate) -> Result<()> {
        let lc_addr = self.get_light_client_address().await?;
        let proof_bytes = bcs::to_bytes(&BlockProof::from_certificate(cert))
            .context("failed to BCS-serialize block proof")?;
        let light_client = ILightClient::new(lc_addr, &self.provider);
        let receipt = light_client
            .registerBlock(proof_bytes.into())
            .send()
            .await
            .context("registerBlock send failed")?
            .get_receipt()
            .await
            .context("registerBlock receipt failed")?;
        tracing::info!(tx = ?receipt.transaction_hash, "registerBlock transaction confirmed");
        Ok(())
    }

    /// Dry-runs the chunked `processBurns(cert, tx_index, positions_in_tx)` call and returns its
    /// gas estimate. `Ok(_)` means the chunk fits under the node's block gas limit; any error
    /// covers both a real revert and the over-block-gas-limit case.
    pub async fn estimate_process_burns_gas(
        &self,
        cert: &ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> alloy::contract::Result<u64> {
        let args = ProvenEvents::new(cert, tx_index, positions_in_tx);
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
                args.siblings,
            )
            .estimate_gas()
            .await
    }

    /// Submits `processBurns(cert, tx_index, positions_in_tx)` and waits for the receipt. Used after
    /// `split_to_fit` returns a chunk.
    pub async fn process_burns(
        &self,
        cert: &ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> Result<()> {
        let args = ProvenEvents::new(cert, tx_index, positions_in_tx);
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        tracing::debug!(
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
                args.siblings,
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
        let light_client = ILightClient::new(lc_addr, &self.provider);
        let epoch = light_client
            .currentEpoch()
            .call()
            .await
            .context("failed to query LightClient.currentEpoch()")?;
        Ok(Epoch(epoch))
    }

    /// Queries the admin-chain height that installed `epoch`'s committee, as
    /// recorded by the LightClient. Returns height 0 for the genesis committee
    /// or an unknown epoch, so callers degrade to a full scan.
    pub async fn committee_height(&self, epoch: Epoch) -> Result<BlockHeight> {
        let lc_addr = self.get_light_client_address().await?;
        let light_client = ILightClient::new(lc_addr, &self.provider);
        let height = light_client
            .committeeHeight(epoch.0)
            .call()
            .await
            .context("failed to query LightClient.committeeHeight()")?;
        Ok(BlockHeight(height))
    }

    /// Relays a committee update to the LightClient contract. Registers the admin block (verifying
    /// its quorum on-chain), then proves the single epoch event's inclusion against it by hash and
    /// submits `addCommittee` — the same register-then-prove path `processBurns` uses for burns.
    pub async fn add_committee(
        &self,
        cert: &ConfirmedBlockCertificate,
        committee_blob: &[u8],
    ) -> Result<alloy::primitives::TxHash> {
        let lc_addr = self.get_light_client_address().await?;
        // Register the admin block so `addCommittee` can reference it by hash, exactly like burns.
        self.register_block(cert).await?;
        // Prove the single committee epoch event against the registered block — the same
        // `ProvenEvents` witness burns use; `committee_blob` is the only extra argument.
        let committee_event = super::committee::find_committee_event(cert)
            .context("block has no committee epoch event")?;
        let tx_index = u32::try_from(committee_event.tx_index).expect("tx index exceeds u32");
        let position = u32::try_from(committee_event.position).expect("event position exceeds u32");
        let proven = ProvenEvents::new(cert, tx_index, &[position]);
        let light_client = ILightClient::new(lc_addr, &self.provider);
        let receipt = light_client
            .addCommittee(
                proven.block_hash,
                proven.event_bcs,
                proven.tx_index,
                proven.num_txs,
                proven.num_events_in_tx,
                proven.positions,
                proven.siblings,
                Bytes::copy_from_slice(committee_blob),
            )
            .send()
            .await
            .context("addCommittee send failed")?
            .get_receipt()
            .await
            .context("addCommittee receipt failed")?;
        Ok(receipt.transaction_hash)
    }
}
