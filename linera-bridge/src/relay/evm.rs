// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized EVM client for all bridge EVM interactions.

use alloy::{
    primitives::{Address, Bytes, B256, U256},
    providers::Provider,
    rpc::types::{Filter, Log},
    sol,
};
use alloy_sol_types::SolCall;
use anyhow::{Context as _, Result};
use linera_base::data_types::{BlockHeight, Epoch};
<<<<<<< HEAD

use crate::{
    evm::light_client::{addCommitteeCall, committeeHeightCall, currentEpochCall},
=======
use linera_chain::types::ConfirmedBlockCertificate;

use crate::{
    block_proof::{BlockProof, ProvenEvents},
    contracts::{IFungibleBridge, ILightClient, IERC20},
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
    proof::deposit_event_signature,
};

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
        function processBurns(bytes calldata data, uint32 txIndex, uint32[] calldata eventPositionsInTx) external;
        function lightClient() external view returns (address);
        function token() external view returns (address);
        function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool);
    }

    #[sol(rpc)]
    interface IERC20Decimals {
        function decimals() external view returns (uint8);
    }
}

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
    /// Creates a new EVM client, optionally pinning the LightClient address.
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

    /// Returns the FungibleBridge contract address.
    pub fn bridge_addr(&self) -> Address {
        self.bridge_addr
    }

    /// Returns the EVM chain's latest block number.
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
    pub async fn forward_cert(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> Result<()> {
        let cert_bytes = bcs::to_bytes(cert).context("failed to BCS-serialize certificate")?;

        tracing::info!(
            size = cert_bytes.len(),
            "Calling addBlock on FungibleBridge..."
        );

        let bridge_contract = IFungibleBridge::new(self.bridge_addr, &self.provider);
        let pending_tx = bridge_contract
            .addBlock(cert_bytes.into())
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

<<<<<<< HEAD
    /// Dry-runs `addBlock(cert)` against the EVM to estimate the gas it
    /// would consume. `Ok(g)` means the call would fit under the node's
    /// current block gas limit (the value is the estimate); a gas-exceeded
    /// RPC error indicates the call would not fit. Other RPC errors bubble
    /// up. Classification is done by `relay::settlement::estimate_fits`.
    pub async fn estimate_add_block_gas(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
    ) -> alloy::contract::Result<u64> {
        let cert_bytes = bcs::to_bytes(cert).expect("BCS-serialize cert");
        let cert_size = cert_bytes.len();
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        let estimate = bridge.addBlock(cert_bytes.into()).estimate_gas().await;
        tracing::debug!(?estimate, cert_size, "addBlock gas estimate");
        estimate
    }

    /// Same as `estimate_add_block_gas` but for
    /// `processBurns(cert, tx_index, positions_in_tx)`.
=======
    /// Dry-runs the chunked `processBurns(cert, tx_index, positions_in_tx)` call and returns its
    /// gas estimate. `Ok(_)` means the chunk fits under the node's block gas limit; any error
    /// covers both a real revert and the over-block-gas-limit case.
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
    pub async fn estimate_process_burns_gas(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> alloy::contract::Result<u64> {
<<<<<<< HEAD
        let cert_bytes = bcs::to_bytes(cert).expect("BCS-serialize cert");
        let cert_size = cert_bytes.len();
        let count = positions_in_tx.len();
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        let estimate = bridge
            .processBurns(cert_bytes.into(), tx_index, positions_in_tx.to_vec())
=======
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
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
            .estimate_gas()
            .await;
        tracing::debug!(
            tx_index,
            count,
            ?estimate,
            cert_size,
            "processBurns gas estimate"
        );
        estimate
    }

    /// Submits `processBurns(cert, tx_index, positions_in_tx)` and waits
    /// for the receipt. Used after `split_to_fit` returns a chunk.
    pub async fn process_burns(
        &self,
        cert: &linera_chain::types::ConfirmedBlockCertificate,
        tx_index: u32,
        positions_in_tx: &[u32],
    ) -> Result<()> {
<<<<<<< HEAD
        let cert_bytes = bcs::to_bytes(cert).expect("BCS-serialize cert");
=======
        let args = ProvenEvents::new(cert, tx_index, positions_in_tx);
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
        let bridge = IFungibleBridge::new(self.bridge_addr, &self.provider);
        tracing::debug!(
            tx_index,
            count = positions_in_tx.len(),
            size = cert_bytes.len(),
            "Calling processBurns on FungibleBridge..."
        );
        let pending_tx = bridge
<<<<<<< HEAD
            .processBurns(cert_bytes.into(), tx_index, positions_in_tx.to_vec())
=======
            .processBurns(
                args.block_hash,
                args.event_bcs,
                args.tx_index,
                args.num_txs,
                args.num_events_in_tx,
                args.positions,
                args.siblings,
            )
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
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

    /// Queries the admin-chain height of the block that created the committee at
    /// `epoch`. Returns 0 for the genesis committee or an unknown epoch, which is
    /// a safe scan origin (the relayer then reconciles from height 0).
    pub async fn committee_height(&self, epoch: Epoch) -> Result<BlockHeight> {
        let lc_addr = self.get_light_client_address().await?;
        let call = committeeHeightCall { epoch: epoch.0 };
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(lc_addr)
            .input(call.abi_encode().into());
        let result = self
            .provider
            .call(tx)
            .await
            .context("failed to query LightClient.committeeHeight()")?;
        let height = committeeHeightCall::abi_decode_returns(&result)
            .context("failed to decode committeeHeight response")?;
        Ok(BlockHeight(height))
    }

    /// Relays a committee update to the LightClient contract. Registers the admin block (verifying
    /// its quorum on-chain), then proves the single epoch event's inclusion against it by hash and
    /// submits `addCommittee` — the same register-then-prove path `processBurns` uses for burns.
    pub async fn add_committee(
        &self,
        certificate_bytes: &[u8],
        committee_blob: &[u8],
        validator_keys: Vec<Vec<u8>>,
    ) -> Result<alloy::primitives::TxHash> {
        let lc_addr = self.get_light_client_address().await?;
<<<<<<< HEAD
        let call = addCommitteeCall {
            data: Bytes::copy_from_slice(certificate_bytes),
            committeeBlob: Bytes::copy_from_slice(committee_blob),
            validators: validator_keys.into_iter().map(Bytes::from).collect(),
        };
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(lc_addr)
            .input(call.abi_encode().into());
        let receipt = self
            .provider
            .send_transaction(tx)
=======
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
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
            .await
            .context("addCommittee send failed")?
            .get_receipt()
            .await
            .context("addCommittee receipt failed")?;
        Ok(receipt.transaction_hash)
    }
}
