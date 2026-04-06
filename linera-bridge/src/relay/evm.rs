// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized EVM client for all bridge EVM interactions.

use alloy::{
    primitives::{Address, B256, U256},
    providers::Provider,
    rpc::types::{Filter, Log},
    sol,
};
use anyhow::{Context as _, Result};

use crate::proof::deposit_event_signature;

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

/// Must match `evm_bridge::BridgeOperation` variant-for-variant for BCS compatibility.
#[derive(serde::Serialize)]
pub(crate) enum BridgeOperation {
    RegisterFungibleApp {
        app_id: linera_base::identifiers::ApplicationId,
    },
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
    VerifyBlockHash {
        block_hash: [u8; 32],
    },
}

/// Maximum block range per `eth_getLogs` query.
const MAX_LOG_BLOCK_RANGE: u64 = 10_000;

/// Centralized client for all EVM interactions. Safe to share via `Arc`.
pub struct EvmClient<P> {
    provider: P,
    bridge_addr: Address,
    relayer_addr: Address,
    deposit_event_sig: B256,
}

impl<P: Provider> EvmClient<P> {
    pub fn new(provider: P, bridge_addr: Address, relayer_addr: Address) -> Self {
        Self {
            provider,
            bridge_addr,
            relayer_addr,
            deposit_event_sig: deposit_event_signature(),
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

    /// Queries ERC-20 Transfer events from the bridge to a recipient.
    pub async fn get_transfer_logs(&self, recipient: Address) -> Result<Vec<Log>> {
        let transfer_sig = alloy::primitives::keccak256("Transfer(address,address,uint256)");
        let filter = Filter::new()
            .address(self.bridge_addr)
            .event_signature(transfer_sig)
            .topic2(B256::left_padding_from(recipient.as_slice()));
        Ok(self.provider.get_logs(&filter).await?)
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
}
