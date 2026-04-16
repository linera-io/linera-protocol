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

use crate::proof::deposit_event_signature;

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
        function lightClient() external view returns (address);
    }
}

sol! {
    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        bytes[] calldata validators
    ) external;

    function currentEpoch() external view returns (uint32);
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
    pub async fn get_current_epoch(&self) -> Result<u32> {
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
        Ok(epoch)
    }

    /// Relays a committee update to the LightClient contract.
    pub async fn add_committee(
        &self,
        certificate_bytes: &[u8],
        committee_blob: &[u8],
        validator_keys: Vec<Vec<u8>>,
    ) -> Result<alloy::primitives::TxHash> {
        let lc_addr = self.get_light_client_address().await?;
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
            .await
            .context("addCommittee send failed")?
            .get_receipt()
            .await
            .context("addCommittee receipt failed")?;
        Ok(receipt.transaction_hash)
    }
}
