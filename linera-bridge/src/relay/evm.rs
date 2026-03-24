// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM interaction: ABI bindings and block forwarding.

use alloy::{primitives::Address, sol};
use anyhow::{Context as _, Result};

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

/// Must match `evm_bridge::BridgeOperation` variant-for-variant for BCS compatibility.
#[derive(serde::Serialize)]
pub(crate) enum BridgeOperation {
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
}

/// BCS-serialize and forward a certified block to FungibleBridge on EVM.
pub(crate) async fn forward_cert_to_evm(
    cert: &impl serde::Serialize,
    bridge_addr: Address,
    provider: &impl alloy::providers::Provider,
) -> Result<()> {
    let cert_bytes = bcs::to_bytes(cert).context("failed to BCS-serialize certificate")?;

    tracing::info!(
        size = cert_bytes.len(),
        "Calling addBlock on FungibleBridge..."
    );

    let bridge_contract = IFungibleBridge::new(bridge_addr, provider);
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
