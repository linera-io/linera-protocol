// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Off-chain deposit proof client for EVM→Linera deposits.
//!
//! The [`DepositProofClient`] trait defines the interface for generating the data
//! needed for a `ProcessDeposit` operation on the evm-bridge app. The concrete
//! [`HttpDepositProofClient`] implementation queries a standard EVM JSON-RPC endpoint
//! (including Base L2).
//!
//! The proof generation pipeline:
//! 1. Fetches the transaction receipt to identify the block and tx index
//! 2. Fetches the full block header (for RLP encoding)
//! 3. Fetches all block receipts and builds the receipts MPT trie locally
//! 4. Generates a Merkle proof for the target receipt
//!
//! All RPC methods used (`eth_getTransactionReceipt`, `eth_getBlockByHash`,
//! `eth_getBlockReceipts`) are standard Ethereum JSON-RPC and available on Base L2.

use alloy::{
    eips::eip2718::Encodable2718,
    primitives::B256,
    providers::{Provider, ProviderBuilder},
};
use alloy_rlp::Encodable;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;

/// All data needed to submit a `ProcessDeposit` operation to the evm-bridge app.
#[derive(Debug, Clone)]
pub struct DepositProof {
    /// RLP-encoded block header (keccak256 of this = block hash).
    pub block_header_rlp: Vec<u8>,
    /// Canonical receipt bytes (EIP-2718 encoded, as stored in the receipts trie).
    pub receipt_rlp: Vec<u8>,
    /// MPT proof nodes for the receipt at `tx_index`.
    pub proof_nodes: Vec<Vec<u8>>,
    /// Transaction index within the block.
    pub tx_index: u64,
    /// Log index within the receipt.
    pub log_index: u64,
}

/// Client interface for generating deposit proofs.
///
/// Implementations query EVM chain data and construct the cryptographic
/// proof needed for a `ProcessDeposit` operation on the evm-bridge app.
#[async_trait]
pub trait DepositProofClient {
    /// Generate a complete deposit proof from a transaction hash.
    ///
    /// The implementation fetches the receipt, locates the `DepositInitiated`
    /// event log automatically, and constructs the MPT proof.
    async fn generate_deposit_proof(&self, tx_hash: B256) -> Result<DepositProof>;
}

/// HTTP-based deposit proof client that queries an EVM JSON-RPC endpoint.
///
/// Works with any standard Ethereum-compatible RPC (including Base L2).
pub struct HttpDepositProofClient {
    provider: Box<dyn Provider>,
}

impl HttpDepositProofClient {
    /// Create a new client connected to the given RPC URL.
    pub fn new(rpc_url: &str) -> Result<Self> {
        let url = rpc_url
            .parse()
            .with_context(|| format!("invalid RPC URL: {rpc_url}"))?;
        let provider = ProviderBuilder::new().connect_http(url);
        Ok(Self {
            provider: Box::new(provider),
        })
    }
}

#[async_trait]
impl DepositProofClient for HttpDepositProofClient {
    async fn generate_deposit_proof(&self, tx_hash: B256) -> Result<DepositProof> {
        let event_sig = crate::proof::deposit_event_signature();

        // 1. Get transaction receipt → block hash, tx index, log index
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await?
            .with_context(|| format!("transaction receipt not found for {tx_hash}"))?;

        let block_hash = receipt
            .block_hash
            .context("receipt missing block_hash (pending tx?)")?;
        let tx_index = receipt
            .transaction_index
            .context("receipt missing transaction_index")?;

        // Find the DepositInitiated event in the receipt logs
        let log_index = receipt
            .inner
            .logs()
            .iter()
            .position(|log| log.topics().first() == Some(&event_sig))
            .with_context(|| {
                format!("no DepositInitiated event found in receipt for tx {tx_hash}")
            })? as u64;

        // 2. Get full block → header RLP
        let block = self
            .provider
            .get_block_by_hash(block_hash)
            .await?
            .with_context(|| format!("block not found for hash {block_hash}"))?;

        let mut header_rlp = Vec::new();
        block.header.inner.encode(&mut header_rlp);

        // Sanity check: header RLP hashes to the expected block hash
        let computed_hash = alloy_primitives::keccak256(&header_rlp);
        if computed_hash != block_hash {
            bail!(
                "header RLP hash mismatch: computed {computed_hash}, expected {block_hash}. \
                 This may indicate the RPC returned non-standard header fields."
            );
        }

        // 3. Get all block receipts
        let all_receipts = self
            .provider
            .get_block_receipts(block.header.number.into())
            .await?
            .with_context(|| format!("block receipts not found for block {block_hash}"))?;

        // 4. Encode each receipt to canonical EIP-2718 form (as stored in the trie).
        //    RPC receipts use alloy_rpc_types::Log; we convert to alloy_primitives::Log
        //    so that Encodable2718 is available.
        let canonical_receipts: Vec<(u64, Vec<u8>)> = all_receipts
            .iter()
            .enumerate()
            .map(|(idx, r)| {
                let consensus_receipt = r.inner.clone().into_primitives_receipt();
                let encoded = consensus_receipt.encoded_2718();
                (idx as u64, encoded)
            })
            .collect();

        // 5. Build receipt trie and generate proof
        let target_receipt_rlp = canonical_receipts
            .iter()
            .find(|(idx, _)| *idx == tx_index)
            .map(|(_, rlp)| rlp.clone())
            .with_context(|| format!("tx_index {tx_index} not found in block receipts"))?;

        let (receipts_root, proof_nodes) =
            crate::proof::build_receipt_proof(&canonical_receipts, tx_index);

        // Sanity check: computed receipts root matches block header
        if receipts_root != block.header.inner.receipts_root {
            bail!(
                "receipts root mismatch: computed {receipts_root}, \
                 header says {}. Receipt encoding may be incorrect.",
                block.header.inner.receipts_root
            );
        }

        Ok(DepositProof {
            block_header_rlp: header_rlp,
            receipt_rlp: target_receipt_rlp,
            proof_nodes,
            tx_index,
            log_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U256};

    use crate::proof::{
        self,
        testing::{build_deposit_event_data, build_test_receipt, build_test_receipt_with_gas},
        ReceiptLog,
    };

    #[test]
    fn test_build_receipt_proof_single_receipt() {
        let receipt = build_test_receipt(&[]);
        let (root, proof_nodes) = proof::build_receipt_proof(&[(0, receipt.clone())], 0);

        let proof_bytes: Vec<alloy_primitives::Bytes> = proof_nodes
            .iter()
            .map(|n| alloy_primitives::Bytes::copy_from_slice(n))
            .collect();
        proof::verify_receipt_inclusion(root, 0, &receipt, &proof_bytes)
            .expect("proof should verify");
    }

    #[test]
    fn test_build_receipt_proof_multiple_receipts() {
        let receipts: Vec<(u64, Vec<u8>)> = (0..10)
            .map(|i| {
                let receipt = build_test_receipt_with_gas(21000 * (i + 1), &[]);
                (i, receipt)
            })
            .collect();

        let target = 5u64;
        let (root, proof_nodes) = proof::build_receipt_proof(&receipts, target);

        let target_receipt = &receipts[target as usize].1;
        let proof_bytes: Vec<alloy_primitives::Bytes> = proof_nodes
            .iter()
            .map(|n| alloy_primitives::Bytes::copy_from_slice(n))
            .collect();
        proof::verify_receipt_inclusion(root, target, target_receipt, &proof_bytes)
            .expect("proof for tx 5 should verify");
    }

    #[test]
    fn test_build_receipt_proof_with_deposit_event() {
        let event_sig = proof::deposit_event_signature();

        let event_data = build_deposit_event_data(
            8453,
            B256::from([0xAA; 32]),
            B256::from([0xBB; 32]),
            B256::from([0xCC; 32]),
            Address::from([0xDD; 20]),
            1_000_000,
        );

        let bridge_address = Address::from([0xBB; 20]);
        let receipt = build_test_receipt(&[ReceiptLog {
            address: bridge_address,
            topics: vec![event_sig],
            data: event_data,
        }]);

        let tx_index = 3u64;
        let receipts: Vec<(u64, Vec<u8>)> = vec![
            (0, build_test_receipt(&[])),
            (1, build_test_receipt_with_gas(42000, &[])),
            (2, build_test_receipt_with_gas(63000, &[])),
            (tx_index, receipt.clone()),
            (4, build_test_receipt_with_gas(105000, &[])),
        ];

        let (root, proof_nodes) = proof::build_receipt_proof(&receipts, tx_index);

        let proof_bytes: Vec<alloy_primitives::Bytes> = proof_nodes
            .iter()
            .map(|n| alloy_primitives::Bytes::copy_from_slice(n))
            .collect();
        proof::verify_receipt_inclusion(root, tx_index, &receipt, &proof_bytes)
            .expect("proof should verify");

        let logs = proof::decode_receipt_logs(&receipt).expect("decode logs");
        let deposit = proof::parse_deposit_event(&logs[0]).expect("parse deposit");
        assert_eq!(deposit.source_chain_id, U256::from(8453u64));
        assert_eq!(deposit.amount, U256::from(1_000_000u64));
    }

    #[test]
    fn test_build_receipt_proof_wrong_index_fails_verification() {
        let receipts: Vec<(u64, Vec<u8>)> = (0..5)
            .map(|i| (i, build_test_receipt_with_gas(21000 * (i + 1), &[])))
            .collect();

        let (root, proof_nodes) = proof::build_receipt_proof(&receipts, 2);

        let wrong_receipt = &receipts[3].1;
        let proof_bytes: Vec<alloy_primitives::Bytes> = proof_nodes
            .iter()
            .map(|n| alloy_primitives::Bytes::copy_from_slice(n))
            .collect();
        let result = proof::verify_receipt_inclusion(root, 2, wrong_receipt, &proof_bytes);
        assert!(result.is_err(), "wrong receipt should fail verification");
    }
}
