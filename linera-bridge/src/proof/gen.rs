// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Off-chain deposit proof client for EVM→Linera deposits.
//!
//! The `DepositProofClient` trait defines the interface for generating the data
//! needed for a `ProcessDeposit` operation on the evm-bridge app. The concrete
//! `HttpDepositProofClient` implementation queries a standard EVM JSON-RPC endpoint
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
use anyhow::{Context, Result};
use async_trait::async_trait;
use op_alloy_network::Optimism;

/// Errors from deposit proof generation, classified for retry logic.
#[derive(Debug, thiserror::Error)]
pub enum ProofError {
    /// Retrying may succeed (e.g. receipt not yet indexed, RPC transport error).
    #[error("transient error: {0:#}")]
    Transient(anyhow::Error),
    /// Retrying will not help (e.g. hash mismatch, missing deposit event).
    #[error("permanent error: {0:#}")]
    Permanent(anyhow::Error),
}

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
    /// Indices of all `DepositInitiated` logs within the receipt.
    pub log_indices: Vec<u64>,
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
    async fn generate_deposit_proof(&self, tx_hash: B256) -> Result<DepositProof, ProofError>;
}

/// HTTP-based deposit proof client that queries an EVM JSON-RPC endpoint.
///
/// Works with any standard Ethereum-compatible RPC (including Base L2).
/// Uses the `Optimism` network type so OP Stack deposit receipts (type 0x7e)
/// deserialize correctly.
pub struct HttpDepositProofClient {
    provider: Box<dyn Provider<Optimism>>,
}

impl HttpDepositProofClient {
    /// Create a new client connected to the given RPC URL.
    pub fn new(rpc_url: &str) -> Result<Self> {
        let url = rpc_url
            .parse()
            .with_context(|| format!("invalid RPC URL: {rpc_url}"))?;
        let provider = ProviderBuilder::<_, _, Optimism>::default().connect_http(url);
        Ok(Self {
            provider: Box::new(provider),
        })
    }
}

#[async_trait]
impl DepositProofClient for HttpDepositProofClient {
    async fn generate_deposit_proof(&self, tx_hash: B256) -> Result<DepositProof, ProofError> {
        // 1. Get transaction receipt → block hash, tx index
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| ProofError::Transient(e.into()))?
            .ok_or_else(|| {
                ProofError::Transient(anyhow::anyhow!(
                    "transaction receipt not found for {tx_hash}"
                ))
            })?;

        let block_hash = receipt.inner.block_hash.ok_or_else(|| {
            ProofError::Transient(anyhow::anyhow!("receipt missing block_hash (pending tx?)"))
        })?;
        let tx_index = receipt.inner.transaction_index.ok_or_else(|| {
            ProofError::Transient(anyhow::anyhow!("receipt missing transaction_index"))
        })?;

        // 2. Get full block → header RLP
        let block = self
            .provider
            .get_block_by_hash(block_hash)
            .await
            .map_err(|e| ProofError::Transient(e.into()))?
            .ok_or_else(|| {
                ProofError::Transient(anyhow::anyhow!("block not found for hash {block_hash}"))
            })?;

        let mut block_header_rlp = Vec::new();
        block.header.inner.encode(&mut block_header_rlp);

        // Sanity check: header RLP hashes to the expected block hash
        let computed_hash = alloy_primitives::keccak256(&block_header_rlp);
        if computed_hash != block_hash {
            return Err(ProofError::Permanent(anyhow::anyhow!(
                "header RLP hash mismatch: computed {computed_hash}, expected {block_hash}. \
                 This may indicate the RPC returned non-standard header fields."
            )));
        }

        // 3. Get all block receipts
        let all_receipts = self
            .provider
            .get_block_receipts(block.header.number.into())
            .await
            .map_err(|e| ProofError::Transient(e.into()))?
            .ok_or_else(|| {
                ProofError::Transient(anyhow::anyhow!(
                    "block receipts not found for block {block_hash}"
                ))
            })?;

        // 4. Encode each receipt to canonical EIP-2718 form (as stored in the trie).
        //    Convert RPC logs → primitives logs so Encodable2718 is available.
        //    OpReceiptEnvelope handles both standard types and OP deposit (0x7e).
        let canonical_receipts: Vec<(u64, Vec<u8>)> = all_receipts
            .into_iter()
            .enumerate()
            .map(|(idx, r)| {
                let consensus_receipt = r.inner.inner.map_logs(|log| log.inner);
                let encoded = consensus_receipt.encoded_2718();
                (idx as u64, encoded)
            })
            .collect();

        // 5. Build receipt trie and generate proof
        let receipt_rlp = canonical_receipts
            .iter()
            .find(|(idx, _)| *idx == tx_index)
            .map(|(_, rlp)| rlp.clone())
            .ok_or_else(|| {
                ProofError::Permanent(anyhow::anyhow!(
                    "tx_index {tx_index} not found in block receipts"
                ))
            })?;

        let (receipts_root, proof_nodes) =
            crate::proof::build_receipt_proof(&canonical_receipts, tx_index);

        // Sanity check: computed receipts root matches block header
        if receipts_root != block.header.inner.receipts_root {
            return Err(ProofError::Permanent(anyhow::anyhow!(
                "receipts root mismatch: computed {receipts_root}, \
                 header says {}. Receipt encoding may be incorrect.",
                block.header.inner.receipts_root
            )));
        }

        // Find all DepositInitiated log indices from the canonical receipt
        let logs =
            crate::proof::decode_receipt_logs(&receipt_rlp).map_err(ProofError::Permanent)?;
        let log_indices = crate::proof::find_deposit_log_indices(&logs);
        if log_indices.is_empty() {
            return Err(ProofError::Permanent(anyhow::anyhow!(
                "no DepositInitiated event found in receipt for tx {tx_hash}"
            )));
        }

        Ok(DepositProof {
            block_header_rlp,
            receipt_rlp,
            proof_nodes,
            tx_index,
            log_indices,
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

    /// One-off fixture generator: fetches Base block 10M via RPC, encodes all receipts
    /// to canonical EIP-2718 form, and writes a JSON fixture for offline testing.
    ///
    /// Run with: `cargo test -p linera-bridge -- --ignored generate_base_block_fixture`
    ///
    /// Set `BASE_RPC_URL` env var to use a different RPC endpoint (the public Base
    /// RPC does not support `eth_getBlockReceipts`, so this falls back to fetching
    /// each receipt individually via `eth_getTransactionReceipt`).
    #[test]
    #[ignore]
    fn generate_base_block_fixture() {
        use alloy::{
            eips::{eip2718::Encodable2718, BlockNumberOrTag},
            providers::{Provider, ProviderBuilder},
        };
        use alloy_rlp::Encodable;
        use op_alloy_network::Optimism;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let rpc_url = std::env::var("BASE_RPC_URL")
                .unwrap_or_else(|_| "https://mainnet.base.org".to_string());
            let provider =
                ProviderBuilder::<_, _, Optimism>::default().connect_http(rpc_url.parse().unwrap());

            // Fetch block 10M
            let block = provider
                .get_block_by_number(BlockNumberOrTag::Number(10_000_000))
                .await
                .unwrap()
                .expect("block 10M not found");

            // RLP-encode header
            let mut header_rlp = Vec::new();
            block.header.inner.encode(&mut header_rlp);

            let block_hash = alloy_primitives::keccak256(&header_rlp);
            let receipts_root = block.header.inner.receipts_root;

            // Fetch all receipts — try batch first, fall back to per-tx
            let all_receipts = match provider
                .get_block_receipts(block.header.number.into())
                .await
            {
                Ok(Some(receipts)) => receipts,
                _ => {
                    eprintln!(
                        "eth_getBlockReceipts unsupported, fetching receipts individually..."
                    );
                    let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
                    let mut receipts = Vec::with_capacity(tx_hashes.len());
                    for (i, hash) in tx_hashes.iter().enumerate() {
                        if i % 50 == 0 {
                            eprintln!("  fetching receipt {}/{}", i + 1, tx_hashes.len());
                        }
                        let receipt = provider
                            .get_transaction_receipt(*hash)
                            .await
                            .unwrap()
                            .unwrap_or_else(|| panic!("receipt not found for tx {hash}"));
                        receipts.push(receipt);
                    }
                    receipts
                }
            };

            // Encode receipts to canonical EIP-2718 form
            let canonical_receipts: Vec<serde_json::Value> = all_receipts
                .into_iter()
                .enumerate()
                .map(|(idx, r)| {
                    let consensus_receipt = r.inner.inner.map_logs(|log| log.inner);
                    let encoded = consensus_receipt.encoded_2718();
                    // EIP-2718: first byte < 0x80 is a type prefix (e.g. 0x02 = EIP-1559,
                    // 0x7e = OP deposit); first byte >= 0x80 starts an RLP list header,
                    // meaning legacy (type 0) with no prefix.
                    let tx_type = if encoded[0] < 0x80 {
                        encoded[0] as u64
                    } else {
                        0
                    };
                    serde_json::json!({
                        "tx_index": idx,
                        "tx_type": tx_type,
                        "encoded": format!("0x{}", hex::encode(&encoded))
                    })
                })
                .collect();

            let fixture = serde_json::json!({
                "block_number": 10_000_000u64,
                "block_hash": format!("{block_hash}"),
                "receipts_root": format!("{receipts_root}"),
                "header_rlp": format!("0x{}", hex::encode(&header_rlp)),
                "receipts": canonical_receipts
            });

            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/proof/testdata/base_block_10000000.json");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let json = serde_json::to_string_pretty(&fixture).unwrap();
            std::fs::write(&path, json.as_bytes()).unwrap();

            println!("Wrote fixture to {}", path.display());
            println!("Block hash: {block_hash}");
            println!("Receipts root: {receipts_root}");
            println!("Total receipts: {}", canonical_receipts.len());
        });
    }

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
            0,
        );

        let bridge_address = Address::from([0xBB; 20]);
        let depositor = Address::from([0xEE; 20]);
        let mut depositor_topic = [0u8; 32];
        depositor_topic[12..32].copy_from_slice(depositor.as_slice());
        let receipt = build_test_receipt(&[ReceiptLog {
            address: bridge_address,
            topics: vec![event_sig, B256::from(depositor_topic)],
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
        let deposit = proof::parse_deposit_event(&logs[0], bridge_address).expect("parse deposit");
        assert_eq!(deposit.source_chain_id, U256::from(8453u64));
        assert_eq!(deposit.amount, U256::from(1_000_000u64));
    }

    /// `find_deposit_log_indices` returns indices of all `DepositInitiated` logs.
    #[test]
    fn test_finds_all_deposit_events_in_receipt() {
        let event_sig = proof::deposit_event_signature();
        let bridge_address = Address::from([0xBB; 20]);
        let depositor = Address::from([0xEE; 20]);
        let mut depositor_topic = [0u8; 32];
        depositor_topic[12..32].copy_from_slice(depositor.as_slice());

        let make_log = |amount: u64, nonce: u64| ReceiptLog {
            address: bridge_address,
            topics: vec![event_sig, B256::from(depositor_topic)],
            data: build_deposit_event_data(
                8453,
                B256::ZERO,
                B256::ZERO,
                B256::ZERO,
                Address::from([0xAA; 20]),
                amount,
                nonce,
            ),
        };

        // All logs are deposit events
        let logs = vec![make_log(100, 0), make_log(200, 1)];
        assert_eq!(proof::find_deposit_log_indices(&logs), vec![0, 1]);

        // Non-deposit logs interspersed
        let non_deposit_log = ReceiptLog {
            address: Address::from([0xCC; 20]),
            topics: vec![B256::from([0xFF; 32])],
            data: vec![1, 2, 3],
        };
        let mixed_logs = vec![
            non_deposit_log.clone(),
            make_log(100, 0),
            non_deposit_log,
            make_log(200, 1),
        ];
        assert_eq!(proof::find_deposit_log_indices(&mixed_logs), vec![1, 3]);

        // No deposit logs
        let empty: Vec<ReceiptLog> = vec![];
        assert!(proof::find_deposit_log_indices(&empty).is_empty());
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
