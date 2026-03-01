// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM receipt proof verification for the EVM→Linera bridge.
//!
//! This module implements the cryptographic verification pipeline for processing
//! EVM deposit events on Linera. When a user deposits ERC-20 tokens into the
//! `FungibleBridge` contract on an EVM chain (e.g. Base), a `DepositInitiated` event
//! is emitted. An off-chain relayer constructs a Merkle Patricia Trie (MPT) inclusion
//! proof for that event's transaction receipt, which the bridge Wasm application on
//! Linera then verifies before minting wrapped tokens.
//!
//! # Verification pipeline
//!
//! The pipeline has four stages, each corresponding to a public function:
//!
//! 1. **[`decode_block_header`]** — Extract the `receipts_root` and `block_hash` from
//!    an RLP-encoded Ethereum block header.
//! 2. **[`verify_receipt_inclusion`]** — Verify that a transaction receipt is included
//!    in the block's receipt trie using an MPT proof.
//! 3. **[`decode_receipt_logs`]** — Decode the receipt's RLP to extract its event logs.
//!    Handles both legacy and EIP-2718 typed receipts.
//! 4. **[`parse_deposit_event`]** — Parse and validate a `DepositInitiated` log entry,
//!    checking the event signature and ABI-decoding the fields.
//!
//! # Example
//!
//! ```rust,no_run
//! use alloy_primitives::Bytes;
//! use linera_bridge::proof::{
//!     decode_block_header, decode_receipt_logs, parse_deposit_event, verify_receipt_inclusion,
//! };
//!
//! fn verify_deposit(
//!     block_header_rlp: &[u8],
//!     receipt_rlp: &[u8],
//!     proof_nodes: &[Bytes],
//!     tx_index: u64,
//!     log_index: usize,
//! ) -> anyhow::Result<()> {
//!     // 1. Decode the block header to get the receipts root.
//!     let (block_hash, receipts_root) = decode_block_header(block_header_rlp)?;
//!
//!     // 2. Verify the receipt is included in the block's receipt trie.
//!     verify_receipt_inclusion(receipts_root, tx_index, receipt_rlp, proof_nodes)?;
//!
//!     // 3. Decode the receipt to extract its logs.
//!     let logs = decode_receipt_logs(receipt_rlp)?;
//!
//!     // 4. Parse the deposit event from the target log.
//!     let deposit = parse_deposit_event(&logs[log_index])?;
//!
//!     println!(
//!         "Verified deposit: {} tokens from chain {} in block {:?}",
//!         deposit.amount, deposit.source_chain_id, block_hash,
//!     );
//!     Ok(())
//! }
//! ```
//!
//! # Replay protection
//!
//! This module is stateless — it only verifies proofs. The caller (bridge Wasm app) is
//! responsible for replay protection using the canonical key
//! `(source_chain_id, block_hash, tx_index, log_index)` and for checking block finality
//! via an HTTP oracle before accepting a deposit.

/// Off-chain deposit proof generation via EVM JSON-RPC.
#[cfg(not(feature = "chain"))]
pub mod gen;

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rlp::Encodable;
use alloy_trie::{proof::ProofRetainer, HashBuilder, Nibbles};
use anyhow::{anyhow, ensure, Result};

/// A decoded log from an EVM transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptLog {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
}

/// Parsed `DepositInitiated` event data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepositEvent {
    pub source_chain_id: U256,
    pub target_chain_id: B256,
    pub target_application_id: B256,
    pub target_account_owner: B256,
    pub token: Address,
    pub amount: U256,
}

/// Returns the keccak256 hash of the `DepositInitiated` event signature.
pub fn deposit_event_signature() -> B256 {
    keccak256(b"DepositInitiated(uint256,bytes32,bytes32,bytes32,address,uint256)")
}

/// Known keccak256 hash of the `DepositInitiated` event signature, for regression testing.
/// If the event signature string changes, this constant must be updated.
pub const DEPOSIT_EVENT_SIGNATURE_HASH: [u8; 32] = [
    0x28, 0x4c, 0x86, 0x6e, 0xdd, 0x78, 0xc8, 0x53, 0xde, 0x6f, 0xf1, 0x5d, 0xd1, 0x2a, 0x71, 0xa3,
    0x56, 0xfd, 0x7b, 0x7a, 0x62, 0x17, 0xa8, 0x84, 0x1a, 0xc0, 0x17, 0x02, 0xdf, 0x36, 0xe5, 0x1f,
];

/// Shared test helpers for building synthetic proofs.
///
/// Available when running tests (`#[cfg(test)]`) or when the `testing` feature is enabled.
/// The `testing` feature lets downstream crates (e.g. `evm-bridge`) reuse these helpers
/// in their own integration tests.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use alloy_primitives::{Address, Bloom, Bytes, FixedBytes, B256, U256};
    use alloy_rlp::Encodable;

    use super::ReceiptLog;

    /// Builds a minimal RLP-encoded Ethereum block header with the given receipts root.
    ///
    /// All other header fields are set to zero/default values. This produces a valid
    /// RLP list that can be decoded by [`super::decode_block_header`].
    pub fn build_test_header(receipts_root: B256) -> Vec<u8> {
        let mut payload = Vec::new();
        B256::ZERO.encode(&mut payload); // 0: parentHash
        B256::ZERO.encode(&mut payload); // 1: ommersHash
        Address::ZERO.encode(&mut payload); // 2: beneficiary
        B256::ZERO.encode(&mut payload); // 3: stateRoot
        B256::ZERO.encode(&mut payload); // 4: transactionsRoot
        receipts_root.encode(&mut payload); // 5: receiptsRoot
        Bloom::ZERO.encode(&mut payload); // 6: logsBloom
        0u64.encode(&mut payload); // 7: difficulty
        12345u64.encode(&mut payload); // 8: number
        30_000_000u64.encode(&mut payload); // 9: gasLimit
        21_000u64.encode(&mut payload); // 10: gasUsed
        1_700_000_000u64.encode(&mut payload); // 11: timestamp
        Bytes::new().encode(&mut payload); // 12: extraData
        B256::ZERO.encode(&mut payload); // 13: mixHash
        FixedBytes::<8>::ZERO.encode(&mut payload); // 14: nonce

        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut out);
        out.extend_from_slice(&payload);
        out
    }

    /// Builds a minimal legacy receipt RLP with the given logs and default gas (21000).
    pub fn build_test_receipt(logs: &[ReceiptLog]) -> Vec<u8> {
        build_test_receipt_with_gas(21_000, logs)
    }

    /// Builds a minimal legacy receipt RLP with the given cumulative gas and logs.
    pub fn build_test_receipt_with_gas(cumulative_gas: u64, logs: &[ReceiptLog]) -> Vec<u8> {
        let mut payload = Vec::new();
        1u8.encode(&mut payload); // status = success
        cumulative_gas.encode(&mut payload); // cumulative_gas_used
        Bloom::ZERO.encode(&mut payload); // logs_bloom

        let mut logs_payload = Vec::new();
        for log in logs {
            encode_log(log, &mut logs_payload);
        }
        alloy_rlp::Header {
            list: true,
            payload_length: logs_payload.len(),
        }
        .encode(&mut payload);
        payload.extend_from_slice(&logs_payload);

        let mut out = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut out);
        out.extend_from_slice(&payload);
        out
    }

    fn encode_log(log: &ReceiptLog, out: &mut Vec<u8>) {
        let mut payload = Vec::new();
        log.address.encode(&mut payload);

        let mut topics_payload = Vec::new();
        for topic in &log.topics {
            topic.encode(&mut topics_payload);
        }
        alloy_rlp::Header {
            list: true,
            payload_length: topics_payload.len(),
        }
        .encode(&mut payload);
        payload.extend_from_slice(&topics_payload);

        Bytes::copy_from_slice(&log.data).encode(&mut payload);

        alloy_rlp::Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(out);
        out.extend_from_slice(&payload);
    }

    /// Builds ABI-encoded data for a `DepositInitiated` event.
    pub fn build_deposit_event_data(
        source_chain_id: u64,
        target_chain_id: B256,
        target_application_id: B256,
        target_account_owner: B256,
        token: Address,
        amount: u64,
    ) -> Vec<u8> {
        let mut data = Vec::with_capacity(192);
        data.extend_from_slice(&U256::from(source_chain_id).to_be_bytes::<32>());
        data.extend_from_slice(target_chain_id.as_slice());
        data.extend_from_slice(target_application_id.as_slice());
        data.extend_from_slice(target_account_owner.as_slice());
        data.extend_from_slice(&[0u8; 12]);
        data.extend_from_slice(token.as_slice());
        data.extend_from_slice(&U256::from(amount).to_be_bytes::<32>());
        data
    }

    /// Builds a receipts MPT trie from `(tx_index, receipt_rlp)` pairs and returns
    /// the trie root and proof nodes for `target_tx_index`.
    pub fn build_receipt_trie(
        receipts: &[(u64, Vec<u8>)],
        target_tx_index: u64,
    ) -> (B256, Vec<Bytes>) {
        let (root, proof) = super::build_receipt_proof(receipts, target_tx_index);
        (root, proof.into_iter().map(Into::into).collect())
    }
}

/// Decodes an RLP-encoded Ethereum block header, returning `(block_hash, receipts_root)`.
///
/// The block hash is `keccak256(header_rlp)`. The receipts root is field index 5
/// in the RLP list.
pub fn decode_block_header(header_rlp: &[u8]) -> Result<(B256, B256)> {
    let block_hash = keccak256(header_rlp);

    let mut data = header_rlp;
    let list_header =
        alloy_rlp::Header::decode(&mut data).map_err(|e| anyhow!("invalid RLP header: {e}"))?;
    ensure!(list_header.list, "block header must be an RLP list");

    // Limit reads to the header's declared payload.
    ensure!(
        data.len() >= list_header.payload_length,
        "block header payload extends past available data"
    );
    let mut data = &data[..list_header.payload_length];

    // Skip first 5 fields: parentHash, ommersHash, beneficiary, stateRoot, transactionsRoot
    for i in 0..5 {
        skip_rlp_item(&mut data).map_err(|e| anyhow!("failed to skip header field {i}: {e}"))?;
    }

    // Field index 5: receiptsRoot (B256)
    let receipts_root = <B256 as alloy_rlp::Decodable>::decode(&mut data)
        .map_err(|e| anyhow!("failed to decode receipts_root: {e}"))?;

    Ok((block_hash, receipts_root))
}

/// Verifies that a receipt is included in the receipts trie via MPT proof.
pub fn verify_receipt_inclusion(
    receipts_root: B256,
    tx_index: u64,
    receipt_rlp: &[u8],
    proof_nodes: &[Bytes],
) -> Result<()> {
    let key = receipt_trie_key(tx_index);
    alloy_trie::proof::verify_proof(receipts_root, key, Some(receipt_rlp.to_vec()), proof_nodes)
        .map_err(|e| anyhow!("MPT proof verification failed: {e}"))
}

/// Decodes a receipt's RLP and extracts its logs.
///
/// Handles EIP-2718 typed receipts (type byte prefix < 0x80).
pub fn decode_receipt_logs(receipt_rlp: &[u8]) -> Result<Vec<ReceiptLog>> {
    ensure!(!receipt_rlp.is_empty(), "empty receipt RLP");

    let mut data: &[u8] = receipt_rlp;
    // EIP-2718: if first byte < 0x80, it's a transaction type prefix
    if data[0] < 0x80 {
        data = &data[1..];
    }

    let list_header =
        alloy_rlp::Header::decode(&mut data).map_err(|e| anyhow!("invalid receipt RLP: {e}"))?;
    ensure!(list_header.list, "receipt must be an RLP list");

    // Limit reads to the receipt's declared payload.
    ensure!(
        data.len() >= list_header.payload_length,
        "receipt payload extends past available data"
    );
    let mut data = &data[..list_header.payload_length];

    // Skip: status (0), cumulative_gas_used (1), logs_bloom (2)
    for i in 0..3 {
        skip_rlp_item(&mut data).map_err(|e| anyhow!("failed to skip receipt field {i}: {e}"))?;
    }

    // Decode the logs list
    let logs_header =
        alloy_rlp::Header::decode(&mut data).map_err(|e| anyhow!("invalid logs list RLP: {e}"))?;
    ensure!(logs_header.list, "logs must be an RLP list");
    ensure!(
        data.len() >= logs_header.payload_length,
        "logs payload extends past receipt boundary"
    );

    let mut logs_data = &data[..logs_header.payload_length];
    let mut logs = Vec::new();
    while !logs_data.is_empty() {
        logs.push(decode_log(&mut logs_data)?);
    }

    Ok(logs)
}

/// Parses a `DepositInitiated` event from a receipt log.
///
/// Verifies that `topic[0]` matches the event signature and ABI-decodes the data fields.
/// All event parameters are non-indexed, so they are encoded in the log data.
pub fn parse_deposit_event(log: &ReceiptLog) -> Result<DepositEvent> {
    ensure!(
        log.topics.first() == Some(&deposit_event_signature()),
        "event topic does not match DepositInitiated signature"
    );
    ensure!(
        log.data.len() == 192,
        "expected 192 bytes of event data (6 x 32), got {}",
        log.data.len()
    );

    let d = &log.data;

    // ABI encodes addresses as left-padded 32-byte words; the first 12 bytes must be zero.
    ensure!(
        d[128..140] == [0u8; 12],
        "invalid ABI encoding: address padding bytes (128..140) must be zero"
    );

    Ok(DepositEvent {
        source_chain_id: U256::from_be_slice(&d[0..32]),
        target_chain_id: B256::from_slice(&d[32..64]),
        target_application_id: B256::from_slice(&d[64..96]),
        target_account_owner: B256::from_slice(&d[96..128]),
        token: Address::from_slice(&d[140..160]),
        amount: U256::from_be_slice(&d[160..192]),
    })
}

// -- internal helpers --

/// Computes the receipt trie key for a given transaction index.
/// The key is the RLP encoding of the index, converted to nibbles.
pub fn receipt_trie_key(tx_index: u64) -> Nibbles {
    let mut key_bytes = Vec::new();
    tx_index.encode(&mut key_bytes);
    Nibbles::unpack(&key_bytes)
}

/// Builds a receipts MPT trie from `(tx_index, receipt_bytes)` pairs and generates
/// a Merkle proof for the receipt at `target_tx_index`.
///
/// Returns `(receipts_root, proof_nodes)`.
pub fn build_receipt_proof(
    receipts: &[(u64, Vec<u8>)],
    target_tx_index: u64,
) -> (B256, Vec<Vec<u8>>) {
    let mut entries: Vec<(Nibbles, &[u8])> = receipts
        .iter()
        .map(|(idx, rlp)| (receipt_trie_key(*idx), rlp.as_slice()))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let target_key = receipt_trie_key(target_tx_index);
    let retainer = ProofRetainer::new(vec![target_key]);
    let mut builder = HashBuilder::default().with_proof_retainer(retainer);

    for (key, value) in &entries {
        builder.add_leaf(*key, value);
    }

    let root = builder.root();
    let proof_nodes = builder.take_proof_nodes();
    let proof = proof_nodes
        .matching_nodes_sorted(&target_key)
        .into_iter()
        .map(|(_, bytes)| bytes.to_vec())
        .collect();

    (root, proof)
}

/// Skips one RLP item (string or list) by reading its header and advancing past the payload.
fn skip_rlp_item(data: &mut &[u8]) -> Result<()> {
    let header = alloy_rlp::Header::decode(data).map_err(|e| anyhow!("invalid RLP item: {e}"))?;
    ensure!(
        data.len() >= header.payload_length,
        "not enough data to skip RLP item"
    );
    *data = &data[header.payload_length..];
    Ok(())
}

/// Decodes a single log entry from RLP.
///
/// Enforces the declared payload boundary: after decoding address, topics, and data,
/// verifies that exactly `payload_length` bytes were consumed.
fn decode_log(data: &mut &[u8]) -> Result<ReceiptLog> {
    let log_header =
        alloy_rlp::Header::decode(data).map_err(|e| anyhow!("invalid log RLP: {e}"))?;
    ensure!(log_header.list, "log must be an RLP list");
    ensure!(
        data.len() >= log_header.payload_length,
        "log payload extends past available data"
    );

    // Limit reads to the declared payload boundary.
    let mut log_data_buf = &data[..log_header.payload_length];
    *data = &data[log_header.payload_length..];

    let address = <Address as alloy_rlp::Decodable>::decode(&mut log_data_buf)
        .map_err(|e| anyhow!("invalid log address: {e}"))?;

    // Decode topics list
    let topics_header = alloy_rlp::Header::decode(&mut log_data_buf)
        .map_err(|e| anyhow!("invalid topics list RLP: {e}"))?;
    ensure!(topics_header.list, "topics must be an RLP list");
    ensure!(
        log_data_buf.len() >= topics_header.payload_length,
        "topics payload extends past log boundary"
    );

    let mut topics_data = &log_data_buf[..topics_header.payload_length];
    log_data_buf = &log_data_buf[topics_header.payload_length..];

    let mut topics = Vec::new();
    while !topics_data.is_empty() {
        let topic = <B256 as alloy_rlp::Decodable>::decode(&mut topics_data)
            .map_err(|e| anyhow!("invalid topic: {e}"))?;
        topics.push(topic);
    }

    // Decode log data (byte string)
    let data_header = alloy_rlp::Header::decode(&mut log_data_buf)
        .map_err(|e| anyhow!("invalid log data RLP: {e}"))?;
    ensure!(
        !data_header.list,
        "log data must be a byte string, not a list"
    );
    ensure!(
        log_data_buf.len() >= data_header.payload_length,
        "log data extends past log boundary"
    );
    let log_bytes = log_data_buf[..data_header.payload_length].to_vec();
    log_data_buf = &log_data_buf[data_header.payload_length..];

    ensure!(
        log_data_buf.is_empty(),
        "trailing data after log fields ({} unexpected bytes)",
        log_data_buf.len()
    );

    Ok(ReceiptLog {
        address,
        topics,
        data: log_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        testing::{
            build_deposit_event_data, build_receipt_trie, build_test_header, build_test_receipt,
        },
        *,
    };

    #[test]
    fn test_deposit_event_signature_known_hash() {
        let hash = deposit_event_signature();
        assert_eq!(
            hash.0, DEPOSIT_EVENT_SIGNATURE_HASH,
            "deposit event signature hash has changed! \
             Update DEPOSIT_EVENT_SIGNATURE_HASH to: {:?}",
            hash.0
        );
    }

    // -- block header tests --

    #[test]
    fn test_decode_block_header() {
        let receipts_root = B256::from([0xAB; 32]);
        let header_rlp = build_test_header(receipts_root);

        let (block_hash, decoded_root) = decode_block_header(&header_rlp).unwrap();

        assert_eq!(decoded_root, receipts_root);
        assert_eq!(block_hash, keccak256(&header_rlp));
    }

    #[test]
    fn test_decode_block_header_invalid_rlp() {
        assert!(decode_block_header(&[0xFF, 0x01, 0x02]).is_err());
    }

    // -- MPT proof tests --

    #[test]
    fn test_verify_receipt_inclusion_valid() {
        let receipt = build_test_receipt(&[]);
        let (root, proof) = build_receipt_trie(&[(1, receipt.clone())], 1);

        verify_receipt_inclusion(root, 1, &receipt, &proof).unwrap();
    }

    #[test]
    fn test_verify_receipt_inclusion_multiple_receipts() {
        let receipt0 = build_test_receipt(&[]);
        let receipt1 = build_test_receipt(&[ReceiptLog {
            address: Address::from([0xCC; 20]),
            topics: vec![B256::from([0xDD; 32])],
            data: vec![1, 2, 3],
        }]);
        let receipt2 = build_test_receipt(&[]);

        let (root, proof) =
            build_receipt_trie(&[(0, receipt0), (1, receipt1.clone()), (2, receipt2)], 1);

        verify_receipt_inclusion(root, 1, &receipt1, &proof).unwrap();
    }

    #[test]
    fn test_verify_receipt_inclusion_wrong_root() {
        let receipt = build_test_receipt(&[]);
        let (_, proof) = build_receipt_trie(&[(1, receipt.clone())], 1);
        let wrong_root = B256::from([0xFF; 32]);

        assert!(verify_receipt_inclusion(wrong_root, 1, &receipt, &proof).is_err());
    }

    #[test]
    fn test_verify_receipt_inclusion_tampered_receipt() {
        let receipt = build_test_receipt(&[]);
        let (root, proof) = build_receipt_trie(&[(1, receipt.clone())], 1);

        let mut tampered = receipt;
        let last = tampered.len() - 1;
        tampered[last] ^= 0xFF;

        assert!(verify_receipt_inclusion(root, 1, &tampered, &proof).is_err());
    }

    // -- receipt log decoding tests --

    #[test]
    fn test_decode_receipt_logs_empty() {
        let receipt = build_test_receipt(&[]);
        let logs = decode_receipt_logs(&receipt).unwrap();
        assert!(logs.is_empty());
    }

    #[test]
    fn test_decode_receipt_logs_single_log() {
        let log = ReceiptLog {
            address: Address::from([0xAA; 20]),
            topics: vec![B256::from([0xBB; 32])],
            data: vec![1, 2, 3, 4],
        };
        let receipt = build_test_receipt(&[log.clone()]);

        let decoded = decode_receipt_logs(&receipt).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], log);
    }

    #[test]
    fn test_decode_receipt_logs_multiple_logs() {
        let logs = vec![
            ReceiptLog {
                address: Address::from([0xAA; 20]),
                topics: vec![B256::from([0xBB; 32])],
                data: vec![1, 2],
            },
            ReceiptLog {
                address: Address::from([0xCC; 20]),
                topics: vec![],
                data: vec![3, 4, 5],
            },
        ];
        let receipt = build_test_receipt(&logs);

        let decoded = decode_receipt_logs(&receipt).unwrap();
        assert_eq!(decoded, logs);
    }

    #[test]
    fn test_decode_receipt_logs_typed_receipt() {
        let log = ReceiptLog {
            address: Address::from([0xCC; 20]),
            topics: vec![],
            data: vec![42],
        };
        let legacy_receipt = build_test_receipt(&[log.clone()]);

        // Prepend type byte 0x02 (EIP-1559)
        let mut typed_receipt = vec![0x02];
        typed_receipt.extend_from_slice(&legacy_receipt);

        let decoded = decode_receipt_logs(&typed_receipt).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], log);
    }

    // -- deposit event parsing tests --

    #[test]
    fn test_parse_deposit_event_valid() {
        let target_chain_id = B256::from([0x11; 32]);
        let target_app_id = B256::from([0x22; 32]);
        let target_owner = B256::from([0x33; 32]);
        let token = Address::from([0x44; 20]);

        let data = build_deposit_event_data(
            8453,
            target_chain_id,
            target_app_id,
            target_owner,
            token,
            1_000_000,
        );

        let log = ReceiptLog {
            address: Address::from([0xFF; 20]),
            topics: vec![deposit_event_signature()],
            data,
        };

        let event = parse_deposit_event(&log).unwrap();
        assert_eq!(event.source_chain_id, U256::from(8453));
        assert_eq!(event.target_chain_id, target_chain_id);
        assert_eq!(event.target_application_id, target_app_id);
        assert_eq!(event.target_account_owner, target_owner);
        assert_eq!(event.token, token);
        assert_eq!(event.amount, U256::from(1_000_000));
    }

    #[test]
    fn test_parse_deposit_event_wrong_topic() {
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![B256::from([0xFF; 32])],
            data: vec![0; 192],
        };
        assert!(parse_deposit_event(&log).is_err());
    }

    #[test]
    fn test_parse_deposit_event_no_topics() {
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![],
            data: vec![0; 192],
        };
        assert!(parse_deposit_event(&log).is_err());
    }

    #[test]
    fn test_parse_deposit_event_wrong_data_length() {
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![deposit_event_signature()],
            data: vec![0; 100], // too short
        };
        assert!(parse_deposit_event(&log).is_err());
    }

    #[test]
    fn test_parse_deposit_event_data_too_long() {
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![deposit_event_signature()],
            data: vec![0; 193], // one byte too many
        };
        assert!(parse_deposit_event(&log).is_err());
    }

    #[test]
    fn test_parse_deposit_event_data_off_by_one_short() {
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![deposit_event_signature()],
            data: vec![0; 191], // one byte too short
        };
        assert!(parse_deposit_event(&log).is_err());
    }

    #[test]
    fn test_parse_deposit_event_nonzero_address_padding() {
        // Build valid event data, then corrupt the ABI padding for the address field.
        // Bytes 128..140 should be zero (left-padding for a 20-byte address in a 32-byte slot).
        let mut data = build_deposit_event_data(
            1,
            B256::ZERO,
            B256::ZERO,
            B256::ZERO,
            Address::from([0xAA; 20]),
            100,
        );
        // Corrupt the padding: set byte 128 to nonzero.
        data[128] = 0xFF;
        let log = ReceiptLog {
            address: Address::ZERO,
            topics: vec![deposit_event_signature()],
            data,
        };
        assert!(
            parse_deposit_event(&log).is_err(),
            "should reject non-zero address padding"
        );
    }

    // -- block header edge cases --

    #[test]
    fn test_decode_block_header_empty() {
        assert!(decode_block_header(&[]).is_err());
    }

    #[test]
    fn test_decode_block_header_non_list() {
        // RLP string instead of list: 0x83 = 3-byte string
        assert!(decode_block_header(&[0x83, 0x01, 0x02, 0x03]).is_err());
    }

    #[test]
    fn test_decode_block_header_truncated() {
        // Valid list with only 3 fields — not enough to reach receipts_root at index 5
        let mut payload = Vec::new();
        B256::ZERO.encode(&mut payload); // field 0
        B256::ZERO.encode(&mut payload); // field 1
        Address::ZERO.encode(&mut payload); // field 2

        let mut header = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut header);
        header.extend_from_slice(&payload);

        assert!(decode_block_header(&header).is_err());
    }

    // -- receipt decoding edge cases --

    #[test]
    fn test_decode_receipt_logs_non_list() {
        // RLP byte string, not a list
        assert!(decode_receipt_logs(&[0x83, 0x01, 0x02, 0x03]).is_err());
    }

    #[test]
    fn test_decode_receipt_logs_truncated_fields() {
        // A valid receipt list with only 2 fields — not enough (needs 4: status, gas, bloom, logs)
        let mut payload = Vec::new();
        1u8.encode(&mut payload); // status
        21_000u64.encode(&mut payload); // cumulative_gas_used
                                        // Missing: logs_bloom, logs

        let mut receipt = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: payload.len(),
        }
        .encode(&mut receipt);
        receipt.extend_from_slice(&payload);

        assert!(decode_receipt_logs(&receipt).is_err());
    }

    #[test]
    fn test_decode_receipt_logs_type_byte_zero() {
        // Type byte 0x00 + valid legacy receipt body
        let log = ReceiptLog {
            address: Address::from([0xAA; 20]),
            topics: vec![],
            data: vec![42],
        };
        let legacy = build_test_receipt(&[log.clone()]);
        let mut typed = vec![0x00];
        typed.extend_from_slice(&legacy);

        let decoded = decode_receipt_logs(&typed).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], log);
    }

    #[test]
    fn test_decode_receipt_logs_type_byte_max() {
        // Type byte 0x7F (max EIP-2718 type) + valid legacy receipt body
        let legacy = build_test_receipt(&[]);
        let mut typed = vec![0x7F];
        typed.extend_from_slice(&legacy);

        let decoded = decode_receipt_logs(&typed).unwrap();
        assert!(decoded.is_empty());
    }

    // -- verify_receipt_inclusion edge cases --

    #[test]
    fn test_verify_receipt_inclusion_empty_proof() {
        let receipt = build_test_receipt(&[]);
        assert!(verify_receipt_inclusion(B256::ZERO, 0, &receipt, &[]).is_err());
    }

    #[test]
    fn test_verify_receipt_inclusion_wrong_tx_index() {
        let receipt = build_test_receipt(&[]);
        let (root, proof) = build_receipt_trie(&[(0, receipt.clone())], 0);
        // Proof was generated for tx_index=0, but we verify with tx_index=1
        assert!(verify_receipt_inclusion(root, 1, &receipt, &proof).is_err());
    }

    #[test]
    fn test_verify_receipt_inclusion_empty_receipt() {
        assert!(verify_receipt_inclusion(B256::ZERO, 0, &[], &[]).is_err());
    }

    // -- decode_log edge cases --

    #[test]
    fn test_decode_receipt_with_log_boundary_overflow() {
        // Build a receipt where the log header claims a smaller payload than actual content.
        // This tests that decode_log enforces the declared payload boundary.
        //
        // Construct a log where: address + topics + data > declared log payload_length
        // by manually building the RLP.

        let mut receipt_payload = Vec::new();
        1u8.encode(&mut receipt_payload); // status
        21_000u64.encode(&mut receipt_payload); // cumulative_gas
        alloy_primitives::Bloom::ZERO.encode(&mut receipt_payload); // logs_bloom

        // Build a log that claims to be very short but contains full data
        let mut log_payload = Vec::new();
        Address::from([0xAA; 20]).encode(&mut log_payload); // address (21 bytes encoded)

        // Topics: empty list
        alloy_rlp::Header {
            list: true,
            payload_length: 0,
        }
        .encode(&mut log_payload);

        // Data: 4 bytes
        alloy_primitives::Bytes::copy_from_slice(&[1, 2, 3, 4]).encode(&mut log_payload);

        // Now build the log header but claim payload is only 10 bytes (less than actual ~28)
        let mut bad_log = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: 10, // lie about size
        }
        .encode(&mut bad_log);
        bad_log.extend_from_slice(&log_payload);

        // Wrap in logs list
        let mut logs_list = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: bad_log.len(),
        }
        .encode(&mut logs_list);
        logs_list.extend_from_slice(&bad_log);

        receipt_payload.extend_from_slice(&logs_list);

        let mut receipt = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: receipt_payload.len(),
        }
        .encode(&mut receipt);
        receipt.extend_from_slice(&receipt_payload);

        // This should fail because the log header lies about its payload size
        assert!(
            decode_receipt_logs(&receipt).is_err(),
            "should reject log with mismatched payload length"
        );
    }

    #[test]
    fn test_decode_receipt_trailing_data_in_logs() {
        // Build a valid receipt, but append extra bytes inside the logs list.
        // The logs list header declares a payload larger than the actual log entries.

        let mut receipt_payload = Vec::new();
        1u8.encode(&mut receipt_payload); // status
        21_000u64.encode(&mut receipt_payload); // cumulative_gas
        alloy_primitives::Bloom::ZERO.encode(&mut receipt_payload); // logs_bloom

        // An empty logs list would have payload_length=0.
        // Instead, declare payload_length=5 but have no actual logs — just garbage.
        let mut logs_list = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: 5,
        }
        .encode(&mut logs_list);
        logs_list.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00]); // garbage

        receipt_payload.extend_from_slice(&logs_list);

        let mut receipt = Vec::new();
        alloy_rlp::Header {
            list: true,
            payload_length: receipt_payload.len(),
        }
        .encode(&mut receipt);
        receipt.extend_from_slice(&receipt_payload);

        // Should fail trying to decode the garbage as a log
        assert!(decode_receipt_logs(&receipt).is_err());
    }
}
