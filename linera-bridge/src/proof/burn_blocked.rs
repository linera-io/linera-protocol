// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Parser for the `BurnBlocked(uint64,uint32,address,bytes32,bytes,uint128)`
//! event emitted by `FungibleBridge.blockBurn`.
//!
//! Off-chain relayers consume this event to build a Linera-side refund proof
//! without re-fetching the certificate. The Wasm bridge app then verifies the
//! MPT receipt proof and credits the refund.

use alloy_primitives::{keccak256, Address, B256};
use anyhow::{ensure, Result};
use linera_base::{
    crypto::CryptoHash,
    data_types::Amount,
    identifiers::{AccountOwner, ChainId},
};

use crate::proof::ReceiptLog;

/// Parsed `BurnBlocked` event data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BurnBlockedFields {
    pub height: u64,
    pub event_index: u32,
    pub blocked_by: Address,
    pub source_chain_id: ChainId,
    pub source_owner: AccountOwner,
    pub amount: Amount,
}

/// Replay-protection key for a refund.
///
/// Independent from [`super::DepositKey`] so the deposit and refund domains
/// cannot collide on-chain.
#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct RefundKey {
    pub source_chain_id: u64,
    pub block_hash: B256,
    pub tx_index: u64,
    pub log_index: u64,
}

impl RefundKey {
    /// Deterministic keccak-256 hash of the refund key fields.
    pub fn hash(&self) -> [u8; 32] {
        let mut data = [0u8; 56];
        data[0..8].copy_from_slice(&self.source_chain_id.to_le_bytes());
        data[8..40].copy_from_slice(self.block_hash.as_slice());
        data[40..48].copy_from_slice(&self.tx_index.to_le_bytes());
        data[48..56].copy_from_slice(&self.log_index.to_le_bytes());
        keccak256(data).0
    }
}

/// Returns the keccak256 hash of the `BurnBlocked` event signature.
pub fn burn_blocked_event_signature() -> B256 {
    keccak256(b"BurnBlocked(uint64,uint32,address,bytes32,bytes,uint128)")
}

/// Parses a `BurnBlocked` event from a receipt log.
///
/// Verifies that `topic[0]` matches the event signature, that the log was emitted
/// by the `expected_emitter` (bridge contract address), and ABI-decodes the data
/// fields. The `height`, `eventIndex`, and `blocked_by` fields are indexed
/// (stored in `topics[1..4]`); the remaining parameters are non-indexed and
/// encoded in the log data.
pub fn parse_burn_blocked_event(
    log: &ReceiptLog,
    expected_emitter: Address,
) -> Result<BurnBlockedFields> {
    ensure!(
        log.address == expected_emitter,
        "log emitter {:?} does not match expected bridge contract {:?}",
        log.address,
        expected_emitter
    );
    ensure!(
        log.topics.first() == Some(&burn_blocked_event_signature()),
        "event topic does not match BurnBlocked signature"
    );
    ensure!(
        log.topics.len() == 4,
        "expected exactly 4 topics (signature + 3 indexed fields), got {}",
        log.topics.len()
    );

    // Indexed `height` (uint64) in topics[1], left-padded to 32 bytes.
    let height_topic = log.topics[1];
    ensure!(
        height_topic.as_slice()[..24] == [0u8; 24],
        "invalid ABI encoding: height topic padding bytes (0..24) must be zero"
    );
    let mut height_bytes = [0u8; 8];
    height_bytes.copy_from_slice(&height_topic.as_slice()[24..32]);
    let height = u64::from_be_bytes(height_bytes);

    // Indexed `eventIndex` (uint32) in topics[2], left-padded to 32 bytes.
    let event_index_topic = log.topics[2];
    ensure!(
        event_index_topic.as_slice()[..28] == [0u8; 28],
        "invalid ABI encoding: eventIndex topic padding bytes (0..28) must be zero"
    );
    let mut event_index_bytes = [0u8; 4];
    event_index_bytes.copy_from_slice(&event_index_topic.as_slice()[28..32]);
    let event_index = u32::from_be_bytes(event_index_bytes);

    // Indexed `blocked_by` (address) in topics[3], left-padded to 32 bytes.
    let blocked_by_topic = log.topics[3];
    ensure!(
        blocked_by_topic.as_slice()[..12] == [0u8; 12],
        "invalid ABI encoding: blocked_by topic padding bytes (0..12) must be zero"
    );
    let blocked_by = Address::from_slice(&blocked_by_topic.as_slice()[12..32]);

    // Non-indexed data: head is 3 words (96 bytes), then `bytes` tail.
    let d = &log.data;
    ensure!(
        d.len() >= 96,
        "expected at least 96 bytes of event data head, got {}",
        d.len()
    );

    // Head[0..32]: source_chain_id (bytes32).
    let mut source_chain_id_bytes = [0u8; 32];
    source_chain_id_bytes.copy_from_slice(&d[0..32]);
    let source_chain_id = ChainId(CryptoHash::from(source_chain_id_bytes));

    // Head[32..64]: offset to `source_owner_bcs` tail, MUST be 96 (0x60) — three head words.
    let mut offset_bytes = [0u8; 32];
    offset_bytes.copy_from_slice(&d[32..64]);
    ensure!(
        offset_bytes[..24] == [0u8; 24],
        "invalid ABI encoding: source_owner_bcs offset high bytes (0..24) must be zero"
    );
    let mut offset_low = [0u8; 8];
    offset_low.copy_from_slice(&offset_bytes[24..32]);
    let offset = u64::from_be_bytes(offset_low);
    ensure!(
        offset == 96,
        "invalid ABI encoding: source_owner_bcs offset must be 96, got {offset}"
    );

    // Head[64..96]: amount (uint128), left-padded to 32 bytes.
    ensure!(
        d[64..80] == [0u8; 16],
        "invalid ABI encoding: amount high bytes (64..80) must be zero"
    );
    let mut amount_bytes = [0u8; 16];
    amount_bytes.copy_from_slice(&d[80..96]);
    let amount = Amount::from_attos(u128::from_be_bytes(amount_bytes));

    // Tail at offset 96: 32-byte length, then `length` bytes padded to a 32-byte boundary.
    ensure!(
        d.len() >= 128,
        "expected at least 128 bytes of event data for bytes length, got {}",
        d.len()
    );
    ensure!(
        d[96..120] == [0u8; 24],
        "invalid ABI encoding: source_owner_bcs length high bytes (96..120) must be zero"
    );
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&d[120..128]);
    let bcs_len = u64::from_be_bytes(len_bytes) as usize;

    let padded_len = bcs_len.div_ceil(32) * 32;
    ensure!(
        d.len() == 128 + padded_len,
        "expected exactly {} bytes of event data (head + length + padded bytes), got {}",
        128 + padded_len,
        d.len()
    );
    let source_owner_bcs = &d[128..128 + bcs_len];

    // Padding tail beyond the declared length must be zero.
    ensure!(
        d[128 + bcs_len..].iter().all(|b| *b == 0),
        "invalid ABI encoding: source_owner_bcs trailing padding must be zero"
    );

    let source_owner = bcs::from_bytes::<AccountOwner>(source_owner_bcs)
        .map_err(|e| anyhow::anyhow!("malformed source_owner_bcs: {e}"))?;

    Ok(BurnBlockedFields {
        height,
        event_index,
        blocked_by,
        source_chain_id,
        source_owner,
        amount,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a `blocked_by` topic (left-padded address in a 32-byte word).
    fn blocked_by_topic(addr: Address) -> B256 {
        let mut topic = [0u8; 32];
        topic[12..32].copy_from_slice(addr.as_slice());
        B256::from(topic)
    }

    /// Builds an indexed-uint topic by left-padding a big-endian integer to 32 bytes.
    fn uint_topic(value: u128, byte_len: usize) -> B256 {
        let mut topic = [0u8; 32];
        let be = value.to_be_bytes();
        topic[32 - byte_len..32].copy_from_slice(&be[16 - byte_len..16]);
        B256::from(topic)
    }

    /// Builds ABI-encoded data for a `BurnBlocked` event:
    /// `bytes32 source_chain_id, bytes source_owner_bcs, uint128 amount`.
    fn build_burn_blocked_event_data(
        source_chain_id: B256,
        source_owner_bcs: &[u8],
        amount: u128,
    ) -> Vec<u8> {
        let mut data = Vec::new();
        // Head[0..32]: source_chain_id
        data.extend_from_slice(source_chain_id.as_slice());
        // Head[32..64]: offset to source_owner_bcs tail = 96
        let mut offset = [0u8; 32];
        offset[24..32].copy_from_slice(&96u64.to_be_bytes());
        data.extend_from_slice(&offset);
        // Head[64..96]: amount (uint128 left-padded to 32 bytes)
        let mut amount_word = [0u8; 32];
        amount_word[16..32].copy_from_slice(&amount.to_be_bytes());
        data.extend_from_slice(&amount_word);
        // Tail[96..128]: bytes length
        let mut len_word = [0u8; 32];
        len_word[24..32].copy_from_slice(&(source_owner_bcs.len() as u64).to_be_bytes());
        data.extend_from_slice(&len_word);
        // Tail[128..]: bytes data, padded to a 32-byte boundary
        data.extend_from_slice(source_owner_bcs);
        let pad = (32 - source_owner_bcs.len() % 32) % 32;
        data.extend(std::iter::repeat_n(0u8, pad));
        data
    }

    /// A constant bridge address used in burn-blocked parsing tests.
    const TEST_BRIDGE: Address = Address::new([0xFF; 20]);

    #[test]
    fn test_parse_burn_blocked_event_valid() {
        let source_chain_id = B256::from([0x11; 32]);
        let source_owner_bcs = bcs::to_bytes(&AccountOwner::CHAIN).unwrap();
        let blocked_by = Address::from([0x55; 20]);

        let data = build_burn_blocked_event_data(
            source_chain_id,
            &source_owner_bcs,
            1_000_000_000_000_000_000u128,
        );

        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(42, 8),
                uint_topic(5, 4),
                blocked_by_topic(blocked_by),
            ],
            data,
        };

        let fields = parse_burn_blocked_event(&log, TEST_BRIDGE).unwrap();
        assert_eq!(fields.height, 42);
        assert_eq!(fields.event_index, 5);
        assert_eq!(fields.blocked_by, blocked_by);
        assert_eq!(
            fields.source_chain_id,
            ChainId(CryptoHash::from(source_chain_id.0))
        );
        assert_eq!(fields.source_owner, AccountOwner::CHAIN);
        assert_eq!(fields.amount.to_attos(), 1_000_000_000_000_000_000u128);
    }

    #[test]
    fn test_parse_burn_blocked_event_wrong_emitter() {
        let source_owner_bcs = bcs::to_bytes(&AccountOwner::CHAIN).unwrap();
        let data = build_burn_blocked_event_data(B256::ZERO, &source_owner_bcs, 1);
        let log = ReceiptLog {
            address: Address::from([0xAA; 20]),
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data,
        };
        assert!(
            parse_burn_blocked_event(&log, TEST_BRIDGE).is_err(),
            "should reject log from wrong emitter"
        );
    }

    #[test]
    fn test_parse_burn_blocked_event_wrong_topic() {
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                B256::from([0xFF; 32]),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data: vec![0; 128],
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_no_topics() {
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![],
            data: vec![0; 128],
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_missing_indexed_topics() {
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![burn_blocked_event_signature(), uint_topic(1, 8)],
            data: vec![0; 128],
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_extra_topic() {
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
                B256::ZERO, // extra topic
            ],
            data: vec![0; 128],
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_short_data() {
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data: vec![0; 50],
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_wrong_offset() {
        let source_owner_bcs = bcs::to_bytes(&AccountOwner::CHAIN).unwrap();
        let mut data = build_burn_blocked_event_data(B256::ZERO, &source_owner_bcs, 1);
        // Corrupt the offset word: set last byte to a non-96 value.
        data[63] = 0x40;
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data,
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_nonzero_amount_padding() {
        let source_owner_bcs = bcs::to_bytes(&AccountOwner::CHAIN).unwrap();
        let mut data = build_burn_blocked_event_data(B256::ZERO, &source_owner_bcs, 1);
        // Bytes 64..80 are the high 16 bytes of the uint128 word and must be zero.
        data[64] = 0xFF;
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data,
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_parse_burn_blocked_event_nonzero_tail_padding() {
        let source_owner_bcs = bcs::to_bytes(&AccountOwner::CHAIN).unwrap();
        let mut data = build_burn_blocked_event_data(B256::ZERO, &source_owner_bcs, 1);
        // Corrupt the padding zero byte just past the declared bcs length.
        let pad_start = 128 + source_owner_bcs.len();
        if pad_start < data.len() {
            data[pad_start] = 0xFF;
            let log = ReceiptLog {
                address: TEST_BRIDGE,
                topics: vec![
                    burn_blocked_event_signature(),
                    uint_topic(0, 8),
                    uint_topic(0, 4),
                    blocked_by_topic(Address::ZERO),
                ],
                data,
            };
            assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
        }
    }

    #[test]
    fn test_parse_burn_blocked_event_malformed_owner() {
        let bogus_owner_bcs = vec![0xFFu8];
        let data = build_burn_blocked_event_data(B256::ZERO, &bogus_owner_bcs, 1);
        let log = ReceiptLog {
            address: TEST_BRIDGE,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(0, 8),
                uint_topic(0, 4),
                blocked_by_topic(Address::ZERO),
            ],
            data,
        };
        assert!(parse_burn_blocked_event(&log, TEST_BRIDGE).is_err());
    }

    #[test]
    fn test_refund_key_hash_is_deterministic() {
        let key = RefundKey {
            source_chain_id: 8453,
            block_hash: B256::repeat_byte(0x11),
            tx_index: 7,
            log_index: 3,
        };
        let h = key.hash();
        assert_eq!(h, key.hash(), "hash must be deterministic");
        assert_ne!(
            h,
            RefundKey {
                source_chain_id: 1,
                ..key.clone()
            }
            .hash()
        );
    }
}
