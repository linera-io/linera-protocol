// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ScyllaDB Wasm UDFs to decode and encode Linera `RootKey` partition keys directly in cqlsh.
//!
//! The ScyllaDB backend wraps every root key with a single `0x00` tag byte (see
//! `get_big_root_key` in `linera-views::backends::scylla_db`); both UDFs handle that wrapping
//! transparently.
//!
//! `RootKey` is mirrored here from `linera-storage::db_storage` because compiling the full
//! `linera-storage` crate to `wasm32-wasip1` would drag in the validator dependency tree. The
//! `roundtrip` test in `linera-storage/src/db_storage.rs` (added in the same change) asserts
//! that the two definitions encode to the same bytes.

use linera_base::{
    crypto::CryptoHash,
    identifiers::{BlobId, BlobType, ChainId},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RootKey {
    NetworkDescription,
    BlockExporterState(u32),
    ChainState(ChainId),
    BlockHash(CryptoHash),
    BlobId(BlobId),
    Event(ChainId),
    BlockByHeight(ChainId),
    EventBlockHeight(ChainId),
}

const SCYLLA_TAG: u8 = 0x00;

/// Decode a `RootKey` partition key blob into its `Debug` representation.
///
/// Expects the ScyllaDB `0x00` tag byte at the start (every `root_key` column in
/// `linera-views::backends::scylla_db` is wrapped this way). Returns an `ERR:` prefix on
/// failure so `cqlsh` shows useful output instead of NULL.
#[scylla_udf::export_udf]
fn decode_root_key(raw: Vec<u8>) -> String {
    decode_root_key_impl(&raw)
}

/// Encode a `RootKey` partition key from a variant name and a hex payload.
///
/// Output includes the ScyllaDB `0x00` tag byte so the result can be used directly as a
/// `root_key` value in `WHERE root_key = ?` clauses. Returns an empty blob on error so
/// `WHERE` clauses fail explicitly rather than silently matching the wrong rows.
///
/// Payload format per variant:
/// - `NetworkDescription`: empty
/// - `BlockExporterState`: 4 bytes (u32, little-endian)
/// - `ChainState` / `Event` / `BlockByHeight` / `EventBlockHeight`: 32 bytes (CryptoHash)
/// - `BlockHash`: 32 bytes (CryptoHash)
/// - `BlobId`: 32 bytes (CryptoHash) + 1 byte (BlobType BCS variant tag)
#[scylla_udf::export_udf]
fn encode_root_key(variant: String, payload_hex: String) -> Vec<u8> {
    encode_root_key_impl(&variant, &payload_hex)
}

fn decode_root_key_impl(raw: &[u8]) -> String {
    let Some((&SCYLLA_TAG, payload)) = raw.split_first() else {
        return "ERR: missing ScyllaDB tag byte".to_string();
    };
    match bcs::from_bytes::<RootKey>(payload) {
        Ok(key) => format!("{key:?}"),
        Err(error) => format!("ERR: {error}"),
    }
}

fn encode_root_key_impl(variant: &str, payload_hex: &str) -> Vec<u8> {
    let Ok(key) = build_root_key(variant, payload_hex) else {
        return Vec::new();
    };
    let mut bytes = vec![SCYLLA_TAG];
    bytes.extend(key.bytes());
    bytes
}

impl RootKey {
    pub fn bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).expect("BCS encoding of RootKey cannot fail")
    }
}

fn build_root_key(variant: &str, payload_hex: &str) -> Result<RootKey, BuildError> {
    let payload = hex::decode(payload_hex.strip_prefix("0x").unwrap_or(payload_hex))
        .map_err(|_| BuildError::Hex)?;
    match variant {
        "NetworkDescription" => {
            require_len(&payload, 0)?;
            Ok(RootKey::NetworkDescription)
        }
        "BlockExporterState" => {
            require_len(&payload, 4)?;
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&payload);
            Ok(RootKey::BlockExporterState(u32::from_le_bytes(buf)))
        }
        "ChainState" => Ok(RootKey::ChainState(ChainId(read_hash(&payload)?))),
        "BlockHash" => Ok(RootKey::BlockHash(read_hash(&payload)?)),
        "Event" => Ok(RootKey::Event(ChainId(read_hash(&payload)?))),
        "BlockByHeight" => Ok(RootKey::BlockByHeight(ChainId(read_hash(&payload)?))),
        "EventBlockHeight" => Ok(RootKey::EventBlockHeight(ChainId(read_hash(&payload)?))),
        "BlobId" => {
            require_len(&payload, 33)?;
            let hash = read_hash(&payload[..32])?;
            let blob_type =
                bcs::from_bytes::<BlobType>(&payload[32..]).map_err(|_| BuildError::Payload)?;
            Ok(RootKey::BlobId(BlobId::new(hash, blob_type)))
        }
        _ => Err(BuildError::Variant),
    }
}

fn read_hash(payload: &[u8]) -> Result<CryptoHash, BuildError> {
    require_len(payload, 32)?;
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(payload);
    Ok(CryptoHash::from(bytes))
}

fn require_len(payload: &[u8], expected: usize) -> Result<(), BuildError> {
    if payload.len() == expected {
        Ok(())
    } else {
        Err(BuildError::Payload)
    }
}

#[derive(Debug)]
enum BuildError {
    Hex,
    Variant,
    Payload,
}

#[cfg(test)]
mod tests {
    use linera_base::{crypto::CryptoHash, identifiers::BlobType};

    use super::*;

    fn wrap(bytes: Vec<u8>) -> Vec<u8> {
        let mut out = vec![SCYLLA_TAG];
        out.extend(bytes);
        out
    }

    #[test]
    fn decode_network_description() {
        let raw = wrap(RootKey::NetworkDescription.bytes());
        assert_eq!(decode_root_key_impl(&raw), "NetworkDescription");
    }

    #[test]
    fn decode_block_exporter_state() {
        let raw = wrap(RootKey::BlockExporterState(42).bytes());
        assert_eq!(decode_root_key_impl(&raw), "BlockExporterState(42)");
    }

    #[test]
    fn decode_chain_state_includes_hash() {
        let hash = CryptoHash::test_hash("udf");
        let raw = wrap(RootKey::ChainState(ChainId(hash)).bytes());
        let decoded = decode_root_key_impl(&raw);
        assert!(decoded.starts_with("ChainState("), "{decoded}");
        assert!(decoded.contains(&hash.to_string()), "{decoded}");
    }

    #[test]
    fn decode_without_scylla_tag_errors() {
        let result = decode_root_key_impl(&[]);
        assert!(result.starts_with("ERR:"), "{result}");
    }

    #[test]
    fn decode_garbage_returns_error_string() {
        let result = decode_root_key_impl(&wrap(vec![0xff, 0xff]));
        assert!(result.starts_with("ERR:"), "{result}");
    }

    #[test]
    fn encode_decode_roundtrip_chain_state() {
        let hash = CryptoHash::test_hash("roundtrip");
        let encoded = encode_root_key_impl("ChainState", &hex::encode(hash.as_bytes()));
        let decoded = decode_root_key_impl(&encoded);
        assert!(decoded.starts_with("ChainState("), "{decoded}");
        assert!(decoded.contains(&hash.to_string()), "{decoded}");
    }

    #[test]
    fn encode_decode_roundtrip_block_exporter_state() {
        let encoded = encode_root_key_impl("BlockExporterState", &hex::encode(42u32.to_le_bytes()));
        assert_eq!(decode_root_key_impl(&encoded), "BlockExporterState(42)");
    }

    #[test]
    fn encode_decode_roundtrip_network_description() {
        let encoded = encode_root_key_impl("NetworkDescription", "");
        assert_eq!(decode_root_key_impl(&encoded), "NetworkDescription");
    }

    #[test]
    fn encode_decode_roundtrip_blob_id() {
        let hash = CryptoHash::test_hash("blob");
        let mut payload = hex::encode(hash.as_bytes());
        payload.push_str(&hex::encode(bcs::to_bytes(&BlobType::Data).unwrap()));
        let encoded = encode_root_key_impl("BlobId", &payload);
        let decoded = decode_root_key_impl(&encoded);
        assert!(decoded.starts_with("BlobId("), "{decoded}");
    }

    #[test]
    fn encode_unknown_variant_returns_empty() {
        assert!(encode_root_key_impl("NotARealVariant", "").is_empty());
    }

    #[test]
    fn encode_accepts_0x_prefix() {
        let hash = CryptoHash::test_hash("prefix");
        let encoded =
            encode_root_key_impl("ChainState", &format!("0x{}", hex::encode(hash.as_bytes())));
        let decoded = decode_root_key_impl(&encoded);
        assert!(decoded.contains(&hash.to_string()), "{decoded}");
    }
}
