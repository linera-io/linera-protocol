// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Domain-agnostic EIP-712 hash computation from JSON typed data.
//!
//! This module lets any `Signer` implementation compute the EIP-712 signing hash
//! from the typed data JSON without depending on `linera-chain`.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::keccak256;
use serde::Deserialize;

/// Top-level EIP-712 typed data structure.
#[derive(Deserialize)]
struct TypedData {
    types: BTreeMap<String, Vec<TypeField>>,
    #[serde(rename = "primaryType")]
    primary_type: String,
    domain: serde_json::Value,
    message: serde_json::Value,
}

/// A single field in an EIP-712 type definition.
#[derive(Deserialize)]
struct TypeField {
    name: String,
    #[serde(rename = "type")]
    type_: String,
}

/// Computes the EIP-712 signing hash from a JSON typed data string.
///
/// Returns `keccak256("\x19\x01" || domainSeparator || hashStruct(primaryType, message))`.
pub fn compute_eip712_hash(typed_data_json: &str) -> [u8; 32] {
    let data: TypedData =
        serde_json::from_str(typed_data_json).expect("invalid EIP-712 typed data JSON");

    let domain_separator = hash_struct(&data.types, "EIP712Domain", &data.domain);
    let message_hash = hash_struct(&data.types, &data.primary_type, &data.message);

    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.extend_from_slice(b"\x19\x01");
    buf.extend_from_slice(&domain_separator);
    buf.extend_from_slice(&message_hash);
    keccak256(&buf).0
}

/// Computes `hashStruct(type_name, value) = keccak256(typeHash || encodeData(...))`.
fn hash_struct(
    types: &BTreeMap<String, Vec<TypeField>>,
    type_name: &str,
    value: &serde_json::Value,
) -> [u8; 32] {
    let type_hash = compute_type_hash(types, type_name);
    let fields = types
        .get(type_name)
        .expect("type not found in types definition");

    let mut buf = Vec::with_capacity((1 + fields.len()) * 32);
    buf.extend_from_slice(&type_hash);

    for field in fields {
        let field_value = &value[&field.name];
        buf.extend_from_slice(&encode_field(types, &field.type_, field_value));
    }

    keccak256(&buf).0
}

/// Computes the EIP-712 type hash: `keccak256(encodeType(type_name))`.
///
/// The type encoding includes the primary type followed by all referenced struct types
/// sorted alphabetically.
fn compute_type_hash(types: &BTreeMap<String, Vec<TypeField>>, type_name: &str) -> [u8; 32] {
    let mut referenced = BTreeSet::new();
    collect_referenced_types(types, type_name, &mut referenced);
    // Remove the primary type from referenced set (it comes first, not sorted among refs).
    referenced.remove(type_name);

    let mut type_string = encode_type_string(types, type_name);
    for ref_type in &referenced {
        type_string.push_str(&encode_type_string(types, ref_type));
    }

    keccak256(type_string.as_bytes()).0
}

/// Builds the type encoding string for a single type, e.g.
/// `"TransferOp(string owner,string recipient,uint128 amount)"`.
fn encode_type_string(types: &BTreeMap<String, Vec<TypeField>>, type_name: &str) -> String {
    let fields = types.get(type_name).expect("type not found");
    let mut s = String::new();
    s.push_str(type_name);
    s.push('(');
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&field.type_);
        s.push(' ');
        s.push_str(&field.name);
    }
    s.push(')');
    s
}

/// Recursively collects all struct types referenced from `type_name`.
fn collect_referenced_types(
    types: &BTreeMap<String, Vec<TypeField>>,
    type_name: &str,
    referenced: &mut BTreeSet<String>,
) {
    if !referenced.insert(type_name.to_string()) {
        return;
    }
    if let Some(fields) = types.get(type_name) {
        for field in fields {
            let base_type = strip_array_suffix(&field.type_);
            if types.contains_key(base_type) {
                collect_referenced_types(types, base_type, referenced);
            }
        }
    }
}

/// Strips trailing `[]` from array types (e.g. `"TransferOp[]"` → `"TransferOp"`).
fn strip_array_suffix(type_name: &str) -> &str {
    type_name.strip_suffix("[]").unwrap_or(type_name)
}

/// Encodes a single field value according to its EIP-712 type.
fn encode_field(
    types: &BTreeMap<String, Vec<TypeField>>,
    type_name: &str,
    value: &serde_json::Value,
) -> [u8; 32] {
    // Array types: keccak256(concat(encode(elem)...))
    if let Some(element_type) = type_name.strip_suffix("[]") {
        let arr = value.as_array().expect("expected JSON array for [] type");
        let mut buf = Vec::with_capacity(arr.len() * 32);
        for elem in arr {
            if types.contains_key(element_type) {
                buf.extend_from_slice(&hash_struct(types, element_type, elem));
            } else {
                buf.extend_from_slice(&encode_atomic(element_type, elem));
            }
        }
        return keccak256(&buf).0;
    }

    // Struct types: hashStruct
    if types.contains_key(type_name) {
        return hash_struct(types, type_name, value);
    }

    // Atomic types
    encode_atomic(type_name, value)
}

/// Encodes an atomic EIP-712 value to a 32-byte ABI-encoded word.
fn encode_atomic(type_name: &str, value: &serde_json::Value) -> [u8; 32] {
    match type_name {
        "string" => {
            let s = value.as_str().expect("expected string value");
            keccak256(s).0
        }
        "bytes32" => {
            let s = value.as_str().expect("expected hex string for bytes32");
            let s = s.strip_prefix("0x").unwrap_or(s);
            let bytes = hex::decode(s).expect("invalid hex in bytes32");
            assert!(bytes.len() == 32, "bytes32 must be exactly 32 bytes");
            let mut buf = [0u8; 32];
            buf.copy_from_slice(&bytes);
            buf
        }
        "uint64" => {
            // Accept both JSON number and decimal string (string avoids JS precision loss).
            let n = value
                .as_u64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
                .expect("expected u64 value (number or decimal string)");
            let mut buf = [0u8; 32];
            buf[24..32].copy_from_slice(&n.to_be_bytes());
            buf
        }
        "uint128" => {
            // uint128 is encoded as a decimal string in JSON (exceeds JS safe integer).
            let s = value.as_str().expect("expected string for uint128");
            let n: u128 = s.parse().expect("invalid uint128 string");
            let mut buf = [0u8; 32];
            buf[16..32].copy_from_slice(&n.to_be_bytes());
            buf
        }
        "uint32" => {
            let n = value.as_u64().expect("expected integer value for uint32") as u32;
            let mut buf = [0u8; 32];
            buf[28..32].copy_from_slice(&n.to_be_bytes());
            buf
        }
        "bool" => {
            let b = value.as_bool().expect("expected boolean value");
            let mut buf = [0u8; 32];
            if b {
                buf[31] = 1;
            }
            buf
        }
        _ => panic!("unsupported EIP-712 atomic type: {type_name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_domain_hash() {
        let json = r#"{
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"}
                ]
            },
            "primaryType": "EIP712Domain",
            "domain": {"name": "Linera", "version": "1"},
            "message": {"name": "Linera", "version": "1"}
        }"#;
        let hash = compute_eip712_hash(json);
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn test_deterministic() {
        let json = r#"{
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"}
                ],
                "Test": [
                    {"name": "value", "type": "uint64"}
                ]
            },
            "primaryType": "Test",
            "domain": {"name": "Linera", "version": "1"},
            "message": {"value": 42}
        }"#;
        let hash1 = compute_eip712_hash(json);
        let hash2 = compute_eip712_hash(json);
        assert_eq!(hash1, hash2);
    }
}
