// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::ParseIntError;

#[cfg(not(target_arch = "wasm32"))]
use alloy::rpc::json_rpc;
use alloy::rpc::types::eth::Log;
use alloy_primitives::{Address, B256, U256};
use num_bigint::{BigInt, BigUint};
use num_traits::cast::ToPrimitive;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EthereumQueryError {
    /// The ID should be matching
    #[error("the ID should be matching")]
    IdIsNotMatching,

    /// wrong JSON-RPC version
    #[error("wrong JSON-RPC version")]
    WrongJsonRpcVersion,
}

#[derive(Debug, Error)]
pub enum EthereumServiceError {
    /// The database is not coherent
    #[error(transparent)]
    EthereumQueryError(#[from] EthereumQueryError),

    /// Parsing error
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),

    #[error("Unsupported Ethereum type")]
    UnsupportedEthereumTypeError,

    #[error("Event parsing error")]
    EventParsingError,

    /// Parse big int error
    #[error(transparent)]
    ParseBigIntError(#[from] num_bigint::ParseBigIntError),

    /// Ethereum parsing error
    #[error("Ethereum parsing error")]
    EthereumParsingError,

    /// Parse bool error
    #[error("Parse bool error")]
    ParseBoolError,

    /// Hex parsing error
    #[error(transparent)]
    FromHexError(#[from] alloy_primitives::hex::FromHexError),

    /// `serde_json` error
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    /// RPC error
    #[error(transparent)]
    #[cfg(not(target_arch = "wasm32"))]
    RpcError(#[from] json_rpc::RpcError<alloy::transports::TransportErrorKind>),

    /// URL parsing error
    #[error(transparent)]
    #[cfg(not(target_arch = "wasm32"))]
    UrlParseError(#[from] url::ParseError),

    /// Alloy Reqwest error
    #[error(transparent)]
    #[cfg(not(target_arch = "wasm32"))]
    AlloyReqwestError(#[from] alloy::transports::http::reqwest::Error),
}

/// A single primitive data type. This is used for example for the
/// entries of Ethereum events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthereumDataType {
    Address(String),
    Uint256(U256),
    Uint64(u64),
    Int64(i64),
    Uint32(u32),
    Int32(i32),
    Uint16(u16),
    Int16(i16),
    Uint8(u8),
    Int8(i8),
    Bool(bool),
}

/// Converts an entry named
/// `Event(type1 indexed,type2 indexed)` into `Event(type1,type2)`.
/// `event_name_expanded` is needed for parsing the obtained log.
pub fn event_name_from_expanded(event_name_expanded: &str) -> String {
    event_name_expanded.replace(" indexed", "").to_string()
}

fn parse_uint<T>(entry: B256, parser: impl FnOnce(&BigUint) -> Option<T>) -> Result<T, EthereumServiceError> {
    let entry = BigUint::from_bytes_be(&entry.0);
    parser(&entry).ok_or(EthereumServiceError::EthereumParsingError)
}

fn parse_int<T>(entry: B256, parser: impl FnOnce(&BigInt) -> Option<T>) -> Result<T, EthereumServiceError> {
    let entry = BigInt::from_signed_bytes_be(&entry.0);
    parser(&entry).ok_or(EthereumServiceError::EthereumParsingError)
}

fn parse_entry(entry: B256, ethereum_type: &str) -> Result<EthereumDataType, EthereumServiceError> {
    if ethereum_type == "address" {
        let address = Address::from_word(entry);
        let address = format!("{:?}", address);
        return Ok(EthereumDataType::Address(address));
    }
    if ethereum_type == "uint256" {
        let entry = U256::from_be_bytes(entry.0);
        return Ok(EthereumDataType::Uint256(entry));
    }
    if ethereum_type == "uint64" {
        let entry = parse_uint(entry, BigUint::to_u64)?;
        return Ok(EthereumDataType::Uint64(entry));
    }
    if ethereum_type == "int64" {
        let entry = parse_int(entry, BigInt::to_i64)?;
        return Ok(EthereumDataType::Int64(entry));
    }
    if ethereum_type == "uint32" {
        let entry = parse_uint(entry, BigUint::to_u32)?;
        return Ok(EthereumDataType::Uint32(entry));
    }
    if ethereum_type == "int32" {
        let entry = parse_int(entry, BigInt::to_i32)?;
        return Ok(EthereumDataType::Int32(entry));
    }
    if ethereum_type == "uint16" {
        let entry = parse_uint(entry, BigUint::to_u16)?;
        return Ok(EthereumDataType::Uint16(entry));
    }
    if ethereum_type == "int16" {
        let entry = parse_int(entry, BigInt::to_i16)?;
        return Ok(EthereumDataType::Int16(entry));
    }
    if ethereum_type == "uint8" {
        let entry = parse_uint(entry, BigUint::to_u8)?;
        return Ok(EthereumDataType::Uint8(entry));
    }
    if ethereum_type == "int8" {
        let entry = parse_int(entry, BigInt::to_i8)?;
        return Ok(EthereumDataType::Int8(entry));
    }
    if ethereum_type == "bool" {
        let entry = parse_uint(entry, BigUint::to_u8)?;
        let entry = match entry {
            1 => true,
            0 => false,
            _ => {
                return Err(EthereumServiceError::ParseBoolError);
            }
        };
        return Ok(EthereumDataType::Bool(entry));
    }
    Err(EthereumServiceError::UnsupportedEthereumTypeError)
}

/// The data type for an Ethereum event emitted by a smart contract
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EthereumEvent {
    pub values: Vec<EthereumDataType>,
    pub block_number: u64,
}

fn get_inner_event_type(event_name_expanded: &str) -> Result<String, EthereumServiceError> {
    if let Some(opening_paren_index) = event_name_expanded.find('(') {
        if let Some(closing_paren_index) = event_name_expanded.find(')') {
            // Extract the substring between the parentheses
            let inner_types = &event_name_expanded[opening_paren_index + 1..closing_paren_index];
            return Ok(inner_types.to_string());
        }
    }
    Err(EthereumServiceError::EventParsingError)
}

pub fn parse_log(
    event_name_expanded: &str,
    log: &Log,
) -> Result<EthereumEvent, EthereumServiceError> {
    let inner_types = get_inner_event_type(event_name_expanded)?;
    let ethereum_types = if inner_types.is_empty() {
        Vec::new()
    } else {
        inner_types.split(',').map(str::to_string).collect::<Vec<_>>()
    };
    let mut values = Vec::new();
    let mut topic_index = 0;
    let mut data_index = 0;
    let mut vec = [0_u8; 32];
    let log_data = log.data();
    let topics = log_data.topics();
    for ethereum_type in ethereum_types {
        values.push(match ethereum_type.strip_suffix(" indexed") {
            None => {
                let start = data_index * 32;
                let end = start + 32;
                let chunk = log_data
                    .data
                    .get(start..end)
                    .ok_or(EthereumServiceError::EthereumParsingError)?;
                for (i, val) in vec.iter_mut().enumerate() {
                    *val = chunk[i];
                }
                data_index += 1;
                let entry = vec.into();
                parse_entry(entry, &ethereum_type)?
            }
            Some(ethereum_type) => {
                topic_index += 1;
                let topic = topics
                    .get(topic_index)
                    .copied()
                    .ok_or(EthereumServiceError::EthereumParsingError)?;
                parse_entry(topic, ethereum_type)?
            }
        });
    }
    let block_number = log
        .block_number
        .ok_or(EthereumServiceError::EthereumParsingError)?;
    Ok(EthereumEvent {
        values,
        block_number,
    })
}

#[cfg(test)]
mod tests {
    use alloy::rpc::types::eth::Log;
    use alloy_primitives::{hex, Address, Bytes, LogData};

    use super::{parse_log, EthereumServiceError};

    fn make_log(data: Vec<u8>, block_number: Option<u64>) -> Log {
        Log {
            inner: alloy_primitives::Log {
                address: Address::ZERO,
                data: LogData::new_unchecked(vec![], Bytes::from(data)),
            },
            block_hash: None,
            block_number,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[test]
    fn parse_log_rejects_out_of_range_integer() {
        let log = make_log(
            hex::decode(format!("01{}", "00".repeat(31))).expect("test data should decode"),
            Some(1),
        );
        let error = parse_log("Value(uint8)", &log).unwrap_err();
        assert!(matches!(error, EthereumServiceError::EthereumParsingError));
    }

    #[test]
    fn parse_log_rejects_truncated_data() {
        let log = make_log(vec![1], Some(1));
        let error = parse_log("Value(uint256)", &log).unwrap_err();
        assert!(matches!(error, EthereumServiceError::EthereumParsingError));
    }

    #[test]
    fn parse_log_rejects_missing_block_number() {
        let log = make_log(vec![0; 32], None);
        let error = parse_log("Value(uint256)", &log).unwrap_err();
        assert!(matches!(error, EthereumServiceError::EthereumParsingError));
    }

    #[test]
    fn parse_log_accepts_empty_event_arguments() {
        let log = make_log(vec![], Some(7));
        let event = parse_log("Value()", &log).expect("empty event arguments should parse");
        assert!(event.values.is_empty());
        assert_eq!(event.block_number, 7);
    }
}
