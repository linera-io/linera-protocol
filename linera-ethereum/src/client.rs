// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, fmt::Debug};

use alloy::{
    primitives::{Address, U256},
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        BlockId, BlockNumberOrTag, Filter, Log,
    },
};
use alloy_primitives::{Bytes, U64};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use thiserror::Error;

use crate::common::{event_name_from_expanded, parse_log, EthereumEvent, EthereumServiceError};

/// The basic JsonRpcClient that we need for running the Json queries
#[async_trait]
pub trait JsonRpcClient {
    type Error: From<serde_json::Error>;
    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error>;
    async fn request<T: Debug + Serialize + Send + Sync, R: DeserializeOwned + Send>(
        &self,
        method: &str,
        params: T,
    ) -> Result<R, Self::Error> {
        let payload = Request::new(method, params);
        let payload = serde_json::to_vec(&payload)?;
        let body = self.request_inner(payload).await?;
        let result = serde_json::from_slice::<Response>(&body)?;
        let raw = match result {
            Response::Success { result, .. } => result.to_owned(),
            Response::Error { error: _, .. } => {
                // Needs to handle this better, but the error handling is problematic
                panic!();
            }
        };
        let res = serde_json::from_str(raw.get())?;
        Ok(res)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Request<'a, T> {
    id: u64,
    jsonrpc: &'a str,
    method: &'a str,
    params: T,
}

impl<'a, T> Request<'a, T> {
    /// Creates a new JSON RPC request, the id does not matter
    pub fn new(method: &'a str, params: T) -> Self {
        Self {
            id: 1,
            jsonrpc: "2.0",
            method,
            params,
        }
    }
}

#[derive(Deserialize, Debug, Clone, Error)]
pub struct JsonRpcError {
    /// The error code
    pub code: i64,
    /// The error message
    pub message: String,
    /// Additional data
    pub data: Option<Value>,
}

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(code: {}, message: {}, data: {:?})",
            self.code, self.message, self.data
        )
    }
}

#[derive(Debug)]
pub enum Response<'a> {
    Success { id: u64, result: &'a RawValue },
    Error { id: u64, error: JsonRpcError },
}

impl<'de: 'a, 'a> Deserialize<'de> for Response<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[allow(dead_code)]
        struct ResponseVisitor<'a>(&'a ());
        impl<'de: 'a, 'a> serde::de::Visitor<'de> for ResponseVisitor<'a> {
            type Value = Response<'a>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid jsonrpc 2.0 response object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut jsonrpc = false;

                // response & error
                let mut id = None;
                // only response
                let mut result = None;
                // only error
                let mut error = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "jsonrpc" => {
                            if jsonrpc {
                                return Err(serde::de::Error::duplicate_field("jsonrpc"));
                            }

                            let value = map.next_value()?;
                            if value != "2.0" {
                                return Err(serde::de::Error::invalid_value(
                                    serde::de::Unexpected::Str(value),
                                    &"2.0",
                                ));
                            }

                            jsonrpc = true;
                        }
                        "id" => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }

                            let value: u64 = map.next_value()?;
                            id = Some(value);
                        }
                        "result" => {
                            if result.is_some() {
                                return Err(serde::de::Error::duplicate_field("result"));
                            }

                            let value: &RawValue = map.next_value()?;
                            result = Some(value);
                        }
                        "error" => {
                            if error.is_some() {
                                return Err(serde::de::Error::duplicate_field("error"));
                            }

                            let value: JsonRpcError = map.next_value()?;
                            error = Some(value);
                        }
                        key => {
                            return Err(serde::de::Error::unknown_field(
                                key,
                                &["id", "jsonrpc", "result", "error"],
                            ))
                        }
                    }
                }

                // jsonrpc version must be present in all responses
                if !jsonrpc {
                    return Err(serde::de::Error::missing_field("jsonrpc"));
                }

                match (id, result, error) {
                    (Some(id), Some(result), None) => Ok(Response::Success { id, result }),
                    (Some(id), None, Some(error)) => Ok(Response::Error { id, error }),
                    _ => Err(serde::de::Error::custom(
                        "response must be either a success/error or notification object",
                    )),
                }
            }
        }

        deserializer.deserialize_map(ResponseVisitor(&()))
    }
}

/// The basic Ethereum queries that can be used from a smart contract and do not require
/// gas to be executed.
#[async_trait]
pub trait EthereumQueries {
    type Error;

    /// Lists all the accounts of the Ethereum node.
    async fn get_accounts(&self) -> Result<Vec<String>, Self::Error>;

    /// Gets the latest block number of the Ethereum node.
    async fn get_block_number(&self) -> Result<u64, Self::Error>;

    /// Gets the balance of the specified address at the specified block number.
    /// if no block number is specified then the balance of the latest block is
    /// returned.
    async fn get_balance(
        &self,
        address: &str,
        block_number: Option<u64>,
    ) -> Result<U256, Self::Error>;

    /// Reads the events of the smart contract.
    /// This is done from a specified `contract_address` and `event_name_expanded`.
    /// That is one should have "MyEvent(type1 indexed,type2)" instead
    /// of the usual "MyEvent(type1,type2)"
    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        starting_block: u64,
    ) -> Result<Vec<EthereumEvent>, Self::Error>;

    /// The operation done with `eth_call` on Ethereum returns
    /// a result but are not executed. This can be useful for example
    /// for executing function that are const and allow to inspect
    /// the contract without modifying it.
    async fn non_executive_call(
        &self,
        contract_address: &str,
        data: Bytes,
        from: &str,
    ) -> Result<Bytes, Self::Error>;
}

pub(crate) fn get_block_id(block_number: Option<u64>) -> BlockId {
    let number = match block_number {
        None => BlockNumberOrTag::Latest,
        Some(val) => BlockNumberOrTag::Number(val),
    };
    BlockId::Number(number)
}

#[async_trait]
impl<C> EthereumQueries for C
where
    C: JsonRpcClient+ Sync,
    EthereumServiceError: From<<C as JsonRpcClient>::Error>,
{
    type Error = EthereumServiceError;

    async fn get_accounts(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.request("eth_accounts", ()).await?)
    }

    async fn get_block_number(&self) -> Result<u64, Self::Error> {
        let result = self.request::<_,U64>("eth_blockNumber", ()).await?;
        Ok(result.to::<u64>())
    }

    async fn get_balance(
        &self,
        address: &str,
        block_number: Option<u64>,
    ) -> Result<U256, Self::Error> {
        let address = address.parse::<Address>()?;
        let tag = get_block_id(block_number);
        Ok(self.request("eth_getBalance", (address, tag)).await?)
    }

    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        starting_block: u64,
    ) -> Result<Vec<EthereumEvent>, Self::Error> {
        let contract_address = contract_address.parse::<Address>()?;
        let event_name = event_name_from_expanded(event_name_expanded);
        let filter = Filter::new()
            .address(contract_address)
            .event(&event_name)
            .from_block(starting_block);
        let events = self.request::<_,Vec<Log>>("eth_getLogs", (filter,)).await?;
        events
            .into_iter()
            .map(|x| parse_log(event_name_expanded, x))
            .collect::<Result<_, _>>()
    }

    async fn non_executive_call(
        &self,
        contract_address: &str,
        data: Bytes,
        from: &str,
    ) -> Result<Bytes, Self::Error> {
        let contract_address = contract_address.parse::<Address>()?;
        let from = from.parse::<Address>()?;
        let input = TransactionInput::new(data);
        let tx = TransactionRequest::default()
            .from(from)
            .to(contract_address)
            .input(input);
        Ok(self.request::<_,Bytes>("eth_call", (tx,)).await?)
    }
}
