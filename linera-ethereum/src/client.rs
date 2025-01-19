// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use alloy::{
    primitives::{Address, Bytes, U256, U64},
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        BlockId, BlockNumberOrTag, Filter, Log,
    },
};
use async_trait::async_trait;
use linera_base::ensure;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::common::{
    event_name_from_expanded, parse_log, EthereumEvent, EthereumQueryError, EthereumServiceError,
};

/// A basic RPC client for making JSON queries
#[async_trait]
pub trait JsonRpcClient {
    type Error: From<serde_json::Error> + From<EthereumQueryError>;

    /// The inner function that has to be implemented and access the client
    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error>;

    /// Gets a new ID for the next message.
    async fn get_id(&self) -> u64;

    /// The function doing the parsing of the input and output.
    async fn request<T, R>(&self, method: &str, params: T) -> Result<R, Self::Error>
    where
        T: Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send,
    {
        let id = self.get_id().await;
        let payload = JsonRpcRequest::new(id, method, params);
        let payload = serde_json::to_vec(&payload)?;
        let body = self.request_inner(payload).await?;
        let result = serde_json::from_slice::<JsonRpcResponse>(&body)?;
        let raw = result.result;
        let res = serde_json::from_str(raw.get())?;
        ensure!(id == result.id, EthereumQueryError::IdIsNotMatching);
        ensure!(
            *"2.0" == result.jsonrpc,
            EthereumQueryError::WrongJsonRpcVersion
        );
        Ok(res)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonRpcRequest<'a, T> {
    id: u64,
    jsonrpc: &'a str,
    method: &'a str,
    params: T,
}

impl<'a, T> JsonRpcRequest<'a, T> {
    /// Creates a new JSON RPC request, the id does not matter
    pub fn new(id: u64, method: &'a str, params: T) -> Self {
        Self {
            id,
            jsonrpc: "2.0",
            method,
            params,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse {
    id: u64,
    jsonrpc: String,
    result: Box<RawValue>,
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
    async fn get_balance(&self, address: &str, block_number: u64) -> Result<U256, Self::Error>;

    /// Reads the events of the smart contract.
    ///
    /// This is done from a specified `contract_address` and `event_name_expanded`.
    /// That is one should have "MyEvent(type1 indexed,type2)" instead
    /// of the usual "MyEvent(type1,type2)"
    ///
    /// The `from_block` is inclusive.
    /// The `to_block` is exclusive (contrary to Ethereum where it is inclusive)
    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<EthereumEvent>, Self::Error>;

    /// The operation done with `eth_call` on Ethereum returns
    /// a result but are not committed to the blockchain. This can be useful for example
    /// for executing function that are const and allow to inspect
    /// the contract without modifying it.
    async fn non_executive_call(
        &self,
        contract_address: &str,
        data: Bytes,
        from: &str,
        block: u64,
    ) -> Result<Bytes, Self::Error>;
}

pub(crate) fn get_block_id(block_number: u64) -> BlockId {
    let number = BlockNumberOrTag::Number(block_number);
    BlockId::Number(number)
}

#[async_trait]
impl<C> EthereumQueries for C
where
    C: JsonRpcClient + Sync,
    EthereumServiceError: From<<C as JsonRpcClient>::Error>,
{
    type Error = EthereumServiceError;

    async fn get_accounts(&self) -> Result<Vec<String>, Self::Error> {
        let results: Vec<String> = self.request("eth_accounts", ()).await?;
        Ok(results
            .into_iter()
            .map(|x| x.to_lowercase())
            .collect::<Vec<_>>())
    }

    async fn get_block_number(&self) -> Result<u64, Self::Error> {
        let result = self.request::<_, U64>("eth_blockNumber", ()).await?;
        Ok(result.to::<u64>())
    }

    async fn get_balance(&self, address: &str, block_number: u64) -> Result<U256, Self::Error> {
        let address = address.parse::<Address>()?;
        let tag = get_block_id(block_number);
        Ok(self.request("eth_getBalance", (address, tag)).await?)
    }

    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<EthereumEvent>, Self::Error> {
        let contract_address = contract_address.parse::<Address>()?;
        let event_name = event_name_from_expanded(event_name_expanded);
        let filter = Filter::new()
            .address(contract_address)
            .event(&event_name)
            .from_block(from_block)
            .to_block(to_block - 1);
        let events = self
            .request::<_, Vec<Log>>("eth_getLogs", (filter,))
            .await?;
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
        block: u64,
    ) -> Result<Bytes, Self::Error> {
        let contract_address = contract_address.parse::<Address>()?;
        let from = from.parse::<Address>()?;
        let input = TransactionInput::new(data);
        let tx = TransactionRequest::default()
            .from(from)
            .to(contract_address)
            .input(input);
        let tag = get_block_id(block);
        Ok(self.request::<_, Bytes>("eth_call", (tx, tag)).await?)
    }
}
