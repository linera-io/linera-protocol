// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy::rpc::types::eth::Log;
use async_trait::async_trait;
use alloy::{
    primitives::{Address, U256},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        BlockId, BlockNumberOrTag, Filter,
    },
    transports::http::reqwest::Client,
};
use alloy_primitives::Bytes;
use alloy_primitives::U64;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use url::Url;

use crate::common::{event_name_from_expanded, parse_log, EthereumEvent, EthereumServiceError};
pub type HttpProvider = RootProvider<alloy::transports::http::Http<Client>>;

/// The basic JsonRpcClient that we need for running the Json queroies
#[async_trait]
pub trait JsonRpcClient {
    type Error;
    async fn request<T: Debug + Serialize + Send + Sync, R: DeserializeOwned + Send>(&self, method: &str, params: T) -> Result<R, Self::Error>;
}

#[async_trait]
impl JsonRpcClient for EthereumEndpointSimplified {
    type Error = EthereumServiceError;
    async fn request<T: Debug + Serialize + Send + Sync, R: DeserializeOwned + Send>(&self, method: &str, params: T) -> Result<R, Self::Error> {
        let params = serde_json::to_string(&params).expect("Failed to serialize parameters");
        let _url = format!("{}?method={method}&params={params}", self.url);
        let json = "triggering_entry";
        //        let json = contract_system_api::fetch_json(&url);
        Ok(serde_json::from_str(&json).expect("Failed to deserialize JSON response"))
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


/// The Ethereum endpoint and its provider used for accessing the ethereum node.
pub struct EthereumEndpointSimplified {
    pub url: String,
}

fn get_block_id(block_number: Option<u64>) -> BlockId {
    let number = match block_number {
        None => BlockNumberOrTag::Latest,
        Some(val) => BlockNumberOrTag::Number(val),
    };
    BlockId::Number(number)
}

#[async_trait]
impl EthereumQueries for EthereumEndpointSimplified {
    type Error = EthereumServiceError;

    async fn get_accounts(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.request("eth_accounts", ()).await?)
    }

    async fn get_block_number(&self) -> Result<u64, Self::Error> {
        let result: U64 = self.request("eth_blockNumber", ()).await?;
        Ok(result.to::<u64>())
    }

    async fn get_balance(
        &self,
        address: &str,
        block_number: Option<u64>,
    ) -> Result<U256, Self::Error> {
        let address = address.parse::<Address>()?;
        let tag = get_block_id(block_number);
        self.request("eth_getBalance", (address, tag)).await
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
        let events: Vec<Log> = self.request("eth_getLogs", (filter,)).await?;
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
        let result: Bytes = self.request("eth_call", (tx,)).await?;
        Ok(result)
    }
}

impl EthereumEndpointSimplified {
    /// Connects to an existing Ethereum node and creates an `EthereumEndpoint`
    /// if successful.
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        let endpoint = Self { url };
        Ok(endpoint)
    }
}


pub struct EthereumEndpoint<M> {
    pub provider: M,
}

#[async_trait]
impl EthereumQueries for EthereumEndpoint<HttpProvider> {
    type Error = EthereumServiceError;

    async fn get_accounts(&self) -> Result<Vec<String>, EthereumServiceError> {
        Ok(self
            .provider
            .get_accounts()
            .await?
            .into_iter()
            .map(|x| format!("{:?}", x))
            .collect::<Vec<_>>())
    }

    async fn get_block_number(&self) -> Result<u64, EthereumServiceError> {
        Ok(self.provider.get_block_number().await?)
    }

    async fn get_balance(
        &self,
        address: &str,
        block_number: Option<u64>,
    ) -> Result<U256, EthereumServiceError> {
        let address = address.parse::<Address>()?;
        let number = match block_number {
            None => BlockNumberOrTag::Latest,
            Some(val) => BlockNumberOrTag::Number(val),
        };
        let block_id = BlockId::Number(number);
        Ok(self.provider.get_balance(address, block_id).await?)
    }

    async fn read_events(
        &self,
        contract_address: &str,
        event_name_expanded: &str,
        starting_block: u64,
    ) -> Result<Vec<EthereumEvent>, EthereumServiceError> {
        let contract_address = contract_address.parse::<Address>()?;
        let event_name = event_name_from_expanded(event_name_expanded);
        let filter = Filter::new()
            .address(contract_address)
            .event(&event_name)
            .from_block(starting_block);
        let events = self.provider.get_logs(&filter).await?;
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
    ) -> Result<Bytes, EthereumServiceError> {
        let contract_address = contract_address.parse::<Address>()?;
        let from = from.parse::<Address>()?;
        let input = TransactionInput::new(data);
        let tx = TransactionRequest::default()
            .from(from)
            .to(contract_address)
            .input(input);
        Ok(self.provider.call(&tx).await?)
    }
}

impl EthereumEndpoint<HttpProvider> {
    /// Connects to an existing Ethereum node and creates an `EthereumEndpoint`
    /// if successful.
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        let rpc_url = Url::parse(&url)?;
        let provider = ProviderBuilder::new().on_http(rpc_url);
        let endpoint = Self { provider };
        Ok(endpoint)
    }
}
