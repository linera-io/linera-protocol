// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//use std::time::Duration;

use alloy::{
    primitives::{Address, U256},
    providers::{Provider, RootProvider, ProviderBuilder},
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        BlockId, BlockNumberOrTag, Filter,
    },
};

use alloy_primitives::Bytes;
use eyre::Result;

use crate::common::{event_name_from_expanded, parse_log, EthereumEvent, EthereumServiceError};

use reqwest::Client;
pub type HttpProvider = RootProvider<alloy::transports::http::Http<Client>>;

/// The Ethereum endpoint and its provider used for accessing the ethereum node.
pub struct EthereumEndpoint<M> {
    pub provider: M,
}

//
impl EthereumEndpoint<HttpProvider>
{
    /// Lists all the accounts of the Ethereum node.
    pub async fn get_accounts(&self) -> Result<Vec<String>, EthereumServiceError> {
        let mut accounts = Vec::new();
        for account in self.provider.get_accounts().await? {
            let account = format!("{:?}", account);
            accounts.push(account);
        }
        Ok(accounts)
    }

    /// Gets the latest block number of the Ethereum node.
    pub async fn get_block_number(&self) -> Result<u64, EthereumServiceError> {
        Ok(self.provider.get_block_number().await?)
    }

    /// Gets the balance of the specified address at the specified block number.
    /// if no block number is specified then the balance of the latest block is
    /// returned.
    pub async fn get_balance(
        &self,
        address: &str,
        block_nr: Option<u64>,
    ) -> Result<U256, EthereumServiceError> {
        let address = address.parse::<Address>()?;
        let number = match block_nr {
            None => BlockNumberOrTag::Latest,
            Some(val) => BlockNumberOrTag::Number(val),
        };
        let block_id = BlockId::Number(number);
        let balance = self.provider.get_balance(address, block_id).await?;
        Ok(balance)
    }

    /// Reads the events of the smart contract.
    /// This is done from a specified `contract_address` and `event_name_expanded`.
    /// That is one should have "MyEvent(type1 indexed,type2)" instead
    /// of the usual "MyEvent(type1,type2)"
    pub async fn read_events(
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
        let events = events
            .into_iter()
            .map(|x| parse_log(event_name_expanded, x))
            .collect::<Result<_, _>>()?;
        Ok(events)
    }

    /// The operation done with `eth_call` on Ethereum returns
    /// a result but are not executed. This can be useful for example
    /// for executing function that are const and allow to inspect
    /// the contract without modifying it.
    pub async fn non_executive_call(
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
        let eth_call = self.provider.call(&tx).await?;
        Ok(eth_call)
    }
}

impl EthereumEndpoint<HttpProvider> {
    /// Connects to an existing Ethereum node and creates an `EthereumEndpoint`
    /// if successful.
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        let rpc_url = reqwest::Url::parse(&url)?;
        let provider = ProviderBuilder::new().on_http(rpc_url);
        let endpoint = Self { provider };
        Ok(endpoint)
    }
}
