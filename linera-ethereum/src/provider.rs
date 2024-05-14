// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy::{
    primitives::{Address, U256},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::types::eth::{
        request::{TransactionInput, TransactionRequest},
        Filter,
    },
    transports::http::reqwest::{header::CONTENT_TYPE, Client},
};
use alloy_primitives::Bytes;
use async_lock::Mutex;
use async_trait::async_trait;
use url::Url;

use crate::client::{EthereumQueries, JsonRpcClient};

pub type HttpProvider = RootProvider<alloy::transports::http::Http<Client>>;

use crate::{
    client::get_block_id,
    common::{event_name_from_expanded, parse_log, EthereumEvent, EthereumServiceError},
};

/// The Ethereum endpoint and its provider used for accessing the Ethereum node.
pub struct EthereumClientSimplified {
    pub url: String,
    pub id: Mutex<u64>,
}

#[async_trait]
impl JsonRpcClient for EthereumClientSimplified {
    type Error = EthereumServiceError;

    async fn get_id(&self) -> u64 {
        let mut id = self.id.lock().await;
        *id += 1;
        *id
    }

    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let res = Client::new()
            .post(self.url.clone())
            .body(payload)
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await?;
        let body = res.bytes().await?;
        Ok(body.as_ref().to_vec())
    }
}

impl EthereumClientSimplified {
    /// Connects to an existing Ethereum node and creates an `EthereumEndpoint`
    /// if successful.
    pub fn new(url: String) -> Self {
        let id = Mutex::new(1);
        Self { url, id }
    }
}

pub struct EthereumClient<M> {
    pub provider: M,
}

#[async_trait]
impl EthereumQueries for EthereumClient<HttpProvider> {
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
        let block_id = get_block_id(block_number);
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

impl EthereumClient<HttpProvider> {
    /// Connects to an existing Ethereum node and creates an `EthereumClient`
    /// if successful.
    pub fn new(url: String) -> Result<Self, EthereumServiceError> {
        let rpc_url = Url::parse(&url)?;
        let provider = ProviderBuilder::new().on_http(rpc_url);
        let endpoint = Self { provider };
        Ok(endpoint)
    }
}
