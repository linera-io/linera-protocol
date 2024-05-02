// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for Linera applications that interact with Ethereum or other EVM contracts.

use std::fmt::Debug;

use ethers::providers::{JsonRpcClient, ProviderError};
use serde::{de::DeserializeOwned, Serialize};

use crate::contract::wit::contract_system_api;

/// A wrapper for a URL that implements `JsonRpcClient` and uses the JSON oracle to make requests.
#[derive(Debug)]
pub struct EthereumClient {
    /// The URL of the JSON-RPC server, without the method or parameters.
    pub url: String,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl JsonRpcClient for EthereumClient {
    type Error = ProviderError;

    async fn request<T, R>(&self, method: &str, params: T) -> Result<R, Self::Error>
    where
        T: Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send,
    {
        let params = serde_json::to_string(&params).expect("Failed to serialize parameters");
        let url = format!("{}?method={method}&params={params}", self.url);
        let json = contract_system_api::fetch_json(&url);
        Ok(serde_json::from_str(&json).expect("Failed to deserialize JSON response"))
    }
}
