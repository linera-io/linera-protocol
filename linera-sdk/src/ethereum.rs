// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for Linera applications that interact with Ethereum or other EVM contracts.

use std::fmt::Debug;

use linera_ethereum::{client::JsonRpcClient, common::EthereumServiceError};
use serde::{de::DeserializeOwned, Serialize};

use crate::contract::wit::contract_system_api;

/// A wrapper for a URL that implements `JsonRpcClient` and uses the JSON oracle to make requests.
#[derive(Debug)]
pub struct EthereumClient {
    /// The URL of the JSON-RPC server, without the method or parameters.
    pub url: String,
    pub id: Mutex<u64>,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl JsonRpcClient for EthereumClient {
    type Error = EthereumServiceError;

    async fn get_id(&self) -> u64 {
        let mut id = self.id.lock().await;
        *id += 1;
        *id
    }

    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error>
    {
        contract_system_api::fetch_json(&self.url, payload)
    }
}
