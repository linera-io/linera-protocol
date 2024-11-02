// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for Linera applications that interact with Ethereum or other EVM contracts.

use std::fmt::Debug;

use async_graphql::scalar;
use async_trait::async_trait;
use linera_base::http;
pub use linera_ethereum::{
    client::EthereumQueries,
    common::{EthereumDataType, EthereumEvent},
};
use linera_ethereum::{client::JsonRpcClient, common::EthereumServiceError};
use serde::{Deserialize, Serialize};

use crate::contract::wit::contract_system_api;

/// A wrapper for a URL that implements `JsonRpcClient` and uses the JSON oracle to make requests.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct EthereumClient {
    /// The URL of the JSON-RPC server, without the method or parameters.
    pub url: String,
}
scalar!(EthereumClient);

impl EthereumClient {
    /// Creates a new `EthereumClient` from an URL.
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

#[async_trait]
impl JsonRpcClient for EthereumClient {
    type Error = EthereumServiceError;

    async fn get_id(&self) -> u64 {
        1
    }

    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let response = contract_system_api::http_request(
            &http::Request {
                method: http::Method::Post,
                url: self.url.clone(),
                headers: Vec::from([("Content-Type".to_owned(), b"application/json".to_vec())]),
                body: payload,
            }
            .into(),
        );

        Ok(response.body)
    }
}
