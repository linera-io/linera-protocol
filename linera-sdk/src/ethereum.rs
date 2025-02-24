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

use crate::{
    contract::wit::base_runtime_api as contract_wit, service::wit::base_runtime_api as service_wit,
};

// TODO(#3143): Unify the two types into a single `EthereumClient` type.

/// A wrapper for a URL that implements `JsonRpcClient` and uses the JSON oracle to make requests.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ContractEthereumClient {
    /// The URL of the JSON-RPC server, without the method or parameters.
    pub url: String,
}
scalar!(ContractEthereumClient);

impl ContractEthereumClient {
    /// Creates a new [`ContractEthereumClient`] from an URL.
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

#[async_trait]
impl JsonRpcClient for ContractEthereumClient {
    type Error = EthereumServiceError;

    async fn get_id(&self) -> u64 {
        1
    }

    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let response = contract_wit::perform_http_request(
            &http::Request {
                method: http::Method::Post,
                url: self.url.clone(),
                headers: Vec::from([http::Header::new("Content-Type", b"application/json")]),
                body: payload,
            }
            .into(),
        );

        Ok(response.body)
    }
}

/// A wrapper for a URL that implements `JsonRpcClient` and uses the JSON oracle to make requests.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ServiceEthereumClient {
    /// The URL of the JSON-RPC server, without the method or parameters.
    pub url: String,
}
scalar!(ServiceEthereumClient);

impl ServiceEthereumClient {
    /// Creates a new [`ServiceEthereumClient`] from an URL.
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

#[async_trait]
impl JsonRpcClient for ServiceEthereumClient {
    type Error = EthereumServiceError;

    async fn get_id(&self) -> u64 {
        1
    }

    async fn request_inner(&self, payload: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let response = service_wit::perform_http_request(
            &http::Request {
                method: http::Method::Post,
                url: self.url.clone(),
                headers: Vec::from([http::Header::new("Content-Type", b"application/json")]),
                body: payload,
            }
            .into(),
        );

        Ok(response.body)
    }
}
