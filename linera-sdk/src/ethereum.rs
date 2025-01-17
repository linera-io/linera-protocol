// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for Linera applications that interact with Ethereum or other EVM contracts.

use std::fmt::Debug;

use async_graphql::scalar;
use async_trait::async_trait;
pub use linera_ethereum::{
    client::EthereumQueries,
    common::{EthereumDataType, EthereumEvent},
};
use linera_ethereum::{client::JsonRpcClient, common::EthereumServiceError};
use serde::{Deserialize, Serialize};

use crate::{contract::wit::contract_system_api, service::wit::service_system_api};

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
        let content_type = "application/json";
        Ok(contract_system_api::http_post(
            &self.url,
            content_type,
            &payload,
        ))
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
        let content_type = "application/json";
        Ok(service_system_api::http_post(
            &self.url,
            content_type,
            &payload,
        ))
    }
}
