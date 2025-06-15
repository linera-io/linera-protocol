// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy::transports::http::reqwest::{header::CONTENT_TYPE, Client};
use async_lock::Mutex;
use async_trait::async_trait;

use crate::{client::JsonRpcClient, common::EthereumServiceError};

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
    /// Connects to an existing Ethereum node and creates an `EthereumClientSimplified`
    /// if successful.
    pub fn new(url: String) -> Self {
        let id = Mutex::new(1);
        Self { url, id }
    }
}
