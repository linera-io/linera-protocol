// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcMessage;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MassClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tonic transport: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("conversion error: {0}")]
    Conversion(#[from] crate::conversions::ProtoConversionError),
    #[error("error while making a remote call: {0}")]
    Rpc(#[from] tonic::Status),
}

#[async_trait]
pub trait MassClient: Send + Sync {
    async fn send(&mut self, requests: Vec<RpcMessage>)
        -> Result<Vec<RpcMessage>, MassClientError>;
}
