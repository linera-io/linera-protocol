// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use thiserror::Error;

use crate::RpcMessage;

#[derive(Error, Debug)]
pub enum MassClientError {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("tonic transport: {0}")]
    TonicTransport(#[from] crate::grpc::transport::Error),
    #[error("conversion error: {0}")]
    Conversion(#[from] crate::grpc::GrpcProtoConversionError),
    #[error("error while making a remote call: {0}")]
    Rpc(#[from] tonic::Status),
}

#[async_trait]
pub trait MassClient {
    async fn send(&self, requests: Vec<RpcMessage>) -> Result<Vec<RpcMessage>, MassClientError>;
}
