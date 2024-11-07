// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use thiserror::Error;

use crate::RpcMessage;

#[derive(Error, Debug)]
pub enum MassClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Tonic transport error: {0}")]
    TonicTransport(#[from] crate::grpc::transport::Error),
    #[error("Conversion error: {0}")]
    Conversion(#[from] crate::grpc::GrpcProtoConversionError),
    #[error("RPC error: {0}")]
    Rpc(#[from] tonic::Status),
}

#[async_trait]
pub trait MassClient {
    async fn send(
        &self,
        requests: Vec<RpcMessage>,
        max_in_flight: usize,
    ) -> Result<Vec<RpcMessage>, MassClientError>;
}
