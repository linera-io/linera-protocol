// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod client;
mod conversions;
mod node_provider;
pub mod pool;
#[cfg(with_server)]
mod server;
pub mod transport;

pub use client::*;
pub use conversions::*;
pub use node_provider::*;
#[cfg(with_server)]
pub use server::*;

pub mod api {
    tonic::include_proto!("rpc.v1");
}

#[derive(thiserror::Error, Debug)]
pub enum GrpcError {
    #[error("failed to connect to address: {0}")]
    ConnectionFailed(#[from] transport::Error),

    #[error("failed to communicate cross-chain queries: {0}")]
    CrossChain(#[from] tonic::Status),

    #[error("failed to execute task to completion")]
    Join(#[from] futures::channel::oneshot::Canceled),

    #[error("failed to parse socket address: {0}")]
    SocketAddr(#[from] std::net::AddrParseError),

    #[error(transparent)]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),

    #[cfg(with_server)]
    #[error(transparent)]
    Reflection(#[from] tonic_reflection::server::Error),
}

const MEBIBYTE: usize = 1024 * 1024;
pub const GRPC_MAX_MESSAGE_SIZE: usize = 16 * MEBIBYTE;

/// Limit of gRPC message size up to which we will try to populate with data when estimating.
/// We leave 30% of buffer for the rest of the message and potential underestimation.
pub const GRPC_CHUNKED_MESSAGE_FILL_LIMIT: usize = GRPC_MAX_MESSAGE_SIZE * 7 / 10;
