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

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod api {
    tonic::include_proto!("rpc.v1");
}

#[derive(thiserror::Error, Debug)]
pub enum GrpcError {
    #[error("failed to connect to address: {0}")]
    ConnectionFailed(#[from] transport::Error),

    #[error("failed to communicate cross-chain queries: {0}")]
    CrossChain(#[from] tonic::Status),

    #[error("failed to execute task to completion: {0}")]
    Join(#[from] tokio::sync::oneshot::error::RecvError),

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
