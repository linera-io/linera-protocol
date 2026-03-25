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

    #[error("failed to execute task to completion: {0}")]
    Join(#[from] futures::channel::oneshot::Canceled),

    #[error("failed to parse socket address: {0}")]
    SocketAddr(#[from] std::net::AddrParseError),

    #[cfg(with_server)]
    #[error(transparent)]
    Reflection(#[from] tonic_reflection::server::Error),
}

const MEBIBYTE: usize = 1024 * 1024;
pub const GRPC_MAX_MESSAGE_SIZE: usize = 16 * MEBIBYTE;

/// Limit of gRPC message size up to which we will try to populate with data when estimating.
/// We leave 30% of buffer for the rest of the message and potential underestimation.
pub const GRPC_CHUNKED_MESSAGE_FILL_LIMIT: usize = GRPC_MAX_MESSAGE_SIZE * 7 / 10;

/// Prometheus label for the gRPC method name.
pub const METHOD_NAME_LABEL: &str = "method_name";

/// Prometheus label for distinguishing organic vs synthetic (benchmark) traffic.
pub const TRAFFIC_TYPE_LABEL: &str = "traffic_type";

/// Extracts the gRPC method name from a request URI path.
///
/// gRPC paths have the form `/{package}.{Service}/{Method}` — the first segment
/// always contains a dot. Non-gRPC requests (health checks, bot probes, etc.) return
/// `"non_grpc"` to prevent unbounded label cardinality.
pub fn extract_grpc_method_name(path: &str) -> &str {
    let parts: Vec<&str> = path.splitn(3, '/').collect();
    if parts.len() == 3 && parts[1].contains('.') {
        parts[2]
    } else {
        "non_grpc"
    }
}

#[cfg(test)]
mod method_name_tests {
    use super::*;

    #[test]
    fn grpc_unary_method() {
        assert_eq!(
            extract_grpc_method_name("/rpc.v1.ValidatorNode/HandleBlockProposal"),
            "HandleBlockProposal"
        );
    }

    #[test]
    fn grpc_streaming_method() {
        assert_eq!(
            extract_grpc_method_name("/rpc.v1.ValidatorNode/SubscribeToNotifications"),
            "SubscribeToNotifications"
        );
    }

    #[test]
    fn health_check_path() {
        assert_eq!(
            extract_grpc_method_name("/grpc.health.v1.Health/Check"),
            "Check"
        );
    }

    #[test]
    fn non_grpc_root_path() {
        assert_eq!(extract_grpc_method_name("/"), "non_grpc");
    }

    #[test]
    fn non_grpc_plain_path() {
        assert_eq!(extract_grpc_method_name("/healthz"), "non_grpc");
    }

    #[test]
    fn non_grpc_no_dot_in_service() {
        assert_eq!(
            extract_grpc_method_name("/NoDotService/Method"),
            "non_grpc"
        );
    }

    #[test]
    fn empty_path() {
        assert_eq!(extract_grpc_method_name(""), "non_grpc");
    }
}
