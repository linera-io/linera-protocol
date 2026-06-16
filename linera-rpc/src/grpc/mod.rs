// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod client;
mod conversions;
mod node_provider;
/// A pool of reusable gRPC transport channels.
pub mod pool;
#[cfg(with_server)]
mod server;
/// Transport-level configuration and channel construction for gRPC.
pub mod transport;

pub use client::*;
pub use conversions::*;
pub use node_provider::*;
#[cfg(with_server)]
pub use server::*;

/// The gRPC service and message types generated from `proto/rpc.proto`.
pub mod api {
    // Generated gRPC bindings; the generated items cannot carry doc comments.
    #![allow(missing_docs)]
    tonic::include_proto!("rpc.v1");
}

#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
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
/// The maximum gRPC message size, in bytes.
pub const GRPC_MAX_MESSAGE_SIZE: usize = 16 * MEBIBYTE;

/// Limit of gRPC message size up to which we will try to populate with data when estimating.
/// We leave 30% of buffer for the rest of the message and potential underestimation.
pub const GRPC_CHUNKED_MESSAGE_FILL_LIMIT: usize = GRPC_MAX_MESSAGE_SIZE * 7 / 10;

/// Prometheus label for the gRPC method name.
pub const METHOD_NAME_LABEL: &str = "method_name";

/// Prometheus label for distinguishing organic vs synthetic (benchmark) traffic.
pub const TRAFFIC_TYPE_LABEL: &str = "traffic_type";

/// Prometheus label for the error variant name, e.g. `"WorkerError::UnexpectedBlockHeight"`.
pub const ERROR_TYPE_LABEL: &str = "error_type";

/// Maximum length of a single proto identifier accepted as a service or method name.
/// Real proto identifiers are far shorter than this; the cap guards against attacker
/// input being recorded verbatim as a Prometheus label value.
const MAX_PROTO_IDENT_LEN: usize = 128;

/// Returns `true` if `s` is a valid protobuf identifier (`[A-Za-z][A-Za-z0-9_]*`)
/// within the length cap.
fn is_proto_identifier(s: &str) -> bool {
    if s.is_empty() || s.len() > MAX_PROTO_IDENT_LEN {
        return false;
    }
    let mut bytes = s.bytes();
    let first = bytes.next().expect("non-empty checked above");
    first.is_ascii_alphabetic() && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Returns `true` if `s` is a fully-qualified proto Service-Name: two or more
/// proto identifiers joined by dots, e.g. `package.Service` or `outer.inner.Service`.
fn is_proto_service_name(s: &str) -> bool {
    let mut parts = s.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    let Some(second) = parts.next() else {
        return false;
    };
    is_proto_identifier(first) && is_proto_identifier(second) && parts.all(is_proto_identifier)
}

/// Extracts the gRPC method name from a request URI path.
///
/// gRPC paths follow the HTTP/2 form `"/" Service-Name "/" {method name}`, where
/// `Service-Name` is `{proto package} "." {service name}` and the method name is a
/// single proto identifier. Anything that does not match this exact shape — health
/// probes, browser requests, bot scans probing for `.env` files, etc. — is mapped
/// to `"non_grpc"` so the value is safe to use as a Prometheus label without
/// blowing up cardinality or label-value length.
pub fn extract_grpc_method_name(path: &str) -> &str {
    let mut parts = path.splitn(3, '/');
    let (Some(""), Some(service), Some(method)) = (parts.next(), parts.next(), parts.next()) else {
        return "non_grpc";
    };
    if is_proto_service_name(service) && is_proto_identifier(method) {
        method
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
        assert_eq!(extract_grpc_method_name("/NoDotService/Method"), "non_grpc");
    }

    #[test]
    fn empty_path() {
        assert_eq!(extract_grpc_method_name(""), "non_grpc");
    }

    #[test]
    fn dot_env_bot_scan_does_not_leak_into_label() {
        assert_eq!(
            extract_grpc_method_name(
                "/.env.local/.env.production/.env.staging/.env.development/.env.test"
            ),
            "non_grpc"
        );
    }

    #[test]
    fn service_starting_with_dot_is_rejected() {
        assert_eq!(extract_grpc_method_name("/.foo.bar/Method"), "non_grpc");
    }

    #[test]
    fn method_with_extra_path_segments_is_rejected() {
        assert_eq!(
            extract_grpc_method_name("/foo.bar/Method/extra"),
            "non_grpc"
        );
    }

    #[test]
    fn method_with_invalid_characters_is_rejected() {
        assert_eq!(extract_grpc_method_name("/foo.bar/Method-x"), "non_grpc");
        assert_eq!(extract_grpc_method_name("/foo.bar/Method?x"), "non_grpc");
        assert_eq!(extract_grpc_method_name("/foo.bar/.Method"), "non_grpc");
    }

    #[test]
    fn identifier_starting_with_underscore_is_rejected() {
        assert_eq!(extract_grpc_method_name("/foo.bar/_Method"), "non_grpc");
        assert_eq!(extract_grpc_method_name("/_foo.bar/Method"), "non_grpc");
        assert_eq!(
            extract_grpc_method_name("/foo.bar/________________"),
            "non_grpc"
        );
    }

    #[test]
    fn empty_method_segment_is_rejected() {
        assert_eq!(extract_grpc_method_name("/foo.bar/"), "non_grpc");
    }

    #[test]
    fn empty_service_segment_is_rejected() {
        assert_eq!(extract_grpc_method_name("//Method"), "non_grpc");
    }

    #[test]
    fn service_with_consecutive_dots_is_rejected() {
        assert_eq!(extract_grpc_method_name("/foo..bar/Method"), "non_grpc");
    }

    #[test]
    fn overlong_method_is_rejected() {
        let long_method = "M".repeat(MAX_PROTO_IDENT_LEN + 1);
        let path = format!("/foo.bar/{long_method}");
        assert_eq!(extract_grpc_method_name(&path), "non_grpc");
    }

    #[test]
    fn path_without_leading_slash_is_rejected() {
        assert_eq!(extract_grpc_method_name("foo.bar/Method"), "non_grpc");
    }
}
