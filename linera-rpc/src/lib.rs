// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! This module provides network abstractions and the data schemas for remote procedure
//! calls (RPCs) in the Linera protocol.

#![deny(missing_docs)]
// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]

/// Network configuration types for validators, shards, and cross-chain messaging.
pub mod config;
/// Construction of validator-node clients from network configuration.
pub mod node_provider;

/// A network-agnostic client for talking to a validator node.
pub mod client;

mod cross_chain_message_queue;
mod message;
/// Propagation of OpenTelemetry trace context across RPC boundaries.
#[cfg(feature = "opentelemetry")]
pub mod propagation;
/// The simple custom-TCP/UDP network transport.
#[cfg(with_simple_network)]
pub mod simple;

/// The gRPC network transport.
pub mod grpc;

pub use client::Client;
pub use message::RpcMessage;
pub use node_provider::{NodeOptions, NodeProvider, DEFAULT_MAX_BACKOFF};

/// A request to handle a lite certificate.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleLiteCertRequest<'a> {
    /// The lite certificate to handle.
    pub certificate: linera_chain::types::LiteCertificate<'a>,
    /// Whether to wait for the resulting cross-chain messages to be delivered.
    pub wait_for_outgoing_messages: bool,
}

/// A request to handle a confirmed-block certificate.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleConfirmedCertificateRequest {
    /// The confirmed-block certificate to handle.
    pub certificate: linera_chain::types::ConfirmedBlockCertificate,
    /// Whether to wait for the resulting cross-chain messages to be delivered.
    pub wait_for_outgoing_messages: bool,
}

/// A request to handle a validated-block certificate.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleValidatedCertificateRequest {
    /// The validated-block certificate to handle.
    pub certificate: linera_chain::types::ValidatedBlockCertificate,
}

/// A request to handle a timeout certificate.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleTimeoutCertificateRequest {
    /// The timeout certificate to handle.
    pub certificate: linera_chain::types::TimeoutCertificate,
}

/// The protobuf file descriptor set for the RPC service, used for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("file_descriptor_set");

/// A self-signed TLS certificate (PEM), generated at build time for local testing.
#[cfg(not(target_arch = "wasm32"))]
pub const CERT_PEM: &str = include_str!(concat!(env!("OUT_DIR"), "/self_signed_cert.pem"));
/// The private key (PEM) matching [`CERT_PEM`], generated at build time for local testing.
#[cfg(not(target_arch = "wasm32"))]
pub const KEY_PEM: &str = include_str!(concat!(env!("OUT_DIR"), "/private_key.pem"));

/// Computes a jittered exponential backoff delay.
///
/// Uses the gRPC-recommended approach: compute `min(cap, base * 2^attempt)`,
/// then apply ±20% jitter. This guarantees a minimum delay of 80% of the
/// computed backoff, preventing instant retries.
///
/// Reference: <https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md>
pub(crate) fn jittered_backoff_delay(
    base_delay: std::time::Duration,
    attempt: u32,
    max_backoff: std::time::Duration,
) -> std::time::Duration {
    use rand::Rng as _;
    let exponential_delay =
        base_delay.saturating_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX));
    let capped_delay_ms = exponential_delay.min(max_backoff).as_millis() as u64;
    let min_delay_ms = capped_delay_ms * 4 / 5; // 80%
    let max_delay_ms = capped_delay_ms * 6 / 5; // 120%
    std::time::Duration::from_millis(rand::thread_rng().gen_range(min_delay_ms..=max_delay_ms))
}
