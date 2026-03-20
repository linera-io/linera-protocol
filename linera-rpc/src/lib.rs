// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides network abstractions and the data schemas for remote procedure
//! calls (RPCs) in the Linera protocol.

// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]

pub mod config;
pub mod node_provider;

pub mod client;

mod cross_chain_message_queue;
mod message;
#[cfg(feature = "opentelemetry")]
pub mod propagation;
#[cfg(with_simple_network)]
pub mod simple;

pub mod grpc;

pub use client::Client;
pub use message::RpcMessage;
pub use node_provider::{NodeOptions, NodeProvider, DEFAULT_MAX_BACKOFF};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleLiteCertRequest<'a> {
    pub certificate: linera_chain::types::LiteCertificate<'a>,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleConfirmedCertificateRequest {
    pub certificate: linera_chain::types::ConfirmedBlockCertificate,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleValidatedCertificateRequest {
    pub certificate: linera_chain::types::ValidatedBlockCertificate,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleTimeoutCertificateRequest {
    pub certificate: linera_chain::types::TimeoutCertificate,
}

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("file_descriptor_set");

#[cfg(not(target_arch = "wasm32"))]
pub const CERT_PEM: &str = include_str!(concat!(env!("OUT_DIR"), "/self_signed_cert.pem"));
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
