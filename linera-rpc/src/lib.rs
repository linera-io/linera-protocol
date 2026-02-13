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
#[cfg(with_simple_network)]
pub mod simple;

pub mod grpc;

#[cfg(feature = "opentelemetry")]
pub mod propagation;

pub use client::Client;
pub use message::{RpcMessage, ShardInfo};
pub use node_provider::{NodeOptions, NodeProvider};

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

/// Maximum bit-shift exponent for u32: `1u32 << 31` is the largest valid shift before overflow.
const MAX_SHIFT_EXPONENT: u32 = 31;

/// Absolute cap on backoff delay. Google Cloud uses 30s, AWS uses 20s, gRPC uses 120s.
/// We use 30s as a reasonable middle ground.
/// References:
/// - <https://cloud.google.com/storage/docs/retry-strategy>
/// - <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>
/// - <https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md>
const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

/// Computes a Full Jitter delay for exponential backoff.
///
/// Uses the AWS-recommended formula: `sleep = random(0, min(cap, base * 2^attempt))`.
/// Reference: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
pub(crate) fn full_jitter_delay(
    base_delay: std::time::Duration,
    attempt: u32,
) -> std::time::Duration {
    use rand::Rng as _;
    let exponential_delay = base_delay.saturating_mul(1u32 << attempt.min(MAX_SHIFT_EXPONENT));
    let capped_delay = exponential_delay.min(MAX_BACKOFF);
    std::time::Duration::from_millis(
        rand::thread_rng().gen_range(0..=capped_delay.as_millis() as u64),
    )
}
