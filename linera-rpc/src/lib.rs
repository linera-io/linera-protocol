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

/// Computes a Full Jitter delay for exponential backoff.
///
/// Uses the AWS-recommended formula: `sleep = random(0, min(cap, base * 2^attempt))`.
/// Reference: <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>
pub(crate) fn full_jitter_delay(
    base_delay: std::time::Duration,
    attempt: u32,
    max_backoff: std::time::Duration,
) -> std::time::Duration {
    use rand::Rng as _;
    let exponential_delay =
        base_delay.saturating_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX));
    let capped_delay = exponential_delay.min(max_backoff);
    std::time::Duration::from_millis(
        rand::thread_rng().gen_range(0..=capped_delay.as_millis() as u64),
    )
}
