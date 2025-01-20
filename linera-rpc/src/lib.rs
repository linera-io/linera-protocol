// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides network abstractions and the data schemas for remote procedure
//! calls (RPCs) in the Linera protocol.

#![deny(clippy::large_futures)]
// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]
#![allow(clippy::blocks_in_conditions)]

pub mod config;
pub mod mass_client;
pub mod node_provider;

pub mod client;

mod message;
#[cfg(with_simple_network)]
pub mod simple;

pub mod grpc;

pub use message::RpcMessage;
pub use node_provider::NodeOptions;

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
