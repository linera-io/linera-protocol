// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides network abstractions and the data schemas for remote procedure
//! calls (RPCs) in the Linera protocol.

// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]
#![allow(clippy::blocks_in_conditions)]

pub mod config;
pub mod mass_client;
pub mod node_provider;

pub mod client;

mod message;

pub mod grpc;
pub mod simple;

pub use message::RpcMessage;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct HandleLiteCertificateRequest<'a> {
    pub certificate: linera_chain::data_types::LiteCertificate<'a>,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct HandleCertificateRequest {
    pub certificate: linera_chain::data_types::Certificate,
    pub wait_for_outgoing_messages: bool,
    pub blobs: Vec<linera_chain::data_types::HashedValue>,
}
