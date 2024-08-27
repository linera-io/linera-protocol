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
    pub certificate: linera_chain::data_types::LiteCertificate<'a>,
    pub wait_for_outgoing_messages: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct HandleCertificateRequest {
    pub certificate: linera_chain::data_types::Certificate,
    pub wait_for_outgoing_messages: bool,
    pub blobs: Vec<linera_base::data_types::Blob>,
}

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("file_descriptor_set");
