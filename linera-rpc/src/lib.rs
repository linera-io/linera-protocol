// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides network abstractions and the data schemas for remote procedure
//! calls (RPCs) in the Linera protocol.

pub mod config;
pub mod grpc_network;
pub mod grpc_pool;
pub mod mass;
pub mod node_provider;
pub mod simple_network;
pub mod transport;

mod client;
mod codec;
mod conversions;
mod rpc;

pub use rpc::{HandleCertificateRequest, HandleLiteCertificateRequest, RpcMessage};
