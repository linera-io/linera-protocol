// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper module to call the binaries of `linera-service` with appropriate command-line
//! arguments.

/// How to run Linera validators locally as native processes.
pub mod local_net;
/// How to run a linera wallet and its GraphQL service.
mod wallet;
/// How to run Linera validators locally as a Kubernetes deployment.
pub mod local_kubernetes_net;
/// How to run kind operations
mod kind_wrapper;
/// How to run docker operations
mod docker_wrapper;
/// How to run helm operations
mod helm_wrapper;
/// Util functions for the wrappers
mod util;

pub use wallet::{ApplicationWrapper, ClientWrapper, Faucet, NodeService};

use anyhow::Result;
use async_trait::async_trait;

/// The information needed to start a Linera net of a particular kind.
#[async_trait]
pub trait LineraNetConfig {
    type Net: LineraNet + Sized + Send + Sync + 'static;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)>;
}

/// A running Linera net.
#[async_trait]
pub trait LineraNet {
    fn ensure_is_running(&mut self) -> Result<()>;

    fn make_client(&mut self) -> ClientWrapper;

    async fn terminate(mut self) -> Result<()>;
}

/// Network protocol in use outside and inside a Linera net.
#[derive(Copy, Clone)]
pub enum Network {
    Grpc,
    Simple,
}

impl Network {
    fn internal(&self) -> &'static str {
        match self {
            Network::Grpc => "\"Grpc\"",
            Network::Simple => "{ Simple = \"Tcp\" }",
        }
    }

    fn external(&self) -> &'static str {
        match self {
            Network::Grpc => "\"Grpc\"",
            Network::Simple => "{ Simple = \"Tcp\" }",
        }
    }

    fn external_short(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Simple => "tcp",
        }
    }
}
