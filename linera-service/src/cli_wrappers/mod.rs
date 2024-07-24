// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper module to call the binaries of `linera-service` with appropriate command-line
//! arguments.

#[cfg(feature = "kubernetes")]
/// How to run docker operations
pub mod docker;

#[cfg(feature = "kubernetes")]
/// How to run helmfile operations
mod helmfile;
#[cfg(feature = "kubernetes")]
/// How to run kind operations
mod kind;
#[cfg(feature = "kubernetes")]
/// How to run kubectl operations
mod kubectl;
#[cfg(feature = "kubernetes")]
/// How to run Linera validators locally as a Kubernetes deployment.
pub mod local_kubernetes_net;
/// How to run Linera validators locally as native processes.
pub mod local_net;
#[cfg(all(with_testing, feature = "remote-net"))]
/// How to connect to running GCP DevNet.
pub mod remote_net;
#[cfg(feature = "kubernetes")]
/// Util functions for the wrappers
mod util;
/// How to run a linera wallet and its GraphQL service.
mod wallet;

use anyhow::Result;
use async_trait::async_trait;
use linera_execution::ResourceControlPolicy;
pub use wallet::{ApplicationWrapper, ClientWrapper, Faucet, FaucetOption, NodeService};

/// The information needed to start a Linera net of a particular kind.
#[async_trait]
pub trait LineraNetConfig {
    type Net: LineraNet + Sized + Send + Sync + 'static;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)>;

    async fn policy(&self) -> ResourceControlPolicy;
}

/// A running Linera net.
#[async_trait]
pub trait LineraNet {
    async fn ensure_is_running(&mut self) -> Result<()>;

    async fn make_client(&mut self) -> ClientWrapper;

    async fn terminate(&mut self) -> Result<()>;
}

/// Network protocol in use outside and inside a Linera net.
#[derive(Copy, Clone)]
pub enum Network {
    Grpc,
    Tcp,
    Udp,
}

impl Network {
    fn internal(&self) -> &'static str {
        match self {
            Network::Grpc => "{ Grpc = \"ClearText\" }",
            Network::Tcp => "{ Simple = \"Tcp\" }",
            Network::Udp => "{ Simple = \"Udp\" }",
        }
    }

    fn external(&self) -> &'static str {
        match self {
            Network::Grpc => "{ Grpc = \"ClearText\" }",
            Network::Tcp => "{ Simple = \"Tcp\" }",
            Network::Udp => "{ Simple = \"Udp\" }",
        }
    }

    pub fn external_short(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Tcp => "tcp",
            Network::Udp => "udp",
        }
    }
}
