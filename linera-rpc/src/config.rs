// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::ChainId;
use linera_execution::committee::ValidatorName;
use serde::{Deserialize, Serialize};

#[cfg(with_simple_network)]
use crate::simple;

#[derive(Clone, Debug, clap::Parser)]
pub struct CrossChainConfig {
    /// Number of cross-chain messages allowed before dropping them.
    #[arg(long = "cross-chain-queue-size", default_value = "1000")]
    pub(crate) queue_size: usize,

    /// Maximum number of retries for a cross-chain message.
    #[arg(long = "cross-chain-max-retries", default_value = "10")]
    pub(crate) max_retries: u32,

    /// Delay before retrying of cross-chain message.
    #[arg(long = "cross-chain-retry-delay-ms", default_value = "2000")]
    pub(crate) retry_delay_ms: u64,

    /// Introduce a delay before sending every cross-chain message (e.g. for testing purpose).
    #[arg(long = "cross-chain-sender-delay-ms", default_value = "0")]
    pub(crate) sender_delay_ms: u64,

    /// Drop cross-chain messages randomly at the given rate (0 <= rate < 1) (meant for testing).
    #[arg(long = "cross-chain-sender-failure-rate", default_value = "0.0")]
    pub(crate) sender_failure_rate: f32,

    /// How many concurrent tasks to spawn for cross-chain message handling RPCs.
    #[arg(long = "cross-chain-max-tasks", default_value = "10")]
    pub(crate) max_concurrent_tasks: usize,
}

#[derive(Clone, Debug, clap::Parser)]
pub struct NotificationConfig {
    /// Number of notifications allowed before blocking the main server loop
    #[arg(long = "notification-queue-size", default_value = "1000")]
    pub(crate) notification_queue_size: usize,
}

pub type ShardId = usize;

/// The network configuration of a shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardConfig {
    /// The host name (e.g an IP address).
    pub host: String,
    /// The port.
    pub port: u16,
    /// The host on which metrics are served.
    pub metrics_host: String,
    /// The port on which metrics are served.
    pub metrics_port: Option<u16>,
}

impl ShardConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn http_address(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

/// The network protocol.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkProtocol {
    #[cfg(with_simple_network)]
    Simple(simple::TransportProtocol),
    Grpc(TlsConfig),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TlsConfig {
    ClearText,
    Tls,
}

impl NetworkProtocol {
    fn scheme(&self) -> &'static str {
        match self {
            #[cfg(with_simple_network)]
            NetworkProtocol::Simple(transport) => transport.scheme(),
            NetworkProtocol::Grpc(tls) => match tls {
                TlsConfig::ClearText => "http",
                TlsConfig::Tls => "https",
            },
        }
    }
}

/// The network configuration for all shards.
pub type ValidatorInternalNetworkConfig = ValidatorInternalNetworkPreConfig<NetworkProtocol>;

/// The public network configuration for a validator.
pub type ValidatorPublicNetworkConfig = ValidatorPublicNetworkPreConfig<NetworkProtocol>;

/// The network configuration for all shards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInternalNetworkPreConfig<P> {
    /// The name of the validator.
    pub name: ValidatorName,
    /// The network protocol to use for all shards.
    pub protocol: P,
    /// The available shards. Each chain UID is mapped to a unique shard in the vector in
    /// a static way.
    pub shards: Vec<ShardConfig>,
    /// The host name of the proxy on the internal network (IP or hostname).
    pub host: String,
    /// The port the proxy listens on the internal network.
    pub port: u16,
    /// The host name of the proxy's metrics endpoint.
    pub metrics_host: String,
    /// The port of the proxy's metrics endpoint.
    pub metrics_port: u16,
}

impl<P> ValidatorInternalNetworkPreConfig<P> {
    pub fn clone_with_protocol<Q>(&self, protocol: Q) -> ValidatorInternalNetworkPreConfig<Q> {
        ValidatorInternalNetworkPreConfig {
            name: self.name,
            protocol,
            shards: self.shards.clone(),
            host: self.host.clone(),
            port: self.port,
            metrics_host: self.metrics_host.clone(),
            metrics_port: self.metrics_port,
        }
    }
}

impl ValidatorInternalNetworkConfig {
    pub fn proxy_address(&self) -> String {
        format!("{}://{}:{}", self.protocol.scheme(), self.host, self.port)
    }
}

impl ValidatorPublicNetworkConfig {
    pub fn http_address(&self) -> String {
        format!("{}://{}:{}", self.protocol.scheme(), self.host, self.port)
    }
}

/// The public network configuration for a validator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorPublicNetworkPreConfig<P> {
    /// The network protocol to use for the validator frontend.
    pub protocol: P,
    /// The host name of the validator (IP or hostname).
    pub host: String,
    /// The port the validator listens on.
    pub port: u16,
}

impl<P> ValidatorPublicNetworkPreConfig<P> {
    pub fn clone_with_protocol<Q>(&self, protocol: Q) -> ValidatorPublicNetworkPreConfig<Q> {
        ValidatorPublicNetworkPreConfig {
            protocol,
            host: self.host.clone(),
            port: self.port,
        }
    }
}

impl<P> std::fmt::Display for ValidatorPublicNetworkPreConfig<P>
where
    P: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.protocol, self.host, self.port)
    }
}

impl std::fmt::Display for NetworkProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(with_simple_network)]
            NetworkProtocol::Simple(protocol) => write!(f, "{:?}", protocol),
            NetworkProtocol::Grpc(tls) => match tls {
                TlsConfig::ClearText => write!(f, "grpc"),
                TlsConfig::Tls => write!(f, "grpcs"),
            },
        }
    }
}

impl<P> std::str::FromStr for ValidatorPublicNetworkPreConfig<P>
where
    P: std::str::FromStr,
    P::Err: std::fmt::Display,
{
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(':').collect::<Vec<_>>();
        anyhow::ensure!(
            parts.len() == 3,
            "Expecting format `(tcp|udp|grpc|grpcs):host:port`"
        );
        let protocol = parts[0].parse().map_err(|s| anyhow::anyhow!("{}", s))?;
        let host = parts[1].to_owned();
        let port = parts[2].parse()?;
        Ok(ValidatorPublicNetworkPreConfig {
            protocol,
            host,
            port,
        })
    }
}

impl std::str::FromStr for NetworkProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let protocol = match s {
            "grpc" => Self::Grpc(TlsConfig::ClearText),
            "grpcs" => Self::Grpc(TlsConfig::Tls),
            #[cfg(with_simple_network)]
            s => Self::Simple(simple::TransportProtocol::from_str(s)?),
            #[cfg(not(with_simple_network))]
            s => return Err(format!("unsupported protocol: {s:?}")),
        };
        Ok(protocol)
    }
}

impl<P> ValidatorInternalNetworkPreConfig<P> {
    /// Static shard assignment
    pub fn get_shard_id(&self, chain_id: ChainId) -> ShardId {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        // Use the validator public key to randomise shard assignment.
        self.name.hash(&mut s);
        chain_id.hash(&mut s);
        (s.finish() as ShardId) % self.shards.len()
    }

    pub fn shard(&self, shard_id: ShardId) -> &ShardConfig {
        &self.shards[shard_id]
    }

    /// Gets the [`ShardConfig`] of the shard assigned to the `chain_id`.
    pub fn get_shard_for(&self, chain_id: ChainId) -> &ShardConfig {
        self.shard(self.get_shard_id(chain_id))
    }
}
