// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::transport::TransportProtocol;
use linera_base::messages::ChainId;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
pub struct CrossChainConfig {
    /// Number of cross-chains messages allowed before blocking the main server loop
    #[structopt(long = "cross_chain_queue_size", default_value = "1")]
    pub(crate) queue_size: usize,
    /// Maximum number of retries for a cross-chain message.
    #[structopt(long = "cross_chain_max_retries", default_value = "10")]
    pub(crate) max_retries: usize,
    /// Delay before retrying of cross-chain message.
    #[structopt(long = "cross_chain_retry_delay_ms", default_value = "2000")]
    pub(crate) retry_delay_ms: u64,
}

pub type ShardId = usize;

/// The network configuration of a shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Address {
    /// The host name (e.g an IP address).
    host: String,
    /// The port.
    port: u16,
}

impl Address {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn http_address(&self) -> String {
        format!("http://{}", self)
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// The network protocol.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkProtocol {
    Simple(TransportProtocol),
    Grpc,
}

/// The network configuration for all shards.
pub type ValidatorInternalNetworkConfig = ValidatorInternalNetworkPreConfig<NetworkProtocol>;

/// The public network configuration for a validator.
pub type ValidatorPublicNetworkConfig = ValidatorPublicNetworkPreConfig<NetworkProtocol>;

/// The network configuration for all shards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInternalNetworkPreConfig<P> {
    /// The network protocol to use for all shards.
    pub protocol: P,
    /// The available shards. Each chain UID is mapped to a unique shard in the vector in
    /// a static way.
    pub shards: Shards,
}

impl<P> ValidatorInternalNetworkPreConfig<P> {
    pub fn clone_with_protocol<Q>(&self, protocol: Q) -> ValidatorInternalNetworkPreConfig<Q> {
        ValidatorInternalNetworkPreConfig {
            protocol,
            shards: self.shards.clone(),
        }
    }
}

/// A structure holding the internal sharding configuration
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shards(Vec<Address>);

/// The public network configuration for a validator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorPublicNetworkPreConfig<P> {
    /// The network protocol to use for the validator frontend.
    pub protocol: P,
    /// The address of the validator.
    pub address: Address,
}

impl<P> ValidatorPublicNetworkPreConfig<P> {
    pub fn clone_with_protocol<Q>(&self, protocol: Q) -> ValidatorPublicNetworkPreConfig<Q> {
        ValidatorPublicNetworkPreConfig {
            protocol,
            address: self.address.clone(),
        }
    }

    pub fn http_address(&self) -> String {
        self.address.http_address()
    }
}

impl<P> std::fmt::Display for ValidatorPublicNetworkPreConfig<P>
where
    P: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.protocol, self.address)
    }
}

impl std::fmt::Display for NetworkProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkProtocol::Simple(protocol) => write!(f, "{}", protocol),
            NetworkProtocol::Grpc => write!(f, "grpc"),
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
        let parts: Vec<&str> = s.split(':').collect();
        anyhow::ensure!(
            parts.len() == 3,
            "Expecting format `(tcp|udp|grpc):host:port`"
        );
        let protocol = parts[0].parse().map_err(|s| anyhow::anyhow!("{}", s))?;
        let host = parts[1].to_owned();
        let port = parts[2].parse()?;
        Ok(ValidatorPublicNetworkPreConfig {
            protocol,
            address: Address {
                host,
                port,
            }
        })
    }
}

impl std::str::FromStr for NetworkProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let protocol = match s {
            "grpc" => Self::Grpc,
            _ => Self::Simple(TransportProtocol::from_str(s)?),
        };
        Ok(protocol)
    }
}

impl<P> ValidatorInternalNetworkPreConfig<P> {
    /// Return a reference to internal [`Shards`]
    pub fn shards(&self) -> &Shards {
        &self.shards
    }

    /// Static shard assignment
    pub fn get_shard_id(&self, chain_id: ChainId) -> ShardId {
        self.shards.get_shard_id(chain_id)
    }

    /// Get a shard for a give [`ShardId`]
    pub fn shard_address(&self, shard_id: ShardId) -> &Address {
        self.shards.shard_address(shard_id)
    }

    /// Get the [`ShardConfig`] of the shard assigned to the `chain_id`.
    pub fn get_shard_for(&self, chain_id: ChainId) -> &Address {
        self.shards.get_shard_for(chain_id)
    }
}

impl Shards {
    pub fn get_shard_id(&self, chain_id: ChainId) -> ShardId {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        chain_id.hash(&mut s);
        (s.finish() as ShardId) % self.0.len()
    }

    pub fn shard_address(&self, shard_id: ShardId) -> &Address {
        &self.0[shard_id]
    }

    pub fn get_shard_for(&self, chain_id: ChainId) -> &Address {
        self.shard_address(self.get_shard_id(chain_id))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl FromIterator<Address> for Shards {
    fn from_iter<T: IntoIterator<Item =Address>>(iter: T) -> Self {
        Shards(iter.into_iter().collect())
    }
}
