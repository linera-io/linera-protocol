// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::transport::NetworkProtocol;
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
pub struct ShardConfig {
    /// The host name (e.g an IP address).
    pub host: String,
    /// The port.
    pub port: u16,
}

/// The network configuration for all shards.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInternalNetworkConfig {
    /// The network protocol to use for all shards.
    pub protocol: NetworkProtocol,
    /// The available shards. Each chain UID is mapped to a unique shard in the vector in
    /// a static way.
    pub shards: Vec<ShardConfig>,
}

/// The public network configuration for a validator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorPublicNetworkConfig {
    /// The host name of the validator (IP or hostname).
    pub host: String,
    /// The port the validator listens on.
    pub port: u16,
}

impl std::fmt::Display for ValidatorPublicNetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl std::str::FromStr for ValidatorPublicNetworkConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        anyhow::ensure!(parts.len() == 2, "Expecting format `host:port`");
        let host = parts[0].to_owned();
        let port = parts[1].parse()?;
        Ok(ValidatorPublicNetworkConfig { host, port })
    }
}

impl ValidatorInternalNetworkConfig {
    /// Static shard assignment
    pub fn get_shard_id(&self, chain_id: ChainId) -> ShardId {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        chain_id.hash(&mut s);
        (s.finish() as ShardId) % self.shards.len()
    }

    pub fn shard(&self, shard_id: ShardId) -> &ShardConfig {
        &self.shards[shard_id]
    }

    /// Get the [`ShardConfig`] of the shard assigned to the `chain_id`.
    pub fn get_shard_for(&self, chain_id: ChainId) -> &ShardConfig {
        self.shard(self.get_shard_id(chain_id))
    }
}
