// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_rpc::config::{ExporterServiceConfig, TlsConfig};
use serde::{Deserialize, Serialize};

/// The configuration file for the linera-exporter.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockExporterConfig {
    /// Identity for the block exporter state.
    pub id: u32,

    /// The server configuration for the linera-exporter.
    pub service_config: ExporterServiceConfig,

    /// The configuration file for the export destinations.
    #[serde(default)]
    pub destination_config: DestinationConfig,

    /// The configuration file to impose various limits
    /// on the resources used by the linera-exporter.
    #[serde(default)]
    pub limits: LimitsConfig,
}

/// Configuration file for the exports.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DestinationConfig {
    /// The destination URIs to export to.
    pub destinations: Vec<Destination>,
    /// Export blocks to the current committee.
    #[serde(default)]
    pub committee_destination: bool,
}

// Each destination has an ID and a configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DestinationId {
    address: String,
    kind: DestinationKind,
}

impl DestinationId {
    /// Creates a new destination ID from the address and kind.
    pub fn new(address: String, kind: DestinationKind) -> Self {
        Self { address, kind }
    }

    pub fn validator(address: String) -> Self {
        Self {
            address,
            kind: DestinationKind::Validator,
        }
    }

    /// Returns the address of the destination.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns the kind of the destination.
    pub fn kind(&self) -> DestinationKind {
        self.kind
    }
}

/// The uri to provide export services to.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Destination {
    /// The gRPC network protocol.
    pub tls: TlsConfig,
    /// The host name of the target destination (IP or hostname).
    pub endpoint: String,
    /// The port number of the target destination.
    pub port: u16,
    /// The description for the gRPC based destination.
    /// Discriminates the export mode and the client to use.
    pub kind: DestinationKind,
}

/// The description for the gRPC based destination.
/// Discriminates the export mode and the client to use.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum DestinationKind {
    /// The indexer description.
    Indexer,
    /// The validator description.
    Validator,
}

/// The configuration file to impose various limits
/// on the resources used by the linera-exporter.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct LimitsConfig {
    /// Time period in milliseconds between periodic persistence
    /// to the shared storage.
    pub persistence_period_ms: u32,
    /// Maximum size of the work queue i.e. maximum number
    /// of blocks queued up for exports per destination.
    pub work_queue_size: u16,
    /// Maximum weight of the blob cache in megabytes.
    pub blob_cache_weight_mb: u16,
    /// Estimated number of elements for the blob cache.
    pub blob_cache_items_capacity: u16,
    /// Maximum weight of the block cache in megabytes.
    pub block_cache_weight_mb: u16,
    /// Estimated number of elements for the block cache.
    pub block_cache_items_capacity: u16,
    /// Maximum weight in megabytes for the combined
    /// cache, consisting of small miscellaneous items.
    pub auxiliary_cache_size_mb: u16,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            persistence_period_ms: 299 * 1000,
            work_queue_size: 256,
            blob_cache_weight_mb: 1024,
            blob_cache_items_capacity: 8192,
            block_cache_weight_mb: 1024,
            block_cache_items_capacity: 8192,
            auxiliary_cache_size_mb: 1024,
        }
    }
}

impl Destination {
    pub fn address(&self) -> String {
        match self.kind {
            DestinationKind::Indexer => {
                let tls = match self.tls {
                    TlsConfig::ClearText => "http",
                    TlsConfig::Tls => "https",
                };

                format!("{}://{}:{}", tls, self.endpoint, self.port)
            }

            DestinationKind::Validator => {
                format!("{}:{}:{}", "grpc", self.endpoint, self.port)
            }
        }
    }

    pub fn id(&self) -> DestinationId {
        DestinationId {
            address: self.address(),
            kind: self.kind,
        }
    }
}
