// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, net::SocketAddr};

use linera_rpc::config::{ExporterServiceConfig, TlsConfig};
use serde::{
    de::{Error, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

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

    /// The address to expose the `/metrics` endpoint on.
    pub metrics_port: u16,
}

impl BlockExporterConfig {
    /// Returns the address to expose the `/metrics` endpoint on.
    pub fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.metrics_port))
    }
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    Indexer {
        /// The gRPC network protocol.
        tls: TlsConfig,
        /// The host name of the target destination (IP or hostname).
        endpoint: String,
        /// The port number of the target destination.
        port: u16,
    },
    Validator {
        /// The host name of the target destination (IP or hostname).
        endpoint: String,
        /// The port number of the target destination.
        port: u16,
    },
    Logging {
        /// The host name of the target destination (IP or hostname).
        file_name: String,
    },
}

/// The description for the gRPC based destination.
/// Discriminates the export mode and the client to use.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum DestinationKind {
    /// The indexer description.
    Indexer,
    /// The validator description.
    Validator,
    /// The logging target.
    Logging,
}

impl Serialize for Destination {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            Destination::Indexer {
                tls,
                endpoint,
                port,
            } => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("kind", "Indexer")?;
                map.serialize_entry("tls", tls)?;
                map.serialize_entry("endpoint", endpoint)?;
                map.serialize_entry("port", port)?;
                map.end()
            }
            Destination::Validator { endpoint, port } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("kind", "Validator")?;
                map.serialize_entry("endpoint", endpoint)?;
                map.serialize_entry("port", port)?;
                map.end()
            }
            Destination::Logging { file_name } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("kind", "Logging")?;
                map.serialize_entry("file_name", file_name)?;
                map.end()
            }
        }
    }
}

struct DestinationVisitor;

impl<'de> Visitor<'de> for DestinationVisitor {
    type Value = Destination;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a map with a 'kind' field")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Destination, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut kind: Option<String> = None;
        let mut tls: Option<TlsConfig> = None;
        let mut endpoint: Option<String> = None;
        let mut port: Option<u16> = None;
        let mut file_name: Option<String> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "kind" => {
                    if kind.is_some() {
                        return Err(V::Error::duplicate_field("kind"));
                    }
                    kind = Some(map.next_value()?);
                }
                "tls" => {
                    if tls.is_some() {
                        return Err(V::Error::duplicate_field("tls"));
                    }
                    tls = Some(map.next_value()?);
                }
                "endpoint" => {
                    if endpoint.is_some() {
                        return Err(V::Error::duplicate_field("endpoint"));
                    }
                    endpoint = Some(map.next_value()?);
                }
                "port" => {
                    if port.is_some() {
                        return Err(V::Error::duplicate_field("port"));
                    }
                    port = Some(map.next_value()?);
                }
                "file_name" => {
                    if file_name.is_some() {
                        return Err(V::Error::duplicate_field("file_name"));
                    }
                    file_name = Some(map.next_value()?);
                }
                _ => {
                    // Ignore unknown fields
                    let _: serde::de::IgnoredAny = map.next_value()?;
                }
            }
        }

        let kind = kind.ok_or_else(|| V::Error::missing_field("kind"))?;

        match kind.as_str() {
            "Indexer" => {
                let tls = tls.ok_or_else(|| V::Error::missing_field("tls"))?;
                let endpoint = endpoint.ok_or_else(|| V::Error::missing_field("endpoint"))?;
                let port = port.ok_or_else(|| V::Error::missing_field("port"))?;
                Ok(Destination::Indexer {
                    tls,
                    endpoint,
                    port,
                })
            }
            "Validator" => {
                let endpoint = endpoint.ok_or_else(|| V::Error::missing_field("endpoint"))?;
                let port = port.ok_or_else(|| V::Error::missing_field("port"))?;
                Ok(Destination::Validator { endpoint, port })
            }
            "Logging" => {
                let file_name = file_name.ok_or_else(|| V::Error::missing_field("file_name"))?;
                Ok(Destination::Logging { file_name })
            }
            _ => Err(V::Error::unknown_variant(
                &kind,
                &["Indexer", "Validator", "Logging"],
            )),
        }
    }
}
impl<'de> Deserialize<'de> for Destination {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DestinationVisitor)
    }
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
        match &self {
            Destination::Indexer {
                tls,
                endpoint,
                port,
            } => {
                let tls = match tls {
                    TlsConfig::ClearText => "http",
                    TlsConfig::Tls => "https",
                };

                format!("{}://{}:{}", tls, endpoint, port)
            }

            Destination::Validator { endpoint, port } => {
                format!("{}:{}:{}", "grpc", endpoint, port)
            }

            Destination::Logging { file_name } => file_name.to_string(),
        }
    }

    pub fn id(&self) -> DestinationId {
        let kind = match self {
            Destination::Indexer { .. } => DestinationKind::Indexer,
            Destination::Validator { .. } => DestinationKind::Validator,
            Destination::Logging { .. } => DestinationKind::Logging,
        };
        DestinationId {
            address: self.address(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_from_str() {
        let input = r#"
                        tls = "ClearText"
                        endpoint = "127.0.0.1"
                        port = 8080
                        kind = "Indexer"
            "#
        .to_string();

        let destination: Destination = toml::from_str(&input).unwrap();
        assert_eq!(
            destination,
            Destination::Indexer {
                tls: TlsConfig::ClearText,
                endpoint: "127.0.0.1".to_owned(),
                port: 8080,
            }
        );

        let input = r#"
                        endpoint = "127.0.0.1"
                        port = 8080
                        kind = "Validator"
        "#
        .to_string();
        let destination: Destination = toml::from_str(&input).unwrap();
        assert_eq!(
            destination,
            Destination::Validator {
                endpoint: "127.0.0.1".to_owned(),
                port: 8080,
            }
        );

        let input = r#"
                        file_name = "export.log"
                        kind = "Logging"
        "#
        .to_string();
        let destination: Destination = toml::from_str(&input).unwrap();
        assert_eq!(
            destination,
            Destination::Logging {
                file_name: "export.log".to_owned(),
            }
        );
    }
}
