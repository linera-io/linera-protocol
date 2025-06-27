// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::iter::IntoIterator;

use linera_base::{
    crypto::{AccountPublicKey, BcsSignable, CryptoHash, ValidatorPublicKey, ValidatorSecretKey},
    data_types::{
        Amount, Blob, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, NetworkDescription,
        Timestamp,
    },
    identifiers::ChainId,
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_persistent as persistent;
use linera_rpc::config::{
    ExporterServiceConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
};
use linera_storage::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("persistence error: {0}")]
    Persistence(Box<dyn std::error::Error + Send + Sync>),
    #[error("storage is already initialized: {0:?}")]
    StorageIsAlreadyInitialized(Box<NetworkDescription>),
    #[error("no admin chain configured")]
    NoAdminChain,
}

use crate::util;

util::impl_from_dynamic!(Error:Persistence, persistent::memory::Error);
#[cfg(web)]
util::impl_from_dynamic!(Error:Persistence, persistent::indexed_db::Error);
#[cfg(feature = "fs")]
util::impl_from_dynamic!(Error:Persistence, persistent::file::Error);

/// The public configuration of a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// The public key of the validator.
    pub public_key: ValidatorPublicKey,
    /// The account key of the validator.
    pub account_key: AccountPublicKey,
    /// The network configuration for the validator.
    pub network: ValidatorPublicNetworkConfig,
}

/// The private configuration of a validator service.
#[derive(Serialize, Deserialize)]
pub struct ValidatorServerConfig {
    pub validator: ValidatorConfig,
    pub validator_secret: ValidatorSecretKey,
    pub internal_network: ValidatorInternalNetworkConfig,
}

/// The (public) configuration for all validators.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct CommitteeConfig {
    pub validators: Vec<ValidatorConfig>,
}

impl CommitteeConfig {
    pub fn into_committee(self, policy: ResourceControlPolicy) -> Committee {
        let validators = self
            .validators
            .into_iter()
            .map(|v| {
                (
                    v.public_key,
                    ValidatorState {
                        network_address: v.network.to_string(),
                        votes: 100,
                        account_public_key: v.account_key,
                    },
                )
            })
            .collect();
        Committee::new(validators, policy)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: Committee,
    pub timestamp: Timestamp,
    pub chains: Vec<ChainDescription>,
    pub network_name: String,
}

impl BcsSignable<'_> for GenesisConfig {}

fn make_chain(
    index: u32,
    public_key: AccountPublicKey,
    balance: Amount,
    timestamp: Timestamp,
) -> ChainDescription {
    let origin = ChainOrigin::Root(index);
    let config = InitialChainConfig {
        application_permissions: Default::default(),
        balance,
        min_active_epoch: Epoch::ZERO,
        max_active_epoch: Epoch::ZERO,
        epoch: Epoch::ZERO,
        ownership: ChainOwnership::single(public_key.into()),
    };
    ChainDescription::new(origin, config, timestamp)
}

impl GenesisConfig {
    /// Creates a `GenesisConfig` with the first chain being the admin chain.
    pub fn new(
        committee: CommitteeConfig,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
        admin_public_key: AccountPublicKey,
        admin_balance: Amount,
    ) -> Self {
        let committee = committee.into_committee(policy);
        let admin_chain = make_chain(0, admin_public_key, admin_balance, timestamp);
        Self {
            committee,
            timestamp,
            chains: vec![admin_chain],
            network_name,
        }
    }

    pub fn add_root_chain(
        &mut self,
        public_key: AccountPublicKey,
        balance: Amount,
    ) -> ChainDescription {
        let description = make_chain(
            self.chains.len() as u32,
            public_key,
            balance,
            self.timestamp,
        );
        self.chains.push(description.clone());
        description
    }

    pub fn admin_chain_description(&self) -> &ChainDescription {
        &self.chains[0]
    }

    pub fn admin_id(&self) -> ChainId {
        self.admin_chain_description().id()
    }

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        if let Some(description) = storage
            .read_network_description()
            .await
            .map_err(linera_chain::ChainError::from)?
        {
            return Err(Error::StorageIsAlreadyInitialized(Box::new(description)));
        }
        let network_description = self.network_description();
        storage
            .write_blob(&self.committee_blob())
            .await
            .map_err(linera_chain::ChainError::from)?;
        storage
            .write_network_description(&network_description)
            .await
            .map_err(linera_chain::ChainError::from)?;
        for description in &self.chains {
            storage.create_chain(description.clone()).await?;
        }
        Ok(())
    }

    pub fn hash(&self) -> CryptoHash {
        CryptoHash::new(self)
    }

    pub fn committee_blob(&self) -> Blob {
        Blob::new_committee(
            bcs::to_bytes(&self.committee).expect("serializing a committee should succeed"),
        )
    }

    pub fn network_description(&self) -> NetworkDescription {
        NetworkDescription {
            name: self.network_name.clone(),
            genesis_config_hash: CryptoHash::new(self),
            genesis_timestamp: self.timestamp,
            genesis_committee_blob_hash: self.committee_blob().id().hash,
            admin_chain_id: self.admin_id(),
        }
    }
}

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
}

// Each destination has an ID and a configuration.
pub type DestinationId = u16;

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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy)]
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
}
