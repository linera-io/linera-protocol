// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    iter::IntoIterator,
    num::{NonZero, NonZeroU32, NonZeroU8},
    path::PathBuf,
};

use linera_base::{
    crypto::{AccountPublicKey, BcsSignable, CryptoHash, ValidatorPublicKey, ValidatorSecretKey},
    data_types::{
        Amount, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, NetworkDescription,
        Timestamp,
    },
    ensure,
    identifiers::ChainId,
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{
    ExporterServiceConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
};
use linera_storage::Storage;
use serde::{Deserialize, Serialize};
use walrus_core::{EncodingType, EpochCount as WalrusEpochCount};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("persistence error: {0}")]
    Persistence(Box<dyn std::error::Error + Send + Sync>),
    #[error("storage is already initialized: {0:?}")]
    StorageIsAlreadyInitialized(NetworkDescription),
    #[error("no admin chain configured")]
    NoAdminChain,
}

use crate::{persistent, util};

util::impl_from_dynamic!(Error:Persistence, persistent::memory::Error);
#[cfg(with_indexed_db)]
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
    committee: &Committee,
    index: u32,
    public_key: AccountPublicKey,
    balance: Amount,
    timestamp: Timestamp,
) -> ChainDescription {
    let committees: BTreeMap<_, _> = [(
        Epoch::ZERO,
        bcs::to_bytes(committee).expect("serializing a committee should not fail"),
    )]
    .into_iter()
    .collect();
    let origin = ChainOrigin::Root(index);
    let config = InitialChainConfig {
        application_permissions: Default::default(),
        balance,
        committees: committees.clone(),
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
        let admin_chain = make_chain(&committee, 0, admin_public_key, admin_balance, timestamp);
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
            &self.committee,
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
            return Err(Error::StorageIsAlreadyInitialized(description));
        }
        let network_description = self.network_description();
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

    pub fn network_description(&self) -> NetworkDescription {
        NetworkDescription {
            name: self.network_name.clone(),
            genesis_config_hash: CryptoHash::new(self),
            genesis_timestamp: self.timestamp,
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
    /// Export data to walrus cold storage.
    pub walrus: Option<WalrusConfig<WalrusStoreConfig>>,
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

impl Destination {
    pub fn indexer_address(&self) -> String {
        let tls = match self.tls {
            TlsConfig::ClearText => "http",
            TlsConfig::Tls => "https",
        };

        format!("{}://{}:{}", tls, self.endpoint, self.port)
    }

    pub fn validator_address(&self) -> String {
        format!("{}:{}:{}", "grpc", self.endpoint, self.port)
    }
}

/// The configuration file to impose various limits
/// on the resources used by the linera-exporter.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(default)]
pub struct LimitsConfig {
    /// Time period in seconds between periodic persistence
    /// to the shared storage.
    pub persistence_period: u16,
    /// Maximum size of the work queue i.e. maximum number
    /// of blocks queued up for exports per destination.
    pub work_queue_size: u16,
    /// Maximum weight of the blob cache in megabytes.
    pub blob_cache_weight: u16,
    /// Estimated number of elements for the blob cache.
    pub blob_cache_items_capacity: u16,
    /// Maximum weight of the block cache in megabytes.
    pub block_cache_weight: u16,
    /// Estimated number of elements for the block cache.
    pub block_cache_items_capacity: u16,
    /// Maximum weight in megabytes for the combined
    /// cache, consisting of small miscellaneous items.
    pub auxiliary_cache_size: u16,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            persistence_period: 299,
            work_queue_size: 256,
            blob_cache_weight: 1024,
            blob_cache_items_capacity: 8192,
            block_cache_weight: 1024,
            block_cache_items_capacity: 8192,
            auxiliary_cache_size: 1024,
        }
    }
}

/// The configuration file for the Linera Walrus client.
///
/// This typically includes the paths to where to look for the resources such as the walrus log directory,
/// the `client_config.yaml` used by the inner Walrus client, and the Sui wallet configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalrusConfig<T> {
    /// The path to the directory where the walrus log files can be found.
    pub log: PathBuf,
    /// The path to `client_config.yaml` file. This file describes the configuration
    /// which is used by the linera walrus client to build the inner/core walrus client.
    ///
    /// The optional sui wallet path parameter in this file, if included, will be
    /// overidden by the wallet path provided directly to the linera walrus client.
    pub client_config: PathBuf,
    /// The path to the YAML file which describes the configuration for the sui wallet
    /// to use for all the on-chain actions.
    pub wallet_config: PathBuf,
    /// The configuration context to use for the client, if omitted the default_context is used.
    #[serde(default)]
    pub context: Option<String>,
    /// Extra functionality this configuration is going to be used for.
    /// Typically used for to discriminate read or write access for walrus, for now.
    #[serde(flatten)]
    #[serde(default)]
    pub behaviour: T,
}

/// The configuration file for the Linera Walrus client with on-chain write access capabilities.
///
/// This configuration typically includes parameters required for storing and modifying data, such as
/// epoch ranges, batch sizes, flags for dry-run or deletion, and encoding options.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct WalrusStoreConfig {
    // Maximum size for the log file before rotating
    // and switching to a fresh file.
    pub max_log_size: NonZeroU32,
    /// The epoch argument to specify either the number of epochs to store the blobs.
    #[serde(default)]
    pub epochs: WalrusEpoch,
    /// Approximate maximum size of a batch that is to be uploaded and stored on walrus.
    /// Can be either in weight or numebr of blocks.
    #[serde(default)]
    pub batch_size: WalrusBatchSize,
    /// Timeout duration in seconds before sending a batch.
    /// If a batch is pending, this will be the maximum time between subsequent batches.
    pub batch_timeout: NonZeroU8,
    /// If set, performs a dry-run of the store actions without performing any actions on chain.
    ///
    /// This assumes that the force field is set i.e., it does not check the current status of the blobs.
    #[serde(default)]
    pub dry_run: bool,
    /// If set, the linera walrus client does not check for the blob statuses before storing them.
    ///
    /// This will create a new blob even if the blob is already certified for a sufficient
    /// duration.
    #[serde(default)]
    pub force: bool,
    /// Ignore the storage resources owned by the wallet.
    ///
    /// The client will not check if it can reuse existing resources, and just check the blob
    /// status on chain.
    pub ignore_resources: bool,
    /// If set, marks the blobs as deletable.
    ///
    /// Deletable blobs can be removed from Walrus before their expiration time.
    #[serde(default)]
    pub deletable: bool,
    /// If set, puts the blobs into a shared blobs object.
    pub share: bool,
    /// The encoding type to use for encoding the files.
    #[serde(default)]
    pub encoding_type: Option<EncodingType>,
}

impl Default for WalrusStoreConfig {
    fn default() -> Self {
        Self {
            max_log_size: NonZero::new(8 * 1024 * 1024).unwrap(),
            batch_timeout: NonZero::new(2).unwrap(),
            batch_size: WalrusBatchSize::default(),
            epochs: WalrusEpoch::default(),
            ignore_resources: true,
            encoding_type: None,
            deletable: false,
            dry_run: false,
            force: false,
            share: true,
        }
    }
}

/// The number of epochs to store the blobs for.
///
/// Can be either a non-zero number of epochs or the special value `max`, which will store the blobs
/// for the maximum number of epochs allowed by the system object on chain.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default, Copy)]
pub enum WalrusEpoch {
    /// Store the blobs for the maximum number of epochs allowed.
    #[default]
    #[serde(rename = "max")]
    Max,

    /// The number of epochs to store the blobs for.
    #[serde(untagged)]
    Epochs(NonZeroU32),
}

impl WalrusEpoch {
    /// Tries to convert the `WalrusEpoch` into an `WalrusEpochCount` value.
    ///
    /// If the `WalrusEpoch` is `Max`, the `max_epochs_ahead` is used as the maximum number of
    /// epochs that can be stored ahead.
    pub fn try_into_epoch_count(
        &self,
        max_epochs_ahead: WalrusEpochCount,
    ) -> anyhow::Result<WalrusEpochCount> {
        match self {
            WalrusEpoch::Max => Ok(max_epochs_ahead),
            WalrusEpoch::Epochs(epochs) => {
                let epochs = epochs.get();
                ensure!(
                    epochs <= max_epochs_ahead,
                    anyhow::anyhow!("blobs can only be stored for up to {} epochs ahead; {} epochs were requested",
                    max_epochs_ahead,
                    epochs)
                );
                Ok(epochs)
            }
        }
    }
}

/// Approximate maximum size of a batch that is to be uploaded and stored on walrus.
/// Can be either in weight or numebr of blocks.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum WalrusBatchSize {
    /// The batch will be bounded by the weight in megabytes.
    Weight(NonZeroU8),
    /// The batch will be bounded by the number of blocks.
    Blocks(NonZeroU8),
}

impl Default for WalrusBatchSize {
    fn default() -> Self {
        Self::Blocks(32.try_into().unwrap())
    }
}
