// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    iter::IntoIterator,
    ops::{Deref, DerefMut},
};

use linera_base::{
    crypto::{
        AccountPublicKey, AccountSecretKey, BcsSignable, CryptoHash, CryptoRng, Ed25519SecretKey,
        ValidatorPublicKey, ValidatorSecretKey,
    },
    data_types::{Amount, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{
    ExporterServiceConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig
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
}

use crate::{
    persistent, util,
    wallet::{UserChain, Wallet},
};

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

#[cfg(web)]
use crate::persistent::{LocalPersist as Persist, LocalPersistExt as _};
#[cfg(not(web))]
use crate::persistent::{Persist, PersistExt as _};

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

/// The runtime state of the wallet, persisted atomically on change via an instance of
/// [`Persist`].
pub struct WalletState<W> {
    wallet: W,
    prng: Box<dyn CryptoRng>,
}

impl<W: Persist<Target = Wallet>> WalletState<W> {
    pub async fn add_chains<Chains: IntoIterator<Item = UserChain>>(
        &mut self,
        chains: Chains,
    ) -> Result<(), Error> {
        self.wallet.as_mut().extend(chains);
        W::persist(&mut self.wallet)
            .await
            .map_err(|e| Error::Persistence(Box::new(e)))
    }
}

impl<W: Deref> Deref for WalletState<W> {
    type Target = W::Target;
    fn deref(&self) -> &W::Target {
        self.wallet.deref()
    }
}

impl<W: DerefMut> DerefMut for WalletState<W> {
    fn deref_mut(&mut self) -> &mut W::Target {
        self.wallet.deref_mut()
    }
}

impl<W: Persist<Target = Wallet>> Persist for WalletState<W> {
    type Error = W::Error;

    fn as_mut(&mut self) -> &mut Wallet {
        self.wallet.as_mut()
    }

    async fn persist(&mut self) -> Result<(), W::Error> {
        self.wallet
            .mutate(|w| w.refresh_prng_seed(&mut self.prng))
            .await?;
        tracing::trace!("Persisted user chains");
        Ok(())
    }

    fn into_value(self) -> Wallet {
        self.wallet.into_value()
    }
}

#[cfg(feature = "fs")]
impl WalletState<persistent::File<Wallet>> {
    pub fn create_from_file(path: &std::path::Path, wallet: Wallet) -> Result<Self, Error> {
        Ok(Self::new(persistent::File::read_or_create(path, || {
            Ok(wallet)
        })?))
    }

    pub fn read_from_file(path: &std::path::Path) -> Result<Self, Error> {
        Ok(Self::new(persistent::File::read(path)?))
    }
}

#[cfg(with_indexed_db)]
impl WalletState<persistent::IndexedDb<Wallet>> {
    pub async fn create_from_indexed_db(key: &str, wallet: Wallet) -> Result<Self, Error> {
        Ok(Self::new(
            persistent::IndexedDb::read_or_create(key, wallet).await?,
        ))
    }

    pub async fn read_from_indexed_db(key: &str) -> Result<Option<Self>, Error> {
        Ok(persistent::IndexedDb::read(key).await?.map(Self::new))
    }
}

impl<W: Deref<Target = Wallet>> WalletState<W> {
    pub fn new(wallet: W) -> Self {
        Self {
            prng: wallet.make_prng(),
            wallet,
        }
    }

    pub fn generate_key_pair(&mut self) -> AccountSecretKey {
        AccountSecretKey::Ed25519(Ed25519SecretKey::generate_from(&mut self.prng))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: CommitteeConfig,
    pub admin_id: ChainId,
    pub timestamp: Timestamp,
    pub chains: Vec<(AccountPublicKey, Amount)>,
    pub policy: ResourceControlPolicy,
    pub network_name: String,
}

impl BcsSignable<'_> for GenesisConfig {}

impl GenesisConfig {
    pub fn new(
        committee: CommitteeConfig,
        admin_id: ChainId,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
    ) -> Self {
        Self {
            committee,
            admin_id,
            timestamp,
            chains: Vec::new(),
            policy,
            network_name,
        }
    }

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let committee = self.create_committee();
        for (chain_number, (public_key, balance)) in (0..).zip(&self.chains) {
            let description = ChainDescription::Root(chain_number);
            storage
                .create_chain(
                    committee.clone(),
                    self.admin_id,
                    description,
                    (*public_key).into(),
                    *balance,
                    self.timestamp,
                )
                .await?;
        }
        Ok(())
    }

    pub fn create_committee(&self) -> Committee {
        self.committee.clone().into_committee(self.policy.clone())
    }

    pub fn hash(&self) -> CryptoHash {
        CryptoHash::new(self)
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
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Destination {
    /// The gRPC network protocol.
    pub tls: TlsConfig,
    /// The host name of the target destination (IP or hostname).
    pub endpoint: String,
    /// The port number of the target destination.
    pub port: u16,
}

impl Destination {
    pub fn address(&self) -> String {
        let tls = match self.tls {
            TlsConfig::ClearText => "http",
            TlsConfig::Tls => "https",
        };

        format!("{}://{}:{}", tls, self.endpoint, self.port)
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
