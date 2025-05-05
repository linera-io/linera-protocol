// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    iter::IntoIterator,
    ops::{Deref, DerefMut},
};

use linera_base::{
    crypto::{
        AccountPublicKey, BcsSignable, CryptoHash, InMemorySigner, ValidatorPublicKey,
        ValidatorSecretKey,
    },
    data_types::{Amount, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Timestamp},
    identifiers::ChainId,
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{
    ExporterServiceConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
};
use linera_storage::{NetworkDescription, Storage};
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
    StorageIsAlreadyInitialized(NetworkDescription),
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
use crate::persistent::LocalPersist as Persist;
#[cfg(not(web))]
use crate::persistent::Persist;

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
        self.wallet.persist().await?;
        tracing::trace!("Persisted user chains");
        Ok(())
    }

    fn into_value(self) -> Wallet {
        self.wallet.into_value()
    }
}

#[cfg(feature = "fs")]
impl WalletState<persistent::File<Wallet>> {
    /// Reads the wallet from the given path, creating it if it does not exist.
    /// The wallet is created with the given `wallet` value.
    pub fn read_or_create(path: &std::path::Path, wallet: Wallet) -> Result<Self, Error> {
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
        Self { wallet }
    }
}

pub struct SignerState<S> {
    signer: S,
}

impl<S: Deref> Deref for SignerState<S> {
    type Target = S::Target;
    fn deref(&self) -> &S::Target {
        self.signer.deref()
    }
}

impl<S: DerefMut> DerefMut for SignerState<S> {
    fn deref_mut(&mut self) -> &mut S::Target {
        self.signer.deref_mut()
    }
}

impl<S: Persist<Target = InMemorySigner>> Persist for SignerState<S> {
    type Error = S::Error;

    fn as_mut(&mut self) -> &mut InMemorySigner {
        self.signer.as_mut()
    }

    async fn persist(&mut self) -> Result<(), S::Error> {
        self.signer.persist().await?;
        tracing::trace!("Persisted signer struct");
        Ok(())
    }

    fn into_value(self) -> InMemorySigner {
        self.signer.into_value()
    }
}

#[cfg(feature = "fs")]
impl SignerState<persistent::File<InMemorySigner>> {
    /// Reads the wallet from the given path, creating it if it does not exist.
    /// The wallet is created with the given `wallet` value.
    pub fn read_or_create(path: &std::path::Path, signer: InMemorySigner) -> Result<Self, Error> {
        Ok(Self::new(persistent::File::read_or_create(path, || {
            Ok(signer)
        })?))
    }

    pub fn read_from_file(path: &std::path::Path) -> Result<Self, Error> {
        Ok(Self::new(persistent::File::read(path)?))
    }
}

#[cfg(with_indexed_db)]
impl SignerState<persistent::IndexedDb<InMemorySigner>> {
    pub async fn create_from_indexed_db(key: &str, wallet: InMemorySigner) -> Result<Self, Error> {
        Ok(Self::new(
            persistent::IndexedDb::read_or_create(key, wallet).await?,
        ))
    }

    pub async fn read_from_indexed_db(key: &str) -> Result<Option<Self>, Error> {
        Ok(persistent::IndexedDb::read(key).await?.map(Self::new))
    }
}

impl<S: Deref<Target = InMemorySigner>> SignerState<S> {
    pub fn new(signer: S) -> Self {
        Self { signer }
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
        if let Some(description) = storage
            .read_network_description()
            .await
            .map_err(linera_chain::ChainError::from)?
        {
            return Err(Error::StorageIsAlreadyInitialized(description));
        }
        let committee = self.create_committee();
        let committees: BTreeMap<_, _> = [(
            Epoch::ZERO,
            bcs::to_bytes(&committee).expect("serializing a committee should not fail"),
        )]
        .into_iter()
        .collect();
        for (chain_number, (public_key, balance)) in (0..).zip(&self.chains) {
            let origin = ChainOrigin::Root(chain_number);
            let config = InitialChainConfig {
                admin_id: if chain_number == 0 {
                    None
                } else {
                    Some(self.admin_id)
                },
                application_permissions: Default::default(),
                balance: *balance,
                committees: committees.clone(),
                epoch: Epoch::ZERO,
                ownership: ChainOwnership::single((*public_key).into()),
            };
            let description = ChainDescription::new(origin, config, self.timestamp);
            storage.create_chain(description).await?;
        }
        let network_description = NetworkDescription {
            name: self.network_name.clone(),
            genesis_config_hash: CryptoHash::new(self),
            genesis_timestamp: self.timestamp,
        };
        storage
            .write_network_description(&network_description)
            .await
            .map_err(linera_chain::ChainError::from)?;
        Ok(())
    }

    pub fn create_committee(&self) -> Committee {
        self.committee.clone().into_committee(self.policy.clone())
    }

    pub fn hash(&self) -> CryptoHash {
        CryptoHash::new(self)
    }

    pub fn network_description(&self) -> NetworkDescription {
        NetworkDescription {
            name: self.network_name.clone(),
            genesis_config_hash: CryptoHash::new(self),
            genesis_timestamp: self.timestamp,
        }
    }
}

/// The configuration file for the linera-exporter.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockExporterConfig {
    /// The server configuration for the linera-exporter.
    pub service_config: ExporterServiceConfig,

    /// The configuration file for the export destinations.
    #[serde(default)]
    pub destination_config: DestinationConfig,

    /// Identity for the block exporter state.
    pub id: u32,
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
    /// The host name of the target destination (IP or hostname).
    pub endpoint: String,
    /// The port number of the target destination.
    pub port: u16,
}
