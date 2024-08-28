// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{iter::IntoIterator, ops::{Deref, DerefMut}};

use linera_base::{
    crypto::{BcsSignable, CryptoHash, CryptoRng, KeyPair, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("persistence error: {0}")]
    Persistence(Box<dyn std::error::Error + Send + Sync>),
}

use crate::{
    persistent::{self, LocalPersist, Persist},
    util,
    wallet::{UserChain, Wallet},
};

util::impl_from_dynamic!(Error:Persistence, persistent::memory::Error);
#[cfg(with_local_storage)]
util::impl_from_dynamic!(Error:Persistence, persistent::local_storage::Error);
#[cfg(feature = "fs")]
util::impl_from_dynamic!(Error:Persistence, persistent::file::Error);

/// The public configuration of a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// The public key of the validator.
    pub name: ValidatorName,
    /// The network configuration for the validator.
    pub network: ValidatorPublicNetworkConfig,
}

/// The private configuration of a validator service.
#[derive(Serialize, Deserialize)]
pub struct ValidatorServerConfig {
    pub validator: ValidatorConfig,
    pub key: KeyPair,
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
                    v.name,
                    ValidatorState {
                        network_address: v.network.to_string(),
                        votes: 100,
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

impl<W: LocalPersist<Target = Wallet>> WalletState<W> {
    pub async fn add_chains<Chains: IntoIterator<Item = UserChain>>(&mut self, chains: Chains) -> Result<(), Error> {
        self.wallet.as_mut().extend(chains);
        W::persist(&mut self.wallet).await.map_err(|e| Error::Persistence(Box::new(e)))
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

impl<W: LocalPersist<Target = Wallet>> LocalPersist for WalletState<W> {
    type Error = W::Error;

    fn as_mut(&mut self) -> &mut Wallet {
        self.wallet.as_mut()
    }

    async fn persist(&mut self) -> Result<(), W::Error> {
        self.wallet.mutate(|w| w.refresh_prng_seed(&mut self.prng)).await?;
        tracing::debug!("Persisted user chains");
        Ok(())
    }

    fn into_value(self) -> Wallet {
        self.wallet.into_value()
    }
}


impl<W: Persist<Target = Wallet> + Send> Persist for WalletState<W> {
    type Error = W::Error;

    fn as_mut(&mut self) -> &mut Wallet {
        self.wallet.as_mut()
    }

    async fn persist(&mut self) -> Result<(), W::Error> {
        self.wallet.mutate(|w| w.refresh_prng_seed(&mut self.prng)).await?;
        tracing::debug!("Persisted user chains");
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

#[cfg(with_local_storage)]
impl WalletState<persistent::LocalStorage<Wallet>> {
    pub fn create_from_local_storage(key: &str, wallet: Wallet) -> Result<Self, Error> {
        Ok(Self::new(persistent::LocalStorage::read_or_create(
            key,
            || Ok(wallet),
        )?))
    }

    pub fn read_from_local_storage(key: &str) -> Result<Self, Error> {
        Ok(Self::new(persistent::LocalStorage::read(key)?))
    }
}

impl<W: Deref<Target = Wallet>> WalletState<W> {
    pub fn new(wallet: W) -> Self {
        Self {
            prng: wallet.make_prng(),
            wallet,
        }
    }

    pub fn generate_key_pair(&mut self) -> KeyPair {
        KeyPair::generate_from(&mut self.prng)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: CommitteeConfig,
    pub admin_id: ChainId,
    pub timestamp: Timestamp,
    pub chains: Vec<(PublicKey, Amount)>,
    pub policy: ResourceControlPolicy,
    pub network_name: String,
}

impl BcsSignable for GenesisConfig {}

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
        ViewError: From<S::StoreError>,
    {
        let committee = self.create_committee();
        for (chain_number, (public_key, balance)) in (0..).zip(&self.chains) {
            let description = ChainDescription::Root(chain_number);
            storage
                .create_chain(
                    committee.clone(),
                    self.admin_id,
                    description,
                    *public_key,
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
