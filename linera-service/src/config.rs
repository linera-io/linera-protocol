// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{iter::IntoIterator, path::Path};

use linera_base::{
    crypto::{BcsSignable, CryptoRng, KeyPair, PublicKey},
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

use crate::{
    persistent::{self, Persist},
    wallet::{UserChain, Wallet},
};

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
                        votes: 1,
                    },
                )
            })
            .collect();
        Committee::new(validators, policy)
    }
}

/// The runtime state of the wallet, persisted atomically on change via an instance of
/// [`Persist`].
pub struct WalletState {
    wallet: persistent::File<Wallet>,
    prng: Box<dyn CryptoRng>,
}

impl std::ops::Deref for WalletState {
    type Target = Wallet;

    fn deref(&self) -> &Wallet {
        &self.wallet
    }
}

impl Persist for WalletState {
    type Error = anyhow::Error;

    fn persist(this: &mut Self) -> anyhow::Result<()> {
        Persist::mutate(&mut this.wallet).refresh_prng_seed(&mut this.prng);
        tracing::debug!("Persisted user chains");
        Ok(())
    }

    fn as_mut(this: &mut Self) -> &mut Wallet {
        Persist::as_mut(&mut this.wallet)
    }
}

impl Extend<UserChain> for WalletState {
    fn extend<Chains: IntoIterator<Item = UserChain>>(&mut self, chains: Chains) {
        Persist::mutate(self).extend(chains);
    }
}

impl WalletState {
    fn new(wallet: persistent::File<Wallet>) -> Self {
        Self {
            prng: wallet.make_prng(),
            wallet,
        }
    }

    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        Ok(Self::new(persistent::File::read_or_create(path, || {
            anyhow::bail!("wallet file not found: {}", path.display())
        })?))
    }

    pub fn create(path: &Path, wallet: Wallet) -> Result<Self, anyhow::Error> {
        Ok(Self::new(persistent::File::read_or_create(path, || {
            Ok(wallet)
        })?))
    }

    pub fn generate_key_pair(&mut self) -> KeyPair {
        KeyPair::generate_from(&mut self.prng)
    }

    pub fn into_value(self) -> Wallet {
        self.wallet.into_value()
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

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
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
}
