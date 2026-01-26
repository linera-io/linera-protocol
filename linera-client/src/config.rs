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
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_storage::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("chain error: {0}")]
    Chain(#[from] linera_chain::ChainError),
    #[error("storage is already initialized: {0:?}")]
    StorageIsAlreadyInitialized(Box<NetworkDescription>),
    #[error("no admin chain configured")]
    NoAdminChain,
}

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

    pub fn admin_chain_id(&self) -> ChainId {
        self.admin_chain_description().id()
    }

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), Error>
    where
        S: Storage + Clone + 'static,
    {
        if let Some(description) = storage
            .read_network_description()
            .await
            .map_err(linera_chain::ChainError::from)?
        {
            if description != self.network_description() {
                // We can't initialize storage with a different network description.
                tracing::error!(
                    current_network=?description,
                    new_network=?self.network_description(),
                    "storage already initialized"
                );
                return Err(Error::StorageIsAlreadyInitialized(Box::new(description)));
            }
            tracing::debug!(?description, "storage already initialized");
            return Ok(());
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
            admin_chain_id: self.admin_chain_id(),
        }
    }
}

#[cfg(with_testing)]
mod test {
    use linera_base::data_types::Timestamp;
    use linera_core::test_utils::{MemoryStorageBuilder, TestBuilder};
    use linera_rpc::{
        config::{NetworkProtocol, ValidatorPublicNetworkPreConfig},
        simple::TransportProtocol,
    };

    use super::*;
    use crate::config::{CommitteeConfig, GenesisConfig, ValidatorConfig};

    impl GenesisConfig {
        /// Create a new local `GenesisConfig` for testing.
        pub fn new_testing(builder: &TestBuilder<MemoryStorageBuilder>) -> Self {
            let network = ValidatorPublicNetworkPreConfig {
                protocol: NetworkProtocol::Simple(TransportProtocol::Tcp),
                host: "localhost".to_string(),
                port: 8080,
            };
            let validators = builder
                .initial_committee
                .validators()
                .iter()
                .map(|(public_key, state)| ValidatorConfig {
                    public_key: *public_key,
                    network: network.clone(),
                    account_key: state.account_public_key,
                })
                .collect();
            let mut genesis_chains = builder.genesis_chains().into_iter();
            let (admin_public_key, admin_balance) = genesis_chains
                .next()
                .expect("should have at least one chain");
            let mut genesis_config = Self::new(
                CommitteeConfig { validators },
                Timestamp::from(0),
                builder.initial_committee.policy().clone(),
                "test network".to_string(),
                admin_public_key,
                admin_balance,
            );
            for (public_key, amount) in genesis_chains {
                genesis_config.add_root_chain(public_key, amount);
            }
            genesis_config
        }
    }
}
