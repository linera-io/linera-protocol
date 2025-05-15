// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, iter::IntoIterator};

use linera_base::{
    crypto::{AccountPublicKey, BcsSignable, CryptoHash, ValidatorPublicKey, ValidatorSecretKey},
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
    admin_id: Option<ChainId>,
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
        admin_id,
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
        let admin_chain = make_chain(
            &committee,
            0,
            None,
            admin_public_key,
            admin_balance,
            timestamp,
        );
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
            Some(self.admin_id()),
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
        for description in &self.chains {
            storage.create_chain(description.clone()).await?;
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
