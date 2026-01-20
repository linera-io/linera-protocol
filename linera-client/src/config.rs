// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::iter::IntoIterator;

use linera_base::{
    crypto::{AccountPublicKey, ValidatorPublicKey, ValidatorSecretKey},
    data_types::Timestamp,
};
// Re-export GenesisConfig from linera-core for backwards compatibility.
pub use linera_core::{GenesisConfig, GenesisConfigError};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    GenesisConfig(#[from] GenesisConfigError),
    #[error("no admin chain configured")]
    NoAdminChain,
}

impl From<linera_chain::ChainError> for Error {
    fn from(error: linera_chain::ChainError) -> Self {
        Error::GenesisConfig(GenesisConfigError::Chain(error))
    }
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

/// Extension trait to create a `GenesisConfig` from a `CommitteeConfig`.
pub trait GenesisConfigExt {
    /// Creates a `GenesisConfig` with the first chain being the admin chain.
    fn new(
        committee: CommitteeConfig,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
        admin_public_key: AccountPublicKey,
        admin_balance: linera_base::data_types::Amount,
    ) -> Self;
}

impl GenesisConfigExt for GenesisConfig {
    fn new(
        committee: CommitteeConfig,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
        admin_public_key: AccountPublicKey,
        admin_balance: linera_base::data_types::Amount,
    ) -> Self {
        let committee = committee.into_committee(policy);
        Self::new_from_committee(
            committee,
            timestamp,
            network_name,
            admin_public_key,
            admin_balance,
        )
    }
}

/// Extension trait for creating test genesis configs.
#[cfg(with_testing)]
pub trait GenesisConfigTestExt {
    /// Create a new local `GenesisConfig` for testing.
    fn new_testing(
        builder: &linera_core::test_utils::TestBuilder<
            linera_core::test_utils::MemoryStorageBuilder,
        >,
    ) -> Self;
}

#[cfg(with_testing)]
impl GenesisConfigTestExt for GenesisConfig {
    fn new_testing(
        builder: &linera_core::test_utils::TestBuilder<
            linera_core::test_utils::MemoryStorageBuilder,
        >,
    ) -> Self {
        use linera_base::data_types::Timestamp;
        use linera_rpc::{
            config::{NetworkProtocol, ValidatorPublicNetworkPreConfig},
            simple::TransportProtocol,
        };

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
        let mut genesis_config = <GenesisConfig as GenesisConfigExt>::new(
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
