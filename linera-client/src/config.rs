// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::iter::IntoIterator;

use linera_base::{
    crypto::{AccountPublicKey, ValidatorPublicKey, ValidatorSecretKey},
    data_types::Timestamp,
};
pub use linera_core::genesis_config::{Error as GenesisConfigError, GenesisConfig};
use linera_execution::{
    committee::{Committee, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use serde::{Deserialize, Serialize};

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
    /// The public configuration of the validator.
    pub validator: ValidatorConfig,
    /// The secret key of the validator.
    pub validator_secret: ValidatorSecretKey,
    /// The internal network configuration of the validator.
    pub internal_network: ValidatorInternalNetworkConfig,
}

/// The (public) configuration for all validators.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct CommitteeConfig {
    /// The public configurations of all validators in the committee.
    pub validators: Vec<ValidatorConfig>,
}

impl CommitteeConfig {
    /// Converts this committee config into a `Committee` using the given resource control policy.
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

impl CommitteeConfig {
    /// Creates a `GenesisConfig` from this committee config with the first chain being the admin chain.
    pub fn into_genesis(
        self,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
        admin_public_key: AccountPublicKey,
        admin_balance: linera_base::data_types::Amount,
    ) -> GenesisConfig {
        let committee = self.into_committee(policy);
        GenesisConfig::new(
            committee,
            timestamp,
            network_name,
            admin_public_key,
            admin_balance,
        )
    }
}
