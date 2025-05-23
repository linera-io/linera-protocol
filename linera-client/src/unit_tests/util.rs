// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::Timestamp;
use linera_core::test_utils::{MemoryStorageBuilder, TestBuilder};
use linera_rpc::{
    config::{NetworkProtocol, ValidatorPublicNetworkPreConfig},
    simple::TransportProtocol,
};

use crate::config::{CommitteeConfig, GenesisConfig, ValidatorConfig};

pub fn make_genesis_config(builder: &TestBuilder<MemoryStorageBuilder>) -> GenesisConfig {
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
    let mut genesis_config = GenesisConfig::new(
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
