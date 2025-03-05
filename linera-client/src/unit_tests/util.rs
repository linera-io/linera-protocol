// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::Timestamp;
use linera_core::test_utils::{MemoryStorageBuilder, TestBuilder};
use linera_execution::ResourceControlPolicy;
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
    let mut genesis_config = GenesisConfig::new(
        CommitteeConfig { validators },
        builder.admin_id(),
        Timestamp::from(0),
        ResourceControlPolicy::default(),
        "test network".to_string(),
    );
    genesis_config.chains.extend(builder.genesis_chains());
    genesis_config
}
