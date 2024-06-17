// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, TimeDelta, Timestamp},
    identifiers::{ChainDescription, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{ChainClient, Client},
    node::CrossChainMessageDelivery,
    test_utils::{MemoryStorageBuilder, NodeProvider, StorageBuilder as _, TestBuilder},
};
use linera_execution::{
    system::{Recipient, UserData},
    ResourceControlPolicy,
};
use linera_rpc::{
    config::{NetworkProtocol, ValidatorPublicNetworkPreConfig},
    simple::TransportProtocol,
};
use linera_storage::{MemoryStorage, TestClock};
use rand::SeedableRng as _;

use crate::{
    chain_listener::{self, ChainListener, ChainListenerConfig, ClientContext as _},
    config::{CommitteeConfig, GenesisConfig, ValidatorConfig},
    wallet::{UserChain, Wallet},
};

type TestStorage = MemoryStorage<TestClock>;
type TestProvider = NodeProvider<TestStorage>;

struct ClientContext {
    wallet: Wallet,
    client: Arc<Client<TestProvider, TestStorage>>,
}

#[async_trait]
impl chain_listener::ClientContext for ClientContext {
    type ValidatorNodeProvider = TestProvider;
    type Storage = TestStorage;

    fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<TestProvider, TestStorage> {
        let chain = self
            .wallet
            .get(chain_id)
            .unwrap_or_else(|| panic!("Unknown chain: {}", chain_id));
        let known_key_pairs = chain
            .key_pair
            .as_ref()
            .map(|kp| kp.copy())
            .into_iter()
            .collect();
        self.client.build(
            chain_id,
            known_key_pairs,
            self.wallet.genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_block.clone(),
            chain.pending_blobs.clone(),
        )
    }

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        if self.wallet.get(chain_id).is_none() {
            self.wallet.insert(UserChain {
                chain_id,
                key_pair: key_pair.as_ref().map(|kp| kp.copy()),
                block_hash: None,
                timestamp,
                next_block_height: BlockHeight::ZERO,
                pending_block: None,
                pending_blobs: BTreeMap::new(),
            });
        }
    }

    async fn update_wallet<'a>(
        &'a mut self,
        client: &'a mut ChainClient<TestProvider, TestStorage>,
    ) {
        self.wallet.update_from_state(client).await;
    }
}

fn make_genesis_config(builder: &TestBuilder<MemoryStorageBuilder>) -> GenesisConfig {
    let network = ValidatorPublicNetworkPreConfig {
        protocol: NetworkProtocol::Simple(TransportProtocol::Tcp),
        host: "localhost".to_string(),
        port: 8080,
    };
    let validator_names = builder.initial_committee.validators().keys();
    let validators = validator_names
        .map(|name| ValidatorConfig {
            name: *name,
            network: network.clone(),
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

/// Tests that the chain listener, if there is a message in the inbox, will continue requesting
/// timeout certificates until it becomes the leader and can process the inbox.
#[test_log::test(tokio::test)]
async fn test_chain_listener() -> anyhow::Result<()> {
    // Create two chains.
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description0 = ChainDescription::Root(0);
    let description1 = ChainDescription::Root(1);
    let chain_id0 = ChainId::from(description0);
    let mut client0 = builder.add_initial_chain(description0, Amount::ONE).await?;
    let mut client1 = builder.add_initial_chain(description1, Amount::ONE).await?;

    // Start a chain listener for chain 0 with a new key.
    let genesis_config = make_genesis_config(&builder);
    let storage = builder.make_storage().await?;
    let delivery = CrossChainMessageDelivery::NonBlocking;
    let mut context = ClientContext {
        wallet: Wallet::new(genesis_config, Some(37)),
        client: Arc::new(Client::new(
            builder.make_node_provider(),
            storage.clone(),
            10,
            delivery,
        )),
    };
    let key_pair = KeyPair::generate_from(&mut rng);
    let public_key = key_pair.public();
    context.update_wallet_for_new_chain(chain_id0, Some(key_pair), clock.current_time());
    let context = Arc::new(Mutex::new(context));
    let listener = ChainListener::new(config, Default::default());
    listener.run(context, storage).await;

    // Transfer ownership of chain 0 to the chain listener and some other key. The listener will
    // be leader in ~10% of the rounds.
    let owners = [(public_key, 1), (PublicKey::test_key(1), 9)];
    let timeout_config = TimeoutConfig {
        base_timeout: TimeDelta::from_secs(1),
        timeout_increment: TimeDelta::ZERO,
        ..TimeoutConfig::default()
    };
    client0
        .change_ownership(ChainOwnership::multiple(owners, 0, timeout_config))
        .await?;

    // Transfer one token to chain 0. The listener should eventually become leader and receive
    // the message.
    let recipient0 = Recipient::chain(chain_id0);
    client1
        .transfer(None, Amount::ONE, recipient0, UserData::default())
        .await?;
    for i in 0.. {
        client0.synchronize_from_validators().boxed().await?;
        let balance = client0.local_balance().await?;
        if balance == Amount::from_tokens(2) {
            break;
        }
        clock.add(TimeDelta::from_secs(1));
        if i == 30 {
            panic!("Unexpected local balance: {}", balance);
        }
    }

    Ok(())
}
