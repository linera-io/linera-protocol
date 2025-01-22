// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, TimeDelta, Timestamp},
    identifiers::ChainId,
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{ChainClient, Client},
    node::CrossChainMessageDelivery,
    test_utils::{MemoryStorageBuilder, NodeProvider, StorageBuilder as _, TestBuilder},
    DEFAULT_GRACE_PERIOD,
};
use linera_execution::system::Recipient;
use linera_storage::{DbStorage, TestClock};
use linera_views::memory::MemoryStore;
use rand::SeedableRng as _;

use super::util::make_genesis_config;
use crate::{
    chain_listener::{self, ChainListener, ChainListenerConfig, ClientContext as _},
    wallet::{UserChain, Wallet},
    Error,
};

type TestStorage = DbStorage<MemoryStore, TestClock>;
type TestProvider = NodeProvider<TestStorage>;

struct ClientContext {
    wallet: Wallet,
    client: Arc<Client<TestProvider, TestStorage>>,
}

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl chain_listener::ClientContext for ClientContext {
    type ValidatorNodeProvider = TestProvider;
    type Storage = TestStorage;

    fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainClient<TestProvider, TestStorage>, Error> {
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
        Ok(self.client.create_chain_client(
            chain_id,
            known_key_pairs,
            self.wallet.genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_block.clone(),
            chain.pending_blobs.clone(),
        ))
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
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

        Ok(())
    }

    async fn update_wallet(
        &mut self,
        client: &ChainClient<TestProvider, TestStorage>,
    ) -> Result<(), Error> {
        self.wallet.update_from_state(client).await;
        Ok(())
    }
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
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id0 = client0.chain_id();
    let client1 = builder.add_root_chain(1, Amount::ONE).await?;

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
            false,
            [chain_id0],
            format!("Client node for {:.8}", chain_id0),
            NonZeroUsize::new(20).expect("Chain worker LRU cache size must be non-zero"),
            DEFAULT_GRACE_PERIOD,
        )),
    };
    let key_pair = KeyPair::generate_from(&mut rng);
    let owner = key_pair.public().into();
    context
        .update_wallet_for_new_chain(chain_id0, Some(key_pair), clock.current_time())
        .await?;
    let context = Arc::new(Mutex::new(context));
    let listener = ChainListener::new(config);
    listener.run(context, storage).await;

    // Transfer ownership of chain 0 to the chain listener and some other key. The listener will
    // be leader in ~10% of the rounds.
    let owners = [(owner, 1), (PublicKey::test_key(1).into(), 9)];
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
    client1.transfer(None, Amount::ONE, recipient0).await?;
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
