// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{AccountPublicKey, InMemSigner},
    data_types::{Amount, BlockHeight, TimeDelta, Timestamp},
    identifiers::{AccountOwner, ChainId},
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
use tokio_util::sync::CancellationToken;

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
        let preferred_owner = self.wallet.assigned_keys.get(&chain_id).copied();
        Ok(self.client.create_chain_client(
            chain_id,
            self.wallet.genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_proposal.clone(),
            preferred_owner,
        ))
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        if self.wallet.get(chain_id).is_none() {
            self.wallet.insert(UserChain {
                chain_id,
                owner,
                block_hash: None,
                timestamp,
                next_block_height: BlockHeight::ZERO,
                pending_proposal: None,
            });

            if let Some(owner) = owner {
                self.wallet.assigned_keys.insert(chain_id, owner);
            }
        }

        Ok(())
    }

    async fn update_wallet(
        &mut self,
        client: &ChainClient<TestProvider, TestStorage>,
    ) -> Result<(), Error> {
        self.wallet.update_from_state(client).await?;
        Ok(())
    }
}

/// Tests that the chain listener, if there is a message in the inbox, will continue requesting
/// timeout certificates until it becomes the leader and can process the inbox.
#[test_log::test(tokio::test)]
async fn test_chain_listener() -> anyhow::Result<()> {
    // Create two chains.
    let mut signer = InMemSigner::new(Some(42));
    let key_pair = signer.generate_new();
    let owner: AccountOwner = key_pair.into();
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, &mut signer).await?;
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id0 = client0.chain_id();
    let client1 = builder.add_root_chain(1, Amount::ONE).await?;
    // Start a chain listener for chain 0 with a new key.
    let genesis_config = make_genesis_config(&builder);
    let storage = builder.make_storage().await?;
    let delivery = CrossChainMessageDelivery::NonBlocking;

    let mut context = ClientContext {
        wallet: Wallet::new(genesis_config),
        client: Arc::new(Client::new(
            builder.make_node_provider(),
            storage.clone(),
            Box::new(signer),
            10,
            delivery,
            false,
            [chain_id0],
            format!("Client node for {:.8}", chain_id0),
            NonZeroUsize::new(20).expect("Chain worker LRU cache size must be non-zero"),
            DEFAULT_GRACE_PERIOD,
            Duration::from_secs(1),
        )),
    };
    context
        .update_wallet_for_new_chain(chain_id0, Some(owner), clock.current_time())
        .await?;
    context
        .update_wallet_for_new_chain(
            client1.chain_id(),
            client1.preferred_owner(),
            clock.current_time(),
        )
        .await?;

    // Transfer ownership of chain 0 to the chain listener and some other key. The listener will
    // be leader in ~10% of the rounds.
    let owners = [(owner, 1), (AccountPublicKey::test_key(1).into(), 9)];
    let timeout_config = TimeoutConfig {
        base_timeout: TimeDelta::from_secs(1),
        timeout_increment: TimeDelta::ZERO,
        ..TimeoutConfig::default()
    };
    client0
        .change_ownership(ChainOwnership::multiple(owners, 0, timeout_config))
        .await?;

    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let handle = linera_base::task::spawn(async move {
        ChainListener::new(config, context, storage, child_token)
            .run()
            .await
            .unwrap()
    });
    // Transfer one token to chain 0. The listener should eventually become leader and receive
    // the message.
    let recipient0 = Recipient::chain(chain_id0);
    client1
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient0)
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

    cancellation_token.cancel();
    handle.await?;

    Ok(())
}
