// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{AccountPublicKey, InMemorySigner},
    data_types::{Amount, BlockHeight, Epoch, TimeDelta, Timestamp},
    identifiers::{Account, AccountOwner, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{chain_client, ChainClient, Client, ListeningMode},
    environment,
    test_utils::{MemoryStorageBuilder, StorageBuilder as _, TestBuilder},
    wallet,
};
use linera_storage::Storage;
use tokio_util::sync::CancellationToken;

use crate::{
    chain_listener::{self, ChainListener, ChainListenerConfig, ClientContext as _},
    config::GenesisConfig,
    Error,
};

struct ClientContext {
    client: Arc<Client<environment::Test>>,
}

impl chain_listener::ClientContext for ClientContext {
    type Environment = environment::Test;

    fn wallet(&self) -> &environment::TestWallet {
        self.client.wallet()
    }

    fn storage(&self) -> &environment::TestStorage {
        self.client.storage_client()
    }

    fn client(&self) -> &Arc<linera_core::client::Client<Self::Environment>> {
        &self.client
    }

    fn timing_sender(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>> {
        None
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error> {
        // Ignore if chain already exists in wallet; test mock doesn't care.
        self.wallet()
            .try_insert(chain_id, wallet::Chain::new(owner, epoch, timestamp));
        Ok(())
    }

    async fn update_wallet(
        &mut self,
        client: &ChainClient<environment::Test>,
    ) -> Result<(), Error> {
        let info = client.chain_info().await?;
        let client_owner = client.preferred_owner();
        let pending_proposal = client.pending_proposal().clone();
        self.wallet().insert(
            info.chain_id,
            wallet::Chain {
                pending_proposal,
                owner: client_owner,
                ..info.as_ref().into()
            },
        );
        Ok(())
    }
}

/// Tests that the chain listener, if there is a message in the inbox, will continue requesting
/// timeout certificates until it becomes the leader and can process the inbox.
#[test_log::test(tokio::test)]
async fn test_chain_listener() -> anyhow::Result<()> {
    // Create two chains.
    let mut signer = InMemorySigner::new(Some(42));
    let key_pair = signer.generate_new();
    let owner: AccountOwner = key_pair.into();
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id0 = client0.chain_id();
    let client1 = builder.add_root_chain(1, Amount::ONE).await?;
    // Start a chain listener for chain 0 with a new key.
    let genesis_config = GenesisConfig::new_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;
    let epoch0 = client0.chain_info().await?.epoch;
    let epoch1 = client1.chain_info().await?.epoch;

    let mut context = ClientContext {
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
                wallet: environment::TestWallet::default(),
            },
            admin_chain_id,
            false,
            [(chain_id0, ListeningMode::FullChain)],
            format!("Client node for {:.8}", chain_id0),
            Duration::from_secs(30),
            Duration::from_secs(1),
            chain_client::Options::test_default(),
            5_000,
            10_000,
            linera_core::client::RequestsSchedulerConfig::default(),
        )),
    };
    context
        .update_wallet_for_new_chain(chain_id0, Some(owner), clock.current_time(), epoch0)
        .await?;
    context
        .update_wallet_for_new_chain(
            client1.chain_id(),
            client1.preferred_owner(),
            clock.current_time(),
            epoch1,
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
    let chain_listener = ChainListener::new(
        config,
        context,
        storage,
        child_token,
        tokio::sync::mpsc::unbounded_channel().1,
        false, // Unit test doesn't need background sync
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::task::spawn(async move { chain_listener.await.unwrap() });
    // Transfer one token to chain 0. The listener should eventually become leader and receive
    // the message.
    let recipient0 = Account::chain(chain_id0);
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
    handle.await;

    Ok(())
}

/// Tests that a follow-only chain listener does NOT process its inbox when receiving messages.
/// We set up a listener with two chains: chain A (follow-only but owned) and chain B (FullChain).
/// The sender sends a message to A first, then to B. Once the listener processes B's inbox
/// (which we can observe), we know it must have also seen A's notification - but A's inbox
/// should remain unprocessed because it's follow-only (not because of missing ownership).
#[test_log::test(tokio::test)]
async fn test_chain_listener_follow_only() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;

    // Create three chains: sender, chain_a (will be follow-only), chain_b (will be FullChain).
    let sender = builder.add_root_chain(0, Amount::from_tokens(10)).await?;
    let chain_a = builder.add_root_chain(1, Amount::ZERO).await?;
    let chain_b = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_a_id = chain_a.chain_id();
    let chain_b_id = chain_b.chain_id();

    let genesis_config = GenesisConfig::new_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;
    let chain_a_info = chain_a.chain_info().await?;
    let chain_b_info = chain_b.chain_info().await?;

    let context = ClientContext {
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
                wallet: environment::TestWallet::default(),
            },
            admin_chain_id,
            false,
            [
                (chain_a_id, ListeningMode::FollowChain),
                (chain_b_id, ListeningMode::FullChain),
            ],
            "Client node with follow-only and owned chains".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(1),
            chain_client::Options::test_default(),
            5_000,
            10_000,
            linera_core::client::RequestsSchedulerConfig::default(),
        )),
    };

    // Add chain A as follow-only (no owner configured).
    context.wallet().insert(
        chain_a_id,
        wallet::Chain {
            owner: None,
            block_hash: chain_a_info.block_hash,
            next_block_height: chain_a_info.next_block_height,
            timestamp: clock.current_time(),
            pending_proposal: None,
            epoch: Some(chain_a_info.epoch),
        },
    );

    // Add chain B with an owner (not follow-only).
    context.wallet().insert(
        chain_b_id,
        wallet::Chain {
            owner: chain_b.preferred_owner(),
            block_hash: chain_b_info.block_hash,
            next_block_height: chain_b_info.next_block_height,
            timestamp: clock.current_time(),
            pending_proposal: None,
            epoch: Some(chain_b_info.epoch),
        },
    );

    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let chain_listener = ChainListener::new(
        config,
        context.clone(),
        storage.clone(),
        child_token,
        tokio::sync::mpsc::unbounded_channel().1,
        false, // Unit test doesn't need background sync
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::task::spawn(async move { chain_listener.await.unwrap() });

    // Send a message to chain A first (follow-only). This notification should be ignored.
    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, Account::chain(chain_a_id))
        .await?;

    // Then send a message to chain B (owned). The listener should process this inbox.
    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, Account::chain(chain_b_id))
        .await?;

    // Wait until chain B processes its inbox. Once this happens, we know the listener
    // has seen both notifications (A's came first), but should have only acted on B's.
    for i in 0.. {
        tokio::task::yield_now().await;

        chain_b.synchronize_from_validators().await?;
        let chain_b_info = chain_b.chain_info().await?;
        // Chain B should have height 1 after processing its inbox.
        if chain_b_info.next_block_height >= BlockHeight::from(1) {
            break;
        }
        if i >= 50 {
            panic!(
                "Chain B's inbox was not processed by the listener. Expected height >= 1, got {}",
                chain_b_info.next_block_height
            );
        }
    }

    // Now verify that chain A's inbox was NOT processed (follow-only ignores NewIncomingBundle).
    chain_a.synchronize_from_validators().await?;
    let chain_a_info = chain_a.chain_info().await?;
    assert_eq!(
        chain_a_info.next_block_height,
        BlockHeight::ZERO,
        "Follow-only chain A should not have had its inbox processed"
    );

    // Verify that the listener's wallet still shows chain A at height 0.
    let wallet_chain_a = context.lock().await.wallet().get(chain_a_id).unwrap();
    assert_eq!(
        wallet_chain_a.next_block_height,
        BlockHeight::ZERO,
        "Wallet should show chain A at height 0"
    );

    // Now have the original chain_a client process its inbox, creating a block.
    chain_a.process_inbox().await?;

    // Wait for the chain listener to see the NewBlock notification and update its wallet.
    // This verifies that follow-only mode DOES process NewBlock notifications.
    for i in 0.. {
        tokio::task::yield_now().await;

        let wallet_chain_a = context.lock().await.wallet().get(chain_a_id).unwrap();
        if wallet_chain_a.next_block_height >= BlockHeight::from(1) {
            break;
        }
        if i >= 50 {
            panic!(
                "Wallet not updated after chain A created a block. Expected height >= 1, got {}",
                wallet_chain_a.next_block_height
            );
        }
    }

    // Verify the wallet was updated and chain A is still follow-only (no owner).
    let wallet_chain_a = context.lock().await.wallet().get(chain_a_id).unwrap();
    assert!(
        wallet_chain_a.is_follow_only(),
        "chain A should still be follow-only (no owner)"
    );

    cancellation_token.cancel();
    handle.await;

    Ok(())
}

/// Tests that the chain listener always listens to the admin chain.
#[test_log::test(tokio::test)]
async fn test_chain_listener_admin_chain() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let genesis_config = GenesisConfig::new_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;

    let context = ClientContext {
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
                wallet: environment::TestWallet::default(),
            },
            admin_chain_id,
            false,
            std::iter::empty::<(ChainId, ListeningMode)>(),
            "Client node with no chains".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(1),
            chain_client::Options::test_default(),
            5_000,
            10_000,
            linera_core::client::RequestsSchedulerConfig::default(),
        )),
    };
    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let chain_listener = ChainListener::new(
        config,
        context,
        storage.clone(),
        child_token,
        tokio::sync::mpsc::unbounded_channel().1,
        false, // Unit test doesn't need background sync
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::task::spawn(async move { chain_listener.await.unwrap() });
    let committee = builder.initial_committee.clone();
    // Stage a committee (this will emit events that the listener should be listening to).
    let certificate = client0.stage_new_committee(committee).await?.unwrap();
    for i in 0.. {
        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        let result = storage.read_certificate(certificate.hash()).await?;
        if result.as_ref() == Some(&certificate) {
            break;
        }
        if i == 5 {
            panic!("Failed to learn about new block.");
        }
    }

    cancellation_token.cancel();
    handle.await;

    Ok(())
}

/// Tests that the ListenerCommand::Listen actually adds chains to the wallet.
#[test_log::test(tokio::test)]
async fn test_chain_listener_listen_command_adds_chains_to_wallet() -> anyhow::Result<()> {
    use std::collections::BTreeMap;

    use crate::chain_listener::ListenerCommand;

    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;

    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id0 = client0.chain_id();

    let genesis_config = GenesisConfig::new_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;

    let context = ClientContext {
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
                wallet: environment::TestWallet::default(),
            },
            admin_chain_id,
            false,
            std::iter::empty::<(ChainId, ListeningMode)>(),
            "Client node with no chains".to_string(),
            Duration::from_secs(30),
            Duration::from_secs(1),
            chain_client::Options::test_default(),
            5_000,
            10_000,
            linera_core::client::RequestsSchedulerConfig::default(),
        )),
    };

    assert!(
        context.wallet().get(chain_id0).is_none(),
        "Wallet should not contain chain_id0 initially"
    );

    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
    let chain_listener = ChainListener::new(
        config,
        context.clone(),
        storage.clone(),
        child_token,
        command_receiver,
        false,
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::task::spawn(async move { chain_listener.await.unwrap() });

    let mut chains_to_listen = BTreeMap::new();
    chains_to_listen.insert(chain_id0, ListeningMode::FullChain);
    command_sender
        .send(ListenerCommand::Listen(chains_to_listen))
        .expect("Failed to send Listen command");

    for i in 0.. {
        tokio::task::yield_now().await;

        if context.lock().await.wallet().get(chain_id0).is_some() {
            break;
        }
        if i >= 50 {
            panic!("Wallet was not updated with chain_id0 after Listen command");
        }
    }

    let wallet_chain = context.lock().await.wallet().get(chain_id0).unwrap();
    assert_eq!(
        wallet_chain.next_block_height,
        BlockHeight::ZERO,
        "Chain should be at height 0"
    );

    cancellation_token.cancel();
    handle.await;

    Ok(())
}
