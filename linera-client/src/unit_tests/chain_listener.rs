// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc, time::Duration};

use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{AccountPublicKey, InMemorySigner},
    data_types::{Amount, BlockHeight, Bytecode, Epoch, TimeDelta, Timestamp},
    identifiers::{Account, AccountOwner, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
};
use linera_core::{
    client::{chain_client, ChainClient, Client, ListeningMode},
    environment,
    test_utils::{
        ClientOutcomeResultExt as _, MemoryStorageBuilder, StorageBuilder as _, TestBuilder,
    },
    wallet,
    worker::{Reason, DEFAULT_BLOCK_CACHE_SIZE, DEFAULT_EXECUTION_STATE_CACHE_SIZE},
    Environment,
};
use linera_execution::{wasm_test, Operation, ResourceControlPolicy, WasmRuntime};
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
        let existing_owner = self.wallet().get(info.chain_id).and_then(|c| c.owner);
        let pending_proposal = client
            .pending_proposal()
            .await
            .filter(|p| p.round.is_some_and(|r| r.is_fast()));
        self.wallet().insert(
            info.chain_id,
            wallet::Chain {
                pending_proposal,
                owner: existing_owner,
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
    let genesis_config = GenesisConfig::new_for_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;

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
            format!("Client node for {chain_id0:.8}"),
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        )),
    };
    context
        .update_wallet_for_new_chain(chain_id0, Some(owner), clock.current_time(), Epoch::ZERO)
        .await?;
    context
        .update_wallet_for_new_chain(
            client1.chain_id(),
            client1.preferred_owner(),
            clock.current_time(),
            Epoch::ZERO,
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

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });
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
            panic!("Unexpected local balance: {balance}");
        }
    }

    cancellation_token.cancel();
    handle.await;

    Ok(())
}

/// Tests that a follow-only chain listener does NOT process its inbox when receiving messages.
/// We set up a listener with two chains: chain A (follow-only, no owner) and chain B (owned).
/// The sender sends a message to A first, then to B. Once the listener processes B's inbox
/// (which we can observe), we know it must have also seen A's notification - but A's inbox
/// should remain unprocessed because it's follow-only (no owner key).
#[test_log::test(tokio::test)]
async fn test_chain_listener_follow_only() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;

    // Create three chains: sender, chain_a (will be follow-only), chain_b (will be owned).
    let sender = builder.add_root_chain(0, Amount::from_tokens(10)).await?;
    let chain_a = builder.add_root_chain(1, Amount::ZERO).await?;
    let chain_b = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_a_id = chain_a.chain_id();
    let chain_b_id = chain_b.chain_id();

    let genesis_config = GenesisConfig::new_for_testing(&builder);
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
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        )),
    };

    // Add chain A as follow-only (no owner = follow-only mode).
    context.wallet().insert(
        chain_a_id,
        wallet::Chain {
            owner: None, // No owner means follow-only mode
            block_hash: chain_a_info.block_hash,
            next_block_height: chain_a_info.next_block_height,
            timestamp: clock.current_time(),
            pending_proposal: None,
            epoch: Some(chain_a_info.epoch),
        },
    );

    // Add chain B as owned (has owner = not follow-only).
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
    let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
    let chain_listener = ChainListener::new(
        config,
        context.clone(),
        storage.clone(),
        child_token,
        command_receiver,
        false, // Unit test doesn't need background sync
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });

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
    let genesis_config = GenesisConfig::new_for_testing(&builder);
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
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
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

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });
    // Burn one token.
    let certificate = client0
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await?
        .unwrap();
    for i in 0.. {
        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        let result = storage.read_certificate(certificate.hash()).await?;
        if result.as_deref() == Some(&certificate) {
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

    let genesis_config = GenesisConfig::new_for_testing(&builder);
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
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
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

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });

    let mut chains_to_listen = BTreeMap::new();
    chains_to_listen.insert(chain_id0, Some(client0.identity().await?));
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

/// Tests that user-initiated operations sign blocks with the "dynamic" owner while
/// the chain listener signs inbox-processing blocks with the "autosigner" owner.
///
/// This reproduces the bug where `update_wallet` overwrites the wallet's owner with
/// the ChainClient's `preferred_owner`, causing the listener to use the wrong signer.
#[test_log::test(tokio::test)]
async fn test_listener_uses_autosigner_for_incoming_messages() -> anyhow::Result<()> {
    let mut signer = InMemorySigner::new(Some(42));
    let autosigner_key = signer.generate_new();
    let autosigner_owner: AccountOwner = autosigner_key.into();
    let dynamic_key = signer.generate_new();
    let dynamic_owner: AccountOwner = dynamic_key.into();

    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;

    // Chain 0: the chain under test (owned by both autosigner and dynamic).
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let chain_id0 = client0.chain_id();
    // Chain 1: sender of incoming messages.
    let client1 = builder.add_root_chain(1, Amount::ONE).await?;

    // Transfer ownership to both the autosigner and dynamic owners.
    // Use multi_leader_rounds > 0 so both owners can propose without waiting for leadership.
    let timeout_config = TimeoutConfig {
        base_timeout: TimeDelta::from_secs(1),
        timeout_increment: TimeDelta::ZERO,
        ..TimeoutConfig::default()
    };
    client0
        .change_ownership(ChainOwnership::multiple(
            [(autosigner_owner, 1), (dynamic_owner, 1)],
            100,
            timeout_config,
        ))
        .await?;

    let genesis_config = GenesisConfig::new_for_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;

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
            format!("Client node for {chain_id0:.8}"),
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        )),
    };

    // Set wallet owner to the autosigner (as wallet.setOwner() would in the web client).
    let chain0_info = client0.chain_info().await?;
    context.wallet().insert(
        chain_id0,
        wallet::Chain {
            owner: Some(autosigner_owner),
            block_hash: chain0_info.block_hash,
            next_block_height: chain0_info.next_block_height,
            timestamp: clock.current_time(),
            pending_proposal: None,
            epoch: Some(chain0_info.epoch),
        },
    );
    context
        .update_wallet_for_new_chain(
            client1.chain_id(),
            client1.preferred_owner(),
            clock.current_time(),
            Epoch::ZERO,
        )
        .await?;

    // Simulate the web client's client.chain({owner: dynamicAddress}) followed by an
    // operation (e.g. addOwner). This calls update_wallet, which with the bug overwrites
    // the wallet owner with the ChainClient's preferred_owner.
    let mut chain_client = context.make_chain_client(chain_id0).await?;
    chain_client.set_preferred_owner(dynamic_owner);
    context.update_wallet(&chain_client).await?;

    // Start the chain listener. It creates its ChainClient via make_chain_client(),
    // reading the owner from the wallet.
    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let chain_listener = ChainListener::new(
        config,
        context,
        storage.clone(),
        child_token,
        tokio::sync::mpsc::unbounded_channel().1,
        false,
    )
    .run()
    .await
    .unwrap();

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });

    // Send a message from chain 1 to chain 0. The listener should process the inbox.
    let recipient0 = Account::chain(chain_id0);
    client1
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient0)
        .await?;

    // Wait for the listener to process the inbox (creating a block after the
    // change_ownership block).
    for i in 0.. {
        client0.synchronize_from_validators().boxed().await?;
        let balance = client0.local_balance().await?;
        if balance == Amount::from_tokens(2) {
            break;
        }
        clock.add(TimeDelta::from_secs(1));
        if i == 30 {
            panic!("Listener did not process inbox. Balance: {balance}");
        }
    }

    // Stop the listener before doing user-initiated operations.
    cancellation_token.cancel();
    handle.await;

    // The change_ownership block is at height 0 (created by builder's original owner).
    // The inbox-processing block is at height 1 (created by the listener).
    // Verify the listener's block was signed by the autosigner, not the dynamic signer.
    let certs = storage
        .read_certificates_by_heights(chain_id0, &[BlockHeight::from(1)])
        .await?;
    let inbox_cert = certs[0]
        .as_ref()
        .expect("certificate should exist at height 1");
    let inbox_block = inbox_cert.inner().block();
    assert_eq!(
        inbox_block.header.authenticated_signer,
        Some(autosigner_owner),
        "Listener should sign inbox blocks with the autosigner, not the dynamic signer"
    );

    // Reuse the chain_client (which has preferred_owner = dynamic_owner) for a
    // user-initiated operation and verify the block is signed by the dynamic signer.
    chain_client
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(client1.chain_id()),
        )
        .await?;

    let certs = storage
        .read_certificates_by_heights(chain_id0, &[BlockHeight::from(2)])
        .await?;
    let user_cert = certs[0]
        .as_ref()
        .expect("certificate should exist at height 2");
    let user_block = user_cert.inner().block();
    assert_eq!(
        user_block.header.authenticated_signer,
        Some(dynamic_owner),
        "User-initiated blocks should be signed by the dynamic signer"
    );

    Ok(())
}

/// Helper to read Wasm bytecodes and publish them, returning the module ID.
async fn publish_wasm_example<Env: Environment>(
    client: &ChainClient<Env>,
    name: &str,
) -> anyhow::Result<linera_base::identifiers::ModuleId> {
    let (contract_path, service_path) = wasm_test::get_example_bytecode_paths(name)?;
    let contract_bytecode = Bytecode::load_from_file(contract_path)?;
    let service_bytecode = Bytecode::load_from_file(service_path)?;
    let (module_id, _cert) = client
        .publish_module(contract_bytecode, service_bytecode, VmRuntime::Wasm)
        .await
        .unwrap_ok_committed();
    Ok(module_id)
}

/// Tests that the chain listener, when subscribed to a publisher chain's events, downloads
/// only event-bearing blocks (sparse download) via `NewEvents` notifications.
///
/// The test creates a sender/receiver pair with a social-app event subscription, starts
/// the chain listener, waits for the initial sync, then has the sender create new blocks
/// (post, burn, post). Only the post blocks should be downloaded via sparse download.
#[cfg(feature = "wasmer")]
#[test_log::test(tokio::test)]
async fn test_chain_listener_sparse_event_download() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::with_wasm_runtime(WasmRuntime::Wasmer);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone())
        .await?
        .with_policy(ResourceControlPolicy::no_fees());

    // The receiver is at index 0, making it the admin chain. This ensures the sender
    // chain is only discovered via event subscriptions and listened to in EventsOnly mode.
    let receiver = builder.add_root_chain(0, Amount::ONE).await?;
    let sender = builder.add_root_chain(1, Amount::ONE).await?;
    let receiver_id = receiver.chain_id();

    // Publish and create the social app on the receiver chain.
    let module_id = publish_wasm_example(&receiver, "social").await?;
    let module_id = module_id.with_abi::<social::SocialAbi, (), ()>();

    let (application_id, _cert) = receiver
        .create_application(module_id, &(), &(), vec![])
        .await
        .unwrap_ok_committed();

    // Subscribe to the sender's events. This is a local runtime call
    // (subscribe_to_events); no cross-chain message to the sender.
    let subscribe_cert = receiver
        .execute_operation(Operation::user(
            application_id,
            &social::Operation::Subscribe {
                chain_id: sender.chain_id(),
            },
        )?)
        .await
        .unwrap_ok_committed();

    // Set up a chain listener for the receiver chain.
    // No sender posts yet — the sender chain has no blocks beyond genesis.
    let genesis_config = GenesisConfig::new_for_testing(&builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;
    let receiver_info = receiver.chain_info().await?;

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
            [(receiver_id, ListeningMode::FullChain)],
            format!("Client node for {:.8}", receiver_id),
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(1)),
            HashSet::new(),
            HashSet::new(),
            chain_client::Options::test_default(),
            &linera_core::client::RequestsSchedulerConfig::default(),
            DEFAULT_BLOCK_CACHE_SIZE,
            DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        )),
    };
    context.wallet().insert(
        receiver_id,
        wallet::Chain {
            owner: receiver.preferred_owner(),
            block_hash: receiver_info.block_hash,
            next_block_height: receiver_info.next_block_height,
            timestamp: clock.current_time(),
            pending_proposal: None,
            epoch: Some(Epoch::ZERO),
        },
    );

    // Subscribe to local node notifications for the receiver chain before starting
    // the listener, so we don't miss any notifications.
    let mut notifications = context.client.subscribe(vec![receiver_id]);

    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let chain_listener = ChainListener::new(
        config,
        context.clone(),
        storage.clone(),
        child_token,
        tokio::sync::mpsc::unbounded_channel().1,
        false,
    )
    .run()
    .await?;

    let handle = linera_base::Task::spawn(async move { chain_listener.await.unwrap() });

    // Wait for the chain listener to sync the receiver chain by waiting for a
    // NewBlock notification at or after the subscribe cert's height.
    let subscribe_height = subscribe_cert.block().header.height;
    loop {
        let notification = tokio::time::timeout(Duration::from_secs(10), notifications.recv())
            .await
            .expect("Timed out waiting for receiver chain sync")
            .expect("Notification channel closed");
        if let Reason::NewBlock { height, .. } = notification.reason {
            if height >= subscribe_height {
                break;
            }
        }
    }

    // Wait for the chain listener to discover event subscriptions and start
    // the sender chain listener (happens during process_notification).
    let sender_id = sender.chain_id();
    loop {
        if context
            .lock()
            .await
            .client()
            .chain_mode(sender_id)
            .is_some()
        {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Now create sender blocks AFTER the listener has finished its initial sync.
    // These will be handled via NewEvents → download_event_bearing_blocks (sparse path).

    // Post 1 (has events).
    let cert_post1 = sender
        .execute_operation(Operation::user(
            application_id,
            &social::Operation::Post {
                text: "First post".to_string(),
                image_url: None,
            },
        )?)
        .await
        .unwrap_ok_committed();

    // Burn (no events).
    let cert_burn = sender
        .burn(AccountOwner::CHAIN, Amount::from_millis(1))
        .await
        .unwrap_ok_committed();

    // Post 2 (has events).
    let cert_post2 = sender
        .execute_operation(Operation::user(
            application_id,
            &social::Operation::Post {
                text: "Second post".to_string(),
                image_url: None,
            },
        )?)
        .await
        .unwrap_ok_committed();

    // Wait for the chain listener to process the new events.
    // The receiver should create at least one block via process_inbox_without_prepare,
    // which emits a BlockExecuted notification.
    loop {
        let notification = tokio::time::timeout(Duration::from_secs(10), notifications.recv())
            .await
            .expect("Timed out waiting for event processing")
            .expect("Notification channel closed");
        if let Reason::BlockExecuted { height, .. } = notification.reason {
            if height >= receiver_info.next_block_height {
                break;
            }
        }
    }

    // Verify sparse download: only event-bearing blocks from the new sender blocks
    // should be stored. The burn block should NOT be stored because the listener
    // handles the sender chain in EventsOnly mode — NewBlock notifications are ignored,
    // only NewEvents triggers download_event_bearing_blocks.
    assert!(
        storage.read_certificate(cert_post1.hash()).await?.is_some(),
        "Event-bearing post block 1 should be stored"
    );
    assert!(
        storage.read_certificate(cert_burn.hash()).await?.is_none(),
        "Non-event burn block should NOT be stored (sparse download)"
    );
    assert!(
        storage.read_certificate(cert_post2.hash()).await?.is_some(),
        "Event-bearing post block 2 should be stored"
    );

    cancellation_token.cancel();
    handle.await;

    Ok(())
}
