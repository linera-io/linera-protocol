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
};
use linera_storage::Storage;
use tokio_util::sync::CancellationToken;

use super::util::make_genesis_config;
use crate::{
    chain_listener::{self, ChainListener, ChainListenerConfig, ClientContext as _},
    wallet::{UserChain, Wallet},
    Error,
};

struct ClientContext {
    wallet: Wallet,
    client: Arc<Client<environment::Test>>,
}

impl chain_listener::ClientContext for ClientContext {
    type Environment = environment::Test;

    fn wallet(&self) -> &Wallet {
        &self.wallet
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
        if self.wallet.get(chain_id).is_none() {
            self.wallet.insert(UserChain {
                chain_id,
                owner,
                block_hash: None,
                timestamp,
                next_block_height: BlockHeight::ZERO,
                pending_proposal: None,
                epoch: Some(epoch),
                listening_mode: ListeningMode::FullChain,
            });
        }

        Ok(())
    }

    async fn update_wallet(
        &mut self,
        client: &ChainClient<environment::Test>,
    ) -> Result<(), Error> {
        let info = client.chain_info().await?;
        let client_owner = client.preferred_owner();
        let pending_proposal = client.pending_proposal().clone();
        self.wallet
            .update_from_info(pending_proposal, client_owner, &info);
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
    let genesis_config = make_genesis_config(&builder);
    let admin_id = genesis_config.admin_id();
    let storage = builder.make_storage().await?;
    let epoch0 = client0.chain_info().await?.epoch;
    let epoch1 = client1.chain_info().await?.epoch;

    let mut context = ClientContext {
        wallet: Wallet::new(genesis_config),
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
            },
            admin_id,
            false,
            [chain_id0],
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
    let chain_listener = ChainListener::new(config, context, storage, child_token)
        .run(false) // Unit test doesn't need background sync
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

/// Tests that the chain listener always listens to the admin chain.
#[test_log::test(tokio::test)]
async fn test_chain_listener_admin_chain() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(Some(42));
    let config = ChainListenerConfig::default();
    let storage_builder = MemoryStorageBuilder::default();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer.clone()).await?;
    let client0 = builder.add_root_chain(0, Amount::ONE).await?;
    let genesis_config = make_genesis_config(&builder);
    let admin_id = genesis_config.admin_id();
    let storage = builder.make_storage().await?;

    let context = ClientContext {
        wallet: Wallet::new(genesis_config),
        client: Arc::new(Client::new(
            environment::Impl {
                storage: storage.clone(),
                network: builder.make_node_provider(),
                signer,
            },
            admin_id,
            false,
            [],
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
    let chain_listener = ChainListener::new(config, context, storage.clone(), child_token)
        .run(false) // Unit test doesn't need background sync
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

/// Tests that a chain followed with `SkipSenders` mode does NOT process its inbox.
/// This verifies that the chain_listener correctly respects the SkipSenders mode.
#[test_log::test(tokio::test)]
async fn test_chain_listener_skip_senders_no_inbox_processing() -> anyhow::Result<()> {
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
    let client1 = builder.add_root_chain(1, Amount::from_tokens(2)).await?;
    // Start a chain listener for chain 0 with SkipSenders mode.
    let genesis_config = make_genesis_config(&builder);
    let admin_id = genesis_config.admin_id();
    let listener_storage = builder.make_storage().await?;
    let epoch0 = client0.chain_info().await?.epoch;

    let mut context = ClientContext {
        wallet: Wallet::new(genesis_config),
        client: Arc::new(Client::new(
            environment::Impl {
                storage: listener_storage.clone(),
                network: builder.make_node_provider(),
                signer,
            },
            admin_id,
            false,
            [chain_id0],
            format!("Client node for {:.8}", chain_id0),
            Duration::from_secs(30),
            Duration::from_secs(1),
            chain_client::Options::test_default(),
            5_000,
            10_000,
            linera_core::client::RequestsSchedulerConfig::default(),
        )),
    };

    // Add chain 0 with SkipSenders listening mode.
    context.wallet.insert(UserChain {
        chain_id: chain_id0,
        owner: Some(owner),
        block_hash: None,
        timestamp: clock.current_time(),
        next_block_height: BlockHeight::ZERO,
        pending_proposal: None,
        epoch: Some(epoch0),
        listening_mode: ListeningMode::SkipSenders,
    });

    // Create a block on chain 0 (change ownership). This certificate should be synced.
    let owners = [(owner, 1), (AccountPublicKey::test_key(1).into(), 9)];
    let timeout_config = TimeoutConfig {
        base_timeout: TimeDelta::from_secs(1),
        timeout_increment: TimeDelta::ZERO,
        ..TimeoutConfig::default()
    };
    let chain0_cert = client0
        .change_ownership(ChainOwnership::multiple(owners, 0, timeout_config))
        .await?
        .unwrap();
    let chain0_cert_hash = chain0_cert.hash();

    // Create a chain listener that follows chain 0 in SkipSenders mode.
    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let chain_listener = ChainListener::new(config, context, listener_storage.clone(), child_token)
        .run(false) // Disable background sync to test core SkipSenders behavior
        .await
        .unwrap();
    let handle = linera_base::task::spawn(async move { chain_listener.await.unwrap() });

    // Verify that the followed chain's block (chain 0) WAS synced.
    // This confirms the listener is working and syncing chain0's blocks.
    let chain0_cert_in_storage = listener_storage.read_certificate(chain0_cert_hash).await?;
    assert!(
        chain0_cert_in_storage.is_some(),
        "Chain 0's certificate should be synced even in SkipSenders mode"
    );

    // Transfer one token from chain 1 to chain 0. This creates a block on chain 1.
    let recipient0 = Account::chain(chain_id0);
    client1
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient0)
        .await?
        .unwrap();

    // Wait for the listener to potentially process notifications
    for i in 0.. {
        clock.add(TimeDelta::from_secs(1));
        // Give async tasks time to run
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        client1.synchronize_from_validators().boxed().await?;
        let balance = client1.local_balance().await?;
        if balance == Amount::ONE {
            break;
        }
        clock.add(TimeDelta::from_secs(1));
        if i == 30 {
            panic!("Unexpected local balance: {}", balance);
        }
    }

    let chain_balance = client0.query_balance().await?;
    assert_eq!(
        chain_balance,
        Amount::ONE,
        "Chain 0's balance should remain unchanged as the inbox is not processed in SkipSenders mode"
    );

    // Verify that inbox was NOT processed by checking the listener's view of chain0.
    // In SkipSenders mode, the listener should not process incoming messages,
    // so the chain state should remain at height 1 (just the ownership change block).
    let chain0_state = listener_storage.load_chain(chain_id0).await?;
    let chain0_tip = chain0_state.tip_state.get();
    assert_eq!(
        chain0_tip.next_block_height,
        BlockHeight::from(1),
        "Chain 0 should be at height 0 (only the ownership change block). \
         If next_block_height > 1, the inbox was incorrectly processed in SkipSenders mode."
    );

    cancellation_token.cancel();
    handle.await;

    Ok(())
}
