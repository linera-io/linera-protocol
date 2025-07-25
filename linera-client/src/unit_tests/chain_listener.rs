// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{sync::Arc, time::Duration};

use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{AccountPublicKey, InMemorySigner},
    data_types::{Amount, BlockHeight, TimeDelta, Timestamp},
    identifiers::{AccountOwner, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{ChainClient, ChainClientOptions, Client},
    environment,
    test_utils::{MemoryStorageBuilder, StorageBuilder as _, TestBuilder},
};
use linera_execution::system::Recipient;
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
            ChainClientOptions::test_default(),
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
            ChainClientOptions::test_default(),
        )),
    };
    let context = Arc::new(Mutex::new(context));
    let cancellation_token = CancellationToken::new();
    let child_token = cancellation_token.child_token();
    let handle = linera_base::task::spawn({
        let storage = storage.clone();
        async move {
            ChainListener::new(config, context, storage, child_token)
                .run()
                .await
                .unwrap()
        }
    });
    // Burn one token.
    let certificate = client0
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await?
        .unwrap();
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
    handle.await?;

    Ok(())
}
