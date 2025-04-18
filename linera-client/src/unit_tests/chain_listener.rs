// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{AccountPublicKey, AccountSecretKey, Secp256k1SecretKey},
    data_types::{Amount, TimeDelta},
    identifiers::AccountOwner,
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{BlanketMessagePolicy, Client},
    join_set_ext::JoinSet,
    node::CrossChainMessageDelivery,
    test_utils::{MemoryStorageBuilder, StorageBuilder as _, TestBuilder},
    DEFAULT_GRACE_PERIOD,
};
use linera_execution::system::Recipient;
use rand::SeedableRng as _;
use tokio_util::sync::CancellationToken;

use super::util::make_genesis_config;
use crate::{
    chain_listener::{ChainListener, ChainListenerConfig},
    client_context::ClientContext,
    config::WalletState,
    wallet::Wallet,
};

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
    let wallet = crate::persistent::memory::Memory::new(Wallet::new(genesis_config, Some(37)));
    let mut context = ClientContext {
        wallet: WalletState::new(wallet),
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
            Duration::from_secs(1),
        )),
        send_timeout: Duration::from_millis(4000),
        recv_timeout: Duration::from_millis(4000),
        retry_delay: Duration::from_millis(1000),
        max_retries: 10,
        chain_listeners: JoinSet::new(),
        blanket_message_policy: BlanketMessagePolicy::Accept,
        restrict_chain_ids_to: None,
    };
    let key_pair = AccountSecretKey::Secp256k1(Secp256k1SecretKey::generate_from(&mut rng));
    let owner = key_pair.public().into();
    context
        .update_wallet_for_new_chain(chain_id0, Some(key_pair), clock.current_time())
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
