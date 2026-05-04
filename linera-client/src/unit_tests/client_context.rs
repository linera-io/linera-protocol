// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for [`ClientContext::update_wallet_from_client`].

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Round, TimeDelta},
    identifiers::{AccountOwner, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::{
    client::{chain_client, Client, ListeningMode},
    environment,
    join_set_ext::JoinSet,
    test_utils::{FaultType, MemoryStorageBuilder, TestBuilder},
    worker::{DEFAULT_BLOCK_CACHE_SIZE, DEFAULT_EXECUTION_STATE_CACHE_SIZE},
};
use linera_rpc::node_provider::DEFAULT_MAX_BACKOFF;

use crate::{client_context::ClientContext, config::GenesisConfig};

/// Builds a production [`ClientContext`] with a fresh in-memory wallet, sharing validators
/// with the given [`TestBuilder`].
async fn make_context(
    builder: &mut TestBuilder<MemoryStorageBuilder>,
    signer: InMemorySigner,
    chain_id: ChainId,
) -> anyhow::Result<ClientContext<environment::Test>> {
    let genesis_config = GenesisConfig::new_for_testing(builder);
    let admin_chain_id = genesis_config.admin_chain_id();
    let storage = builder.make_storage().await?;
    let client = Client::new(
        environment::Impl {
            storage,
            network: builder.make_node_provider(),
            signer,
            wallet: environment::TestWallet::default(),
        },
        admin_chain_id,
        false,
        [(chain_id, ListeningMode::FullChain)],
        format!("Client node for {:.8}", chain_id),
        Some(Duration::from_secs(30)),
        Some(Duration::from_secs(1)),
        chain_client::Options::test_default(),
        DEFAULT_BLOCK_CACHE_SIZE,
        DEFAULT_EXECUTION_STATE_CACHE_SIZE,
        &linera_core::client::RequestsSchedulerConfig::default(),
    );
    Ok(ClientContext {
        client: Arc::new(client),
        genesis_config,
        send_timeout: Duration::from_secs(4),
        recv_timeout: Duration::from_secs(4),
        retry_delay: Duration::from_secs(1),
        max_retries: 10,
        max_backoff: DEFAULT_MAX_BACKOFF,
        chain_listeners: JoinSet::default(),
        default_chain: None,
        client_metrics: None,
    })
}

/// A fast-round pending proposal must be persisted in the wallet.
#[test_log::test(tokio::test)]
async fn test_wallet_persists_fast_pending_proposal() -> anyhow::Result<()> {
    let signer = InMemorySigner::new(None);
    let mut builder =
        TestBuilder::new(MemoryStorageBuilder::default(), 4, 0, signer.clone()).await?;
    let mut client = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    client.options_mut().allow_fast_blocks = true;
    let chain_id = client.chain_id();
    let owner = client.identity().await?;

    let timeout_config = TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_secs(5)),
        ..TimeoutConfig::default()
    };
    let ownership = ChainOwnership {
        super_owners: BTreeSet::from_iter([owner]),
        owners: BTreeMap::default(),
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config,
    };
    client.change_ownership(ownership).await?;

    // Three offline validators make the burn fail; the proposal stays pending.
    builder.set_fault_type([1, 2, 3], FaultType::OfflineWithInfo);
    assert!(client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await
        .is_err());
    let pending = client
        .pending_proposal()
        .await
        .expect("expected fast pending proposal after failed burn");
    assert_eq!(pending.round, Some(Round::Fast));

    let context = make_context(&mut builder, signer, chain_id).await?;
    context.update_wallet_from_client(&client).await?;
    let stored = context
        .wallet()
        .get(chain_id)
        .expect("wallet missing chain entry")
        .pending_fast_proposal
        .expect("wallet missing fast pending proposal");
    assert_eq!(stored.round, Some(Round::Fast));
    Ok(())
}

/// A non-fast pending proposal must NOT be persisted in the wallet.
#[test_log::test(tokio::test)]
async fn test_wallet_drops_non_fast_pending_proposal() -> anyhow::Result<()> {
    let mut signer = InMemorySigner::new(None);
    let mut builder =
        TestBuilder::new(MemoryStorageBuilder::default(), 4, 0, signer.clone()).await?;
    let client = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let chain_id = client.chain_id();
    let owner0 = client.identity().await?;
    let owner1: AccountOwner = signer.generate_new().into();

    let timeout_config = TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_secs(5)),
        ..TimeoutConfig::default()
    };
    let ownership = ChainOwnership::multiple([(owner0, 100), (owner1, 100)], 10, timeout_config);
    client.change_ownership(ownership).await?;

    // Three offline validators make the burn fail; the proposal stays pending.
    builder.set_fault_type([1, 2, 3], FaultType::OfflineWithInfo);
    assert!(client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await
        .is_err());
    let pending = client
        .pending_proposal()
        .await
        .expect("expected non-fast pending proposal after failed burn");
    assert_eq!(pending.round, Some(Round::MultiLeader(0)));

    let context = make_context(&mut builder, signer, chain_id).await?;
    context.update_wallet_from_client(&client).await?;
    let stored = context
        .wallet()
        .get(chain_id)
        .expect("wallet missing chain entry");
    assert!(stored.pending_fast_proposal.is_none());
    Ok(())
}
