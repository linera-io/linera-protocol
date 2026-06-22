// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::cast_possible_truncation)]

mod test_helpers;
#[path = "./wasm_client_tests.rs"]
mod wasm;

use std::collections::{BTreeMap, BTreeSet};

use assert_matches::assert_matches;
use futures::StreamExt;
use linera_base::{
    crypto::{AccountSecretKey, CryptoHash, InMemorySigner},
    data_types::*,
    identifiers::{Account, AccountOwner, ApplicationId, BlobId, BlobType, GenericApplicationId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{IncomingBundle, MessageAction, MessageBundle, PostedMessage, Transaction},
    manager::LockingBlock,
    types::Timeout,
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::Committee, system::SystemOperation, ExecutionError, Message, MessageKind, Operation,
    QueryOutcome, ResourceControlPolicy, SystemMessage, SystemQuery, SystemResponse,
};
use linera_storage::Storage;
use rand::Rng;
use test_case::test_case;
use test_helpers::{
    assert_fees_exceed_funding, assert_insufficient_balance_during_operation,
    assert_insufficient_funding,
};

#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
#[cfg(feature = "storage-service")]
use crate::test_utils::ServiceStorageBuilder;
use crate::{
    client::{
        chain_client::{self, ChainClient},
        ClientOutcome, ListeningMode,
    },
    local_node::LocalNodeError,
    node::{
        NodeError::{self, ClientIoError},
        ValidatorNode,
    },
    test_utils::{
        ClientOutcomeResultExt as _, FaultType, MemoryStorageBuilder, StorageBuilder, TestBuilder,
    },
    updater::CommunicationError,
    worker::{Notification, Reason, WorkerError},
    Environment,
};

/// A test to ensure that our chain client listener remains `Send`.  This is a bit of a
/// hack, but requires that we not hold a `std::sync::Mutex` over `await` points, a
/// situation that is likely to lead to deadlock.  To further support this mode of
/// testing, `dashmap` references in [`crate::client`] have also been wrapped in a newtype
/// to make them non-`Send`.
#[test_log::test]
#[allow(dead_code)]
fn test_listener_is_send() {
    fn ensure_send(_: &impl Send) {}

    async fn check_listener(
        chain_client: ChainClient<impl Environment>,
    ) -> Result<(), chain_client::Error> {
        let (listener, _abort_notifications, _notifications) = chain_client.listen().await?;
        ensure_send(&listener);
        Ok(())
    }

    // If it compiles, we're okay — no need to do anything at runtime.
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_initiating_valid_transfer_with_notifications<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let chain_2 = builder.add_root_chain(2, Amount::ZERO).await?;
    // Listen to the notifications on the sender chain.
    let mut notifications = sender.subscribe()?;
    let (listener, _listen_handle, _) = sender.listen().await?;
    tokio::spawn(listener);
    {
        let certificate = sender
            .transfer_to_account(
                AccountOwner::CHAIN,
                Amount::from_tokens(3),
                Account::chain(chain_2.chain_id()),
            )
            .await
            .unwrap_ok_committed();
        assert_eq!(
            sender.chain_info().await?.next_block_height,
            BlockHeight::from(1)
        );
        assert!(sender.pending_proposal().await.is_none());
        assert_eq!(
            sender.local_balance().await.unwrap(),
            Amount::from_millis(1000)
        );
        assert_eq!(
            builder
                .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::ZERO, 3)
                .await,
            Some(certificate)
        );
    }
    let executed_block_hash = match notifications.next().await {
        Some(Notification {
            reason: Reason::BlockExecuted { hash, height },
            chain_id,
        }) => {
            assert_eq!(chain_id, sender.chain_id());
            assert_eq!(height, BlockHeight::ZERO);
            hash
        }
        _ => panic!("Expected BlockExecuted notification"),
    };
    // We execute twice in the local node:
    // - first time when setting the proposal as a pending block
    // - second time when processing pending block
    // This results in two BlockExecuted notifications.
    let _notification = notifications.next().await;
    match notifications.next().await {
        Some(Notification {
            reason: Reason::NewBlock { hash, height, .. },
            chain_id,
        }) => {
            assert_eq!(chain_id, sender.chain_id());
            assert_eq!(height, BlockHeight::ZERO);
            assert_eq!(executed_block_hash, hash);
        }
        other => panic!("Expected NewBlock notification, got {other:?}"),
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_claim_amount<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let owner = sender.identity().await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;
    let receiver_id = receiver.chain_id();
    let friend = receiver.identity().await?;
    let cert = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::new(receiver_id, owner),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(cert)
    );
    let cert = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_millis(100),
            Account::new(receiver_id, friend),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(1), 3)
            .await,
        Some(cert)
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(900)
    );
    receiver.synchronize_from_validators().await?;
    assert_eq!(receiver.process_inbox().await?.0.len(), 1);
    // The friend paid to receive the message.
    assert_eq!(
        receiver.local_owner_balance(friend).await.unwrap(),
        Amount::from_millis(100)
    );
    // The received amount is not in the unprotected balance.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        receiver.local_owner_balance(owner).await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        receiver.local_balances_with_owner(owner).await.unwrap(),
        (Amount::ZERO, Some(Amount::from_tokens(3)))
    );
    assert_eq!(receiver.query_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        receiver.query_owner_balance(owner).await.unwrap(),
        Amount::from_millis(3000)
    );
    assert_eq!(
        receiver.query_balances_with_owner(owner).await.unwrap(),
        (Amount::ZERO, Some(Amount::from_millis(3000)))
    );

    // First attempt that should be rejected.
    let cert1 = sender
        .claim(
            owner,
            receiver_id,
            Account::chain(sender.chain_id()),
            Amount::from_tokens(5),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(2), 3)
            .await,
        Some(cert1)
    );
    // Second attempt with a correct amount.
    let cert2 = sender
        .claim(
            owner,
            receiver_id,
            Account::chain(sender.chain_id()),
            Amount::from_tokens(2),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(3), 3)
            .await,
        Some(cert2)
    );

    receiver.synchronize_from_validators().await?;
    let cert = receiver.process_inbox().await?.0.pop().unwrap();
    {
        let messages = cert.block().body.incoming_bundles().collect::<Vec<_>>();
        // Both `Claim` messages were included in the block.
        assert_eq!(messages.len(), 2);
        // The first one was rejected.
        assert_eq!(messages[0].bundle.height, BlockHeight::from(2));
        assert_eq!(messages[0].action, MessageAction::Reject);
        // The second was accepted.
        assert_eq!(messages[1].bundle.height, BlockHeight::from(3));
        assert_eq!(messages[1].action, MessageAction::Accept);
    }

    sender.synchronize_from_validators().await?;
    sender.process_inbox().await?;
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(2900)
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_rotate_key_pair<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let new_public_key = signer.generate_new();
    let new_owner = AccountOwner::from(new_public_key);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let certificate = sender
        .rotate_key_pair(new_public_key)
        .await
        .unwrap_ok_committed();
    sender.set_preferred_owner(new_owner);
    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_eq!(sender.identity().await?, new_owner);
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(4000)
    );
    sender.synchronize_from_validators().await.unwrap();
    // Can still use the chain.
    sender
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await
        .unwrap();
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfer_ownership<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;

    let new_owner: AccountOwner = builder.signer.generate_new().into();
    let certificate = sender.transfer_ownership(new_owner).await.unwrap().unwrap();
    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_matches!(
        sender.identity().await,
        Err(chain_client::Error::NotAnOwner(_))
    );
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(4000)
    );
    sender.synchronize_from_validators().await.unwrap();
    // Cannot use the chain any more.
    assert_matches!(
        sender
            .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
            .await,
        Err(chain_client::Error::NotAnOwner(_))
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_share_ownership<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let new_owner = signer.generate_new().into();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let certificate = sender
        .share_ownership(new_owner, 100)
        .await
        .unwrap_ok_committed();
    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_eq!(sender.identity().await?, sender.preferred_owner().unwrap());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    sender.synchronize_from_validators().await.unwrap();
    // Can still use the chain with the old client.
    sender
        .burn(AccountOwner::CHAIN, Amount::from_tokens(2))
        .await
        .unwrap();
    let sender_info = sender.chain_info().await?;
    assert_eq!(sender_info.next_block_height, BlockHeight::from(2));
    // Make a client to try the new key.
    let mut client = builder
        .make_client(
            sender.chain_id(),
            sender_info.block_hash,
            BlockHeight::from(2),
        )
        .await?;
    client.set_preferred_owner(new_owner);
    // Local balance fails because the client has block height 2 but we haven't downloaded
    // the blocks yet.
    assert_matches!(
        client.local_balance().await,
        Err(chain_client::Error::WalletSynchronizationError)
    );
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );

    // We need at least three validators for making an operation.
    builder.set_fault_type([0, 1], FaultType::Offline);
    let result = client.burn(AccountOwner::CHAIN, Amount::ONE).await;
    assert_matches!(
        result,
        Err(chain_client::Error::CommunicationError(
            CommunicationError::Trusted(ClientIoError { .. }),
        ))
    );
    builder.set_fault_type([0, 1], FaultType::Honest);
    builder.set_fault_type([2, 3], FaultType::Offline);
    assert_matches!(
        sender.burn(AccountOwner::CHAIN, Amount::ONE).await,
        Err(chain_client::Error::CommunicationError(
            CommunicationError::Trusted(ClientIoError { .. })
        ))
    );

    // Half the validators voted for one block, half for the other. We need to make a proposal in
    // the next round to succeed.
    builder.set_fault_type([0, 1, 2, 3], FaultType::Honest);
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    client.clear_pending_proposal().await;
    client
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ONE);

    // The other client doesn't know the new round number yet:
    sender.synchronize_from_validators().await.unwrap();
    sender.process_inbox().await.unwrap();
    assert_eq!(client.chain_info().await?, sender.chain_info().await?);
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ONE);
    sender.clear_pending_proposal().await;
    sender
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();

    // That's it, we spent all our money on this test!
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ZERO);
    client.synchronize_from_validators().await.unwrap();
    client.process_inbox().await.unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
/// Regression test: A super owner should be able to propose even without multi-leader rounds.
async fn test_super_owner_in_single_leader_round<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let regular_owner = signer.generate_new().into();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let super_owner = sender.identity().await?;

    // Configure chain with one super owner and one regular owner, no multi-leader rounds.
    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: vec![super_owner],
        owners: vec![(regular_owner, 100)],
        first_leader: None,
        multi_leader_rounds: 0,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    });
    sender.execute_operation(owner_change_op).await.unwrap();

    // Enable fast blocks so the super owner can propose in the Fast round.
    sender.options_mut().allow_fast_blocks = true;

    // The super owner can still burn tokens since that doesn't use the validation round oracle.
    sender
        .burn(AccountOwner::CHAIN, Amount::from_tokens(2))
        .await
        .unwrap();
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_then_close_it<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let new_public_key = signer.generate_new();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    // New chains use the admin chain to verify their creation certificate.
    let _admin = builder.add_root_chain(0, Amount::ZERO).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    // Open the new chain.
    let (new_description, _certificate) = sender
        .open_chain(
            ChainOwnership::single(new_public_key.into()),
            ApplicationPermissions::default(),
            Amount::ZERO,
        )
        .await
        .unwrap_ok_committed();
    let new_id = new_description.id();

    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_eq!(sender.identity().await?, sender.preferred_owner().unwrap());
    // Make a client to try the new chain.
    let mut client = builder.make_client(new_id, None, BlockHeight::ZERO).await?;
    client.set_preferred_owner(new_public_key.into());
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(client.query_balance().await.unwrap(), Amount::ZERO);
    client.close_chain().await.unwrap();
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfer_then_open_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    // New chains use the admin chain to verify their creation certificate.
    let _admin = builder.add_root_chain(0, Amount::ZERO).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let parent = builder.add_root_chain(2, Amount::ZERO).await?;
    let new_public_key = builder.signer.generate_new();

    let new_chain_config = InitialChainConfig {
        ownership: ChainOwnership::single(new_public_key.into()),
        epoch: Epoch::ZERO,
        balance: Amount::ZERO,
        application_permissions: Default::default(),
    };
    let new_chain_origin = ChainOrigin::Child {
        parent: parent.chain_id(),
        block_height: BlockHeight::ZERO,
        chain_index: 0,
    };
    let new_id =
        ChainDescription::new(new_chain_origin, new_chain_config, clock.current_time()).id();

    // Transfer before creating the chain. The validators will ignore the cross-chain messages.
    let cert = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(new_id),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert)
    );
    // Open the new chain.
    let (new_description2, certificate) = parent
        .open_chain(
            ChainOwnership::single(new_public_key.into()),
            ApplicationPermissions::default(),
            Amount::ZERO,
        )
        .await
        .unwrap_ok_committed();
    let new_id2 = new_description2.id();
    assert_eq!(new_id, new_id2);
    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert_eq!(
        parent.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_eq!(sender.identity().await?, sender.preferred_owner().unwrap());
    assert_matches!(
        &certificate.block().body.transactions[0],
        Transaction::ExecuteOperation(Operation::System(system_op)) if matches!(**system_op, SystemOperation::OpenChain(_)),
        "Unexpected certificate value",
    );
    assert_eq!(
        builder
            .check_that_validators_have_certificate(parent.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    // Make a client to try the new chain.
    let mut client = builder.make_client(new_id, None, BlockHeight::ZERO).await?;
    client.set_preferred_owner(new_public_key.into());
    client.synchronize_from_validators().await.unwrap();
    // Make another block on top of the one that sent the two tokens, so that the validators
    // process the cross-chain messages.
    let cert = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(new_id),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(1), 3)
            .await,
        Some(cert)
    );
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client.query_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await
        .unwrap();
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_then_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    // New chains use the admin chain to verify their creation certificate.
    let _admin = builder.add_root_chain(0, Amount::ZERO).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let new_public_key = builder.signer.generate_new();
    // Open the new chain. We are both regular and super owner.
    let ownership = ChainOwnership::single(new_public_key.into())
        .with_regular_owner(new_public_key.into(), 100);
    let (new_description, _creation_certificate) = sender
        .open_chain(ownership, ApplicationPermissions::default(), Amount::ZERO)
        .await
        .unwrap_ok_committed();
    let new_id = new_description.id();
    // Transfer after creating the chain.
    let cert = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(new_id),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id(), BlockHeight::from(1), 3)
            .await,
        Some(cert)
    );
    assert_eq!(
        sender.chain_info().await?.next_block_height,
        BlockHeight::from(2)
    );
    assert!(sender.pending_proposal().await.is_none());
    assert_eq!(sender.identity().await?, sender.preferred_owner().unwrap());
    // Make a client to try the new chain.
    let mut client = builder.make_client(new_id, None, BlockHeight::ZERO).await?;
    client.set_preferred_owner(new_public_key.into());
    // Must process the creation certificate before using the new chain.
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    client.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client.query_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_close_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let client1 = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let client2 = builder.add_root_chain(2, Amount::from_tokens(4)).await?;

    let certificate = client1.close_chain().await.unwrap().unwrap().unwrap();
    assert_eq!(
        certificate.block().body.transactions.len(),
        1,
        "Unexpected transactions in certificate"
    );
    assert_matches!(
        &certificate.block().body.transactions[0],
        Transaction::ExecuteOperation(Operation::System(system_op)) if matches!(**system_op, SystemOperation::CloseChain),
        "Unexpected certificate value",
    );
    assert_eq!(
        client1.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(client1.pending_proposal().await.is_none());
    assert!(client1.identity().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    // Cannot use the chain for operations any more.
    let result = client1
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await;
    assert!(
        matches!(
            &result,
            Err(chain_client::Error::LocalNodeError(
                LocalNodeError::WorkerError(WorkerError::ChainError(err))
            )) if matches!(**err, ChainError::ClosedChain)
        ),
        "Unexpected result: {result:?}",
    );

    // Incoming messages now get rejected.
    let cert = client2
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(client1.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client2.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert)
    );
    client1.synchronize_from_validators().await.unwrap();
    let (certificates, _) = client1.process_inbox().await.unwrap();
    let block = certificates[0].block();
    assert_eq!(block.body.transactions.len(), 1);
    assert_matches!(
        &block.body.transactions[..],
        [Transaction::ReceiveMessages(IncomingBundle {
            origin: sender,
            action: MessageAction::Reject,
            bundle: MessageBundle {
                messages,
                ..
            },
        })] if *sender == client2.chain_id() && matches!(messages[..],
            [PostedMessage {
                message: Message::System(SystemMessage::Credit { .. }),
                kind: MessageKind::Tracked,
                ..
            }]
        )
    );

    // Since blocks are free of charge on closed chains, empty blocks are not allowed.
    assert_matches!(
        client1.execute_operations(vec![], vec![]).await,
        Err(chain_client::Error::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        )) if matches!(*error, ChainError::EmptyBlock)
    );

    // Trying to close the chain again returns None.
    let maybe_certificate = client1.close_chain().await.unwrap().unwrap();
    assert_matches!(maybe_certificate, None);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_initiating_valid_transfer_too_many_faults<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    builder.set_fault_type([0, 1], FaultType::NoChains);
    let chain_1 = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let chain_2 = builder.add_root_chain(2, Amount::from_tokens(4)).await?;
    let result = chain_1
        .transfer_to_account_unsafe_unconfirmed(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(chain_2.chain_id()),
        )
        .await;
    // Malicious validators always return ArithmeticError when handling a proposal.
    assert_matches!(
        result,
        Err(chain_client::Error::CommunicationError(
            CommunicationError::Trusted(NodeError::InactiveChain(_))
        )),
        "unexpected result"
    );
    assert_eq!(
        chain_1.chain_info().await?.next_block_height,
        BlockHeight::ZERO
    );
    assert!(chain_1.pending_proposal().await.is_some());
    assert_eq!(
        chain_1.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    Ok(())
}

/// Regression test: when the preferred owner changes while a pending proposal exists, the
/// next call to `process_pending_block` must sign the proposal as the original author (the
/// owner that staged it), not as the new preferred owner. Otherwise the worker rejects the
/// proposal with `WorkerError::InvalidSigner` because the operations in the block are
/// authenticated by the original owner.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_pending_block_is_signed_by_original_owner<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut client = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let owner_a = client.identity().await?;

    // Co-own the chain with a second owner `b` and no super-owner, so it runs in
    // multi-leader rounds — mirroring a chain shared by a wallet key and an autosigner.
    let owner_b: AccountOwner = builder.signer.generate_new().into();
    let ownership =
        ChainOwnership::multiple([(owner_a, 50), (owner_b, 50)], 10, TimeoutConfig::default());
    client.change_ownership(ownership).await?;

    // Stage a block as owner `a` that can't reach a quorum, so it stays pending in the
    // shared per-chain queue, authenticated by `a`.
    builder.set_fault_type([0, 1], FaultType::Offline);
    assert_matches!(
        client.burn(AccountOwner::CHAIN, Amount::ONE).await,
        Err(_),
        "the burn should fail to commit with only two of four validators online"
    );
    let pending = client
        .pending_proposal()
        .await
        .expect("a pending proposal authored by owner `a` should remain");
    assert_eq!(pending.block.authenticated_owner, Some(owner_a));

    // Bring the validators back and act as owner `b` on the same shared queue.
    builder.set_fault_type([0, 1], FaultType::Honest);
    client.synchronize_from_validators().await?;
    client.set_preferred_owner(owner_b);

    // Owner `b`'s client retries the pending block. The signer still holds owner `a`'s key,
    // so the proposal is signed as `a` and the worker accepts it.
    let certificate = client
        .process_pending_block()
        .await
        .unwrap_ok_committed()
        .expect("the pending block should be committed");
    assert_eq!(
        certificate.block().header.authenticated_owner,
        Some(owner_a)
    );
    assert!(client.pending_proposal().await.is_none());

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_bidirectional_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client1 = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let client2 = builder.add_root_chain(2, Amount::ZERO).await?;
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        client1.query_system_application(SystemQuery).await.unwrap(),
        QueryOutcome {
            response: SystemResponse {
                chain_id: client1.chain_id(),
                balance: Amount::from_tokens(3),
            },
            operations: vec![],
        }
    );
    let certificate = client1
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(client2.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert_eq!(
        client1.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(client1.pending_proposal().await.is_none());
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client1.query_system_application(SystemQuery).await.unwrap(),
        QueryOutcome {
            response: SystemResponse {
                chain_id: client1.chain_id(),
                balance: Amount::ZERO,
            },
            operations: vec![],
        },
    );

    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id(), BlockHeight::ZERO, 3)
            .await,
        Some(certificate)
    );
    // Local balance is lagging.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Obtain the certificate but do not process the inbox yet.
    client2.synchronize_from_validators().await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client2.query_system_application(SystemQuery).await.unwrap(),
        QueryOutcome {
            response: SystemResponse {
                chain_id: client2.chain_id(),
                balance: Amount::from_tokens(0),
            },
            operations: vec![],
        },
    );

    // Process the inbox and send back some money.
    assert_eq!(
        client2.chain_info().await?.next_block_height,
        BlockHeight::ZERO
    );
    let cert = client2
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(client1.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client2.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert)
    );
    assert_eq!(
        client2.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(client2.pending_proposal().await.is_none());
    assert_eq!(
        client2.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    client1.synchronize_from_validators().await.unwrap();
    client1.process_inbox().await.unwrap();
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ONE);
    // Local balance from client2 is now consolidated.
    assert_eq!(
        client2.query_system_application(SystemQuery).await.unwrap(),
        QueryOutcome {
            response: SystemResponse {
                chain_id: client2.chain_id(),
                balance: Amount::from_tokens(2),
            },
            operations: vec![],
        },
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_receiving_unconfirmed_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let client1 = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let client2 = builder.add_root_chain(2, Amount::ZERO).await?;
    let cert = client1
        .transfer_to_account_unsafe_unconfirmed(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(client2.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert)
    );
    // Transfer was executed locally.
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_millis(1000)
    );
    assert_eq!(
        client1.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(client1.pending_proposal().await.is_none());
    // The receiver doesn't know about the transfer.
    client2.process_inbox().await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client2.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client2.query_balance().await.unwrap(),
        Amount::from_millis(2000)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_receiving_unconfirmed_transfer_with_lagging_sender_balances<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client1 = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let client2 = builder.add_root_chain(2, Amount::ZERO).await?;
    let client3 = builder.add_root_chain(3, Amount::ZERO).await?;

    // Transferring funds from client1 to client2.
    // Confirming to a quorum of nodes only at the end.
    let cert1 = client1
        .transfer_to_account_unsafe_unconfirmed(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(client2.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert1)
    );
    let cert2 = client1
        .transfer_to_account_unsafe_unconfirmed(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(client2.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id(), BlockHeight::from(1), 3)
            .await,
        Some(cert2)
    );
    client1
        .communicate_chain_updates(&builder.initial_committee, None)
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Sending money from client2 fails, as a consequence.
    let obtained_error = client2
        .transfer_to_account_unsafe_unconfirmed(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id()),
        )
        .await;
    assert_insufficient_funding(obtained_error, ChainExecutionContext::Operation(0));
    // There is no pending block, since the proposal wasn't valid at the time.
    assert!(client2
        .process_pending_block()
        .await
        .unwrap_ok_committed()
        .is_none());
    // Retrying the whole command works after synchronization.
    client2.synchronize_from_validators().await.unwrap();
    let cert3 = client2
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client2.chain_id(), BlockHeight::from(0), 3)
            .await,
        Some(cert3)
    );
    // Blocks were executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ONE);
    assert_eq!(
        client1.chain_info().await?.next_block_height,
        BlockHeight::from(2)
    );
    assert!(client1.pending_proposal().await.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client2.chain_info().await?.next_block_height,
        BlockHeight::from(1)
    );
    assert!(client2.pending_proposal().await.is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client3.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client3.query_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_change_voting_rights<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    // To test that no fees are paid for reading or publishing committee blobs, we set the price
    // higher than the chain balance.
    let initial_balance = Amount::from_tokens(3);
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy {
            maximum_wasm_fuel_per_block: 30_000,
            blob_read: initial_balance + Amount::ONE,
            blob_published: initial_balance + Amount::ONE,
            blob_byte_read: initial_balance + Amount::ONE,
            blob_byte_published: initial_balance + Amount::ONE,
            ..ResourceControlPolicy::default()
        });
    let admin = builder.add_root_chain(0, initial_balance).await?;
    let user = builder.add_root_chain(1, Amount::ZERO).await?;
    let validators = builder.initial_committee.validators().clone();

    let committee = Committee::new(validators.clone(), ResourceControlPolicy::only_fuel())?;
    admin.stage_new_committee(committee).await.unwrap();

    // Root chain 1 receives the notification about the new epoch.
    // This must happen before the old committee is removed.
    user.synchronize_from_validators().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.chain_info().await?.epoch, Epoch::from(1));
    admin.revoke_epochs(Epoch::ZERO).await.unwrap();

    // Create a new committee.
    let committee = Committee::new(validators.clone(), ResourceControlPolicy::only_fuel())?;
    admin.stage_new_committee(committee).await.unwrap();
    assert_eq!(
        admin.chain_info().await?.next_block_height,
        BlockHeight::from(5)
    );
    assert!(admin.pending_proposal().await.is_none());
    assert!(admin.identity().await.is_ok());
    assert_eq!(admin.chain_info().await?.epoch, Epoch::from(2));

    // Sending money from the admin chain is supported.
    let cert1 = admin
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(user.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(admin.chain_id(), BlockHeight::from(5), 3)
            .await,
        Some(cert1)
    );
    let cert2 = admin
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(user.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(admin.chain_id(), BlockHeight::from(6), 3)
            .await,
        Some(cert2)
    );

    // User is still at the initial epoch, but we can receive transfers from future
    // epochs AFTER synchronizing the client with the admin chain.
    let info = user.synchronize_chain_state(user.chain_id()).await?;
    user.process_inbox().await.unwrap();
    assert_eq!(info.epoch, Epoch(1));
    assert_eq!(user.local_balance().await?, Amount::ZERO);

    user.synchronize_from_validators().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.chain_info().await?.epoch, Epoch::from(2));
    assert_eq!(user.local_balance().await?, Amount::from_tokens(3));

    // Revoking the current or an already revoked epoch fails.
    assert_matches!(
        admin.revoke_epochs(Epoch::ZERO).await,
        Err(chain_client::Error::EpochAlreadyRevoked)
    );
    assert_matches!(
        admin.revoke_epochs(Epoch::from(3)).await,
        Err(chain_client::Error::CannotRevokeCurrentEpoch(Epoch(2)))
    );

    // Have the admin chain deprecate the previous epoch.
    admin.revoke_epochs(Epoch::from(1)).await.unwrap();

    // Try to make a transfer back to the admin chain.
    let cert3 = user
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(admin.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(user.chain_id(), BlockHeight::from(2), 3)
            .await,
        Some(cert3)
    );
    admin.synchronize_from_validators().await.unwrap();
    assert_eq!(user.chain_info().await?.epoch, Epoch::from(2));

    // Try again to make a transfer back to the admin chain.
    let cert4 = user
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(admin.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        builder
            .check_that_validators_have_certificate(user.chain_id(), BlockHeight::from(3), 3)
            .await,
        Some(cert4)
    );
    admin.synchronize_from_validators().await.unwrap();
    admin.process_inbox().await.unwrap();
    // Transfer goes through and the previous one as well thanks to block chaining.
    assert_eq!(admin.local_balance().await.unwrap(), Amount::from_tokens(3));

    user.change_application_permissions(ApplicationPermissions::new_single(ApplicationId::new(
        CryptoHash::test_hash("foo"),
    )))
    .await?;

    let committee = Committee::new(validators, ResourceControlPolicy::default())?;
    admin.stage_new_committee(committee).await.unwrap();
    assert_eq!(admin.chain_info().await?.epoch, Epoch::from(3));

    // Despite the restrictive application permissions, some system operations are still allowed,
    // and the user chain can migrate to the new epoch.
    user.synchronize_from_validators().await?;
    user.process_inbox().await?;
    assert_eq!(user.chain_info().await?.epoch, Epoch::from(3));

    Ok(())
}

/// Tests that a client whose local view of the admin chain is stale can still use a blob
/// whose publishing certificate was signed by a committee from an epoch the client has
/// not heard of yet.
///
/// The blob-recovery path downloads the publishing certificate from a validator, but
/// validating it locally yields `CheckCertificateResult::FutureEpoch`. The client must
/// react by catching up on the admin chain and retrying, so the blob read succeeds.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_stale_client_reads_blob_published_in_future_epoch<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let admin = builder.add_root_chain(0, Amount::from_tokens(3)).await?;
    let publisher = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let stale = builder.add_root_chain(2, Amount::from_tokens(3)).await?;
    let validators = builder.initial_committee.validators().clone();

    // Move the network to epoch 1. The publisher catches up; `stale` does not.
    let committee = Committee::new(validators, ResourceControlPolicy::default())?;
    admin.stage_new_committee(committee).await?;
    publisher.synchronize_from_validators().await?;
    publisher.process_inbox().await?;
    assert_eq!(publisher.chain_info().await?.epoch, Epoch::from(1));
    assert_eq!(stale.chain_info().await?.epoch, Epoch::ZERO);

    // Publish a data blob under the epoch-1 committee.
    let blob_bytes = b"future-epoch blob".to_vec();
    let blob_id = Blob::new(BlobContent::new_data(blob_bytes.clone())).id();
    let certificate = publisher
        .publish_data_blob(blob_bytes)
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.block().header.epoch, Epoch::from(1));

    // The stale client needs the blob: it is missing locally, so the client downloads
    // the publishing certificate from a validator and must accept it after catching up
    // on the admin chain, rather than choking on the unknown epoch.
    stale.read_data_blob(blob_id.hash).await.unwrap().unwrap();

    Ok(())
}

/// Like `test_stale_client_reads_blob_published_in_future_epoch`, but one validator is
/// offline while the stale client catches up. The admin-chain self-heal races the
/// reachable validators, so a single unresponsive one must not prevent recovery.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_stale_client_reads_future_epoch_blob_with_offline_validator<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let admin = builder.add_root_chain(0, Amount::from_tokens(3)).await?;
    let publisher = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let stale = builder.add_root_chain(2, Amount::from_tokens(3)).await?;
    let validators = builder.initial_committee.validators().clone();

    // Move the network to epoch 1 (the three remaining validators still form a quorum).
    // The publisher catches up; `stale` does not.
    let committee = Committee::new(validators, ResourceControlPolicy::default())?;
    admin.stage_new_committee(committee).await?;
    publisher.synchronize_from_validators().await?;
    publisher.process_inbox().await?;
    assert_eq!(publisher.chain_info().await?.epoch, Epoch::from(1));
    assert_eq!(stale.chain_info().await?.epoch, Epoch::ZERO);

    // Publish a data blob under the epoch-1 committee.
    let blob_bytes = b"future-epoch blob".to_vec();
    let blob_id = Blob::new(BlobContent::new_data(blob_bytes.clone())).id();
    let certificate = publisher
        .publish_data_blob(blob_bytes)
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.block().header.epoch, Epoch::from(1));

    // Take one validator offline. The stale client's admin-chain catch-up still succeeds
    // against the reachable ones.
    builder.set_fault_type([3], FaultType::Offline);

    stale.read_data_blob(blob_id.hash).await.unwrap().unwrap();

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_insufficient_balance<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut policy = ResourceControlPolicy::only_fuel();
    policy.operation = Amount::from_micros(1); // Otherwise BURN passes b/c it will be free.
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(policy);
    let sender = builder.add_root_chain(1, Amount::from_tokens(3)).await?;

    let obtained_error = sender
        .burn(AccountOwner::CHAIN, Amount::from_tokens(4))
        .await;
    assert_insufficient_balance_during_operation(obtained_error, 0);

    let obtained_error = sender
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await;
    // We have balance=3, we try to burn 3 tokens but the operation itself
    // costs 1 microtoken so we don't have enough balance to pay for it.
    assert_fees_exceed_funding(obtained_error);
    Ok(())
}

/// The sender chain should be stored sparsely in the receiver's node: only blocks
/// that sent messages to us should be downloaded, not the intermediate ones. When
/// the sender is a non-root chain (so its `ChainDescription` blob isn't in the
/// receiver's genesis storage) and the sender's height-0 block doesn't send to us,
/// the `ChainDescription` itself should never be downloaded either — not during
/// the initial message processing, and not during a later re-sync that routes the
/// sender through `retry_pending_cross_chain_requests_from_sender_chains`.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_sparse_sender_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let _admin = builder.add_root_chain(0, Amount::ZERO).await?;
    let owner = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;
    let receiver_id = receiver.chain_id();

    // Open the sender as a non-root chain so that its `ChainDescription` isn't
    // pre-populated in the receiver's genesis storage. We also make the sender's
    // first block a burn rather than a transfer to the receiver: that keeps the
    // message-sending blocks at height >= 1, so preprocessing them never requires
    // the sender's `ChainDescription` (only height-0 blocks do).
    let sender_public_key = builder.signer.generate_new();
    let sender_ownership = ChainOwnership::single(sender_public_key.into())
        .with_regular_owner(sender_public_key.into(), 100);
    let (sender_description, _creation_certificate) = Box::pin(owner.open_chain(
        sender_ownership,
        ApplicationPermissions::default(),
        Amount::from_tokens(4),
    ))
    .await
    .unwrap_ok_committed();
    let sender_id = sender_description.id();
    let sender_chain_desc_blob_id = BlobId::new(sender_id.0, BlobType::ChainDescription);

    let mut sender = builder
        .make_client(sender_id, None, BlockHeight::ZERO)
        .await?;
    sender.set_preferred_owner(sender_public_key.into());
    sender.synchronize_from_validators().await?;

    // Heights 0 and 2 are burns; heights 1 and 3 send to the receiver.
    let cert0 = sender
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    let cert1 = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(receiver_id),
        )
        .await
        .unwrap_ok_committed();
    let cert2 = sender
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    let cert3 = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(receiver_id),
        )
        .await
        .unwrap_ok_committed();

    // Process the notification about the most recent incoming message. This walks
    // back along `previous_message_blocks` and preprocesses only the sender blocks
    // that sent to us (heights 1 and 3).
    let notification = Notification {
        chain_id: receiver_id,
        reason: Reason::NewIncomingBundle {
            origin: sender_id,
            height: cert3.block().header.height,
        },
    };
    let validator = builder
        .initial_committee
        .validator_addresses()
        .next()
        .unwrap();
    receiver
        .process_notification_from(notification, validator)
        .await;
    receiver.process_inbox().await?;

    // Only the blocks that sent something to the receiver should be in local
    // storage. The burn blocks in between — and the sender's `ChainDescription`
    // blob itself — should never have been downloaded.
    let storage = receiver.storage_client();
    assert!(!storage.contains_certificate(cert0.hash()).await?);
    assert!(storage.contains_certificate(cert1.hash()).await?);
    assert!(!storage.contains_certificate(cert2.hash()).await?);
    assert!(storage.contains_certificate(cert3.hash()).await?);
    assert!(
        !storage.contains_blob(sender_chain_desc_blob_id).await?,
        "preprocessing non-height-0 sender blocks must not download the ChainDescription",
    );

    // `process_notification_from` does not advance the client's
    // `received_certificate_trackers`, so a subsequent `synchronize_from_validators`
    // still sees (sender, 1) and (sender, 3) in the received log. But the sender's
    // outbox has those heights scheduled locally now, so `find_received_certificates`
    // filters them out and routes the sender through
    // `retry_pending_cross_chain_requests_from_sender_chains`. Before the fix this
    // initialized the sender's chain worker inside the receiver's node and, on
    // failing to find the `ChainDescription` blob in storage, triggered a download.
    receiver.synchronize_from_validators().await?;
    assert!(
        !storage.contains_blob(sender_chain_desc_blob_id).await?,
        "retry_pending_cross_chain_requests must not download the ChainDescription",
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_finalize_locked_block_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client_1a = builder.add_root_chain(1, Amount::ZERO).await?;
    let owner_1a = client_1a.identity().await.unwrap();
    let chain_1 = client_1a.chain_id();
    let pk_1b = builder.signer.generate_new();
    let owner_1b = pk_1b.into();

    let owners = [(owner_1a, 50), (owner_1b, 50)];
    let ownership = ChainOwnership::multiple(owners, 10, TimeoutConfig::default());
    client_1a.change_ownership(ownership).await?;

    let client_1b = builder
        .make_client(
            chain_1,
            client_1a.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;

    let client_2a = builder.add_root_chain(2, Amount::from_tokens(10)).await?;
    let owner_2a = client_2a.identity().await.unwrap();
    let chain_2 = client_2a.chain_id();
    let pk_2b = builder.signer.generate_new();
    let owner_2b = pk_2b.into();

    let owners = [(owner_2a, 50), (owner_2b, 50)];
    let ownership = ChainOwnership::multiple(owners, 10, TimeoutConfig::default());
    client_2a.change_ownership(ownership).await.unwrap();

    let mut client_2b = builder
        .make_client(
            chain_2,
            client_2a.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client_2b.set_preferred_owner(owner_2b);

    let blob0_bytes = b"blob0".to_vec();
    let blob0_id = Blob::new(BlobContent::new_data(blob0_bytes.clone())).id();

    // Try to read a blob without publishing it first, should fail
    let result = client_1a
        .execute_operation(SystemOperation::VerifyBlob { blob_id: blob0_id })
        .await;
    assert_matches!(
        result,
        Err(chain_client::Error::CannotDownloadBlob(blob_id)) if blob_id == blob0_id
    );

    // Take one validator down
    builder.set_fault_type([2], FaultType::Offline);

    // Publish blob on chain 1
    let publish_certificate = client_1a
        .publish_data_blob(blob0_bytes)
        .await
        .unwrap_ok_committed();
    assert!(publish_certificate
        .block()
        .requires_or_creates_blob(&blob0_id));

    // Validators goes back up
    builder.set_fault_type([2], FaultType::Honest);
    // But another one goes down
    builder.set_fault_type([3], FaultType::Offline);

    // Try to read the blob. This is a different client but on the same chain, so when we
    // synchronize this with the validators before executing the block, we'll actually download
    // and cache locally the blobs that were published by `client_a`. So this will succeed.
    client_1b.prepare_chain().await?;
    let certificate = client_1b
        .execute_operation(SystemOperation::VerifyBlob { blob_id: blob0_id })
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.round, Round::MultiLeader(0));
    // The blob is not new on this chain, so it is not required.
    assert!(!certificate.block().requires_or_creates_blob(&blob0_id));

    // Validators 0, 1, 2 now don't process validated block certificates. Client 2A tries to
    // commit a block that reads blob 0 and publishes blob 1. Client 2A will have that block
    // locked now, but the validators won't.
    builder.set_fault_type([0, 1, 2], FaultType::DontProcessValidated);

    client_2a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::new_data(b"blob1".to_vec());
    let blob1_hash = blob1.id().hash;

    let blob_0_1_operations = vec![
        Operation::system(SystemOperation::VerifyBlob { blob_id: blob0_id }),
        Operation::system(SystemOperation::PublishDataBlob {
            blob_hash: blob1_hash,
        }),
    ];
    let b0_result = client_2a
        .execute_operations(blob_0_1_operations.clone(), vec![blob1.clone()])
        .await;

    assert!(b0_result.is_err());
    assert!(client_2a.pending_proposal().await.is_some());

    for i in 0..=2 {
        let info = builder
            .node(i)
            .chain_info_with_manager_values(chain_2)
            .await?;
        assert_eq!(info.manager.requested_locking, None);
    }

    // Now 2 goes offline and the other validators are working again.
    builder.set_fault_type([2], FaultType::Offline);
    builder.set_fault_type([0, 1, 3], FaultType::Honest);

    // We make validator 3 (who does not have the block proposal) process the validated block.
    let info2_a = client_2a.chain_info_with_manager_values().await?;
    let locking = *info2_a.manager.requested_locking.unwrap();
    let LockingBlock::Regular(validated) = locking else {
        panic!("Unexpected locking fast block.");
    };
    {
        let node3 = builder.node(3);
        let content1 = blob1.into_content();
        assert_matches!(node3.handle_pending_blob(chain_2, content1.clone()).await,
            Err(NodeError::WorkerError { error })
            if error.contains("Blob was not required by any pending block")
        );
        let result = node3.handle_validated_certificate(validated.clone()).await;
        assert_matches!(result, Err(NodeError::BlobsNotFound(_)));
        node3.handle_pending_blob(chain_2, content1).await?;
        let response = node3
            .handle_validated_certificate(validated.clone())
            .await?;
        assert_eq!(
            response.info.manager.pending.unwrap().round,
            Round::MultiLeader(0)
        );
    }

    // Client 2B should be able to synchronize the locking block and the blobs from validator 3.
    client_2b.synchronize_from_validators().await.unwrap();
    let info2_b = client_2b.chain_info_with_manager_values().await?;
    assert_eq!(
        LockingBlock::Regular(validated),
        *info2_b.manager.requested_locking.unwrap()
    );
    let recipient = Account::burn_address(client_2b.chain_id());
    let outcome = client_2b
        .transfer_to_account(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await?;

    let ClientOutcome::Conflict(certificate) = outcome else {
        panic!("Unexpected outcome: {outcome:?}");
    };

    // The conflicting block should be b0
    assert_eq!(
        certificate.block().body.operations().collect::<Vec<_>>(),
        blob_0_1_operations.iter().collect::<Vec<_>>(),
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_existing_proposal_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    let client1 = builder.add_root_chain(1, Amount::ZERO).await?;
    let client2_a = builder.add_root_chain(2, Amount::from_tokens(10)).await?;

    let chain_id2 = client2_a.chain_id();

    let owner2_a = client2_a.identity().await.unwrap();
    let owner2_b = builder.signer.generate_new().into();

    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(owner2_a, 50), (owner2_b, 50)],
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    });
    client2_a
        .execute_operation(owner_change_op.clone())
        .await
        .unwrap();

    let mut client2_b = builder
        .make_client(
            chain_id2,
            client2_a.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client2_b.set_preferred_owner(owner2_b);

    // Take one validator down
    builder.set_fault_type([3], FaultType::Offline);

    let blob0_bytes = b"blob0".to_vec();
    let blob0_id = Blob::new(BlobContent::new_data(blob0_bytes.clone())).id();

    // Publish blob on chain 1
    let publish_certificate = client1
        .publish_data_blob(blob0_bytes)
        .await
        .unwrap_ok_committed();
    assert!(publish_certificate
        .block()
        .requires_or_creates_blob(&blob0_id));

    builder.set_fault_type([0, 1, 2], FaultType::DontProcessValidated);

    client2_a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::new_data(b"blob1".to_vec());
    let blob1_hash = blob1.id().hash;

    let blob_0_1_operations = vec![
        Operation::system(SystemOperation::VerifyBlob { blob_id: blob0_id }),
        Operation::system(SystemOperation::PublishDataBlob {
            blob_hash: blob1_hash,
        }),
    ];
    let b0_result = client2_a
        .execute_operations(blob_0_1_operations.clone(), vec![blob1])
        .await;

    assert!(b0_result.is_err());
    assert!(client2_a.pending_proposal().await.is_some());

    for i in 0..=2 {
        let validator_manager = builder
            .node(i)
            .chain_info_with_manager_values(chain_id2)
            .await
            .unwrap()
            .manager;
        assert_eq!(
            validator_manager
                .requested_proposed
                .unwrap()
                .content
                .block
                .operations()
                .collect::<Vec<_>>(),
            blob_0_1_operations.iter().collect::<Vec<_>>(),
        );
        assert!(validator_manager.requested_locking.is_none());
    }

    builder.set_fault_type([2], FaultType::Offline);
    builder.set_fault_type([0, 1, 3], FaultType::Honest);

    client2_b.prepare_chain().await.unwrap();
    let recipient = Account::burn_address(client2_b.chain_id());
    let bt_certificate = client2_b
        .transfer_to_account(AccountOwner::CHAIN, Amount::from_tokens(1), recipient)
        .await
        .unwrap_ok_committed();

    let certificate_values = client2_b
        .read_confirmed_blocks_downward(bt_certificate.hash(), 2)
        .await
        .unwrap();

    // Latest block should be the burn
    assert!(certificate_values[0].block().body.operations().any(|op| *op
        == Operation::system(SystemOperation::Transfer {
            owner: AccountOwner::CHAIN,
            recipient,
            amount: Amount::from_tokens(1),
        })));

    // Previous should be the `ChangeOwnership` operation, as the blob operations shouldn't be executed here.
    assert!(certificate_values[1]
        .block()
        .body
        .operations()
        .any(|op| *op == owner_change_op));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_conflicting_proposals<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let owner2 = signer.generate_new().into();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client1 = builder.add_root_chain(1, Amount::ONE).await?;
    let chain_id = client1.chain_id();
    let owner1 = client1.identity().await?;
    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(owner1, 50), (owner2, 50)],
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    });
    client1
        .execute_operation(owner_change_op.clone())
        .await
        .unwrap();
    let mut client2 = builder
        .make_client(
            chain_id,
            client1.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client2.set_preferred_owner(owner2);
    client2.synchronize_from_validators().await.unwrap();

    // Client 1 makes a proposal to only validators 0 and 1.
    builder.set_fault_type([2, 3], FaultType::OfflineWithInfo);
    assert!(client1
        .burn(AccountOwner::CHAIN, Amount::from_millis(1))
        .await
        .is_err());

    // Client 2's proposal reaches only 2 and 3.
    builder.set_fault_type([0, 1], FaultType::OfflineWithInfo);
    builder.set_fault_type([2, 3], FaultType::Honest);
    assert!(client2
        .burn(AccountOwner::CHAIN, Amount::from_millis(2))
        .await
        .is_err());

    // TODO(#3894): Make test_utils deterministic.
    // The following condition is currently satisfied most of the time, but in some cases
    // the faulty validators return an error before the honest ones process the proposal.

    // for i in 0..4 {
    //     let info = builder
    //         .node(i)
    //         .chain_info_with_manager_values(chain_id)
    //         .await?;
    //     assert_eq!(
    //         AccountOwner::from(info.manager.requested_proposed.unwrap().public_key),
    //         if i < 2 { owner1 } else { owner2 },
    //     );
    // }

    // Once all validators are functional again, one of the blocks should get finalized.
    builder.set_fault_type([0, 1, 2, 3], FaultType::Honest);

    client1.synchronize_from_validators().await.unwrap();
    assert_matches!(
        client1.publish_data_blob(b"foo".to_vec()).await,
        Ok(ClientOutcome::Conflict(_))
    );

    assert_eq!(
        client1.chain_info().await?.next_block_height,
        BlockHeight::from(2)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_re_propose_locked_block_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    let client1 = builder.add_root_chain(1, Amount::ZERO).await?;
    let client2 = builder.add_root_chain(2, Amount::ZERO).await?;
    let client3_a = builder.add_root_chain(3, Amount::from_tokens(10)).await?;

    let chain_id3 = client3_a.chain_id();

    let owner3_a = client3_a.identity().await.unwrap();
    let owner3_b = builder.signer.generate_new().into();
    let owner3_c = builder.signer.generate_new().into();

    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(owner3_a, 50), (owner3_b, 50), (owner3_c, 50)],
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    });

    client3_a
        .execute_operation(owner_change_op.clone())
        .await
        .unwrap();

    let block_hash = client3_a.chain_info().await?.block_hash;
    let mut client3_b = builder
        .make_client(chain_id3, block_hash, BlockHeight::from(1))
        .await?;
    client3_b.set_preferred_owner(owner3_b);

    let mut client3_c = builder
        .make_client(chain_id3, block_hash, BlockHeight::from(1))
        .await?;
    client3_c.set_preferred_owner(owner3_c);

    // Take one validator down
    builder.set_fault_type([3], FaultType::Offline);

    let blob0_bytes = b"blob0".to_vec();
    let blob0_id = Blob::new(BlobContent::new_data(blob0_bytes.clone())).id();

    client1.synchronize_from_validators().await.unwrap();
    // Publish blob0 on chain 1
    let publish_certificate0 = client1
        .publish_data_blob(blob0_bytes)
        .await
        .unwrap_ok_committed();
    assert!(publish_certificate0
        .block()
        .requires_or_creates_blob(&blob0_id));

    let blob2_bytes = b"blob2".to_vec();
    let blob2_id = Blob::new(BlobContent::new_data(blob2_bytes.clone())).id();

    client2.synchronize_from_validators().await.unwrap();
    // Publish blob2 on chain 2
    let publish_certificate2 = client2
        .publish_data_blob(blob2_bytes)
        .await
        .unwrap_ok_committed();
    assert!(publish_certificate2
        .block()
        .requires_or_creates_blob(&blob2_id));

    builder.set_fault_type([0, 1], FaultType::DontProcessValidated);
    builder.set_fault_type([2], FaultType::DontSendConfirmVote);

    client3_a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::new_data(b"blob1".to_vec());
    let blob1_hash = blob1.id().hash;

    let blob_0_1_operations = vec![
        Operation::system(SystemOperation::VerifyBlob { blob_id: blob0_id }),
        Operation::system(SystemOperation::PublishDataBlob {
            blob_hash: blob1_hash,
        }),
    ];
    let b0_result = client3_a
        .execute_operations(blob_0_1_operations.clone(), vec![blob1.clone()])
        .await;

    assert!(b0_result.is_err());
    assert!(client3_a.pending_proposal().await.is_some());

    let manager = client3_a
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    // Validator 2 may or may not have processed the validated block before the update was
    // canceled due to the errors from the faulty validators. Submit it again to make sure
    // it's there, so that client 2 can download and re-propose it later.
    let locking = *manager.requested_locking.unwrap();
    let LockingBlock::Regular(validated_block_certificate) = locking else {
        panic!("Unexpected locking fast block.");
    };
    let resubmission_result = builder
        .node(2)
        .handle_validated_certificate(validated_block_certificate)
        .await;
    assert_matches!(resubmission_result, Err(NodeError::ClientIoError { .. }));

    for i in 0..=2 {
        let validator_manager = builder
            .node(i)
            .chain_info_with_manager_values(chain_id3)
            .await
            .unwrap()
            .manager;
        assert_eq!(
            validator_manager
                .requested_proposed
                .unwrap()
                .content
                .block
                .operations()
                .collect::<Vec<_>>(),
            blob_0_1_operations.iter().collect::<Vec<_>>(),
        );

        if i == 2 {
            let locking = *validator_manager.requested_locking.unwrap();
            let LockingBlock::Regular(validated) = locking else {
                panic!("Unexpected locking fast block.");
            };
            assert_eq!(
                validated.block().body.operations().collect::<Vec<_>>(),
                blob_0_1_operations.iter().collect::<Vec<_>>()
            );
        } else {
            assert!(validator_manager.requested_locking.is_none());
        }
    }

    builder.set_fault_type([2], FaultType::Offline);
    builder.set_fault_type([3], FaultType::DontSendConfirmVote);

    client3_b.synchronize_from_validators().await.unwrap();
    let blob3 = Blob::new_data(b"blob3".to_vec());
    let blob3_hash = blob3.id().hash;

    let blob_2_3_operations = vec![
        Operation::system(SystemOperation::VerifyBlob { blob_id: blob2_id }),
        Operation::system(SystemOperation::PublishDataBlob {
            blob_hash: blob3_hash,
        }),
    ];
    let b1_result = client3_b
        .execute_operations(blob_2_3_operations.clone(), vec![blob3.clone()])
        .await;
    assert!(b1_result.is_err());

    let manager = client3_b
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    // Validator 3 may or may not have processed the validated block before the update was
    // canceled due to the errors from the faulty validators. Submit it again to make sure
    // it's there, so that client 2 can download and re-propose it later.
    let locking = *manager.requested_locking.unwrap();
    let LockingBlock::Regular(validated_block_certificate) = locking else {
        panic!("Unexpected locking fast block.");
    };
    let resubmission_result = builder
        .node(3)
        .handle_validated_certificate(validated_block_certificate)
        .await;
    assert_matches!(resubmission_result, Err(NodeError::ClientIoError { .. }));

    let validator_manager = builder
        .node(3)
        .chain_info_with_manager_values(chain_id3)
        .await
        .unwrap()
        .manager;
    assert_eq!(
        validator_manager
            .requested_proposed
            .unwrap()
            .content
            .block
            .operations()
            .collect::<Vec<_>>(),
        blob_2_3_operations.iter().collect::<Vec<_>>(),
    );
    let locking = *validator_manager.requested_locking.unwrap();
    let LockingBlock::Regular(validated) = locking else {
        panic!("Unexpected locking fast block.");
    };
    assert_eq!(
        validated.block().body.operations().collect::<Vec<_>>(),
        blob_2_3_operations.iter().collect::<Vec<_>>()
    );

    builder.set_fault_type([1], FaultType::Offline);
    builder.set_fault_type([0, 2, 3], FaultType::Honest);

    client3_c.synchronize_from_validators().await.unwrap();
    let blob4_data = b"blob4".to_vec();
    let outcome = client3_c.publish_data_blob(blob4_data).await?;

    let ClientOutcome::Conflict(certificate) = outcome else {
        panic!("Unexpected outcome: {outcome:?}");
    };

    // The conflicting block should be b1
    assert_eq!(
        certificate.block().body.operations().collect::<Vec<_>>(),
        blob_2_3_operations.iter().collect::<Vec<_>>(),
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_request_leader_timeout<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let observer = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_id = client.chain_id();
    let observer_id = observer.chain_id();
    let owner0 = client.identity().await.unwrap();
    let owner1 = AccountSecretKey::generate().public().into();
    // The observer needs to fully execute the client's chain to see its manager
    // state; otherwise the client chain would only be preprocessed.
    observer
        .client
        .extend_chain_mode(chain_id, ListeningMode::FollowChain);

    let owners = [(owner0, 100), (owner1, 100)];
    let ownership = ChainOwnership::multiple(owners, 0, TimeoutConfig::default());
    client.change_ownership(ownership.clone()).await.unwrap();

    let info = observer.synchronize_chain_state(chain_id).await?;
    assert_eq!(info.manager.ownership, ownership);

    let manager = client.chain_info().await.unwrap().manager;

    // The round has not timed out yet, so validators will not sign a timeout certificate.
    // If the malicious and one honest validator happen to be much faster than the other
    // two honest validators, only those two samples may be returned. Otherwise we get
    // a trusted MissingVoteInValidatorResponse, because at least two returned that.
    let result = client.request_leader_timeout().await;
    if !matches!(
        result,
        Err(chain_client::Error::CommunicationError(
            CommunicationError::Trusted(NodeError::ChainError { .. })
        ))
    ) && !matches!(&result,
        Err(chain_client::Error::CommunicationError(CommunicationError::Sample(samples)))
        if samples.iter().any(|(err, _)| matches!(err, NodeError::ChainError { .. }))
    ) {
        panic!("unexpected leader timeout result: {result:?}");
    }

    clock.set(manager.round_timeout.unwrap());

    // After the timeout they will.
    let certificate = client.request_leader_timeout().await.unwrap();
    assert_eq!(
        *certificate.inner(),
        Timeout::new(chain_id, BlockHeight::from(1), Epoch::ZERO)
    );
    assert_eq!(certificate.round, Round::SingleLeader(0));

    let expected_round = Round::SingleLeader(1);
    builder
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(1), expected_round, 3)
        .await;

    // Another client can process the timeout certificate, to arrive at the same round.
    let info = observer.synchronize_chain_state(chain_id).await?;
    assert_eq!(info.manager.current_round, expected_round);

    let round = loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(owner1) {
            break manager.current_round;
        }
        clock.set(manager.round_timeout.unwrap());
        assert!(client.request_leader_timeout().await.is_ok());
    };
    let round_number = match round {
        Round::SingleLeader(round_number) => round_number,
        round => panic!("Unexpected round {round:?}"),
    };

    // The other owner is leader now. Trying to submit a block should return `WaitForTimeout`.
    let result = client
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(observer_id),
        )
        .await
        .unwrap();
    let timeout = match result {
        ClientOutcome::Committed(_) => panic!("Committed a block where we aren't the leader."),
        ClientOutcome::Conflict(_) => panic!("Got conflict where we aren't the leader."),
        ClientOutcome::WaitForTimeout(timeout) => timeout,
    };
    client.clear_pending_proposal().await;
    assert!(client.request_leader_timeout().await.is_err());
    clock.set(timeout.timestamp);
    client.request_leader_timeout().await.unwrap();
    let expected_round = Round::SingleLeader(round_number + 1);
    builder
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(1), expected_round, 3)
        .await;

    loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(owner0) {
            break;
        }
        clock.set(manager.round_timeout.unwrap());
        assert!(client.request_leader_timeout().await.is_ok());
    }

    // Now we are the leader, and the transfer should succeed.
    let _certificate = client
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(observer_id),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );

    let expected_round = Round::SingleLeader(0);
    builder
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(2), expected_round, 3)
        .await;

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_request_leader_timeout_client_behind_validators<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let observer = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_id = client.chain_id();
    let observer_id = observer.chain_id();
    let owner0 = client.identity().await.unwrap();
    let owner1 = AccountSecretKey::generate().public().into();

    // Set up multi-owner chain.
    let owners = [(owner0, 100), (owner1, 100)];
    let ownership = ChainOwnership::multiple(owners, 0, TimeoutConfig::default());
    client.change_ownership(ownership.clone()).await.unwrap();

    // Advance to a round where owner1 is the leader (so owner0 is not).
    let round_where_owner0_not_leader = loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(owner1) {
            break manager.current_round;
        }
        clock.set(manager.round_timeout.unwrap());
        client.request_leader_timeout().await.unwrap();
    };
    let round_number = match round_where_owner0_not_leader {
        Round::SingleLeader(n) => n,
        round => panic!("Unexpected round {round:?}"),
    };

    // Now create a second client on the same chain to advance validators ahead
    // to the next round, where owner0 will be the leader.
    let client2 = builder
        .make_client(chain_id, None, BlockHeight::ZERO)
        .await?;

    // Sync client2 to get the current state.
    client2.synchronize_from_validators().await?;

    // Advance validators one more round using client2.
    let timeout = client2
        .chain_info()
        .await
        .unwrap()
        .manager
        .round_timeout
        .expect("round_timeout should be set after sync");
    clock.set(timeout);
    client2.request_leader_timeout().await.unwrap();

    // Validators are now in round_number + 1.
    let validator_round = Round::SingleLeader(round_number + 1);
    builder
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(1), validator_round, 3)
        .await;

    // At this point:
    // - The client's local state shows it's in round_number (where owner1 is the leader).
    // - The validators are in round_number + 1 (where owner0 is the leader).
    // When the client tries to transfer, it will see it's not the leader in its current round
    // and will try to request a timeout. That timeout request for round_number will be rejected
    // by validators (who are in round_number + 1). The client should handle this error,
    // sync to the actual round, and complete the transfer.

    let result = client
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(observer_id),
        )
        .await;

    // The transfer should succeed after the client discovers it's actually the leader in the validator's current round.
    match result {
        Ok(ClientOutcome::Committed(_)) => {
            // Success! The client handled the round mismatch and completed the transfer.
        }
        Ok(ClientOutcome::WaitForTimeout(_)) => {
            panic!(
                "Transfer returned WaitForTimeout, but the client should have discovered \
                it's the leader in the validator's current round and completed the transfer."
            );
        }
        Ok(ClientOutcome::Conflict(_)) => {
            panic!(
                "Transfer returned Conflict, but the client should have discovered \
                it's the leader in the validator's current round and completed the transfer."
            );
        }
        Err(e) => {
            panic!(
                "Transfer failed with error: {e:?}. The client should have handled the \
                round mismatch automatically by syncing with validators."
            );
        }
    }

    Ok(())
}

/// Exercises the lazy locking-block fetch on proposal rejection.
///
/// Sets up a state where validators 2 and 3 hold a `Regular` locking block at
/// `MultiLeader(0)` but validators 0 and 1 do not. `client_a` — which has no
/// local knowledge of the lock — proposes a new (different) block. All four
/// validators reject the proposal with `ChainError` (each has
/// `validated_vote @ ML(0)` from `client_b`'s earlier attempt, so a fresh
/// proposal at the same round fails the strict-round check). The per-validator
/// updater's `NodeError::ChainError` arm pulls manager state from each
/// rejecter, absorbing the locking block from validators 2 and 3 into
/// `client_a`'s local node. `process_pending_block_without_prepare` detects the
/// snapshot advance and retries; the retry finalizes the absorbed locking block
/// (`client_b`'s transfer) and the chain advances. The outcome is `Conflict`
/// because the committed block is `client_b`'s transfer, not `client_a`'s
/// intended burn.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_lazy_pull_absorbs_locking_block_on_proposal_rejection<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client_a = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_id = client_a.chain_id();
    let recipient_id = recipient.chain_id();
    let owner_a = client_a.identity().await?;
    let owner_b: AccountOwner = builder.signer.generate_new().into();

    let owners = [(owner_a, 100), (owner_b, 100)];
    let ownership = ChainOwnership::multiple(owners, 4, TimeoutConfig::default());
    client_a.change_ownership(ownership).await?;

    let info = client_a.chain_info().await?;
    let mut client_b = builder
        .make_client(chain_id, info.block_hash, info.next_block_height)
        .await?;
    client_b.set_preferred_owner(owner_b);

    // Setup: `client_b` proposes a transfer at `MultiLeader(0)`. All four validators
    // vote to validate, but validators 0 and 1 refuse to absorb the resulting
    // validated certificate and validator 3 refuses to send its confirm vote. So
    // validators 2 and 3 end up holding a `LockingBlock::Regular @ MultiLeader(0)`
    // and a `confirmed_vote` for it; validators 0 and 1 hold only a
    // `validated_vote @ MultiLeader(0)`. The block never reaches a confirmation
    // quorum, so `client_b`'s transfer fails.
    builder.set_fault_type([0, 1], FaultType::DontProcessValidated);
    builder.set_fault_type([3], FaultType::DontSendConfirmVote);
    client_b.synchronize_from_validators().await?;
    let b_result = client_b
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(recipient_id),
        )
        .await;
    assert!(b_result.is_err());

    // Restore honest behavior. `client_a`'s local node has not been touched since
    // `change_ownership` — it has no proposed block and no locking block.
    builder.set_fault_type([0, 1, 2, 3], FaultType::Honest);
    assert!(
        client_a
            .chain_info_with_manager_values()
            .await?
            .manager
            .requested_locking
            .is_none(),
        "client_a must not yet hold a locking block"
    );

    // `client_a` burns. The proposal goes out at `MultiLeader(0)`; every validator
    // rejects it (each has `validated_vote @ ML(0)` from `client_b`'s transfer
    // attempt, so a fresh proposal at the same round fails the strict-round check).
    // The per-validator `NodeError::ChainError` arm pulls manager state, and
    // `client_a`'s local node absorbs the locking block from validators 2/3. The
    // retry then finalizes the locked block via `finalize_locking_block` and the
    // chain advances to height 2 with `client_b`'s transfer committed — classified
    // as `Conflict` because the committed block is not `client_a`'s intended burn.
    let burn_result = client_a.burn(AccountOwner::CHAIN, Amount::ONE).await?;
    assert_matches!(burn_result, ClientOutcome::Conflict(_));
    assert_eq!(
        client_a.chain_info().await?.next_block_height,
        BlockHeight::from(2)
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_finalize_validated<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    // Configure a chain with two regular and no super owners.
    let mut signer = InMemorySigner::new(None);
    let owner1 = signer.generate_new().into();
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client0 = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let chain_id = client0.chain_id();
    let owner0 = client0.preferred_owner().unwrap();

    let owners = [(owner0, 100), (owner1, 100)];
    let timeout_config = TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_secs(5)),
        ..TimeoutConfig::default()
    };
    let ownership = ChainOwnership::multiple(owners, 10, timeout_config);
    client0.change_ownership(ownership).await.unwrap();

    let info = client0.chain_info().await?;
    let mut client1 = builder
        .make_client(chain_id, info.block_hash, info.next_block_height)
        .await?;
    client1.set_preferred_owner(owner1);
    assert!(owner0 != owner1);

    // Client 0 tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, client 0 now has a pending block.
    builder.set_fault_type([2], FaultType::OfflineWithInfo);
    let result = client0
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await;
    assert!(result.is_err());
    // Make sure at least one validator has added the proposal to its state.
    let info = client0.chain_info_with_manager_values().await?;
    let proposal = info.manager.requested_proposed.unwrap();
    builder.node(1).handle_block_proposal(*proposal).await?;

    // Client 1 thinks it is madness to burn 3 tokens! They want to publish a blob instead.
    // The validators are still faulty: They validate blocks but don't confirm them.
    builder.set_fault_type([2], FaultType::DontSendConfirmVote);
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert!(manager.requested_proposed.is_some());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    let result = client1.publish_data_blob(b"blob1".to_vec()).await;
    assert!(result.is_err());
    assert!(!client1
        .pending_proposal()
        .await
        .as_ref()
        .unwrap()
        .blobs
        .is_empty());

    // Finally, enough validators are online and honest again.
    builder.set_fault_type([2], FaultType::Honest);
    client0.synchronize_from_validators().await.unwrap();
    let manager = client0
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert_eq!(
        manager.requested_locking.unwrap().round(),
        Round::MultiLeader(1)
    );
    assert!(client0.pending_proposal().await.is_some());

    // Client 0 now only tries to transfer 1 token. But instead, they automatically finalize the
    // pending block, which publishes the blob.
    assert_matches!(
        client0.burn(AccountOwner::CHAIN, Amount::ONE).await,
        Ok(ClientOutcome::Conflict(_))
    );
    client0.synchronize_from_validators().await.unwrap();
    client0.process_inbox().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(10)
    );
    assert!(client0.pending_proposal().await.is_none());

    // Transfer a token so Client 1 sees that the blob is already published
    client1.prepare_chain().await.unwrap();
    client1.burn(AccountOwner::CHAIN, Amount::ONE).await?;
    client1.synchronize_from_validators().await.unwrap();
    client1.process_inbox().await.unwrap();
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_tokens(9)
    );
    assert!(client1.pending_proposal().await.is_none());
    Ok(())
}

/// On a fresh root chain whose `ChainDescription` is defined by the genesis
/// config (not published by any block), `request_leader_timeout` must succeed
/// once the round has timed out: collecting the timeout certificate and then
/// broadcasting the resulting state to validators (via
/// `send_chain_information` -> `initialize_new_chain_on_validator` ->
/// `send_chain_info_for_blobs`) should treat the chain-description blob as
/// known a priori rather than looking for a publishing block.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_request_leader_timeout_with_genesis_blob<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client = builder.add_root_chain(1, Amount::from_tokens(10)).await?;

    // Advance the clock past the (default 10s) round timeout.
    clock.set(Timestamp::from(20_000_000));

    client.request_leader_timeout().await?;
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_propose_pending_block<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let client = builder.add_root_chain(1, Amount::from_tokens(10)).await?;

    // The client tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, the client now has a pending block.
    builder.set_fault_type([2], FaultType::OfflineWithInfo);
    let result = client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await;
    assert!(result.is_err());

    // Now three validators are online again.
    builder.set_fault_type([2], FaultType::Honest);

    // The client tries to burn another token. But instead, they finalize the
    // pending block, which transfers 3 tokens, leaving 10 - 3 = 7.
    assert_matches!(
        client.burn(AccountOwner::CHAIN, Amount::ONE).await,
        Ok(ClientOutcome::Conflict(_))
    );
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(7)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_re_propose_validated<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    // Configure a chain with two regular and no super owners.
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client0 = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let chain_id = client0.chain_id();
    let owner0 = client0.identity().await.unwrap();
    let owner1 = builder.signer.generate_new().into();

    let owners = [(owner0, 100), (owner1, 100)];
    let timeout_config = TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_secs(5)),
        ..TimeoutConfig::default()
    };
    let ownership = ChainOwnership::multiple(owners, 10, timeout_config);
    client0.change_ownership(ownership).await.unwrap();
    let mut client1 = builder
        .make_client(
            chain_id,
            client0.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client1.set_preferred_owner(owner1);

    // Client 0 tries to burn 3 tokens. Three validators are faulty: 1 and 2 will validate the
    // block but not receive it for confirmation. Validator 3 is offline.
    builder.set_fault_type([1, 2], FaultType::DontProcessValidated);
    builder.set_fault_type([3], FaultType::Offline);

    let result = client0
        .burn(AccountOwner::CHAIN, Amount::from_tokens(3))
        .await;
    assert!(result.is_err());
    let manager = client0
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    // Validator 0 may or may not have processed the validated block before the update was
    // canceled due to the errors from the faulty validators. Submit it again to make sure
    // it's there, so that client 1 can download and re-propose it later.
    let locking = *manager.requested_locking.unwrap();
    let LockingBlock::Regular(validated_block_certificate) = locking else {
        panic!("Unexpected locking fast block.");
    };
    builder
        .node(0)
        .handle_validated_certificate(validated_block_certificate)
        .await
        .unwrap();

    // Client 1 wants to burn 2 tokens. They learn about the proposal in round 0, but now the
    // validator 0 is offline, so they don't learn about the validated block and make their own
    // proposal in round 1.
    builder.set_fault_type([0], FaultType::Offline);
    builder.set_fault_type([3], FaultType::OfflineWithInfo);
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert!(manager.requested_proposed.is_some());
    assert!(manager.requested_locking.is_none());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    let result = client1
        .burn(AccountOwner::CHAIN, Amount::from_tokens(2))
        .await;
    assert!(result.is_err());

    // Finally, three validators are online and honest again. Client 1 realizes there has been a
    // validated block in round 0, and re-proposes it when it tries to burn 4 tokens.
    builder.set_fault_type([0, 1, 2], FaultType::Honest);
    builder.set_fault_type([3], FaultType::Offline);
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert_eq!(
        manager.requested_locking.unwrap().round(),
        Round::MultiLeader(0)
    );
    assert_eq!(manager.current_round, Round::MultiLeader(1));
    assert!(client1.pending_proposal().await.is_some());
    assert_matches!(
        client1
            .burn(AccountOwner::CHAIN, Amount::from_tokens(4))
            .await,
        Ok(ClientOutcome::Conflict(_))
    );

    // Burning 3 tokens got finalized; the pending 2 and the new 4 got skipped.
    client0.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(7)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_re_propose_fast_block<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    // Configure a chain with one regular and one super owner.
    let signer = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut client0 = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    // Enable fast blocks for this test that specifically tests fast block behavior.
    client0.options_mut().allow_fast_blocks = true;
    let chain_id = client0.chain_id();
    let owner0 = client0.identity().await.unwrap();
    let owner1 = builder.signer.generate_new().into();

    let timeout_config = TimeoutConfig {
        fast_round_duration: Some(TimeDelta::from_secs(5)),
        ..TimeoutConfig::default()
    };
    let ownership = ChainOwnership {
        super_owners: BTreeSet::from_iter([owner0]),
        owners: BTreeMap::from_iter([(owner1, 100)]),
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config,
    };
    client0.change_ownership(ownership).await.unwrap();
    let mut client1 = builder
        .make_client(
            chain_id,
            client0.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client1.options_mut().allow_fast_blocks = true;
    client1.set_preferred_owner(owner1);

    // Client 0 transfers 5 tokens from the chain account to themselves.
    client0
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(5),
            Account::new(chain_id, owner0),
        )
        .await?;

    // Client 0 tries to burn 3 of their own tokens, but three validators are faulty.
    builder.set_fault_type([1, 2, 3], FaultType::OfflineWithInfo);

    let result = client0.burn(owner0, Amount::from_tokens(3)).await;
    assert!(result.is_err());
    let manager = client0
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    // Validator 0 may or may not have processed the proposal before the update was
    // canceled due to the errors from the faulty validators. Submit it again to make sure
    // it's there, so that client 1 can download and re-propose it later.
    let locking = *manager.requested_locking.unwrap();
    let LockingBlock::Fast(proposal) = locking else {
        panic!("Unexpected locking regular block.");
    };
    builder
        .node(0)
        .handle_block_proposal(proposal)
        .await
        .unwrap();

    // Round 0 times out.
    clock.add(TimeDelta::from_secs(5));
    builder.set_fault_type([0], FaultType::Offline);
    builder.set_fault_type([1, 2, 3], FaultType::Honest);
    client1.synchronize_from_validators().await.unwrap();
    client1.request_leader_timeout().await.unwrap();

    // Client 1 wants to burn 2 tokens. But now validators 0 and 3 is offline, so they don't learn
    // about the proposed fast block and make their own instead.
    builder.set_fault_type([3], FaultType::Offline);
    let result = client1
        .burn(AccountOwner::CHAIN, Amount::from_tokens(2))
        .await;
    assert!(result.is_err());

    // Finally, three validators are online and honest again. Client 1 realizes there has been a
    // validated block in round 0, and re-proposes it when it tries to burn 4 tokens.
    builder.set_fault_type([0, 1, 2], FaultType::Honest);
    client1.synchronize_from_validators().await.unwrap();
    assert!(client1.pending_proposal().await.is_some());
    // This test involves timeouts and potential conflicts. Handle them appropriately.
    loop {
        match client1
            .burn(AccountOwner::CHAIN, Amount::from_tokens(4))
            .await
        {
            Ok(ClientOutcome::Committed(_)) => break,
            Ok(ClientOutcome::WaitForTimeout(_)) => {
                // Round 0 needs to time out again, so client 1 is actually allowed to propose.
                clock.add(TimeDelta::from_secs(5));
            }
            Ok(ClientOutcome::Conflict(_)) => {
                // A different block was committed. Sync and check if we're done.
                client1.synchronize_from_validators().await.unwrap();
                // The conflicting block might have included our burn. Check balance.
                if client1.local_balance().await.unwrap() == Amount::from_tokens(1) {
                    break; // The expected final state - we're done.
                }
            }
            Err(_) => {
                // Might get an error if balance is insufficient - operations already committed.
                break;
            }
        }
    }
    // Process any pending block. If pending proposal was already committed via conflict,
    // this will return None for the certificate, and a remote conflict may surface as an
    // error here.
    if let Err(error) = client1.process_pending_block().await {
        tracing::debug!("process_pending_block returned error after race: {error}");
    }

    // Burning 3 and 4 tokens got finalized; the pending 2 tokens got skipped.
    client0.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(1)
    );
    assert_eq!(
        client0.local_owner_balance(owner0).await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_message_policy<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let sender2 = builder.add_root_chain(2, Amount::from_tokens(4)).await?;
    let mut receiver = builder.add_root_chain(3, Amount::ZERO).await?;
    let recipient = Account::chain(receiver.chain_id());

    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    receiver.options_mut().message_policy = MessagePolicy {
        blanket: BlanketMessagePolicy::Ignore,
        ..Default::default()
    };
    receiver.synchronize_from_validators().await?;
    assert!(receiver.process_inbox().await?.0.is_empty());
    // The message was ignored.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert!(sender.process_inbox().await?.0.is_empty());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    receiver.options_mut().message_policy = MessagePolicy {
        blanket: BlanketMessagePolicy::Reject,
        ..Default::default()
    };
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    sender.synchronize_from_validators().await?;
    // The message bounces.
    assert_eq!(sender.process_inbox().await?.0.len(), 1);
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );

    // Let's try again.
    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    sender2
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        sender2.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    // The receiver will only accept messages from sender, and not from sender2.
    receiver.options_mut().message_policy = MessagePolicy {
        restrict_chain_ids_to: Some([sender.chain_id()].into_iter().collect()),
        ..Default::default()
    };
    receiver.synchronize_from_validators().await?;
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    // Only the transfer from sender should have been accepted. The other should have been
    // rejected.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ONE);

    // Even if we change the policy, there's no longer a message to receive.
    receiver.options_mut().message_policy = MessagePolicy::default();
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 0);

    // A never-reject application bypasses a blanket Reject policy: the bundle must be accepted.
    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    receiver.synchronize_from_validators().await?;
    receiver.options_mut().message_policy = MessagePolicy {
        blanket: BlanketMessagePolicy::Reject,
        never_reject_application_ids: [GenericApplicationId::System].into_iter().collect(),
        ..Default::default()
    };
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    // The transfer was accepted (not bounced): receiver balance is now 2.
    assert_eq!(
        receiver.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );

    // `restrict_chain_ids_to` still dominates over never-reject: a message from a non-whitelisted
    // chain is still filtered out even if it belongs to a never-reject application.
    sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    receiver.synchronize_from_validators().await?;
    receiver.options_mut().message_policy = MessagePolicy {
        restrict_chain_ids_to: Some([sender2.chain_id()].into_iter().collect()),
        never_reject_application_ids: [GenericApplicationId::System].into_iter().collect(),
        ..Default::default()
    };
    // The message from `sender` gets rejected (tracked, non-protected) and bounces back.
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    assert_eq!(
        receiver.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    sender.synchronize_from_validators().await?;
    assert_eq!(sender.process_inbox().await?.0.len(), 1);

    Ok(())
}

/// Verifies that `process_notification` short-circuits on `NewIncomingBundle`
/// notifications whose origin is filtered by `MessagePolicy`: the sender's
/// block must not be downloaded into local storage.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_process_notification_filters_origin<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let mut receiver = builder.add_root_chain(2, Amount::ZERO).await?;
    let recipient = Account::chain(receiver.chain_id());
    let sender_id = sender.chain_id();
    // `process_notification_from` is point-to-point, so pick an honest validator.
    let validator = builder
        .initial_committee
        .validator_addresses()
        .find(|(public_key, _)| builder.fault_type(public_key) == Some(FaultType::Honest))
        .unwrap();
    let storage = receiver.storage_client().clone();

    // Baseline: with the default (permissive) policy, the sender's block
    // is downloaded when its `NewIncomingBundle` notification is processed.
    let cert_baseline = sender
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    receiver
        .process_notification_from(
            Notification {
                chain_id: receiver.chain_id(),
                reason: Reason::NewIncomingBundle {
                    origin: sender_id,
                    height: cert_baseline.block().header.height,
                },
            },
            validator,
        )
        .await;
    assert!(
        storage.contains_certificate(cert_baseline.hash()).await?,
        "baseline: sender block should be downloaded with the default policy"
    );

    // Each of the three filter modes must short-circuit the download.
    for policy in [
        // Blanket Ignore.
        MessagePolicy {
            blanket: BlanketMessagePolicy::Ignore,
            ..Default::default()
        },
        // Origin in the denylist.
        MessagePolicy {
            ignore_chain_ids: [sender_id].into_iter().collect(),
            ..Default::default()
        },
        // Origin not in the allowlist.
        MessagePolicy {
            restrict_chain_ids_to: Some([receiver.chain_id()].into_iter().collect()),
            ..Default::default()
        },
    ] {
        receiver.options_mut().message_policy = policy;
        let cert = sender
            .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
            .await
            .unwrap_ok_committed();
        assert!(
            !storage.contains_certificate(cert.hash()).await?,
            "block must not be in local storage before notification"
        );
        receiver
            .process_notification_from(
                Notification {
                    chain_id: receiver.chain_id(),
                    reason: Reason::NewIncomingBundle {
                        origin: sender_id,
                        height: cert.block().header.height,
                    },
                },
                validator,
            )
            .await;
        assert!(
            !storage.contains_certificate(cert.hash()).await?,
            "filtered origin: sender block must not be downloaded"
        );
    }

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_priority_bundle_origins<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender_a = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let sender_b = builder.add_root_chain(2, Amount::from_tokens(4)).await?;
    let mut receiver = builder.add_root_chain(3, Amount::ZERO).await?;
    let recipient = Account::chain(receiver.chain_id());

    // Mark sender_b's bundles as priority for the receiver.
    receiver.options_mut().priority_bundle_origins = [sender_b.chain_id()].into_iter().collect();

    // Send from sender_a first (would normally come first by timestamp), then sender_b.
    sender_a
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    sender_b
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await?;
    let cert = receiver.process_inbox().await?.0.pop().unwrap();
    let bundles: Vec<_> = cert.block().body.incoming_bundles().collect();
    assert_eq!(bundles.len(), 2);
    assert_eq!(
        bundles[0].origin,
        sender_b.chain_id(),
        "Priority bundle from sender_b should be first"
    );
    assert_eq!(
        bundles[1].origin,
        sender_a.chain_id(),
        "Non-priority bundle from sender_a should be second"
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_propose_block_with_messages_and_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let blob_bytes = b"blob".to_vec();
    let large_blob_bytes = b"blob+".to_vec();
    let policy = ResourceControlPolicy {
        maximum_blob_size: blob_bytes.len() as u64,
        maximum_block_proposal_size: (blob_bytes.len() * 100) as u64,
        ..ResourceControlPolicy::default()
    };
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer)
        .await?
        .with_policy(policy.clone());
    let mut client1 = builder.add_root_chain(1, Amount::ONE).await?;
    let mut client2 = builder.add_root_chain(2, Amount::ONE).await?;
    let mut client3 = builder.add_root_chain(3, Amount::ONE).await?;
    let chain_id3 = client3.chain_id();

    // Configure the clients as super owners with fast blocks enabled.
    for client in [&mut client1, &mut client2, &mut client3] {
        client.options_mut().allow_fast_blocks = true;
        let owner = client.identity().await?;
        let ownership = ChainOwnership::single_super(owner);
        client.change_ownership(ownership).await.unwrap();
    }

    // Take one validator down
    builder.set_fault_type([3], FaultType::Offline);

    // Publish a blob on chain 1.
    let blob_id = Blob::new(BlobContent::new_data(blob_bytes.clone())).id();
    let certificate = client1
        .publish_data_blob(blob_bytes)
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.round, Round::Fast);

    // Send a message from chain 2 to chain 3.
    let certificate = client2
        .transfer(
            AccountOwner::CHAIN,
            Amount::from_millis(1),
            Account::chain(chain_id3),
        )
        .await
        .unwrap_ok_committed();
    client3.synchronize_from_validators().await.unwrap();
    assert_eq!(certificate.round, Round::Fast);

    builder.set_fault_type([2], FaultType::Offline);
    builder.set_fault_type([3], FaultType::Honest);

    // Client 3 should be able to update validator 3 about the blob and the message.
    let certificate = client3
        .execute_operation(SystemOperation::VerifyBlob { blob_id })
        .await
        .unwrap_ok_committed();

    // This read a new blob, so it cannot be a fast block.
    assert_eq!(certificate.round, Round::MultiLeader(0));
    let block = certificate.block();
    assert_eq!(block.body.incoming_bundles().count(), 1);
    assert_eq!(block.required_blob_ids().len(), 1);

    // This will go way over the limit, because of the different overheads.
    let blob_bytes = (0..100)
        .map(|_| {
            rand::thread_rng()
                .sample_iter(&rand::distributions::Standard)
                .take(policy.maximum_blob_size as usize)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let result = client1.publish_data_blobs(blob_bytes).await;
    assert_matches!(
        result,
        Err(chain_client::Error::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))
        )) if matches!(*chain_error, ChainError::BlockProposalTooLarge(_))
    );

    assert_matches!(
        client1.publish_data_blob(large_blob_bytes).await,
        Err(chain_client::Error::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))
        )) if matches!(&*chain_error, ChainError::ExecutionError(
            error, ChainExecutionContext::Block
        ) if matches!(**error, ExecutionError::BlobTooLarge))
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_blob_fees<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let policy = ResourceControlPolicy {
        blob_read: Amount::from_nanos(10_000),
        blob_published: Amount::from_attos(1_000_000),
        blob_byte_read: Amount::from_nanos(1),
        blob_byte_published: Amount::from_attos(100),
        ..ResourceControlPolicy::default()
    };
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(policy.clone());
    let mut expected_balance = Amount::ONE;
    let client = builder.add_root_chain(0, expected_balance).await?;
    let bytes: &[u8] = b"twelve bytes";
    let blob = Blob::new(BlobContent::new_data(bytes));
    let blob_id = blob.id();
    client
        .publish_data_blob(bytes.to_vec())
        .await
        .unwrap_ok_committed();
    expected_balance = expected_balance
        - policy.blob_published
        - policy.blob_byte_published * (blob.bytes().len() as u128);
    assert_eq!(client.local_balance().await.unwrap(), expected_balance);

    client.read_data_blob(blob_id.hash).await.unwrap().unwrap();
    expected_balance = expected_balance - policy.blob_read;
    assert_eq!(client.local_balance().await.unwrap(), expected_balance);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_validator_outdated_admin_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let new_public_key = signer.generate_new();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    let admin_client = builder.add_root_chain(0, Amount::from_tokens(1000)).await?;
    let client1 = builder.add_root_chain(1, Amount::from_tokens(1000)).await?;

    // Take one validator down - they will miss committee changes.
    builder.set_fault_type([3], FaultType::Offline);

    // Start by creating a block in epoch 0.
    let certificate0 = client1
        .transfer(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(admin_client.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert_eq!(certificate0.block().header.epoch, Epoch::from(0));

    // Advance the epoch.
    admin_client
        .stage_new_committee(builder.initial_committee.clone())
        .await
        .unwrap();

    // Process the inbox to migrate the client's chain.
    client1.synchronize_from_validators().await.unwrap();
    client1.process_inbox().await.unwrap();

    // Open a chain
    let (new_chain_desc, certificate1) = client1
        .open_chain(
            ChainOwnership::single(new_public_key.into()),
            ApplicationPermissions::default(),
            Amount::from_tokens(10),
        )
        .await
        .unwrap_ok_committed();

    // Check that the epoch has been migrated.
    assert_eq!(certificate1.block().header.epoch, Epoch::from(1));

    // Make a client to try the new chain.
    let mut client2 = builder
        .make_client(new_chain_desc.id(), None, BlockHeight::ZERO)
        .await?;
    client2.set_preferred_owner(new_public_key.into());
    client2.synchronize_from_validators().await.unwrap();

    // Let's deactivate another validator and reactivate the one that was offline.
    // Now the client will have to update validator 3 on the admin chain in order for the
    // next blocks to be correctly processed.
    builder.set_fault_type([2], FaultType::Offline);
    builder.set_fault_type([3], FaultType::Honest);

    let admin_tip = builder
        .node(3)
        .chain_info_with_manager_values(admin_client.chain_id())
        .await
        .unwrap()
        .next_block_height;
    // At this point, validator 3 should have zero blocks on the admin chain.
    assert_eq!(admin_tip, 0.into());

    // Update the validators on the chain.
    // If it works, it means the validator has been correctly updated.
    client2.update_validators(None, None).await.unwrap();

    client2
        .transfer(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(admin_client.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let admin_tip = builder
        .node(3)
        .chain_info_with_manager_values(admin_client.chain_id())
        .await
        .unwrap()
        .next_block_height;
    // Validator 3 should be up to date on the admin chain.
    assert_eq!(admin_tip, 2.into());

    Ok(())
}

/// Regression test: when a fresh client syncs a chain that has a block at a
/// non-genesis epoch, `process_certificates` must download the admin chain's
/// `NewCommittee` event for that epoch — cert verification calls
/// `get_committee_hashes` which reads the event. Without the fix this surfaces
/// as `Events not found` for `StreamName(00)` rather than recovering.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_synchronize_downloads_admin_chain_events<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    let admin_client = builder.add_root_chain(0, Amount::from_tokens(1000)).await?;
    let parent = builder.add_root_chain(1, Amount::from_tokens(1000)).await?;

    // Create a new epoch on the admin chain and migrate `parent` to it.
    admin_client
        .stage_new_committee(builder.initial_committee.clone())
        .await
        .unwrap();
    parent.synchronize_from_validators().await.unwrap();
    parent.process_inbox().await.unwrap();
    // The migration block itself is at epoch 0 (the chain's epoch before
    // `ProcessNewEpoch` executes). To make `process_certificates` actually
    // verify against the new committee, the chain must have a follow-up block
    // *at* epoch 1 — its cert's `get_committee_hashes(1..=1)` then reads the
    // admin event during cert verification.
    let cert = parent
        .burn(AccountOwner::CHAIN, Amount::from_tokens(1))
        .await
        .unwrap_ok_committed();
    assert_eq!(cert.block().header.epoch, Epoch::from(1));
    let parent_info = parent.chain_info().await?;

    // A fresh client (its genesis storage has `parent`'s description blob but
    // no admin-chain events).
    let fresh_client = builder
        .make_client(parent.chain_id(), None, BlockHeight::ZERO)
        .await?;

    // Before the fix, `process_certificates` propagated `EventsNotFound` for
    // the epoch-1 transfer cert because cert verification couldn't load the
    // admin chain's `NewCommittee` event. The fix retries after downloading
    // the publisher certificates that contain the missing event.
    fresh_client.synchronize_from_validators().await?;

    assert_eq!(
        fresh_client.chain_info().await?.next_block_height,
        parent_info.next_block_height,
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_prepare_chain_with_cross_chain_messages<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());

    // Create sender and receiver chains.
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;
    let sender2 = builder.add_root_chain(3, Amount::from_tokens(2)).await?;

    // Both senders transfer to receiver.
    let amount1 = Amount::from_tokens(2);
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            amount1,
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // Receiver synchronizes and processes the message.
    receiver.synchronize_from_validators().await?;
    receiver.process_inbox().await?;

    let amount2 = Amount::from_tokens(1);
    sender2
        .transfer_to_account(
            AccountOwner::CHAIN,
            amount2,
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // It does not process the second message.
    receiver.synchronize_from_validators().await?;

    // Verify the first message was processed, but not the second.
    assert_eq!(receiver.local_balance().await.unwrap(), amount1);

    // Create a new client for the same chain. This new client will have the processed
    // inbox message (the removed_bundles state), but won't have the sender blocks yet
    // because we're creating it fresh.
    let receiver_info = receiver.chain_info().await?;
    let receiver2 = builder
        .make_client(
            receiver.chain_id(),
            receiver_info.block_hash,
            receiver_info.next_block_height,
        )
        .await?;

    // Test that prepare_chain does not download any sender chain blocks.
    let info = receiver2.prepare_chain().await?;
    assert_eq!(info.next_block_height, BlockHeight::from(1));

    let local_node = &receiver2.client.local_node;
    let sender_info = local_node.chain_info(sender.chain_id()).await?;
    assert_eq!(
        sender_info.next_block_height,
        BlockHeight::ZERO,
        "prepare_chain should download acknowledged sender blocks"
    );

    // Verify that sender2's block was NOT downloaded.
    let sender2_info = local_node.chain_info(sender2.chain_id()).await;
    assert!(
        sender2_info.is_err() || sender2_info.unwrap().next_block_height == BlockHeight::ZERO,
        "prepare_chain should not download unacknowledged sender blocks"
    );

    // The new client should now be able to make a transfer successfully.
    let amount3 = Amount::from_tokens(1);
    receiver2
        .transfer(
            AccountOwner::CHAIN,
            amount3,
            Account::chain(sender.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert_eq!(receiver2.local_balance().await.unwrap(), amount1 - amount3);

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_rejected_message_bundles_are_free<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::testnet());
    let admin = builder.add_root_chain(1, Amount::from_tokens(2)).await?;
    let user = builder.add_root_chain(1, Amount::ZERO).await?;
    let user_reject = builder
        .make_client_with_options(
            user.chain_id(),
            None,
            BlockHeight::ZERO,
            chain_client::Options {
                message_policy: MessagePolicy {
                    blanket: BlanketMessagePolicy::Reject,
                    ..Default::default()
                },
                ..chain_client::Options::test_default()
            },
            false,
        )
        .await?;

    let recipient = Account::chain(user.chain_id());
    admin
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();

    user_reject.synchronize_from_validators().await?;
    let (certificates, _) = user_reject.process_inbox().await.unwrap();
    assert_eq!(certificates.len(), 1);
    assert_matches!(
        &certificates[0].block().body.transactions[0],
        Transaction::ReceiveMessages(bundle) if bundle.action == MessageAction::Reject
    );

    Ok(())
}

/// Tests that a follow-only client only downloads the followed chain's blocks,
/// not blocks from sender chains that sent messages to it.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_follow_chain_mode<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;

    // Create a follow-only client for the receiver chain.
    let follower = builder
        .make_client_with_options(
            receiver.chain_id(),
            None,
            BlockHeight::ZERO,
            chain_client::Options::test_default(),
            true,
        )
        .await?;

    // The sender transfers tokens to the receiver.
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // The receiver processes its inbox and creates a block.
    receiver.synchronize_from_validators().await?;
    receiver.process_inbox().await?;

    // The follower syncs; since it's follow-only, it should only download the receiver's blocks.
    follower.synchronize_from_validators().await?;

    // The follower should have downloaded the receiver's blocks.
    assert_eq!(
        follower.chain_info().await?.next_block_height,
        BlockHeight::from(1),
        "Follower should have downloaded the receiver's block"
    );

    // The follower should NOT have downloaded the sender's blocks.
    let sender_info = follower
        .client
        .local_node
        .chain_info(sender.chain_id())
        .await?;
    assert_eq!(
        sender_info.next_block_height,
        BlockHeight::ZERO,
        "Follower should not have downloaded the sender's blocks"
    );

    Ok(())
}

/// Tests that transfers succeed even when the block timestamp is in the future relative
/// to the validators' clock (using auto-advance on the test clock to simulate time passing).
///
/// This test verifies that the system handles the case where a block's timestamp
/// (which must be >= the previous block's timestamp) is ahead of the current time.
/// The validators will accept such blocks after their clocks catch up.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_transfer_with_validator_timestamp_retry<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let clock = storage_builder.clock().clone();
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;

    // Set the clock to a future time and make the first transfer.
    // This creates a block with timestamp = future_time.
    let future_time = Timestamp::from(2_000_000); // 2 seconds
    clock.set(future_time);

    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap();

    // Reset the clock to 0 (simulating validator clocks being behind).
    // The next block must have timestamp >= future_time (previous block's time),
    // but validators will initially see current_time = 0.
    clock.set(Timestamp::from(0));

    // Auto-advance for sleeps targeting future_time or later. The InvalidTimestamp
    // retry does two sleeps: sleep_until(block_timestamp) and sleep(skew_duration).
    // In this test, both target future_time initially, but subsequent sleeps may
    // target later times as the clock advances.
    clock.set_sleep_callback(move |target| target >= future_time);

    // Try another transfer. The new block's timestamp must be >= future_time
    // (because it must be >= the previous block's timestamp).
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(receiver.chain_id()),
        )
        .await
        .expect("Transfer should succeed after retrying with advanced clock");

    // Verify the clock advanced to allow the block timestamp.
    assert!(
        clock.current_time() >= future_time,
        "Clock should have advanced to at least the block timestamp"
    );

    Ok(())
}

/// Tests that when a chain is opened for a key we own, the new chain is automatically
/// tracked as a full chain and its inbox is updated when messages are sent to it.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_for_owned_key_is_fully_tracked<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());

    // New chains use the admin chain to verify their creation certificate.
    let _admin = builder.add_root_chain(0, Amount::ZERO).await?;
    let parent = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let sender = builder.add_root_chain(2, Amount::from_tokens(10)).await?;

    // Generate a new key in the same signer that the parent uses.
    let new_public_key = builder.signer.generate_new();

    // Open a new chain for the key we own.
    let (new_description, _certificate) = parent
        .open_chain(
            ChainOwnership::single(new_public_key.into()),
            ApplicationPermissions::default(),
            Amount::from_tokens(1),
        )
        .await
        .unwrap_ok_committed();
    let new_chain_id = new_description.id();

    // Verify the new chain is tracked as FullChain.
    assert_eq!(
        parent.client.chain_mode(new_chain_id),
        Some(ListeningMode::FullChain),
        "New chain should be tracked as FullChain since we own the key"
    );

    // Create a client for the new chain.
    let mut new_chain_client = builder
        .make_client(new_chain_id, None, BlockHeight::ZERO)
        .await?;
    new_chain_client.set_preferred_owner(new_public_key.into());

    // Send a transfer from `sender` to the new chain.
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(new_chain_id),
        )
        .await
        .unwrap_ok_committed();

    // Synchronize the new chain and process its inbox.
    new_chain_client.synchronize_from_validators().await?;
    new_chain_client.process_inbox().await?;

    // Verify the new chain received the funds (initial 1 token + 3 transferred).
    let balance = new_chain_client.local_balance().await?;
    assert!(
        balance >= Amount::from_tokens(3),
        "New chain should have received the transferred funds, got {balance}"
    );

    Ok(())
}

/// Tests the `allow_fast_blocks` option: when enabled, a super owner produces `Fast` blocks;
/// when disabled, they produce `MultiLeader(0)` blocks instead.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_disallow_fast_blocks<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    // Create a chain and get its owner.
    let mut client = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let super_owner = client.identity().await?;

    // Change ownership to make the owner a super owner.
    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: vec![super_owner],
        owners: vec![],
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    });
    client.execute_operation(owner_change_op).await.unwrap();

    // With fast blocks enabled, the super owner creates a block in the Fast round.
    client.options_mut().allow_fast_blocks = true;
    let certificate = client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(1))
        .await
        .unwrap_ok_committed();
    assert_eq!(
        certificate.round,
        Round::Fast,
        "Block should be in Fast round when fast blocks are enabled"
    );

    // With fast blocks disabled, the super owner creates a block in MultiLeader(0) instead.
    client.options_mut().allow_fast_blocks = false;
    let certificate = client
        .burn(AccountOwner::CHAIN, Amount::from_tokens(1))
        .await
        .unwrap_ok_committed();
    assert_eq!(
        certificate.round,
        Round::MultiLeader(0),
        "Block should be in MultiLeader(0) when fast blocks are disabled"
    );

    Ok(())
}

/// Tests that message bundles exceeding block limits are removed (not rejected) when
/// they're not the first transaction, allowing retry in a later block.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_block_limit_removes_bundles_not_rejects<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    // Use a very restrictive block size that allows only one transfer at a time.
    let policy = ResourceControlPolicy {
        maximum_block_size: 350,
        ..ResourceControlPolicy::only_fuel()
    };

    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer)
        .await?
        .with_policy(policy);

    let sender1 = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let sender2 = builder.add_root_chain(2, Amount::from_tokens(4)).await?;
    let receiver = builder.add_root_chain(3, Amount::ZERO).await?;
    let recipient = Account::chain(receiver.chain_id());

    // Both senders send a transfer to the receiver.
    sender1
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();
    sender2
        .transfer(AccountOwner::CHAIN, Amount::ONE, recipient)
        .await
        .unwrap_ok_committed();

    // Receiver synchronizes and processes inbox.
    // Due to block size limits, only one transfer can be processed per block.
    // The second message exceeds the block limit, gets removed from the first block,
    // but stays in the inbox for a subsequent block in the same process_inbox loop.
    receiver.synchronize_from_validators().await?;
    let (certs, _) = receiver.process_inbox().await?;
    // Should have created two blocks: one for each message (due to block size limits).
    assert_eq!(certs.len(), 2);
    // Receiver should have received both tokens.
    assert_eq!(receiver.local_balance().await?, Amount::from_tokens(2));

    // Verify that the senders did NOT receive bounce messages (the messages weren't rejected).
    sender1.synchronize_from_validators().await?;
    sender2.synchronize_from_validators().await?;
    let (certs1, _) = sender1.process_inbox().await?;
    let (certs2, _) = sender2.process_inbox().await?;
    // No bounce messages should have been received.
    assert!(
        certs1.is_empty(),
        "sender1 should not have received any bounce messages"
    );
    assert!(
        certs2.is_empty(),
        "sender2 should not have received any bounce messages"
    );

    Ok(())
}

/// Tests that with `open_multi_leader_rounds`, a client that is not an owner can be assigned
/// to the chain via `prepare_for_owner`, but cannot transfer chain funds.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_multi_leader_rounds<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;

    // Create a chain and get its owner.
    let client = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let owner = client.identity().await?;
    let chain_id = client.chain_id();

    // Configure open multi-leader rounds.
    let owner_change_op = Operation::system(SystemOperation::ChangeOwnership {
        super_owners: vec![],
        owners: vec![(owner, 100)],
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: true,
        timeout_config: TimeoutConfig::default(),
    });
    client.execute_operation(owner_change_op).await.unwrap();
    let info = client.chain_info().await?;

    // Create a new client for the same chain with a non-owner key.
    // This simulates a client that was assigned the chain (e.g., via a faucet with
    // open_multi_leader_rounds) without having the owner key.
    let non_owner: AccountOwner = builder.signer.generate_new().into();
    let mut non_owner_client = builder
        .make_client(chain_id, info.block_hash, info.next_block_height)
        .await?;
    non_owner_client.set_preferred_owner(non_owner);

    // Synchronize to get the latest chain state (with open_multi_leader_rounds enabled).
    non_owner_client.synchronize_from_validators().await?;

    // The non-owner client can successfully prepare_for_owner because open_multi_leader_rounds
    // is enabled. This is the code path used by assign_new_chain_to_key.
    let info = non_owner_client.prepare_for_owner(non_owner).await?;
    assert!(info.manager.ownership.open_multi_leader_rounds);

    // But the non-owner client cannot burn tokens (requires ownership).
    let result = non_owner_client
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await;
    assert_matches!(
        result,
        Err(chain_client::Error::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(ref chain_error))
        )) if matches!(&**chain_error, ChainError::ExecutionError(
            error, ChainExecutionContext::Operation(_)
        ) if matches!(**error, ExecutionError::UnauthenticatedTransferOwner))
    );

    // The original owner can still use the chain.
    let certificate = client
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.round, Round::MultiLeader(0));

    Ok(())
}

/// Tests that cross-chain message chunking works end-to-end: the sender splits large
/// `UpdateRecipient` messages and the receiver processes all chunks correctly.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_cross_chain_message_chunking_end_to_end<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    // Use a tiny chunk limit so every transfer goes into its own UpdateRecipient.
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer)
        .await?
        .with_cross_chain_message_chunk_limit(1);

    let sender = builder.add_root_chain(1, Amount::from_tokens(100)).await?;
    let receiver = builder.add_root_chain(2, Amount::ZERO).await?;

    // Send three transfers from sender to receiver.
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(5),
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(3),
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(2),
            Account::chain(receiver.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // Receiver synchronizes and processes the inbox. With chunk_limit=1, the
    // cross-chain messages were split into multiple UpdateRecipient requests.
    // This verifies the previous_height values are correct so the receiver
    // accepts all chunks without gap detection errors.
    receiver.synchronize_from_validators().await?;
    receiver.process_inbox().await?;

    // Verify the receiver got all three transfers.
    assert_eq!(
        receiver.local_balance().await?,
        Amount::from_tokens(10),
        "Receiver should have received all three transfers"
    );

    Ok(())
}

/// Verifies the end-to-end `CheckpointAck` cycle now that recipients can also checkpoint
/// after consuming incoming messages: producer sends, recipient consumes and
/// checkpoints (sending the ack back), producer consumes the ack and trims its
/// `unfinalized_message_blocks`, producer's next checkpoint emits no `CheckpointAck`
/// back to the recipient (the otherwise-perpetual ping-pong is broken).
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[test_log::test(tokio::test)]
async fn test_checkpoint_ack_cycle_terminates<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let producer = builder.add_root_chain(1, Amount::from_tokens(7)).await?;
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;
    let producer_id = producer.chain_id();
    let recipient_id = recipient.chain_id();

    // Producer.0: transfer to recipient.
    producer
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(recipient_id),
        )
        .await
        .unwrap_ok_committed();

    // Producer.1: checkpoint #1. unfinalized_message_blocks[recipient] = {(0, 0)}.
    producer.checkpoint().await.unwrap().unwrap();

    // Recipient.0: consume the transfer. Now the inbox's `next_cursor_to_remove[producer]`
    // is past the transfer's cursor and `pending_checkpoint_ack_targets` contains the
    // producer.
    recipient.synchronize_from_validators().await?;
    recipient.process_inbox().await?;

    // Recipient.1: checkpoint — only possible because we lifted the
    // "no consumed incoming messages" precondition. The checkpoint's apply hook
    // emits a `SystemMessage::CheckpointAck` to the producer.
    let recipient_checkpoint = recipient.checkpoint().await.unwrap().unwrap();
    let ack_to_producer = recipient_checkpoint
        .block()
        .body
        .messages
        .iter()
        .flatten()
        .find(|msg| {
            msg.destination == producer_id
                && matches!(
                    &msg.message,
                    Message::System(SystemMessage::CheckpointAck { .. })
                )
        })
        .expect("recipient's checkpoint should send a CheckpointAck back to the producer");
    let acked_cursor = match &ack_to_producer.message {
        Message::System(SystemMessage::CheckpointAck {
            latest_received_cursor,
        }) => *latest_received_cursor,
        _ => unreachable!(),
    };

    // Producer consumes the ack. Its `unfinalized_message_blocks[recipient]` is trimmed
    // and — since the only consumed cursor was strictly below the ack — fully evicted.
    producer.synchronize_from_validators().await?;
    producer.process_inbox().await?;
    {
        let producer_state = producer
            .client
            .local_node
            .chain_state_view(producer_id)
            .await?;
        assert!(
            producer_state
                .execution_state
                .system
                .unfinalized_message_blocks
                .get(&recipient_id)
                .await?
                .is_none(),
            "the CheckpointAck at {acked_cursor:?} should evict the recipient entirely",
        );
        assert!(
            !producer_state
                .execution_state
                .system
                .pending_checkpoint_ack_targets
                .contains(&recipient_id)
                .await?,
            "consuming a CheckpointAck must not seed a fresh ack target — that's how \
             the otherwise-perpetual notification ping-pong is broken",
        );
    }

    // Producer's checkpoint #2: nothing left to certify, nothing to notify.
    let checkpoint_2 = producer.checkpoint().await.unwrap().unwrap();
    let block = checkpoint_2.block();
    let outbox_block_hashes = match block.body.oracle_responses.first().and_then(|t| t.first()) {
        Some(OracleResponse::Checkpoint {
            outbox_block_hashes,
            ..
        }) => outbox_block_hashes.clone(),
        other => panic!("expected OracleResponse::Checkpoint, got {other:?}"),
    };
    assert!(
        outbox_block_hashes.is_empty(),
        "all sent messages have been acked; checkpoint #2 has nothing left to certify",
    );
    assert!(
        !block
            .body
            .messages
            .iter()
            .flatten()
            .any(|msg| msg.destination == recipient_id),
        "cycle should terminate: producer's next checkpoint must not notify the recipient",
    );

    Ok(())
}

/// Verifies the push side of the checkpoint flow: a validator that missed the whole
/// chain is brought up to speed by the proposing client pushing only the latest
/// checkpoint plus the pre-checkpoint sender blocks it certifies, not every
/// pre-checkpoint block. The lagging validator's vote is needed for quorum.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[test_log::test(tokio::test)]
async fn test_proposal_pushes_checkpoint_to_lagging_validator<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;

    // Validator 0 refuses everything for the duration of block production, so its
    // storage holds only the chain description blob from `add_root_chain` and never
    // sees any of the chain's certificates. We'll bring it back honest later.
    builder.set_fault_type([0], FaultType::NoChains);

    let producer = builder.add_root_chain(1, Amount::from_tokens(7)).await?;
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_id = producer.chain_id();
    let validator_0_key = builder.node(0).name();

    // Height 0: cross-chain transfer — becomes a pre-checkpoint sender block that the
    // checkpoint certifies via `outbox_block_hashes`. The recipient never consumes it.
    let transfer_0 = producer
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(transfer_0.block().header.height, BlockHeight::ZERO);

    // Height 1: same-chain burn — no outgoing messages, so it's not referenced by any
    // subsequent `outbox_block_hashes`. The push path must skip it.
    let burn_1 = producer
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(burn_1.block().header.height, BlockHeight::from(1));

    // Height 2: the checkpoint that will be pushed to validator 0.
    let checkpoint = producer.checkpoint().await.unwrap().unwrap();
    assert_eq!(checkpoint.block().header.height, BlockHeight::from(2));
    let outbox_block_hashes = match checkpoint
        .block()
        .body
        .oracle_responses
        .first()
        .and_then(|t| t.first())
    {
        Some(OracleResponse::Checkpoint {
            outbox_block_hashes,
            ..
        }) => outbox_block_hashes.clone(),
        other => panic!("Expected OracleResponse::Checkpoint, got {other:?}"),
    };
    assert_eq!(outbox_block_hashes, vec![transfer_0.hash()]);

    // Height 3: post-checkpoint burn — must be pushed to validator 0 as part of the
    // post-checkpoint gap fill.
    let burn_3 = producer
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(burn_3.block().header.height, BlockHeight::from(3));

    let validator_0_storage = builder
        .validator_storages
        .get(&validator_0_key)
        .expect("validator 0 storage")
        .clone();
    for cert in [&transfer_0, &burn_1, &checkpoint, &burn_3] {
        assert!(
            !validator_0_storage
                .contains_certificate(cert.hash())
                .await?,
            "validator 0 should not yet hold height {}",
            cert.block().header.height,
        );
    }

    // Bring validator 0 back honest and take validator 1 offline. Quorum (3 of 4) for
    // the next proposal now requires validators 0, 2, 3 — forcing the client to push
    // enough state to validator 0 for it to vote.
    builder.set_fault_type([0], FaultType::Honest);
    builder.set_fault_type([1], FaultType::Offline);

    // Height 4: a new block whose quorum needs validator 0.
    let burn_4 = producer
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(burn_4.block().header.height, BlockHeight::from(4));

    // Validator 0 received the checkpoint, the pre-checkpoint sender block it
    // certifies, the post-checkpoint burn, and the newly committed proposal.
    assert!(
        validator_0_storage
            .contains_certificate(checkpoint.hash())
            .await?,
        "validator 0 should have received the checkpoint",
    );
    assert!(
        validator_0_storage
            .contains_certificate(transfer_0.hash())
            .await?,
        "validator 0 should have received the pre-checkpoint sender block",
    );
    assert!(
        validator_0_storage
            .contains_certificate(burn_3.hash())
            .await?,
        "validator 0 should have received the post-checkpoint block",
    );
    assert!(
        validator_0_storage
            .contains_certificate(burn_4.hash())
            .await?,
        "validator 0 should have received the new block it voted on",
    );

    // The pre-checkpoint same-chain burn isn't referenced by `outbox_block_hashes`
    // and must not have been pushed.
    assert!(
        !validator_0_storage
            .contains_certificate(burn_1.hash())
            .await?,
        "validator 0 must not have received the pre-checkpoint non-sender block",
    );

    // The trust-mark flow consumed every entry: the validator's first attempt at
    // the checkpoint errored with `BlocksNotFound`, the client uploaded the missing
    // sender block (entering the trust-mark accept path on the worker, which
    // removed the entry), and the second attempt restored the chain cleanly.
    {
        let chain_view = validator_0_storage.load_chain(chain_id).await?;
        let trusted = chain_view.pre_checkpoint_block_trust.indices().await?;
        assert!(
            trusted.is_empty(),
            "validator 0's trust set should be empty after the flow, was {trusted:?}",
        );
    }

    Ok(())
}

/// Verifies that corruption-recovery reset re-executes from the latest checkpoint, not from
/// block 0. After the reset the chain reaches the same tip and state hash, but the
/// pre-checkpoint, message-free block is no longer in `block_hashes`: it sits below the
/// checkpoint and isn't recertified, so a from-checkpoint reset never replays it — which a
/// from-block-0 reset would have.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[test_log::test(tokio::test)]
async fn test_checkpoint_reset_from_latest_checkpoint<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, signer).await?;
    let chain = builder.add_root_chain(1, Amount::from_tokens(7)).await?;
    let target = builder.add_root_chain(2, Amount::ZERO).await?;
    let chain_id = chain.chain_id();

    // Height 0: a message-free burn — below the checkpoint and not recertified, so it is the
    // block a from-checkpoint reset must drop. Height 1: the checkpoint. Height 2: a transfer,
    // so there is post-checkpoint state to replay.
    let burn_cert = chain
        .burn(AccountOwner::CHAIN, Amount::ONE)
        .await
        .unwrap_ok_committed();
    assert_eq!(burn_cert.block().header.height, BlockHeight::ZERO);
    let checkpoint_cert = chain.checkpoint().await.unwrap().unwrap();
    assert_eq!(checkpoint_cert.block().header.height, BlockHeight::from(1));
    chain
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::ONE,
            Account::chain(target.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let info_before = chain.chain_info().await?;
    assert_eq!(info_before.next_block_height, BlockHeight::from(3));
    let state_hash_before = info_before.state_hash;

    // Reset the chain on the producer's own worker and re-execute it.
    chain
        .client
        .local_node
        .reset_and_reexecute_chain(chain_id)
        .await?;

    // It re-executed to the same tip and state hash...
    let info_after = chain.chain_info().await?;
    assert_eq!(info_after.next_block_height, BlockHeight::from(3));
    assert_eq!(info_after.state_hash, state_hash_before);

    // ...but started from the checkpoint: the checkpoint (height 1) and the post-checkpoint
    // transfer (height 2) are present in `block_hashes`, while the pre-checkpoint burn is gone.
    let chain_state = chain.client.local_node.chain_state_view(chain_id).await?;
    assert!(chain_state
        .block_hashes
        .get(&BlockHeight::from(1))
        .await?
        .is_some());
    assert!(chain_state
        .block_hashes
        .get(&BlockHeight::from(2))
        .await?
        .is_some());
    assert!(
        chain_state
            .block_hashes
            .get(&BlockHeight::ZERO)
            .await?
            .is_none(),
        "a from-checkpoint reset must not replay the pre-checkpoint burn",
    );

    Ok(())
}

/// Regression test for #5664: when the chain advances (e.g. a notification or background
/// sync commits another owner's block at our height) while a client is in the middle of
/// `execute_block`, the staged pending proposal is cleared without committing ours. This
/// used to surface as a hard `BlockProposalError("Unexpected block proposal error")`.
/// The client must instead re-stage at the new height and never raise that error.
///
/// Two owners hammer the same multi-owner chain concurrently while each client runs a
/// notification listener that advances its local node underneath the proposer task,
/// reproducing the race. Requires the multi-threaded runtime: on a single thread the
/// listener cannot advance the local node between staging and re-processing.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_execute_block_retries_when_chain_advances<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    const ROUNDS: usize = 50;

    let mut signer = InMemorySigner::new(None);
    let owner1 = signer.generate_new().into();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let client0 = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let chain_id = client0.chain_id();
    let owner0 = client0.identity().await?;

    // Make the chain a two-owner chain so both clients can propose at the same height.
    let ownership = ChainOwnership {
        super_owners: BTreeSet::new(),
        owners: BTreeMap::from_iter([(owner0, 100), (owner1, 100)]),
        first_leader: None,
        multi_leader_rounds: 10,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig::default(),
    };
    client0.change_ownership(ownership).await.unwrap();

    let mut client1 = builder
        .make_client(
            chain_id,
            client0.chain_info().await?.block_hash,
            BlockHeight::from(1),
        )
        .await?;
    client1.set_preferred_owner(owner1);
    client1.synchronize_from_validators().await.unwrap();

    // Run a notification listener on each client so its local node is advanced by the other
    // owner's commits in the background — the same way the node service's background sync
    // advances the chain while `execute_block` is running.
    let (listener0, _abort0, _notifs0) = client0.listen().await?;
    let (listener1, _abort1, _notifs1) = client1.listen().await?;
    tokio::spawn(listener0);
    tokio::spawn(listener1);

    // Both owners publish a stream of data blobs to the same chain concurrently. They
    // collide at the same height repeatedly; whichever loses a race may observe the chain
    // advancing mid-proposal. None of these calls may fail with the "unexpected block
    // proposal error".
    async fn race(client: &ChainClient<impl Environment>, tag: u8) {
        for i in 0..ROUNDS {
            let data = vec![tag, i as u8];
            match client.publish_data_blob(data).await {
                // Committed / Conflict / WaitForTimeout are all acceptable outcomes of a
                // concurrent proposal.
                Ok(_) => {}
                Err(err) => {
                    let message = err.to_string();
                    assert!(
                        !message.contains("Unexpected block proposal error"),
                        "execute_block raised the #5664 error instead of retrying: {message}",
                    );
                    // Other transient errors (communication, conflicts surfaced as errors)
                    // are not what this test guards; resynchronize and continue.
                    client.synchronize_from_validators().await.ok();
                }
            }
        }
    }

    futures::join!(race(&client0, 0), race(&client1, 1));

    Ok(())
}

/// Regression test for the no-signer-key branch in `process_pending_block_without_prepare`:
/// when a pending proposal is authenticated by an owner whose key the signer no longer holds
/// (e.g. an autosigner key that staged the block is later withdrawn), the client must discard
/// the stale proposal and report `Committed(None)` rather than erroring or wedging. This is the
/// second of the two ways `execute_block`'s retry loop can observe `Committed(None)` — the
/// other being the chain advancing past the staged height
/// (`test_execute_block_retries_when_chain_advances`).
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new(); "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_pending_block_discarded_when_signer_key_missing<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut client = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    let owner_a = client.identity().await?;

    // Co-own the chain with a second owner `b` and no super-owner, so it runs in multi-leader
    // (non-fast) rounds — the branch that discards rather than erroring.
    let owner_b: AccountOwner = builder.signer.generate_new().into();
    let ownership =
        ChainOwnership::multiple([(owner_a, 50), (owner_b, 50)], 10, TimeoutConfig::default());
    client.change_ownership(ownership).await?;

    // Stage a block as owner `a` that can't reach a quorum, so it stays pending, authenticated
    // by `a` (signing it here requires `a`'s key, which the signer still holds at this point).
    builder.set_fault_type([0, 1], FaultType::Offline);
    assert_matches!(
        client.burn(AccountOwner::CHAIN, Amount::ONE).await,
        Err(_),
        "the burn should fail to commit with only two of four validators online"
    );
    assert_eq!(
        client
            .pending_proposal()
            .await
            .expect("a pending proposal authored by `a` should remain")
            .block
            .authenticated_owner,
        Some(owner_a),
    );

    // Bring the validators back, then withdraw owner `a`'s key from the signer and act as `b`.
    builder.set_fault_type([0, 1], FaultType::Honest);
    client.synchronize_from_validators().await?;
    assert!(
        builder.signer.forget_key(&owner_a),
        "owner `a`'s key should have been present before we forget it"
    );
    client.set_preferred_owner(owner_b);

    // Processing the pending block must discard the stale proposal (we can no longer sign as `a`)
    // and report `Committed(None)`, rather than raising an error or looping.
    assert_matches!(
        client.process_pending_block().await?,
        ClientOutcome::Committed(None)
    );
    assert!(client.pending_proposal().await.is_none());

    Ok(())
}
