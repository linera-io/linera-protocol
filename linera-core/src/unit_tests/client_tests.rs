// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_client_tests.rs"]
mod wasm;

use crate::{
    client::{
        client_test_utils::{FaultType, MakeMemoryStorage, StorageBuilder, TestBuilder},
        ChainClient, ChainClientError, ClientOutcome, CommunicateAction, MessageAction,
    },
    local_node::LocalNodeError,
    node::{
        CrossChainMessageDelivery,
        NodeError::{self, ClientIoError},
    },
    updater::CommunicationError,
    worker::{Notification, Reason, WorkerError},
};
use futures::{lock::Mutex, StreamExt};
use linera_base::{
    crypto::*,
    data_types::*,
    identifiers::{ChainDescription, ChainId, MessageId, Owner},
};
use linera_chain::{
    data_types::{CertificateValue, Event, ExecutedBlock},
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::{Committee, Epoch},
    policy::ResourceControlPolicy,
    system::{Account, Recipient, SystemOperation, UserData},
    ChainOwnership, ExecutionError, Message, Operation, SystemExecutionError,
    SystemMessage, SystemQuery, SystemResponse,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use std::sync::Arc;
use test_log::test;

#[cfg(feature = "rocksdb")]
use crate::client::client_test_utils::{MakeRocksDbStorage, ROCKS_DB_SEMAPHORE};

#[cfg(feature = "aws")]
use crate::client::client_test_utils::MakeDynamoDbStorage;

#[cfg(feature = "scylladb")]
use crate::client::client_test_utils::MakeScyllaDbStorage;

#[test(tokio::test)]
pub async fn test_memory_initiating_valid_transfer_with_notifications(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_with_notifications(MakeMemoryStorage::default())
        .await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer_with_notifications(
) -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer_with_notifications(MakeRocksDbStorage::default())
        .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_with_notifications(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_with_notifications(MakeDynamoDbStorage::default())
        .await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer_with_notifications(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_with_notifications(MakeScyllaDbStorage::default())
        .await
}

async fn run_test_initiating_valid_transfer_with_notifications<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_certificate());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let sender = Arc::new(Mutex::new(sender));
    // Listen to the notifications on the sender chain.
    let mut notifications = sender.lock().await.subscribe().await?;
    ChainClient::listen(sender.clone()).await?;
    {
        let mut sender = sender.lock().await;
        let certificate = sender
            .transfer_to_account(
                None,
                Amount::from_tokens(3),
                Account::chain(ChainId::root(2)),
                UserData(Some(*b"I paid 0.001 to pay you these 3!")),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sender.next_block_height, BlockHeight::from(1));
        assert!(sender.pending_block.is_none());
        // `local_balance` stages another block execution, which costs another 0.001.
        assert_eq!(
            sender.local_balance().await.unwrap(),
            Amount::from_milli(998)
        );
        assert_eq!(
            builder
                .check_that_validators_have_certificate(
                    sender.chain_id,
                    BlockHeight::ZERO,
                    3
                )
                .await
                .unwrap()
                .value,
            certificate.value
        );
    }
    assert!(matches!(
        notifications.next().await,
        Some(Notification {
            reason: Reason::NewBlock { height, .. },
            chain_id,
        }) if chain_id == ChainId::root(1) && height == BlockHeight::ZERO
    ));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_claim_amount() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_claim_amount(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeScyllaDbStorage::default()).await
}

async fn run_test_claim_amount<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let owner = sender.identity().await?;
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let cert = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::owner(ChainId::root(2), owner),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ONE);
    receiver.receive_certificate(cert).await?;
    receiver.process_inbox().await?;
    // The received amount is not in the unprotected balance.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);

    // First attempt that should be rejected.
    sender
        .claim(
            owner,
            ChainId::root(2),
            Recipient::root(1),
            Amount::from_tokens(5),
            UserData(None),
        )
        .await
        .unwrap();
    // Second attempt with a correct amount.
    let cert = sender
        .claim(
            owner,
            ChainId::root(2),
            Recipient::root(1),
            Amount::from_tokens(2),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert).await?;
    let cert = receiver.process_inbox().await?.0.pop().unwrap();
    {
        let messages = &cert.value().block().unwrap().incoming_messages;
        // Both `Claim` messages were included in the block.
        assert_eq!(messages.len(), 2);
        // The first one was rejected.
        assert_eq!(messages[0].event.height, BlockHeight::from(1));
        assert_eq!(messages[0].action, MessageAction::Reject);
        // The second was accepted.
        assert_eq!(messages[1].event.height, BlockHeight::from(2));
        assert_eq!(messages[1].action, MessageAction::Accept);
    }

    sender.receive_certificate(cert).await?;
    sender.process_inbox().await?;
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    Ok(())
}

#[test(tokio::test)]
async fn test_memory_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_rotate_key_pair(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeScyllaDbStorage::default()).await
}

async fn run_test_rotate_key_pair<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner::from(new_key_pair.public());
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap().unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.identity().await.unwrap(), new_owner);
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_milli(3998)
    );
    assert_eq!(
        sender.synchronize_from_validators().await.unwrap(),
        Amount::from_milli(3998)
    );
    // Can still use the chain.
    sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData::default(),
        )
        .await
        .unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_ownership(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeScyllaDbStorage::default()).await
}

async fn run_test_transfer_ownership<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;

    let new_key_pair = KeyPair::generate();
    let certificate = sender
        .transfer_ownership(new_key_pair.public())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(matches!(
        sender.key_pair().await,
        Err(ChainClientError::CannotFindKeyForChain(_))
    ));
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_milli(3998)
    );
    assert_eq!(
        sender.synchronize_from_validators().await.unwrap(),
        Amount::from_milli(3998)
    );
    // Cannot use the chain any more.
    assert!(matches!(
        sender
            .transfer_to_account(
                None,
                Amount::from_tokens(3),
                Account::chain(ChainId::root(2)),
                UserData::default()
            )
            .await,
        Err(ChainClientError::CannotFindKeyForChain(_))
    ));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_share_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_share_ownership(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeScyllaDbStorage::default()).await
}

async fn run_test_share_ownership<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let certificate = sender
        .share_ownership(new_key_pair.public(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    assert_eq!(
        sender.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(4)
    );
    // Can still use the chain with the old client.
    sender
        .transfer_to_account(
            None,
            Amount::from_tokens(2),
            Account::chain(ChainId::root(2)),
            UserData::default(),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    // Make a client to try the new key.
    let mut client = builder
        .make_client(
            sender.chain_id,
            new_key_pair,
            sender.block_hash,
            BlockHeight::from(2),
        )
        .await?;
    // Local balance fails because the client has block height 2 but we haven't downloaded
    // the blocks yet.
    assert!(matches!(
        client.local_balance().await,
        Err(ChainClientError::WalletSynchronizationError)
    ));
    assert_eq!(
        client.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(2)
    );
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );

    // We need at least three validators for making a transfer.
    builder.set_fault_type(..2, FaultType::Offline).await;
    let result = client
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(ChainClientError::CommunicationError(
                CommunicationError::Trusted(ClientIoError { .. }),
            ))
        ),
        "Unexpected result: {:?}",
        result
    );
    builder.set_fault_type(..2, FaultType::Honest).await;
    builder.set_fault_type(2.., FaultType::Offline).await;
    assert!(matches!(
        sender
            .transfer_to_account(
                None,
                Amount::ONE,
                Account::chain(ChainId::root(3)),
                UserData::default(),
            )
            .await,
        Err(ChainClientError::CommunicationError(
            CommunicationError::Trusted(ClientIoError { .. })
        ))
    ));

    // Half the validators voted for one block, half for the other. We need to make a proposal in
    // the next round to succeed.
    builder.set_fault_type(.., FaultType::Honest).await;
    assert_eq!(
        client.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(2)
    );
    client.clear_pending_block().await;
    client
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .unwrap();

    // The other client doesn't know the new round number yet:
    assert_eq!(
        sender.synchronize_from_validators().await.unwrap(),
        Amount::ONE
    );
    sender.clear_pending_block().await;
    sender
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(2)),
            UserData::default(),
        )
        .await
        .unwrap();

    // That's it, we spent all our money on this test!
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client.synchronize_from_validators().await.unwrap(),
        Amount::ZERO
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_close_it(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeScyllaDbStorage::default()).await
}

async fn run_test_open_chain_then_close_it<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    // Open the new chain.
    let (message_id, certificate) = sender
        .open_chain(ChainOwnership::single(new_key_pair.public()), Amount::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let new_id = ChainId::child(message_id);
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client.synchronize_from_validators().await.unwrap(),
        Amount::ZERO
    );
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    client.close_chain().await.unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_then_open_chain(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeScyllaDbStorage::default()).await
}

async fn run_test_transfer_then_open_chain<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let mut parent = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_id = ChainId::child(MessageId {
        chain_id: ChainId::root(2),
        height: BlockHeight::ZERO,
        index: 0,
    });
    // Transfer before creating the chain. The validators will ignore the cross-chain messages.
    sender
        .transfer_to_account(
            None,
            Amount::from_tokens(2),
            Account::chain(new_id),
            UserData::default(),
        )
        .await
        .unwrap();
    // Open the new chain.
    let (open_chain_message_id, certificate) = parent
        .open_chain(ChainOwnership::single(new_key_pair.public()), Amount::ZERO)
        .await
        .unwrap()
        .unwrap();
    let new_id2 = ChainId::child(open_chain_message_id);
    assert_eq!(new_id, new_id2);
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert_eq!(parent.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(
                parent.chain_id,
                BlockHeight::from(0),
                3
            )
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert!(matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block, .. } if matches!(
            executed_block.block.operations[open_chain_message_id.index as usize],
            Operation::System(SystemOperation::OpenChain { .. }),
        ),
    ));
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    // Make another block on top of the one that sent the two tokens, so that the validators
    // process the cross-chain messages.
    let certificate2 = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(1),
            Account::chain(new_id),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    client.receive_certificate(certificate2).await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    client
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    run_test_open_chain_must_be_first(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_must_be_first(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    run_test_open_chain_must_be_first(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    run_test_open_chain_must_be_first(MakeScyllaDbStorage::default()).await
}

async fn run_test_open_chain_must_be_first<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_id = ChainId::child(MessageId {
        chain_id: ChainId::root(1),
        height: BlockHeight::from(1),
        index: 0,
    });
    // Transfer before creating the chain.
    sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(new_id),
            UserData::default(),
        )
        .await
        .unwrap();
    // Open the new chain.
    let (open_chain_message_id, certificate) = sender
        .open_chain(ChainOwnership::single(new_key_pair.public()), Amount::ZERO)
        .await
        .unwrap()
        .unwrap();
    let new_id2 = ChainId::child(open_chain_message_id);
    assert_eq!(new_id, new_id2);
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(
                sender.chain_id,
                BlockHeight::from(1),
                3
            )
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert!(matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            block.operations[open_chain_message_id.index as usize],
            Operation::System(SystemOperation::OpenChain { .. }),
        ),
    ));
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    let result = client
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await;
    assert!(matches!(
        result,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        )) if matches!(*error, ChainError::CannotSkipMessage {
            event: Event { message: Message::System(SystemMessage::Credit { .. }), .. }, ..
        })
    ));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_transfer(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeScyllaDbStorage::default()).await
}

async fn run_test_open_chain_then_transfer<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    // Open the new chain. We are both regular and super owner.
    let ownership = ChainOwnership::single(new_key_pair.public())
        .with_regular_owner(new_key_pair.public(), 100);
    let (message_id, creation_certificate) = sender
        .open_chain(ownership, Amount::ZERO)
        .await
        .unwrap()
        .unwrap();
    let new_id = ChainId::child(message_id);
    // Transfer after creating the chain.
    let transfer_certificate = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(new_id),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    // Must process the creation certificate before using the new chain.
    client
        .receive_certificate(creation_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    client
        .receive_certificate(transfer_certificate)
        .await
        .unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    client
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_close_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_close_chain(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeScyllaDbStorage::default()).await
}

async fn run_test_close_chain<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let certificate = sender.close_chain().await.unwrap().unwrap();
    assert!(matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            &block.operations[..], &[Operation::System(SystemOperation::CloseChain)]
        ),
    ));
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(matches!(
        sender.key_pair().await,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::InactiveChain(_)
        ))
    ));
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Cannot use the chain any more.
    assert!(matches!(
        sender
            .transfer_to_account(
                None,
                Amount::from_tokens(3),
                Account::chain(ChainId::root(2)),
                UserData::default()
            )
            .await,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::InactiveChain(_)
        ))
    ));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_initiating_valid_transfer_too_many_faults(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer_too_many_faults(
) -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer_too_many_faults(MakeRocksDbStorage::default())
        .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_too_many_faults(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeDynamoDbStorage::default())
        .await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer_too_many_faults(
) -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeScyllaDbStorage::default())
        .await
}

async fn run_test_initiating_valid_transfer_too_many_faults<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 2).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let result = sender
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(ChainClientError::CommunicationError(
                CommunicationError::Trusted(
                    crate::node::NodeError::ArithmeticError { .. }
                )
            ))
        ),
        "Unexpected result {:?}",
        result
    );
    assert_eq!(sender.next_block_height, BlockHeight::ZERO);
    assert!(sender.pending_block.is_some());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_bidirectional_transfer(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylla")]
#[test(tokio::test)]
async fn test_scylla_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeScyllaDbStorage::default()).await
}

async fn run_test_bidirectional_transfer<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        client1.query_system_application(SystemQuery).await.unwrap(),
        SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::from_tokens(3),
        }
    );

    let certificate = client1
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(client2.chain_id),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client1.query_system_application(SystemQuery).await.unwrap(),
        SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::ZERO,
        }
    );

    assert_eq!(
        builder
            .check_that_validators_have_certificate(
                client1.chain_id,
                BlockHeight::ZERO,
                3
            )
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Local balance is lagging.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Force synchronization of local balance.
    assert_eq!(
        client2.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        client2.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    // The local balance from the client is reflecting incoming messages but the
    // SystemResponse only reads the ChainState.
    assert_eq!(
        client2.query_system_application(SystemQuery).await.unwrap(),
        SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::ZERO,
        }
    );

    // Send back some money.
    assert_eq!(client2.next_block_height, BlockHeight::ZERO);
    client2
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(client1.chain_id),
            UserData::default(),
        )
        .await
        .unwrap();
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    assert_eq!(
        client2.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    assert_eq!(
        client1.synchronize_from_validators().await.unwrap(),
        Amount::ONE
    );
    // Local balance from client2 is now consolidated.
    assert_eq!(
        client2.query_system_application(SystemQuery).await.unwrap(),
        SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::from_tokens(2),
        }
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeScyllaDbStorage::default()).await
}

async fn run_test_receiving_unconfirmed_transfer<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_certificate());
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let certificate = client1
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(2),
            Account::chain(client2.chain_id),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    // Transfer was executed locally.
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_milli(998)
    );
    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client2.local_balance().await.unwrap(),
        Amount::from_milli(1999)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeMemoryStorage::default(),
    )
    .await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeRocksDbStorage::default(),
    )
    .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeDynamoDbStorage::default(),
    )
    .await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeScyllaDbStorage::default(),
    )
    .await
}

async fn run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let mut client3 = builder
        .add_initial_chain(ChainDescription::Root(3), Amount::ZERO)
        .await?;

    // Transferring funds from client1 to client2.
    // Confirming to a quorum of nodes only at the end.
    client1
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::ONE,
            Account::chain(client2.chain_id),
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::ONE,
            Account::chain(client2.chain_id),
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .communicate_chain_updates(
            &builder.initial_committee,
            client1.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight {
                height: client1.next_block_height,
                delivery: CrossChainMessageDelivery::NonBlocking,
            },
        )
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Sending money from client2 fails, as a consequence.
    assert!(matches!(client2
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id),
            UserData::default(),
        )
        .await,
        Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(error)))) if matches!(*error, ChainError::ExecutionError(ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }), ChainExecutionContext::Operation(_)))
    ));
    // There is no pending block, since the proposal wasn't valid at the time.
    assert!(client2.retry_pending_block().await.unwrap().is_none());
    // Retrying the whole command works after synchronization.
    assert_eq!(
        client2.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(2)
    );
    let certificate = client2
        .transfer_to_account(
            None,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    // Blocks were executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ONE);
    assert_eq!(client1.next_block_height, BlockHeight::from(2));
    assert!(client1.pending_block.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client3.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client3.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_change_voting_rights() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_change_voting_rights(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeScyllaDbStorage::default()).await
}

async fn run_test_change_voting_rights<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let mut admin = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    let mut user = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ZERO)
        .await?;

    // Create a new committee.
    let validators = builder.initial_committee.validators().clone();
    let committee = Committee::new(validators, ResourceControlPolicy::only_fuel());
    admin.stage_new_committee(committee).await.unwrap();
    assert_eq!(admin.next_block_height, BlockHeight::from(1));
    assert!(admin.pending_block.is_none());
    assert!(admin.key_pair().await.is_ok());
    assert_eq!(admin.epoch().await.unwrap(), Epoch::from(1));

    // Sending money from the admin chain is supported.
    let cert = admin
        .transfer_to_account(
            None,
            Amount::from_tokens(2),
            Account::chain(ChainId::root(1)),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    admin
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(1)),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();

    // User is still at the initial epoch, but we can receive transfers from future
    // epochs AFTER synchronizing the client with the admin chain.
    assert!(matches!(
        user.receive_certificate(cert).await,
        Err(ChainClientError::CommitteeSynchronizationError)
    ));
    assert_eq!(user.epoch().await.unwrap(), Epoch::ZERO);
    assert_eq!(
        user.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(3)
    );

    // User is a genesis chain so the migration message is not even in the inbox yet.
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::ZERO);

    // Now subscribe explicitly to migrations.
    let cert = user.subscribe_to_new_committees().await.unwrap().unwrap();
    admin.receive_certificate(cert).await.unwrap();
    admin.process_inbox().await.unwrap();

    // Have the admin chain deprecate the previous epoch.
    admin.finalize_committee().await.unwrap();

    // Try to make a transfer back to the admin chain.
    let cert = user
        .transfer_to_account(
            None,
            Amount::from_tokens(2),
            Account::chain(ChainId::root(0)),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        admin.receive_certificate(cert).await,
        Err(ChainClientError::CommitteeDeprecationError)
    ));
    // Transfer is blocked because the epoch #0 has been retired by admin.
    assert_eq!(
        admin.synchronize_from_validators().await.unwrap(),
        Amount::ZERO
    );

    // Have the user receive the notification to migrate to epoch #1.
    user.synchronize_from_validators().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));

    // Try again to make a transfer back to the admin chain.
    let cert = user
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(0)),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    admin.receive_certificate(cert).await.unwrap();
    // Transfer goes through and the previous one as well thanks to block chaining.
    assert_eq!(
        admin.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(3)
    );
    Ok(())
}

#[test(tokio::test)]
pub async fn test_memory_insufficient_balance() -> Result<(), anyhow::Error> {
    run_test_insufficient_balance(MakeMemoryStorage::default()).await
}

async fn run_test_insufficient_balance<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let obtained_error = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"I'm giving away all of my money!")),
        )
        .await;
    assert!(matches!(obtained_error,
                     Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(error)))) if matches!(*error, ChainError::ExecutionError(ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }), ChainExecutionContext::Block))
    ));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(MakeMemoryStorage::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_request_leader_timeout(MakeRocksDbStorage::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(MakeDynamoDbStorage::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(MakeScyllaDbStorage::default()).await
}

async fn run_test_request_leader_timeout<B>(
    storage_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let chain_id = ChainId::from(description);
    let mut client = builder
        .add_initial_chain(description, Amount::from_tokens(3))
        .await?;
    let pub_key0 = client.public_key().await.unwrap();
    let pub_key1 = KeyPair::generate().public();

    let owner_change_op = SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: 0,
    }
    .into();
    client.execute_operation(owner_change_op).await.unwrap();
    let manager = client.chain_info().await.unwrap().manager;

    // The round has not timed out yet, so validators will not sign a timeout certificate.
    assert!(matches!(
        client.request_leader_timeout().await,
        Err(ChainClientError::CommunicationError(
            CommunicationError::Trusted(NodeError::MissingVoteInValidatorResponse)
        ))
    ));

    clock.set(manager.round_timeout);

    // After the timeout they will.
    let certificate = client.request_leader_timeout().await.unwrap();
    assert_eq!(
        *certificate.value(),
        CertificateValue::LeaderTimeout {
            chain_id,
            height: BlockHeight::from(1),
            epoch: Epoch::ZERO
        }
    );
    assert_eq!(certificate.round, Round::SingleLeader(0));

    let expected_round = Round::SingleLeader(1);
    builder
        .check_that_validators_are_in_round(
            chain_id,
            BlockHeight::from(1),
            expected_round,
            3,
        )
        .await;

    let round = loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(Owner::from(pub_key1)) {
            break manager.current_round;
        }
        clock.set(manager.round_timeout);
        assert!(client.request_leader_timeout().await.is_ok());
    };
    let round_number = match round {
        Round::SingleLeader(round_number) => round_number,
        round => panic!("Unexpected round {:?}", round),
    };

    // The other owner is leader now. Trying to submit a block should return `WaitForTimeout`.
    let result = client
        .transfer(
            None,
            Amount::from(1),
            Recipient::root(2),
            UserData::default(),
        )
        .await
        .unwrap();
    let timeout = match result {
        ClientOutcome::Committed(_) => {
            panic!("Committed a block where we aren't the leader.")
        }
        ClientOutcome::WaitForTimeout(timeout) => timeout,
    };
    assert!(client.request_leader_timeout().await.is_err());
    clock.set(timeout.timestamp);
    client.request_leader_timeout().await.unwrap();
    let expected_round = Round::SingleLeader(round_number + 1);
    builder
        .check_that_validators_are_in_round(
            chain_id,
            BlockHeight::from(1),
            expected_round,
            3,
        )
        .await;

    loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(Owner::from(pub_key0)) {
            break;
        }
        clock.set(manager.round_timeout);
        assert!(client.request_leader_timeout().await.is_ok());
    }

    // Now we are the leader, and the transfer should succeed.
    let _certificate = client
        .transfer(
            None,
            Amount::from_tokens(1),
            Recipient::root(2),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );

    let expected_round = Round::SingleLeader(0);
    builder
        .check_that_validators_are_in_round(
            chain_id,
            BlockHeight::from(2),
            expected_round,
            3,
        )
        .await;

    Ok(())
}
