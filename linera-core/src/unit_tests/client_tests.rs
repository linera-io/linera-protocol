// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_client_tests.rs"]
mod wasm;

use assert_matches::assert_matches;
use futures::StreamExt;
use linera_base::{
    crypto::*,
    data_types::*,
    identifiers::{Account, ChainDescription, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{CertificateValue, Event, ExecutedBlock, IncomingMessage, Medium, Origin},
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::{Recipient, SystemOperation, UserData},
    ExecutionError, Message, MessageKind, Operation, ResourceControlPolicy, SystemExecutionError,
    SystemMessage, SystemQuery, SystemResponse,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use test_log::test;
#[cfg(not(target_arch = "wasm32"))]
use {
    crate::client::client_test_utils::ServiceStorageBuilder,
    linera_storage_service::child::get_free_port,
};

#[cfg(feature = "aws")]
use crate::client::client_test_utils::DynamoDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::client::client_test_utils::ScyllaDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::client::client_test_utils::{RocksDbStorageBuilder, ROCKS_DB_SEMAPHORE};
use crate::{
    client::{
        client_test_utils::{FaultType, MemoryStorageBuilder, StorageBuilder, TestBuilder},
        ArcChainClient, ChainClientError, ClientOutcome, MessageAction, MessagePolicy,
    },
    local_node::LocalNodeError,
    node::{
        CrossChainMessageDelivery,
        NodeError::{self, ClientIoError},
    },
    updater::CommunicationError,
    worker::{Notification, Reason, WorkerError},
};

#[test(tokio::test)]
pub async fn test_memory_initiating_valid_transfer_with_notifications() -> Result<(), anyhow::Error>
{
    run_test_initiating_valid_transfer_with_notifications(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
pub async fn test_service_initiating_valid_transfer_with_notifications() -> Result<(), anyhow::Error>
{
    let endpoint = get_free_port().await.unwrap();
    run_test_initiating_valid_transfer_with_notifications(ServiceStorageBuilder::new(&endpoint))
        .await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer_with_notifications() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer_with_notifications(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_with_notifications() -> Result<(), anyhow::Error>
{
    run_test_initiating_valid_transfer_with_notifications(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer_with_notifications() -> Result<(), anyhow::Error>
{
    run_test_initiating_valid_transfer_with_notifications(ScyllaDbStorageBuilder::default()).await
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
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let sender = ArcChainClient::new(sender);
    // Listen to the notifications on the sender chain.
    let mut notifications = sender.lock().await.subscribe().await?;
    let (_listen_handle, _) = sender.listen().await?;
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
        assert_eq!(
            sender.local_balance().await.unwrap(),
            Amount::from_millis(999)
        );
        assert_eq!(
            builder
                .check_that_validators_have_certificate(sender.chain_id, BlockHeight::ZERO, 3)
                .await
                .unwrap()
                .value,
            certificate.value
        );
    }
    assert_matches!(
        notifications.next().await,
        Some(Notification {
            reason: Reason::NewBlock { height, .. },
            chain_id,
        }) if chain_id == ChainId::root(1) && height == BlockHeight::ZERO
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_claim_amount() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_claim_amount(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_claim_amount() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_claim_amount(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_claim_amount<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let owner = sender.identity().await?;
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let friend = receiver.identity().await?;
    sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::owner(ChainId::root(2), owner),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    let cert = sender
        .transfer_to_account(
            None,
            Amount::from_millis(100),
            Account::owner(ChainId::root(2), friend),
            UserData(None),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(898)
    );
    receiver.receive_certificate(cert).await?;
    assert_eq!(receiver.process_inbox().await?.0.len(), 1);
    // The friend paid to receive the message.
    assert_eq!(
        receiver.local_owner_balance(friend).await.unwrap(),
        Amount::from_millis(99)
    );
    // The received amount is not in the unprotected balance.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        receiver.local_owner_balance(owner).await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        receiver
            .local_balances_with_owner(Some(owner))
            .await
            .unwrap(),
        (Amount::ZERO, Some(Amount::from_tokens(3)))
    );
    assert_eq!(receiver.query_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        receiver.query_owner_balance(owner).await.unwrap(),
        Amount::from_millis(2999)
    );
    assert_eq!(
        receiver
            .query_balances_with_owner(Some(owner))
            .await
            .unwrap(),
        (Amount::ZERO, Some(Amount::from_millis(2999)))
    );

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
        assert_eq!(messages[0].event.height, BlockHeight::from(2));
        assert_eq!(messages[0].action, MessageAction::Reject);
        // The second was accepted.
        assert_eq!(messages[1].event.height, BlockHeight::from(3));
        assert_eq!(messages[1].action, MessageAction::Accept);
    }

    sender.receive_certificate(cert).await?;
    sender.process_inbox().await?;
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(2895)
    );

    Ok(())
}

#[test(tokio::test)]
async fn test_memory_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_rotate_key_pair() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_rotate_key_pair(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_rotate_key_pair(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_rotate_key_pair<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
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
        Amount::from_millis(3999)
    );
    sender.synchronize_from_validators().await.unwrap();
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
    run_test_transfer_ownership(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_transfer_ownership() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_transfer_ownership(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_ownership(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_transfer_ownership<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
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
    assert_matches!(
        sender.key_pair().await.map(KeyPair::public), // KeyPair isn't Debug; using PublicKey.
        Err(ChainClientError::CannotFindKeyForChain(_))
    );
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
        Amount::from_millis(3999)
    );
    sender.synchronize_from_validators().await.unwrap();
    // Cannot use the chain any more.
    assert_matches!(
        sender
            .transfer_to_account(
                None,
                Amount::from_tokens(3),
                Account::chain(ChainId::root(2)),
                UserData::default()
            )
            .await,
        Err(ChainClientError::CannotFindKeyForChain(_))
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_share_ownership() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_share_ownership(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_share_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_share_ownership(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(ScyllaDbStorageBuilder::default()).await
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
    sender.synchronize_from_validators().await.unwrap();
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
    assert_matches!(
        client.local_balance().await,
        Err(ChainClientError::WalletSynchronizationError)
    );
    client.synchronize_from_validators().await.unwrap();
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
    assert_matches!(
        result,
        Err(ChainClientError::CommunicationError(
            CommunicationError::Trusted(ClientIoError { .. }),
        ))
    );
    builder.set_fault_type(..2, FaultType::Honest).await;
    builder.set_fault_type(2.., FaultType::Offline).await;
    assert_matches!(
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
    );

    // Half the validators voted for one block, half for the other. We need to make a proposal in
    // the next round to succeed.
    builder.set_fault_type(.., FaultType::Honest).await;
    client.synchronize_from_validators().await.unwrap();
    client.process_inbox().await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    client.clear_pending_block();
    client
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();

    // The other client doesn't know the new round number yet:
    sender.synchronize_from_validators().await.unwrap();
    sender.process_inbox().await.unwrap();
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ONE);
    sender.clear_pending_block();
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
    client.synchronize_from_validators().await.unwrap();
    client.process_inbox().await.unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_open_chain_then_close_it(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_close_it(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_open_chain_then_close_it<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
    assert_eq!(client.query_balance().await.unwrap(), Amount::ZERO);
    client.close_chain().await.unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_transfer_then_open_chain(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_then_open_chain(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_transfer_then_open_chain<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
            .check_that_validators_have_certificate(parent.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_matches!(
        certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block, .. } if matches!(
            executed_block.block.operations[open_chain_message_id.index as usize],
            Operation::System(SystemOperation::OpenChain(_)),
        ),
        "Unexpected certificate value",
    );
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
        client.query_balance().await.unwrap(),
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
    run_test_open_chain_must_be_first(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_open_chain_must_be_first(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_must_be_first(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    run_test_open_chain_must_be_first(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_must_be_first() -> Result<(), anyhow::Error> {
    run_test_open_chain_must_be_first(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_open_chain_must_be_first<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(1), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            block.operations[open_chain_message_id.index as usize],
            Operation::System(SystemOperation::OpenChain(_)),
        ),
        "Unexpected certificate value",
    );
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    let result = client
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await;
    assert_matches!(
        result,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        )) if matches!(*error, ChainError::CannotSkipMessage {
            event: Event { message: Message::System(SystemMessage::Credit { .. }), .. }, ..
        })
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_open_chain_then_transfer(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_transfer(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_open_chain_then_transfer<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
        client.query_balance().await.unwrap(),
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
    run_test_close_chain(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_close_chain() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_close_chain(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_close_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_close_chain(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_close_chain<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::from_tokens(4))
        .await?;

    let certificate = client1.close_chain().await.unwrap().unwrap();
    assert_matches!(
        certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            &block.operations[..], &[Operation::System(SystemOperation::CloseChain)]
        ),
        "Unexpected certificate value",
    );
    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    assert!(client1.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Cannot use the chain for operations any more.
    let result = client1
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData::default(),
        )
        .await;
    assert!(
        matches!(
            &result,
            Err(ChainClientError::LocalNodeError(
                LocalNodeError::WorkerError(WorkerError::ChainError(err))
            )) if matches!(**err, ChainError::ClosedChain)
        ),
        "Unexpected result: {:?}",
        result,
    );

    // Incoming messages now get rejected.
    client2
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(1)),
            UserData::default(),
        )
        .await
        .unwrap()
        .unwrap();
    client2
        .subscribe_to_published_bytecodes(ChainId::root(1))
        .await
        .unwrap()
        .unwrap();
    client1.synchronize_from_validators().await.unwrap();
    let (certificates, _) = client1.process_inbox().await.unwrap();
    let block = certificates[0].value().block().unwrap();
    assert!(block.operations.is_empty());
    assert_eq!(block.incoming_messages.len(), 2);
    assert_matches!(
        block.incoming_messages[0],
        IncomingMessage {
            origin: Origin { sender, medium: Medium::Direct },
            action: MessageAction::Reject,
            event: Event {
                kind: MessageKind::Tracked,
                message: Message::System(SystemMessage::Credit { .. }),
                ..
            },
        } if sender == ChainId::root(2)
    );
    assert_matches!(
        block.incoming_messages[1],
        IncomingMessage {
            origin: Origin { sender, medium: Medium::Direct },
            action: MessageAction::Reject,
            event: Event {
                kind: MessageKind::Protected,
                message: Message::System(SystemMessage::Subscribe { .. }),
                ..
            },
        } if sender == ChainId::root(2)
    );

    // Since blocks are free of charge on closed chains, empty blocks are not allowed.
    assert_matches!(
        client1.execute_operations(vec![]).await,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        )) if matches!(*error, ChainError::ClosedChain)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_initiating_valid_transfer_too_many_faults(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer_too_many_faults(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(ScyllaDbStorageBuilder::default()).await
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
    assert_matches!(
        result,
        Err(ChainClientError::CommunicationError(
            CommunicationError::Trusted(crate::node::NodeError::ArithmeticError { .. })
        )),
        "unexpected result"
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
    run_test_bidirectional_transfer(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_bidirectional_transfer() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_bidirectional_transfer(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_bidirectional_transfer(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylla")]
#[test(tokio::test)]
async fn test_scylla_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_bidirectional_transfer<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
            .check_that_validators_have_certificate(client1.chain_id, BlockHeight::ZERO, 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Local balance is lagging.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Obtain the certificate but do not process the inbox yet.
    client2.synchronize_from_validators().await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        client2.query_system_application(SystemQuery).await.unwrap(),
        SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::from_tokens(0),
        }
    );

    // Process the inbox and send back some money.
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
    client1.synchronize_from_validators().await.unwrap();
    client1.process_inbox().await.unwrap();
    assert_eq!(client1.local_balance().await.unwrap(), Amount::ONE);
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
    run_test_receiving_unconfirmed_transfer(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_receiving_unconfirmed_transfer(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_receiving_unconfirmed_transfer<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
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
        Amount::from_millis(999)
    );
    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    // The receiver doesn't know about the transfer.
    client2.process_inbox().await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client2.query_balance().await.unwrap(),
        Amount::from_millis(1999)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MemoryStorageBuilder::default(),
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        ServiceStorageBuilder::new(&endpoint),
    )
    .await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        RocksDbStorageBuilder::default(),
    )
    .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        DynamoDbStorageBuilder::default(),
    )
    .await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        ScyllaDbStorageBuilder::default(),
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
            client1.next_block_height,
            CrossChainMessageDelivery::NonBlocking,
        )
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Sending money from client2 fails, as a consequence.
    // TODO(#1649): Make this code nicer.
    assert_matches!(client2
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id),
            UserData::default(),
        )
        .await,
        Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(error)))) if matches!(*error, ChainError::ExecutionError(ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }), ChainExecutionContext::Operation(_)))
    );
    // There is no pending block, since the proposal wasn't valid at the time.
    assert!(client2
        .process_pending_block()
        .await
        .unwrap()
        .unwrap()
        .is_none());
    // Retrying the whole command works after synchronization.
    client2.synchronize_from_validators().await.unwrap();
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
        client3.query_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_change_voting_rights() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_change_voting_rights(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_change_voting_rights() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_change_voting_rights(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(ScyllaDbStorageBuilder::default()).await
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
    let validators = builder.initial_committee.validators().clone();

    let committee = Committee::new(validators.clone(), ResourceControlPolicy::only_fuel());
    admin.stage_new_committee(committee).await.unwrap();
    admin.finalize_committee().await.unwrap();

    // Root chain 1 receives the notification about the new epoch.
    user.synchronize_from_validators().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));

    // Stop listening for new committees.
    let cert = user
        .unsubscribe_from_new_committees()
        .await
        .unwrap()
        .unwrap();
    admin.receive_certificate(cert).await.unwrap();
    admin.process_inbox().await.unwrap();

    // Create a new committee.
    let committee = Committee::new(validators, ResourceControlPolicy::only_fuel());
    admin.stage_new_committee(committee).await.unwrap();
    assert_eq!(admin.next_block_height, BlockHeight::from(4));
    assert!(admin.pending_block.is_none());
    assert!(admin.key_pair().await.is_ok());
    assert_eq!(admin.epoch().await.unwrap(), Epoch::from(2));

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
    assert_matches!(
        user.receive_certificate(cert).await,
        Err(ChainClientError::CommitteeSynchronizationError)
    );
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));
    user.synchronize_from_validators().await.unwrap();

    // User is a unsubscribed, so the migration message is not even in the inbox yet.
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));

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
    assert_matches!(
        admin.receive_certificate(cert).await,
        Err(ChainClientError::CommitteeDeprecationError)
    );
    // Transfer is blocked because the epoch #0 has been retired by admin.
    admin.synchronize_from_validators().await.unwrap();
    admin.process_inbox().await.unwrap();
    assert_eq!(admin.local_balance().await.unwrap(), Amount::ZERO);

    // Have the user receive the notification to migrate to epoch #2.
    user.synchronize_from_validators().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(2));

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
    admin.process_inbox().await.unwrap();
    // Transfer goes through and the previous one as well thanks to block chaining.
    assert_eq!(admin.local_balance().await.unwrap(), Amount::from_tokens(3));
    Ok(())
}

#[test(tokio::test)]
pub async fn test_memory_insufficient_balance() -> Result<(), anyhow::Error> {
    run_test_insufficient_balance(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
pub async fn test_service_insufficient_balance() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_insufficient_balance(ServiceStorageBuilder::new(&endpoint)).await
}

async fn run_test_insufficient_balance<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let obtained_error = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(4),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"I'm giving away all of my money!")),
        )
        .await;

    // TODO(#1649): Make this code nicer.
    assert_matches!(obtained_error,
        Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error)
        ))) if matches!(*error, ChainError::ExecutionError(
            ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }),
            ChainExecutionContext::Operation(0)
        ))
    );
    let obtained_error = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"I'm giving away all of my money!")),
        )
        .await;
    // TODO(#1649): Make this code nicer.
    assert_matches!(obtained_error,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        )) if matches!(*error,
            ChainError::ExecutionError(ExecutionError::SystemError(
                SystemExecutionError::InsufficientFundingForFees { .. }
            ), ChainExecutionContext::Block)
        )
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_request_leader_timeout() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_request_leader_timeout(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_request_leader_timeout(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_request_leader_timeout() -> Result<(), anyhow::Error> {
    run_test_request_leader_timeout(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_request_leader_timeout<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
        timeout_config: TimeoutConfig::default(),
    }
    .into();
    client.execute_operation(owner_change_op).await.unwrap();
    let manager = client.chain_info().await.unwrap().manager;

    // The round has not timed out yet, so validators will not sign a timeout certificate.
    // If the malicious and one honest validator happen to be much faster than the other
    // two honest validators, only those two samples may be returned. Otherwise we get
    // a trusted MissingVoteInValidatorResponse, because at least two returned that.
    let result = client.request_leader_timeout().await;
    if !matches!(
        result,
        Err(ChainClientError::CommunicationError(
            CommunicationError::Trusted(NodeError::MissingVoteInValidatorResponse)
        ))
    ) && !matches!(&result,
        Err(ChainClientError::CommunicationError(CommunicationError::Sample(samples)))
        if samples.iter().any(|(err, _)| matches!(err, NodeError::MissingVoteInValidatorResponse))
    ) {
        panic!("unexpected leader timeout result: {:?}", result);
    }

    clock.set(manager.round_timeout.unwrap());

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
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(1), expected_round, 3)
        .await;

    let round = loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(Owner::from(pub_key1)) {
            break manager.current_round;
        }
        clock.set(manager.round_timeout.unwrap());
        assert!(client.request_leader_timeout().await.is_ok());
    };
    let round_number = match round {
        Round::SingleLeader(round_number) => round_number,
        round => panic!("Unexpected round {:?}", round),
    };

    // The other owner is leader now. Trying to submit a block should return `WaitForTimeout`.
    let result = client
        .transfer(None, Amount::ONE, Recipient::root(2), UserData::default())
        .await
        .unwrap();
    let timeout = match result {
        ClientOutcome::Committed(_) => panic!("Committed a block where we aren't the leader."),
        ClientOutcome::WaitForTimeout(timeout) => timeout,
    };
    client.clear_pending_block();
    assert!(client.request_leader_timeout().await.is_err());
    clock.set(timeout.timestamp);
    client.request_leader_timeout().await.unwrap();
    let expected_round = Round::SingleLeader(round_number + 1);
    builder
        .check_that_validators_are_in_round(chain_id, BlockHeight::from(1), expected_round, 3)
        .await;

    loop {
        let manager = client.chain_info().await.unwrap().manager;
        if manager.leader == Some(Owner::from(pub_key0)) {
            break;
        }
        clock.set(manager.round_timeout.unwrap());
        assert!(client.request_leader_timeout().await.is_ok());
    }

    // Now we are the leader, and the transfer should succeed.
    let _certificate = client
        .transfer(None, Amount::ONE, Recipient::root(2), UserData::default())
        .await
        .unwrap()
        .unwrap();
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

#[test(tokio::test)]
async fn test_memory_propose_validated() -> Result<(), anyhow::Error> {
    run_test_propose_validated(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_propose_validated() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_propose_validated(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_propose_validated() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_propose_validated(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_propose_validated() -> Result<(), anyhow::Error> {
    run_test_propose_validated(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_propose_validated() -> Result<(), anyhow::Error> {
    run_test_propose_validated(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_propose_validated<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    // Configure a chain with two regular and no super owners.
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let chain_id = ChainId::from(description);
    let mut client0 = builder
        .add_initial_chain(description, Amount::from_tokens(10))
        .await?;
    let pub_key0 = client0.public_key().await.unwrap();
    let key_pair1 = KeyPair::generate();
    let pub_key1 = key_pair1.public();
    let owner_change_op = SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: 10,
        timeout_config: TimeoutConfig {
            fast_round_duration: Some(TimeDelta::from_secs(5)),
            ..TimeoutConfig::default()
        },
    }
    .into();
    client0.execute_operation(owner_change_op).await.unwrap();
    let mut client1 = builder
        .make_client(
            chain_id,
            key_pair1,
            client0.block_hash,
            BlockHeight::from(1),
        )
        .await?;

    // Client 0 tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, client 0 now has a pending block.
    builder
        .set_fault_type(2..3, FaultType::OfflineWithInfo)
        .await;
    let result = client0
        .burn(None, Amount::from_tokens(3), UserData::default())
        .await;
    assert!(result.is_err());

    // Client 1 thinks it is madness to burn 3 tokens! They want to only burn 2.
    // The validators are still faulty: They validate blocks but don't confirm them.
    builder.set_fault_type(2..3, FaultType::NoConfirm).await;
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert!(manager.requested_proposed.is_some());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    let result = client1
        .burn(None, Amount::from_tokens(2), UserData::default())
        .await;
    assert!(result.is_err());

    // Finally, the validators are online and honest again.
    builder.set_fault_type(1..3, FaultType::Honest).await;
    client0.synchronize_from_validators().await.unwrap();
    let manager = client0
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert_eq!(
        manager.highest_validated().unwrap().round,
        Round::MultiLeader(1)
    );

    // Client 0 now only tries to burn 1 token. Before that, they automatically finalize the
    // pending block, which burns 2 tokens, leaving 10 - 2 - 1 = 7.
    client0
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap();
    client0.synchronize_from_validators().await.unwrap();
    client0.process_inbox().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(7)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_propose_pending_block() -> Result<(), anyhow::Error> {
    run_test_propose_pending_block(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn test_service_propose_pending_block() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_propose_pending_block(ServiceStorageBuilder::new(&endpoint)).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_propose_pending_block() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_propose_pending_block(RocksDbStorageBuilder::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_propose_pending_block() -> Result<(), anyhow::Error> {
    run_test_propose_pending_block(DynamoDbStorageBuilder::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_propose_pending_block() -> Result<(), anyhow::Error> {
    run_test_propose_pending_block(ScyllaDbStorageBuilder::default()).await
}

async fn run_test_propose_pending_block<B>(storage_builder: B) -> Result<(), anyhow::Error>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let mut client = builder
        .add_initial_chain(description, Amount::from_tokens(10))
        .await?;

    // The client tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, the client now has a pending block.
    builder
        .set_fault_type(2..3, FaultType::OfflineWithInfo)
        .await;
    let result = client
        .burn(None, Amount::from_tokens(3), UserData::default())
        .await;
    assert!(result.is_err());

    // Now three validators are online again.
    builder.set_fault_type(2..3, FaultType::Honest).await;

    // The client tries to burn another token. Before that, they automatically finalize the
    // pending block, which burns 3 tokens, leaving 10 - 3 - 1 = 6.
    client
        .burn(None, Amount::ONE, UserData::default())
        .await
        .unwrap();
    client.synchronize_from_validators().await.unwrap();
    client.process_inbox().await.unwrap();
    assert_eq!(
        client.local_balance().await.unwrap(),
        Amount::from_tokens(6)
    );
    Ok(())
}

#[test(tokio::test)]
pub async fn test_memory_message_policy() -> Result<(), anyhow::Error> {
    run_test_message_policy(MemoryStorageBuilder::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
pub async fn test_service_message_policy() -> Result<(), anyhow::Error> {
    let endpoint = get_free_port().await.unwrap();
    run_test_message_policy(ServiceStorageBuilder::new(&endpoint)).await
}

async fn run_test_message_policy<B>(storage_builder: B) -> Result<(), anyhow::Error>
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
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let recipient = Recipient::chain(ChainId::root(2));
    let cert = sender
        .transfer(None, Amount::ONE, recipient, UserData(None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    receiver.message_policy = MessagePolicy::Ignore;
    receiver.receive_certificate(cert).await?;
    assert!(receiver.process_inbox().await?.0.is_empty());
    // The message was ignored.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert!(sender.process_inbox().await?.0.is_empty());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    receiver.message_policy = MessagePolicy::Reject;
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    sender
        .receive_certificate(certs.into_iter().next().unwrap())
        .await?;
    // The message bounces.
    assert_eq!(sender.process_inbox().await?.0.len(), 1);
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );

    Ok(())
}
