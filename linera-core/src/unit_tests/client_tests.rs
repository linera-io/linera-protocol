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
    data_types::{
        CertificateValue, ExecutedBlock, IncomingBundle, Medium, MessageBundle, Origin,
        PostedMessage,
    },
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::{Recipient, SystemOperation, UserData},
    ExecutionError, Message, MessageKind, Operation, ResourceControlPolicy, SystemExecutionError,
    SystemMessage, SystemQuery, SystemResponse,
};
use linera_storage::{MemoryStorage, Storage, TestClock};
use linera_views::views::ViewError;
use test_case::test_case;

#[cfg(feature = "dynamodb")]
use crate::test_utils::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
#[cfg(feature = "storage-service")]
use crate::test_utils::ServiceStorageBuilder;
use crate::{
    client::{ChainClient, ChainClientError, ClientOutcome, MessageAction, MessagePolicy},
    local_node::LocalNodeError,
    node::{
        CrossChainMessageDelivery,
        NodeError::{self, ClientIoError},
        ValidatorNode,
    },
    test_utils::{FaultType, MemoryStorageBuilder, NodeProvider, StorageBuilder, TestBuilder},
    updater::CommunicationError,
    worker::{Notification, Reason, WorkerError},
};

type MemoryChainClient =
    ChainClient<NodeProvider<MemoryStorage<TestClock>>, MemoryStorage<TestClock>>;

/// A test to ensure that our chain client listener remains `Send`.  This is a bit of a
/// hack, but requires that we not hold a `std::sync::Mutex` over `await` points, a
/// situation that is likely to lead to deadlock.  To further support this mode of
/// testing, `dashmap` references in [`crate::client`] have also been wrapped in a newtype
/// to make them non-`Send`.
#[test_log::test]
#[allow(dead_code)]
fn test_listener_is_send() {
    fn ensure_send(_: &impl Send) {}

    async fn check_listener(chain_client: MemoryChainClient) -> Result<(), ChainClientError> {
        let (listener, _abort_notifications, _notifications) = chain_client.listen().await?;
        ensure_send(&listener);
        Ok(())
    }

    // If it compiles, we're okay — no need to do anything at runtime.
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_initiating_valid_transfer_with_notifications<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    // Listen to the notifications on the sender chain.
    let mut notifications = sender.subscribe().await?;
    let (listener, _listen_handle, _) = sender.listen().await?;
    tokio::spawn(listener);
    {
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
        assert_eq!(sender.next_block_height(), BlockHeight::from(1));
        assert!(sender.pending_block().is_none());
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_claim_amount<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let owner = sender.identity().await?;
    let receiver = builder
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
    receiver
        .receive_certificate_and_update_validators(cert)
        .await?;
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

    receiver
        .receive_certificate_and_update_validators(cert)
        .await?;
    let cert = receiver.process_inbox().await?.0.pop().unwrap();
    {
        let messages = &cert.value().block().unwrap().incoming_bundles;
        // Both `Claim` messages were included in the block.
        assert_eq!(messages.len(), 2);
        // The first one was rejected.
        assert_eq!(messages[0].bundle.height, BlockHeight::from(2));
        assert_eq!(messages[0].action, MessageAction::Reject);
        // The second was accepted.
        assert_eq!(messages[1].bundle.height, BlockHeight::from(3));
        assert_eq!(messages[1].action, MessageAction::Accept);
    }

    sender
        .receive_certificate_and_update_validators(cert)
        .await?;
    sender.process_inbox().await?;
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_millis(2895)
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_rotate_key_pair<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner::from(new_key_pair.public());
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap().unwrap();
    assert_eq!(sender.next_block_height(), BlockHeight::from(1));
    assert!(sender.pending_block().is_none());
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfer_ownership<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;

    let new_key_pair = KeyPair::generate();
    let certificate = sender
        .transfer_ownership(new_key_pair.public())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height(), BlockHeight::from(1));
    assert!(sender.pending_block().is_none());
    assert_matches!(
        sender.key_pair().await.map(|kp| KeyPair::public(&kp)), // KeyPair isn't Debug; using PublicKey.
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_share_ownership<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let certificate = sender
        .share_ownership(new_key_pair.public(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height(), BlockHeight::from(1));
    assert!(sender.pending_block().is_none());
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
    assert_eq!(sender.next_block_height(), BlockHeight::from(2));
    // Make a client to try the new key.
    let client = builder
        .make_client(
            sender.chain_id,
            new_key_pair,
            sender.block_hash(),
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
    builder.set_fault_type([0, 1], FaultType::Offline).await;
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
    builder.set_fault_type([0, 1], FaultType::Honest).await;
    builder.set_fault_type([2, 3], FaultType::Offline).await;
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
    builder
        .set_fault_type([0, 1, 2, 3], FaultType::Honest)
        .await;
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_then_close_it<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    // Open the new chain.
    let (message_id, certificate) = sender
        .open_chain(
            ChainOwnership::single(new_key_pair.public()),
            ApplicationPermissions::default(),
            Amount::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sender.next_block_height(), BlockHeight::from(1));
    assert!(sender.pending_block().is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let new_id = ChainId::child(message_id);
    let client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client
        .receive_certificate_and_update_validators(certificate)
        .await
        .unwrap();
    assert_eq!(client.query_balance().await.unwrap(), Amount::ZERO);
    client.close_chain().await.unwrap();
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfer_then_open_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let parent = builder
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
        .open_chain(
            ChainOwnership::single(new_key_pair.public()),
            ApplicationPermissions::default(),
            Amount::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    let new_id2 = ChainId::child(open_chain_message_id);
    assert_eq!(new_id, new_id2);
    assert_eq!(sender.next_block_height(), BlockHeight::from(1));
    assert_eq!(parent.next_block_height(), BlockHeight::from(1));
    assert!(sender.pending_block().is_none());
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
    let client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client
        .receive_certificate_and_update_validators(certificate)
        .await
        .unwrap();
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
    client
        .receive_certificate_and_update_validators(certificate2)
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
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_must_be_first<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let sender = builder
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
        .open_chain(
            ChainOwnership::single(new_key_pair.public()),
            ApplicationPermissions::default(),
            Amount::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    let new_id2 = ChainId::child(open_chain_message_id);
    assert_eq!(new_id, new_id2);
    assert_eq!(sender.next_block_height(), BlockHeight::from(2));
    assert!(sender.pending_block().is_none());
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
    let client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    client
        .receive_certificate_and_update_validators(certificate)
        .await
        .unwrap();
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
        )) if matches!(&*error, ChainError::CannotSkipMessage {
            bundle: MessageBundle { messages, .. }, ..
        } if matches!(messages[..], [PostedMessage {
            message: Message::System(SystemMessage::Credit { .. }), ..
        }]))
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_then_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    // Open the new chain. We are both regular and super owner.
    let ownership = ChainOwnership::single(new_key_pair.public())
        .with_regular_owner(new_key_pair.public(), 100);
    let (message_id, creation_certificate) = sender
        .open_chain(ownership, ApplicationPermissions::default(), Amount::ZERO)
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
    assert_eq!(sender.next_block_height(), BlockHeight::from(2));
    assert!(sender.pending_block().is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::ZERO)
        .await?;
    // Must process the creation certificate before using the new chain.
    client
        .receive_certificate_and_update_validators(creation_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Amount::ZERO);
    client
        .receive_certificate_and_update_validators(transfer_certificate)
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_close_chain<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let client2 = builder
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
    assert_eq!(client1.next_block_height(), BlockHeight::from(1));
    assert!(client1.pending_block().is_none());
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
    assert_eq!(block.incoming_bundles.len(), 2);
    assert_matches!(
        &block.incoming_bundles[0],
        IncomingBundle {
            origin: Origin { sender, medium: Medium::Direct },
            action: MessageAction::Reject,
            bundle: MessageBundle {
                messages,
                ..
            },
        } if *sender == ChainId::root(2) && matches!(messages[..],
            [PostedMessage {
                message: Message::System(SystemMessage::Credit { .. }),
                kind: MessageKind::Tracked,
                ..
            }]
        )
    );
    assert_matches!(
        &block.incoming_bundles[1],
        IncomingBundle {
            origin: Origin { sender, medium: Medium::Direct },
            action: MessageAction::Reject,
            bundle: MessageBundle {
                messages,
                ..
            },
        } if *sender == ChainId::root(2) && matches!(messages[..],
            [PostedMessage {
                message: Message::System(SystemMessage::Subscribe { .. }),
                kind: MessageKind::Protected,
                ..
            }]
        )
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_initiating_valid_transfer_too_many_faults<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 2).await?;
    let sender = builder
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
    assert_eq!(sender.next_block_height(), BlockHeight::ZERO);
    assert!(sender.pending_block().is_some());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_bidirectional_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let client2 = builder
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

    assert_eq!(client1.next_block_height(), BlockHeight::from(1));
    assert!(client1.pending_block().is_none());
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
    assert_eq!(client2.next_block_height(), BlockHeight::ZERO);
    client2
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(client1.chain_id),
            UserData::default(),
        )
        .await
        .unwrap();
    assert_eq!(client2.next_block_height(), BlockHeight::from(1));
    assert!(client2.pending_block().is_none());
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_receiving_unconfirmed_transfer<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let client2 = builder
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
    assert_eq!(client1.next_block_height(), BlockHeight::from(1));
    assert!(client1.pending_block().is_none());
    // The receiver doesn't know about the transfer.
    client2.process_inbox().await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client2
        .receive_certificate_and_update_validators(certificate)
        .await
        .unwrap();
    assert_eq!(
        client2.query_balance().await.unwrap(),
        Amount::from_millis(1999)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_receiving_unconfirmed_transfer_with_lagging_sender_balances<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    let client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ZERO)
        .await?;
    let client3 = builder
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
            client1.next_block_height(),
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
    assert_eq!(client1.next_block_height(), BlockHeight::from(2));
    assert!(client1.pending_block().is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    assert_eq!(client2.next_block_height(), BlockHeight::from(1));
    assert!(client2.pending_block().is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Let the receiver confirm in last resort.
    client3
        .receive_certificate_and_update_validators(certificate)
        .await
        .unwrap();
    assert_eq!(
        client3.query_balance().await.unwrap(),
        Amount::from_tokens(2)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_change_voting_rights<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let admin = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    let user = builder
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
    admin
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
    admin.process_inbox().await.unwrap();

    // Create a new committee.
    let committee = Committee::new(validators, ResourceControlPolicy::only_fuel());
    admin.stage_new_committee(committee).await.unwrap();
    assert_eq!(admin.next_block_height(), BlockHeight::from(4));
    assert!(admin.pending_block().is_none());
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
        user.receive_certificate_and_update_validators(cert).await,
        Err(ChainClientError::CommitteeSynchronizationError)
    );
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));
    user.synchronize_from_validators().await.unwrap();

    // User is a unsubscribed, so the migration message is not even in the inbox yet.
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));

    // Now subscribe explicitly to migrations.
    let cert = user.subscribe_to_new_committees().await.unwrap().unwrap();
    admin
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
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
        admin.receive_certificate_and_update_validators(cert).await,
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
    admin
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
    admin.process_inbox().await.unwrap();
    // Transfer goes through and the previous one as well thanks to block chaining.
    assert_eq!(admin.local_balance().await.unwrap(), Amount::from_tokens(3));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_insufficient_balance<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::fuel_and_block());
    let sender = builder
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_finalize_locked_block_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let description1 = ChainDescription::Root(1);
    let chain_id1 = ChainId::from(description1);
    let client1_a = builder
        .add_initial_chain(description1, Amount::ZERO)
        .await?;
    let pub_key1_a = client1_a.public_key().await.unwrap();
    let key_pair1_b = KeyPair::generate();
    let pub_key1_b = key_pair1_b.public();
    let owner_change_op1 = SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key1_a, 50), (pub_key1_b, 50)],
        multi_leader_rounds: 10,
        timeout_config: TimeoutConfig::default(),
    }
    .into();
    client1_a.execute_operation(owner_change_op1).await.unwrap();
    let client1_b = builder
        .make_client(
            chain_id1,
            key_pair1_b,
            client1_a.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    let description2 = ChainDescription::Root(2);
    let chain_id2 = ChainId::from(description2);
    let mut client2_a = builder
        .add_initial_chain(description2, Amount::from_tokens(10))
        .await?;
    let pub_key2_a = client2_a.public_key().await.unwrap();
    let key_pair2_b = KeyPair::generate();
    let pub_key2_b = key_pair2_b.public();
    let owner_change_op2 = SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key2_a, 50), (pub_key2_b, 50)],
        multi_leader_rounds: 10,
        timeout_config: TimeoutConfig::default(),
    }
    .into();
    client2_a.execute_operation(owner_change_op2).await.unwrap();
    let client2_b = builder
        .make_client(
            chain_id2,
            key_pair2_b,
            client2_a.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    let blob0 = Blob::test_data_blob("blob0");
    let blob0_id = blob0.id();

    // Try to read a blob without publishing it first, should fail
    let result = client1_a
        .execute_operation(SystemOperation::ReadBlob { blob_id: blob0_id }.into())
        .await;
    assert_matches!(
        result,
        Err(ChainClientError::BlobNotFound(not_found_blob_id)) if not_found_blob_id == blob0_id
    );

    // Take one validator down
    builder.set_fault_type([2], FaultType::Offline).await;

    // Publish blob on chain 1
    let publish_certificate = client1_a.publish_blob(blob0).await.unwrap().unwrap();
    assert!(publish_certificate
        .value()
        .executed_block()
        .unwrap()
        .requires_blob(&blob0_id));

    // Validators goes back up
    builder.set_fault_type([2], FaultType::Honest).await;
    // But another one goes down
    builder.set_fault_type([3], FaultType::Offline).await;

    // Try to read the blob. This is a different client but on the same chain, so when we synchronize this with the validators
    // before executing the block, we'll actually download and cache locally the blobs that were published by `client_a`.
    // So this will succeed.
    let certificate = client1_b
        .execute_operation(SystemOperation::ReadBlob { blob_id: blob0_id }.into())
        .await?
        .unwrap();
    assert_eq!(certificate.round, Round::MultiLeader(0));
    assert!(certificate
        .value()
        .executed_block()
        .unwrap()
        .requires_blob(&blob0_id));

    builder
        .set_fault_type([0, 1, 2], FaultType::DontSendConfirmVote)
        .await;

    client2_a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::test_data_blob("blob1");
    let blob1_id = blob1.id();

    client2_a.add_pending_blobs(&[blob1]).await;
    let blob_0_1_operations = vec![
        Operation::System(SystemOperation::ReadBlob { blob_id: blob0_id }),
        Operation::System(SystemOperation::PublishBlob { blob_id: blob1_id }),
    ];
    let b0_result = client2_a
        .execute_operations(blob_0_1_operations.clone())
        .await;

    assert!(b0_result.is_err());
    assert!(client2_a.pending_block().is_some());

    for i in 0..=2 {
        let validator_manager = builder
            .node(i)
            .chain_info_with_manager_values(chain_id2)
            .await
            .unwrap()
            .manager;
        assert_eq!(
            validator_manager
                .requested_locked
                .unwrap()
                .value()
                .block()
                .unwrap()
                .operations,
            blob_0_1_operations
        );
    }

    builder.set_fault_type([2], FaultType::Offline).await;
    builder.set_fault_type([0, 1, 3], FaultType::Honest).await;

    client2_b.synchronize_from_validators().await.unwrap();
    let bt_certificate = client2_b
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap()
        .unwrap();

    let hashed_certificate_values = client2_b
        .read_hashed_certificate_values_downward(bt_certificate.value.hash(), 2)
        .await
        .unwrap();

    // Latest block should be the burn
    assert!(hashed_certificate_values[0]
        .inner()
        .block()
        .unwrap()
        .operations
        .contains(&Operation::System(SystemOperation::Transfer {
            owner: None,
            recipient: Recipient::Burn,
            amount: Amount::from_tokens(1),
            user_data: UserData::default(),
        })));

    // Block before that should be b0
    assert_eq!(
        hashed_certificate_values[1]
            .inner()
            .block()
            .unwrap()
            .operations,
        blob_0_1_operations,
    );

    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_existing_proposal_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let description1 = ChainDescription::Root(1);
    let description2 = ChainDescription::Root(2);
    let chain_id2 = ChainId::from(description2);
    let client1 = builder
        .add_initial_chain(description1, Amount::ZERO)
        .await?;
    let mut client2_a = builder
        .add_initial_chain(description2, Amount::from_tokens(10))
        .await?;
    let pub_key2_a = client2_a.public_key().await.unwrap();
    let key_pair2_b = KeyPair::generate();
    let pub_key2_b = key_pair2_b.public();
    let owner_change_op = Operation::System(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key2_a, 50), (pub_key2_b, 50)],
        multi_leader_rounds: 10,
        timeout_config: TimeoutConfig::default(),
    });
    client2_a
        .execute_operation(owner_change_op.clone())
        .await
        .unwrap();
    let client2_b = builder
        .make_client(
            chain_id2,
            key_pair2_b,
            client2_a.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    // Take one validator down
    builder.set_fault_type([3], FaultType::Offline).await;

    let blob0 = Blob::test_data_blob("blob0");
    let blob0_id = blob0.id();

    // Publish blob on chain 1
    let publish_certificate = client1.publish_blob(blob0).await.unwrap().unwrap();
    assert!(publish_certificate
        .value()
        .executed_block()
        .unwrap()
        .requires_blob(&blob0_id));

    builder
        .set_fault_type([0, 1, 2], FaultType::DontProcessValidated)
        .await;

    client2_a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::test_data_blob("blob1");
    let blob1_id = blob1.id();

    client2_a.add_pending_blobs(&[blob1]).await;
    let blob_0_1_operations = vec![
        Operation::System(SystemOperation::ReadBlob { blob_id: blob0_id }),
        Operation::System(SystemOperation::PublishBlob { blob_id: blob1_id }),
    ];
    let b0_result = client2_a
        .execute_operations(blob_0_1_operations.clone())
        .await;

    assert!(b0_result.is_err());
    assert!(client2_a.pending_block().is_some());

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
                .operations,
            blob_0_1_operations,
        );
        assert!(validator_manager.requested_locked.is_none());
    }

    builder.set_fault_type([2], FaultType::Offline).await;
    builder.set_fault_type([0, 1, 3], FaultType::Honest).await;

    let bt_certificate = client2_b
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap()
        .unwrap();

    let hashed_certificate_values = client2_b
        .read_hashed_certificate_values_downward(bt_certificate.value.hash(), 2)
        .await
        .unwrap();

    // Latest block should be the burn
    assert!(hashed_certificate_values[0]
        .inner()
        .block()
        .unwrap()
        .operations
        .contains(&Operation::System(SystemOperation::Transfer {
            owner: None,
            recipient: Recipient::Burn,
            amount: Amount::from_tokens(1),
            user_data: UserData::default(),
        })));

    // Previous should be the `ChangeOwnership` operation, as the blob operations shouldn't be executed here.
    assert!(hashed_certificate_values[1]
        .inner()
        .block()
        .unwrap()
        .operations
        .contains(&owner_change_op));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_re_propose_locked_block_with_blobs<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let description1 = ChainDescription::Root(1);
    let description2 = ChainDescription::Root(2);
    let description3 = ChainDescription::Root(3);
    let chain_id3 = ChainId::from(description3);
    let client1 = builder
        .add_initial_chain(description1, Amount::ZERO)
        .await?;
    let client2 = builder
        .add_initial_chain(description2, Amount::ZERO)
        .await?;
    let mut client3_a = builder
        .add_initial_chain(description3, Amount::from_tokens(10))
        .await?;
    let pub_key3_a = client3_a.public_key().await.unwrap();
    let key_pair3_b = KeyPair::generate();
    let pub_key3_b = key_pair3_b.public();
    let key_pair3_c = KeyPair::generate();
    let pub_key3_c = key_pair3_c.public();
    let owner_change_op = Operation::System(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key3_a, 50), (pub_key3_b, 50), (pub_key3_c, 50)],
        multi_leader_rounds: 10,
        timeout_config: TimeoutConfig::default(),
    });
    client3_a
        .execute_operation(owner_change_op.clone())
        .await
        .unwrap();
    let mut client3_b = builder
        .make_client(
            chain_id3,
            key_pair3_b,
            client3_a.block_hash(),
            BlockHeight::from(1),
        )
        .await?;
    let client3_c = builder
        .make_client(
            chain_id3,
            key_pair3_c,
            client3_a.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    // Take one validator down
    builder.set_fault_type([3], FaultType::Offline).await;

    let blob0 = Blob::test_data_blob("blob0");
    let blob0_id = blob0.id();

    client1.synchronize_from_validators().await.unwrap();
    // Publish blob0 on chain 1
    let publish_certificate0 = client1.publish_blob(blob0).await.unwrap().unwrap();
    assert!(publish_certificate0
        .value()
        .executed_block()
        .unwrap()
        .requires_blob(&blob0_id));

    let blob2 = Blob::test_data_blob("blob2");
    let blob2_id = blob2.id();

    client2.synchronize_from_validators().await.unwrap();
    // Publish blob2 on chain 2
    let publish_certificate2 = client2.publish_blob(blob2).await.unwrap().unwrap();
    assert!(publish_certificate2
        .value()
        .executed_block()
        .unwrap()
        .requires_blob(&blob2_id));

    builder
        .set_fault_type([0, 1], FaultType::DontProcessValidated)
        .await;
    builder
        .set_fault_type([2], FaultType::DontSendConfirmVote)
        .await;

    client3_a.synchronize_from_validators().await.unwrap();
    let blob1 = Blob::test_data_blob("blob1");
    let blob1_id = blob1.id();

    client3_a.add_pending_blobs(&[blob1]).await;
    let blob_0_1_operations = vec![
        Operation::System(SystemOperation::ReadBlob { blob_id: blob0_id }),
        Operation::System(SystemOperation::PublishBlob { blob_id: blob1_id }),
    ];
    let b0_result = client3_a
        .execute_operations(blob_0_1_operations.clone())
        .await;

    assert!(b0_result.is_err());
    assert!(client3_a.pending_block().is_some());

    let manager = client3_a
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    // Validator 2 may or may not have processed the validated block before the update was
    // canceled due to the errors from the faulty validators. Submit it again to make sure
    // it's there, so that client 2 can download and re-propose it later.
    let validated_block_certificate = *manager.requested_locked.unwrap();
    let resubmission_result = builder
        .node(2)
        .handle_certificate(
            validated_block_certificate,
            Vec::new(),
            Vec::new(),
            CrossChainMessageDelivery::Blocking,
        )
        .await;
    assert!(resubmission_result.is_err());

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
                .operations,
            blob_0_1_operations,
        );

        if i == 2 {
            assert_eq!(
                validator_manager
                    .requested_locked
                    .unwrap()
                    .value()
                    .block()
                    .unwrap()
                    .operations,
                blob_0_1_operations,
            );
        } else {
            assert!(validator_manager.requested_locked.is_none());
        }
    }

    builder.set_fault_type([2], FaultType::Offline).await;
    builder
        .set_fault_type([3], FaultType::DontSendConfirmVote)
        .await;

    client3_b.synchronize_from_validators().await.unwrap();
    let blob3 = Blob::test_data_blob("blob3");
    let blob3_id = blob3.id();

    client3_b.add_pending_blobs(&[blob3]).await;
    let blob_2_3_operations = vec![
        Operation::System(SystemOperation::ReadBlob { blob_id: blob2_id }),
        Operation::System(SystemOperation::PublishBlob { blob_id: blob3_id }),
    ];
    let b1_result = client3_b
        .execute_operations(blob_2_3_operations.clone())
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
    let validated_block_certificate = *manager.requested_locked.unwrap();
    let resubmission_result = builder
        .node(3)
        .handle_certificate(
            validated_block_certificate,
            Vec::new(),
            Vec::new(),
            CrossChainMessageDelivery::Blocking,
        )
        .await;
    assert!(resubmission_result.is_err());

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
            .operations,
        blob_2_3_operations,
    );
    assert_eq!(
        validator_manager
            .requested_locked
            .unwrap()
            .value()
            .block()
            .unwrap()
            .operations,
        blob_2_3_operations,
    );

    builder.set_fault_type([1], FaultType::Offline).await;
    builder.set_fault_type([0, 2, 3], FaultType::Honest).await;

    client3_c.synchronize_from_validators().await.unwrap();
    let bt_certificate = client3_c
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap()
        .unwrap();

    let hashed_certificate_values = client3_c
        .read_hashed_certificate_values_downward(bt_certificate.value.hash(), 3)
        .await
        .unwrap();

    // Latest block should be the burn
    assert!(hashed_certificate_values[0]
        .inner()
        .block()
        .unwrap()
        .operations
        .contains(&Operation::System(SystemOperation::Transfer {
            owner: None,
            recipient: Recipient::Burn,
            amount: Amount::from_tokens(1),
            user_data: UserData::default(),
        })));

    // Block before that should be b1
    assert_eq!(
        hashed_certificate_values[1]
            .inner()
            .block()
            .unwrap()
            .operations,
        blob_2_3_operations,
    );

    // Previous should be the `ChangeOwnership` operation
    assert!(hashed_certificate_values[2]
        .inner()
        .block()
        .unwrap()
        .operations
        .contains(&owner_change_op));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_request_leader_timeout<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let chain_id = ChainId::from(description);
    let client = builder
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
        CertificateValue::Timeout {
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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_finalize_validated<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    // Configure a chain with two regular and no super owners.
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let chain_id = ChainId::from(description);
    let client0 = builder
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
    let client1 = builder
        .make_client(
            chain_id,
            key_pair1,
            client0.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    // Client 0 tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, client 0 now has a pending block.
    builder
        .set_fault_type([2], FaultType::OfflineWithInfo)
        .await;
    let result = client0
        .burn(None, Amount::from_tokens(3), UserData::default())
        .await;
    assert!(result.is_err());

    // Client 1 thinks it is madness to burn 3 tokens! They want to publish a blob instead.
    // The validators are still faulty: They validate blocks but don't confirm them.
    builder
        .set_fault_type([2], FaultType::DontSendConfirmVote)
        .await;
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert!(manager.requested_proposed.is_some());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    let result = client1.publish_blob(Blob::test_data_blob("blob1")).await;
    assert!(result.is_err());
    assert!(client1.pending_block().is_some());
    assert!(!client1.pending_blobs().is_empty());

    // Finally, the validators are online and honest again.
    builder.set_fault_type([1, 2], FaultType::Honest).await;
    client0.synchronize_from_validators().await.unwrap();
    let manager = client0
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert_eq!(
        manager.requested_locked.unwrap().round,
        Round::MultiLeader(1)
    );
    assert!(client0.pending_block().is_some());

    // Client 0 now only tries to burn 1 token. Before that, they automatically finalize the
    // pending block, which publishes the blob, leaving 10 - 1 = 9.
    client0
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap();
    client0.synchronize_from_validators().await.unwrap();
    client0.process_inbox().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(9)
    );
    assert!(client0.pending_block().is_none());

    // Burn another token so Client 1 sees that the blob is already published
    client1
        .burn(None, Amount::from_tokens(1), UserData::default())
        .await
        .unwrap();
    client1.synchronize_from_validators().await.unwrap();
    client1.process_inbox().await.unwrap();
    assert_eq!(
        client1.local_balance().await.unwrap(),
        Amount::from_tokens(8)
    );
    assert!(client1.pending_block().is_none());
    assert!(client1.pending_blobs().is_empty());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_propose_pending_block<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1).await?;
    let description = ChainDescription::Root(1);
    let client = builder
        .add_initial_chain(description, Amount::from_tokens(10))
        .await?;

    // The client tries to burn 3 tokens. Two validators are offline, so nothing will get
    // validated or confirmed. However, the client now has a pending block.
    builder
        .set_fault_type([2], FaultType::OfflineWithInfo)
        .await;
    let result = client
        .burn(None, Amount::from_tokens(3), UserData::default())
        .await;
    assert!(result.is_err());

    // Now three validators are online again.
    builder.set_fault_type([2], FaultType::Honest).await;

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

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_re_propose_validated<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    // Configure a chain with two regular and no super owners.
    let mut builder = TestBuilder::new(storage_builder, 4, 0).await?;
    let description = ChainDescription::Root(1);
    let chain_id = ChainId::from(description);
    let client0 = builder
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
    let client1 = builder
        .make_client(
            chain_id,
            key_pair1,
            client0.block_hash(),
            BlockHeight::from(1),
        )
        .await?;

    // Client 0 tries to burn 3 tokens. Three validators are faulty: 1 and 2 will validate the
    // block but not receive it for confirmation. Validator 3 is offline.
    builder
        .set_fault_type([1, 2], FaultType::DontProcessValidated)
        .await;
    builder.set_fault_type([3], FaultType::Offline).await;
    let result = client0
        .burn(None, Amount::from_tokens(3), UserData::default())
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
    let validated_block_certificate = *manager.requested_locked.unwrap();
    builder
        .node(0)
        .handle_certificate(
            validated_block_certificate,
            Vec::new(),
            Vec::new(),
            CrossChainMessageDelivery::Blocking,
        )
        .await
        .unwrap();

    // Client 1 wants to burn 2 tokens. They learn about the proposal in round 0, but now the
    // validator 0 is offline, so they don't learn about the validated block and make their own
    // proposal in round 1.
    builder.set_fault_type([0], FaultType::Offline).await;
    builder
        .set_fault_type([3], FaultType::OfflineWithInfo)
        .await;
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert!(manager.requested_proposed.is_some());
    assert!(manager.requested_locked.is_none());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    let result = client1
        .burn(None, Amount::from_tokens(2), UserData::default())
        .await;
    assert!(result.is_err());

    // Finally, three validators are online and honest again. Client 1 realizes there has been a
    // validated block in round 0, and re-proposes it when it tries to burn 4 tokens.
    builder.set_fault_type([0, 1, 2], FaultType::Honest).await;
    builder.set_fault_type([3], FaultType::Offline).await;
    client1.synchronize_from_validators().await.unwrap();
    let manager = client1
        .chain_info_with_manager_values()
        .await
        .unwrap()
        .manager;
    assert_eq!(
        manager.requested_locked.unwrap().round,
        Round::MultiLeader(0)
    );
    assert_eq!(manager.current_round, Round::MultiLeader(1));
    assert!(client1.pending_block().is_some());
    client1
        .burn(None, Amount::from_tokens(4), UserData::default())
        .await
        .unwrap();

    // Burning 3 and 4 tokens got finalized; the pending 2 tokens got skipped.
    client0.synchronize_from_validators().await.unwrap();
    assert_eq!(
        client0.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "storage-service", test_case(ServiceStorageBuilder::new().await; "storage_service"))]
#[test_log::test(tokio::test)]
async fn test_message_policy<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::StoreError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::only_fuel());
    let sender = builder
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

    receiver.options_mut().message_policy = MessagePolicy::Ignore;
    receiver
        .receive_certificate_and_update_validators(cert)
        .await?;
    assert!(receiver.process_inbox().await?.0.is_empty());
    // The message was ignored.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);
    assert!(sender.process_inbox().await?.0.is_empty());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(3)
    );

    receiver.options_mut().message_policy = MessagePolicy::Reject;
    let certs = receiver.process_inbox().await?.0;
    assert_eq!(certs.len(), 1);
    sender
        .receive_certificate_and_update_validators(certs.into_iter().next().unwrap())
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
