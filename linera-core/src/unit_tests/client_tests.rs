// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_client_tests.rs"]
mod wasm;

use crate::client::{
    client_test_utils::{FaultType, MakeMemoryStoreClient, StoreBuilder, TestBuilder},
    CommunicateAction,
};
use linera_base::{
    crypto::*,
    data_types::*,
    identifiers::{ChainDescription, ChainId, MessageId, Owner},
};
use linera_chain::data_types::{CertificateValue, ExecutedBlock};
use linera_execution::{
    committee::{Committee, Epoch},
    pricing::Pricing,
    system::{Account, Recipient, SystemOperation, UserData},
    ChainOwnership, Operation, SystemQuery, SystemResponse,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use test_log::test;

#[cfg(feature = "rocksdb")]
use crate::client::client_test_utils::{MakeRocksDbStoreClient, ROCKS_DB_SEMAPHORE};

#[cfg(feature = "aws")]
use crate::client::client_test_utils::MakeDynamoDbStoreClient;

#[cfg(feature = "scylladb")]
use crate::client::client_test_utils::MakeScyllaDbStoreClient;

#[test(tokio::test)]
pub async fn test_memory_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_initiating_valid_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let certificate = sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"I paid 0.001 to pay you these 3!")),
        )
        .await
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
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_claim_amount() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_claim_amount(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_claim_amount() -> Result<(), anyhow::Error> {
    run_test_claim_amount(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_claim_amount<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::only_fuel());
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
        .unwrap();
    assert_eq!(sender.local_balance().await.unwrap(), Amount::ONE);
    receiver.receive_certificate(cert).await?;
    receiver.process_inbox().await?;
    // The received amount is not in the unprotected balance.
    assert_eq!(receiver.local_balance().await.unwrap(), Amount::ZERO);

    // First attempt that should be skipped.
    sender
        .claim(
            owner,
            ChainId::root(2),
            Recipient::Account(Account::chain(ChainId::root(1))),
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
            Recipient::Account(Account::chain(ChainId::root(1))),
            Amount::from_tokens(2),
            UserData(None),
        )
        .await
        .unwrap();

    receiver.receive_certificate(cert).await?;
    let cert = receiver.process_inbox().await?.pop().unwrap();

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
    run_test_rotate_key_pair(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_rotate_key_pair(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_rotate_key_pair<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner::from(new_key_pair.public());
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.identity().await.unwrap(), new_owner);
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
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
    run_test_transfer_ownership(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_ownership(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_transfer_ownership<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;

    let new_key_pair = KeyPair::generate();
    let certificate = sender
        .transfer_ownership(new_key_pair.public())
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
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
    assert!(sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData::default()
        )
        .await
        .is_err());
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_share_ownership() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_share_ownership(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_share_ownership<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 0).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let certificate = sender.share_ownership(new_key_pair.public()).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
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
    assert!(client.local_balance().await.is_err());
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
    assert!(client
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .is_err());
    builder.set_fault_type(..2, FaultType::Honest).await;
    builder.set_fault_type(2.., FaultType::Offline).await;
    assert!(sender
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(3)),
            UserData::default(),
        )
        .await
        .is_err());

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
    run_test_open_chain_then_close_it(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_close_it(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_open_chain_then_close_it<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
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
        .open_chain(ChainOwnership::single(new_key_pair.public()))
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let new_id = ChainId::child(message_id);
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
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
    run_test_transfer_then_open_chain(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_transfer_then_open_chain(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_transfer_then_open_chain<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
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
        .open_chain(ChainOwnership::single(new_key_pair.public()))
        .await
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
    assert!(matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            block.operations[open_chain_message_id.index as usize],
            Operation::System(SystemOperation::OpenChain { .. }),
        ),
    ));
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await?;
    client.receive_certificate(certificate).await.unwrap();
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
async fn test_memory_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_open_chain_then_transfer(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_open_chain_then_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ZERO)
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    // Open the new chain.
    let (message_id, creation_certificate) = sender
        .open_chain(ChainOwnership::single(new_key_pair.public()))
        .await
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
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
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
    run_test_close_chain(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_close_chain() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_close_chain(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_close_chain<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::all_categories());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    let certificate = sender.close_chain().await.unwrap();
    assert!(matches!(
        &certificate.value(),
        CertificateValue::ConfirmedBlock { executed_block: ExecutedBlock { block, .. }, .. } if matches!(
            &block.operations[..], &[Operation::System(SystemOperation::CloseChain)]
        ),
    ));
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Cannot use the chain any more.
    assert!(sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData::default()
        )
        .await
        .is_err());
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_initiating_valid_transfer_too_many_faults(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_initiating_valid_transfer_too_many_faults<B>(
    store_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 2).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(4))
        .await?;
    assert!(sender
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .is_err());
    assert_eq!(sender.next_block_height, BlockHeight::from(0));
    assert!(sender.pending_block.is_some());
    assert_eq!(
        sender.local_balance().await.unwrap(),
        Amount::from_tokens(4)
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_bidirectional_transfer(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylla")]
#[test(tokio::test)]
async fn test_scylla_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_bidirectional_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
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
            .check_that_validators_have_certificate(client1.chain_id, BlockHeight::from(0), 3)
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
    assert_eq!(client2.next_block_height, BlockHeight::from(0));
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
    run_test_receiving_unconfirmed_transfer(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_receiving_unconfirmed_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::fuel_and_certificate());
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
        MakeMemoryStoreClient::default(),
    )
    .await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeRocksDbStoreClient::default(),
    )
    .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeDynamoDbStoreClient::default(),
    )
    .await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeScyllaDbStoreClient::default(),
    )
    .await
}

async fn run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances<B>(
    store_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
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
            CommunicateAction::AdvanceToNextBlockHeight(client1.next_block_height),
        )
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Amount::ZERO);
    // Sending money from client2 fails, as a consequence.
    assert!(client2
        .transfer_to_account_unsafe_unconfirmed(
            None,
            Amount::from_tokens(2),
            Account::chain(client3.chain_id),
            UserData::default(),
        )
        .await
        .is_err());
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
    run_test_change_voting_rights(MakeMemoryStoreClient::default()).await
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_change_voting_rights() -> Result<(), anyhow::Error> {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    run_test_change_voting_rights(MakeRocksDbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeDynamoDbStoreClient::default()).await
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeScyllaDbStoreClient::default()).await
}

async fn run_test_change_voting_rights<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut admin = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    let mut user = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ZERO)
        .await?;

    // Create a new committee.
    let validators = builder.initial_committee.validators().clone();
    let committee = Committee::new(validators, Pricing::only_fuel());
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
        .unwrap();
    admin
        .transfer_to_account(
            None,
            Amount::ONE,
            Account::chain(ChainId::root(1)),
            UserData(None),
        )
        .await
        .unwrap();

    // User is still at the initial epoch, but we can receive transfers from future
    // epochs AFTER synchronizing the client with the admin chain.
    assert!(user.receive_certificate(cert).await.is_err());
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(0));
    assert_eq!(
        user.synchronize_from_validators().await.unwrap(),
        Amount::from_tokens(3)
    );

    // User is a genesis chain so the migration message is not even in the inbox yet.
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(0));

    // Now subscribe explicitly to migrations.
    let cert = user.subscribe_to_new_committees().await.unwrap();
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
        .unwrap();
    assert!(admin.receive_certificate(cert).await.is_err());
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
    run_test_insufficient_balance(MakeMemoryStoreClient::default()).await
}

async fn run_test_insufficient_balance<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1)
        .await?
        .with_pricing(Pricing::fuel_and_certificate());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::from_tokens(3))
        .await?;
    assert!(sender
        .transfer_to_account(
            None,
            Amount::from_tokens(3),
            Account::chain(ChainId::root(2)),
            UserData(Some(*b"I'm giving away all of my money!")),
        )
        .await
        .is_err());
    Ok(())
}
