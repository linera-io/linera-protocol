// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::{Amount, Blob, BlockHeight, Epoch, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, ChainId},
};
use linera_chain::{
    block::{Block, BlockBody, BlockHeader},
    data_types::{IncomingBundle, MessageAction, PostedMessage},
};
use linera_execution::{Message, MessageKind};
use linera_service_graphql_client::MessageBundle;

use crate::db::{sqlite::SqliteDatabase, IndexerDatabase};

#[tokio::test]
async fn test_sqlite_database_operations() {
    let db = create_test_database().await;

    // Test blob storage
    let blob = Blob::new_data(b"test blob content".to_vec());
    let blob_hash = blob.id();
    let blob_data = bincode::serialize(&blob).unwrap();

    let mut tx = db.begin_transaction().await.unwrap();
    db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Verify blob was stored
    let retrieved_blob_data = db.get_blob(&blob_hash).await.unwrap();
    assert_eq!(blob_data, retrieved_blob_data);

    // Test block storage (we'd need to create a proper ConfirmedBlockCertificate here)
    // For now, just test that the database operations work
}

#[tokio::test]
async fn test_atomic_transaction_behavior() {
    let db = create_test_database().await;

    // Test that failed transactions are rolled back
    let blob = Blob::new_data(b"test content".to_vec());
    let blob_hash = blob.id();
    let blob_data = bincode::serialize(&blob).unwrap();

    // Start transaction but don't commit
    {
        let mut tx = db.begin_transaction().await.unwrap();
        db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
            .await
            .unwrap();
        // tx is dropped here without commit, should rollback
    }

    // Verify blob was not stored
    assert!(db.get_blob(&blob_hash).await.is_err());

    // Now test successful commit
    let mut tx = db.begin_transaction().await.unwrap();
    db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Verify blob was stored
    let retrieved = db.get_blob(&blob_hash).await.unwrap();
    assert_eq!(blob_data, retrieved);
}

#[tokio::test]
async fn test_high_level_atomic_api() {
    let db = create_test_database().await;

    // Create test data using simple hashes for testing
    let blob1 = Blob::new_data(b"test blob 1".to_vec());
    let blob2 = Blob::new_data(b"test blob 2".to_vec());
    let blob1_data = bincode::serialize(&blob1).unwrap();
    let blob2_data = bincode::serialize(&blob2).unwrap();

    // Create a proper test block
    let chain_id = ChainId(CryptoHash::new(blob2.content()));
    let height = BlockHeight(1);
    let timestamp = Timestamp::now();
    let test_block = create_test_block(chain_id, height);
    let block_hash = Hashed::new(test_block.clone()).hash();
    let block_data = bincode::serialize(&test_block).unwrap();

    let blobs = vec![
        (blob1.id(), blob1_data.clone()),
        (blob2.id(), blob2_data.clone()),
    ];

    // Test atomic storage of block with blobs
    db.store_block_with_blobs(
        &block_hash,
        &chain_id,
        height,
        timestamp,
        &block_data,
        &blobs,
    )
    .await
    .unwrap();

    // Verify block was stored
    let retrieved_block = db.get_block(&block_hash).await.unwrap();
    assert_eq!(block_data, retrieved_block);

    // Verify blobs were stored
    let retrieved_blob1 = db.get_blob(&blob1.id()).await.unwrap();
    let retrieved_blob2 = db.get_blob(&blob2.id()).await.unwrap();
    assert_eq!(blob1_data, retrieved_blob1);
    assert_eq!(blob2_data, retrieved_blob2);
}

#[tokio::test]
async fn test_incoming_bundles_storage_and_query() {
    let db = create_test_database().await;

    // Test that we can create the database schema with the new tables
    // The tables should be created in initialize_schema()

    // Verify the new tables exist by trying to query them
    let bundles_result = sqlx::query("SELECT COUNT(*) FROM incoming_bundles")
        .fetch_one(&db.pool)
        .await;
    assert!(
        bundles_result.is_ok(),
        "incoming_bundles table should exist"
    );

    let messages_result = sqlx::query("SELECT COUNT(*) FROM posted_messages")
        .fetch_one(&db.pool)
        .await;
    assert!(
        messages_result.is_ok(),
        "posted_messages table should exist"
    );

    // First insert a test block that the bundle can reference
    let mut test_block = create_test_block(
        ChainId(CryptoHash::new(&TestString::new("test_chain_id"))),
        BlockHeight(100),
    );

    let incoming_bundle_message = PostedMessage {
        index: 0,
        authenticated_signer: None,
        grant: Amount::from_tokens(100),
        refund_grant_to: None,
        kind: MessageKind::Protected,
        message: Message::User {
            application_id: ApplicationId::new(CryptoHash::new(&TestString::new("test_app_id"))),
            bytes: b"test_message_data".to_vec(),
        },
    };

    let origin_chain_id = ChainId(CryptoHash::new(&TestString::new("origin_chain")));
    let source_cert_hash = CryptoHash::new(&TestString::new("source_cert_hash"));

    let incoming_bundle = IncomingBundle {
        origin: origin_chain_id,
        bundle: MessageBundle {
            height: test_block.header.height,
            timestamp: Timestamp::now(),
            certificate_hash: source_cert_hash,
            transaction_index: 2,
            messages: vec![incoming_bundle_message.clone()],
        },
        action: MessageAction::Reject,
    };

    test_block
        .body
        .transactions
        .push(linera_chain::data_types::Transaction::ReceiveMessages(
            incoming_bundle.clone(),
        ));

    let block_hash = Hashed::new(test_block.clone()).hash();
    let block_data = bincode::serialize(&test_block).unwrap();

    let mut tx = db.begin_transaction().await.unwrap();

    db.insert_block_tx(
        &mut tx,
        &block_hash,
        &test_block.header.chain_id,
        test_block.header.height,
        test_block.header.timestamp,
        &block_data,
    )
    .await
    .unwrap();

    tx.commit().await.unwrap();

    // Test the query methods
    let bundles = db
        .get_incoming_bundles_for_block(&block_hash)
        .await
        .unwrap();
    assert_eq!(bundles.len(), 1);

    let (queried_bundle_id, bundle_info) = &bundles[0];
    assert_eq!(bundle_info.bundle_index, 0);
    assert_eq!(bundle_info.origin_chain_id, origin_chain_id);
    assert_eq!(bundle_info.action, incoming_bundle.action);
    assert_eq!(bundle_info.source_height, incoming_bundle.bundle.height);
    assert_eq!(
        bundle_info.transaction_index,
        incoming_bundle.bundle.transaction_index
    );

    let messages = db
        .get_posted_messages_for_bundle(*queried_bundle_id)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);

    let message_info = &messages[0];
    assert_eq!(message_info.message_index, 0);
    assert_eq!(
        message_info.grant_amount,
        Amount::from_tokens(100).to_string()
    );
    assert_eq!(
        message_info.message_kind,
        incoming_bundle_message.kind.to_string()
    );
    assert!(message_info.authenticated_signer_data.is_none());
    assert!(message_info.refund_grant_to_data.is_none());
    assert_eq!(
        message_info.message_data,
        bincode::serialize(&incoming_bundle_message.message).unwrap()
    );

    // Test querying by origin chain
    let origin_bundles = db
        .get_bundles_from_origin_chain(&origin_chain_id)
        .await
        .unwrap();
    assert_eq!(origin_bundles.len(), 1);
    assert_eq!(origin_bundles[0].0, block_hash);
    assert_eq!(origin_bundles[0].1, *queried_bundle_id);
}

async fn create_test_database() -> SqliteDatabase {
    SqliteDatabase::new("sqlite::memory:")
        .await
        .expect("Failed to create test database")
}

fn create_test_block(chain_id: ChainId, height: BlockHeight) -> Block {
    Block {
        header: BlockHeader {
            chain_id,
            epoch: Epoch::ZERO,
            height,
            timestamp: Timestamp::now(),
            state_hash: CryptoHash::new(&TestString::new("test_state_hash")),
            previous_block_hash: None,
            authenticated_signer: None,
            transactions_hash: CryptoHash::new(&TestString::new("transactions_hash")),
            messages_hash: CryptoHash::new(&TestString::new("messages_hash")),
            previous_message_blocks_hash: CryptoHash::new(&TestString::new("prev_msg_blocks_hash")),
            previous_event_blocks_hash: CryptoHash::new(&TestString::new("prev_event_blocks_hash")),
            oracle_responses_hash: CryptoHash::new(&TestString::new("oracle_responses_hash")),
            events_hash: CryptoHash::new(&TestString::new("events_hash")),
            blobs_hash: CryptoHash::new(&TestString::new("blobs_hash")),
            operation_results_hash: CryptoHash::new(&TestString::new("operation_results_hash")),
        },
        body: BlockBody {
            transactions: vec![],
            messages: vec![],
            previous_message_blocks: Default::default(),
            previous_event_blocks: Default::default(),
            oracle_responses: vec![],
            events: vec![],
            blobs: vec![],
            operation_results: vec![],
        },
    }
}
