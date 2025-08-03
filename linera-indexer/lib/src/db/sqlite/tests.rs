// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use assert_matches::assert_matches;
use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::Blob,
    identifiers::ChainId,
};
use linera_chain::data_types::MessageAction;

use crate::db::{sqlite::SqliteDatabase, IndexerDatabase};

async fn create_test_database() -> SqliteDatabase {
    SqliteDatabase::new("sqlite::memory:")
        .await
        .expect("Failed to create test database")
}

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

    // Use blob hashes for test IDs (simpler than creating proper ones)
    let block_hash = linera_base::crypto::CryptoHash::new(blob1.content());
    let chain_id =
        linera_base::identifiers::ChainId(linera_base::crypto::CryptoHash::new(blob2.content()));
    let height = linera_base::data_types::BlockHeight(1);
    let block_data = b"fake block data".to_vec();

    let blobs = vec![
        (blob1.id(), blob1_data.clone()),
        (blob2.id(), blob2_data.clone()),
    ];

    // Test atomic storage of block with blobs
    db.store_block_with_blobs_and_bundles(
        &block_hash,
        &chain_id,
        height,
        &block_data,
        &blobs,
        vec![],
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

    // Test manual insertion to verify the schema works using string parsing
    let block_hash = CryptoHash::new(&TestString::new("test_block_hash"));
    let origin_chain = ChainId(CryptoHash::new(&TestString::new("origin_chain_id")));
    let source_cert_hash = CryptoHash::new(&TestString::new("source_cert_hash"));

    let mut tx = db.begin_transaction().await.unwrap();

    // First insert a test block that the bundle can reference
    let test_chain_id = ChainId(CryptoHash::new(&TestString::new("test_chain_id")));
    let test_height = 100_i64;
    let test_block_data = b"test_block_data".to_vec();

    sqlx::query("INSERT INTO blocks (hash, chain_id, height, data) VALUES (?1, ?2, ?3, ?4)")
        .bind(block_hash.to_string())
        .bind(test_chain_id.to_string())
        .bind(test_height)
        .bind(&test_block_data)
        .execute(&mut *tx)
        .await
        .expect("Should be able to insert test block");

    // Insert a test incoming bundle
    let bundle_result = sqlx::query(
            r#"
            INSERT INTO incoming_bundles 
            (block_hash, bundle_index, origin_chain_id, action, source_height, source_timestamp, source_cert_hash, transaction_index)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#
        )
        .bind(block_hash.to_string())
        .bind(0_i64)
        .bind(origin_chain.to_string())
        .bind("Accept")
        .bind(10_i64)
        .bind(1234567890_i64)
        .bind(source_cert_hash.to_string())
        .bind(2_i64)
        .execute(&mut *tx)
        .await;

    let bundle_id = bundle_result
        .expect("Should be able to insert into incoming_bundles")
        .last_insert_rowid();

    // Insert a test posted message
    let message_result = sqlx::query(
            r#"
            INSERT INTO posted_messages 
            (bundle_id, message_index, authenticated_signer, grant_amount, refund_grant_to, message_kind, message_data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#
        )
        .bind(bundle_id)
        .bind(0_i64)
        .bind(None::<Vec<u8>>)
        .bind(1000_i64)
        .bind(None::<Vec<u8>>)
        .bind("Protected")
        .bind(b"test_message_data".to_vec())
        .execute(&mut *tx)
        .await;

    assert_matches!(
        message_result,
        Ok(_),
        "Should be able to insert into posted_messages"
    );

    tx.commit().await.unwrap();

    // Test the query methods
    let bundles = db
        .get_incoming_bundles_for_block(&block_hash)
        .await
        .unwrap();
    assert_eq!(bundles.len(), 1);

    let (queried_bundle_id, bundle_info) = &bundles[0];
    assert_eq!(bundle_info.bundle_index, 0);
    assert_eq!(bundle_info.origin_chain_id, origin_chain);
    assert_eq!(bundle_info.action, MessageAction::Accept);
    assert_eq!(
        bundle_info.source_height,
        linera_base::data_types::BlockHeight(10)
    );
    assert_eq!(bundle_info.transaction_index, 2);

    let messages = db
        .get_posted_messages_for_bundle(*queried_bundle_id)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);

    let message_info = &messages[0];
    assert_eq!(message_info.message_index, 0);
    assert_eq!(message_info.grant_amount, 1000);
    assert_eq!(message_info.message_kind, "Protected");
    assert!(message_info.authenticated_signer_data.is_none());
    assert!(message_info.refund_grant_to_data.is_none());
    assert_eq!(message_info.message_data, b"test_message_data");

    // Test querying by origin chain
    let origin_bundles = db
        .get_bundles_from_origin_chain(&origin_chain)
        .await
        .unwrap();
    assert_eq!(origin_bundles.len(), 1);
    assert_eq!(origin_bundles[0].0, block_hash);
    assert_eq!(origin_bundles[0].1, *queried_bundle_id);
}
