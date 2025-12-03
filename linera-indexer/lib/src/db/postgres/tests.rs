// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use dockertest::{waitfor, DockerTest, Image, Source, TestBodySpecification};
use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::{Amount, Blob, BlockHeight, Epoch, Round, Timestamp},
    hashed::Hashed,
    identifiers::{ApplicationId, ChainId},
};
use linera_chain::{
    block::{Block, BlockBody, BlockHeader, ConfirmedBlock},
    data_types::{IncomingBundle, MessageAction, PostedMessage},
    types::ConfirmedBlockCertificate,
};
use linera_execution::{Message, MessageKind};
use linera_service_graphql_client::MessageBundle;

use crate::db::{postgres::PostgresDatabase, IndexerDatabase};

#[tokio::test]
async fn test_postgres_database_operations() {
    run_with_postgres(|database_url| async move {
        let db = PostgresDatabase::new(&database_url)
            .await
            .expect("Failed to create test database");

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
    })
    .await;
}

#[tokio::test]
async fn test_atomic_transaction_behavior() {
    run_with_postgres(|database_url| async move {
        let db = PostgresDatabase::new(&database_url)
            .await
            .expect("Failed to create test database");

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
    })
    .await;
}

#[tokio::test]
async fn test_high_level_atomic_api() {
    run_with_postgres(|database_url| async move {
        let db = PostgresDatabase::new(&database_url)
            .await
            .expect("Failed to create test database");

        // Create test data using simple hashes for testing
        let blob1 = Blob::new_data(b"test blob 1".to_vec());
        let blob2 = Blob::new_data(b"test blob 2".to_vec());
        let blob1_data = bincode::serialize(&blob1).unwrap();
        let blob2_data = bincode::serialize(&blob2).unwrap();

        // Create a proper test block certificate
        let chain_id = ChainId(CryptoHash::new(blob2.content()));
        let height = BlockHeight(1);
        let test_block = create_test_block(chain_id, height);
        let confirmed_block = ConfirmedBlock::new(test_block);
        let block_cert = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);

        let block_hash = block_cert.hash();
        let block_data = bincode::serialize(&block_cert).unwrap();

        let mut pending_blobs = HashMap::new();
        pending_blobs.insert(blob1.id(), blob1_data.clone());
        pending_blobs.insert(blob2.id(), blob2_data.clone());

        // Test atomic storage of block with blobs
        db.store_block_with_blobs(&block_cert, &pending_blobs)
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
    })
    .await;
}

#[tokio::test]
async fn test_incoming_bundles_storage_and_query() {
    run_with_postgres(|database_url| async move {
        let db = PostgresDatabase::new(&database_url)
            .await
            .expect("Failed to create test database");

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
                application_id: ApplicationId::new(CryptoHash::new(&TestString::new(
                    "test_app_id",
                ))),
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
    })
    .await;
}

#[tokio::test]
async fn test_block_with_embedded_blobs() {
    run_with_postgres(|database_url| async move {
        let db = PostgresDatabase::new(&database_url)
            .await
            .expect("Failed to create test database");

        // Create blobs that will be embedded in the block
        let blob1 = Blob::new_data(b"embedded blob 1".to_vec());
        let blob2 = Blob::new_data(b"embedded blob 2".to_vec());
        let blob3 = Blob::new_data(b"embedded blob 3".to_vec());

        // Create a standalone blob (not in the block)
        let standalone_blob = Blob::new_data(b"standalone blob".to_vec());
        let standalone_blob_data = bincode::serialize(&standalone_blob).unwrap();

        // Create a test block with blobs in its body
        let chain_id = ChainId(CryptoHash::new(&TestString::new("test_chain")));
        let height = BlockHeight(1);

        let mut test_block = create_test_block(chain_id, height);
        // Add blobs to two different transactions
        test_block.body.blobs = vec![
            vec![blob1.clone(), blob2.clone()], // Transaction 0 has 2 blobs
            vec![blob3.clone()],                // Transaction 1 has 1 blob
        ];

        let confirmed_block = ConfirmedBlock::new(test_block);
        let block_cert = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);

        let block_hash = block_cert.hash();
        let block_data = bincode::serialize(&block_cert).unwrap();

        // Prepare pending blobs (standalone blobs that arrived before the block)
        let mut pending_blobs = HashMap::new();
        pending_blobs.insert(standalone_blob.id(), standalone_blob_data.clone());

        // Store block with blobs
        // The API will extract blobs from the block body and combine with pending_blobs
        db.store_block_with_blobs(&block_cert, &pending_blobs)
            .await
            .unwrap();

        // Verify block was stored
        let retrieved_block = db.get_block(&block_hash).await.unwrap();
        assert_eq!(block_data, retrieved_block);

        // Verify all blobs were stored
        assert!(db.get_blob(&standalone_blob.id()).await.is_ok());
        assert!(db.get_blob(&blob1.id()).await.is_ok());
        assert!(db.get_blob(&blob2.id()).await.is_ok());
        assert!(db.get_blob(&blob3.id()).await.is_ok());
    })
    .await;
}

/// Helper function to run a test with a Postgres container
async fn run_with_postgres<F, Fut>(test_fn: F)
where
    F: FnOnce(String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(home) = std::env::var("HOME") {
        let docker_desktop_sock = if cfg!(target_os = "macos") {
            format!("{}/.docker/run/docker.sock", home)
        } else {
            format!("{}/var/run/docker.sock", home)
        };
        if std::path::Path::new(&docker_desktop_sock).exists() {
            std::env::set_var("DOCKER_HOST", format!("unix://{}", docker_desktop_sock));
        }
    }

    let mut test = DockerTest::new().with_default_source(Source::DockerHub);

    let mut postgres_composition =
        TestBodySpecification::with_image(Image::with_repository("postgres").tag("16-alpine"))
            .set_publish_all_ports(true)
            .set_wait_for(Box::new(waitfor::MessageWait {
                message: "database system is ready to accept connections".to_string(),
                source: waitfor::MessageSource::Stderr,
                timeout: 30,
            }));

    postgres_composition.modify_env("POSTGRES_PASSWORD", "testpass");
    postgres_composition.modify_env("POSTGRES_USER", "testuser");
    postgres_composition.modify_env("POSTGRES_DB", "testdb");

    test.provide_container(postgres_composition);

    test.run_async(|ops| async move {
        let container = ops.handle("postgres");
        let (_, host_port) = container.host_port(5432).unwrap();

        let database_url = format!(
            "postgresql://testuser:testpass@localhost:{}/testdb",
            host_port
        );

        test_fn(database_url).await;
    })
    .await;
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
