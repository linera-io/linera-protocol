// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dockertest::{waitfor, DockerTest, Image, Source, TestBodySpecification};
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
            authenticated_owner: None,
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
        assert!(message_info.authenticated_owner_data.is_none());
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
            authenticated_owner: None,
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
