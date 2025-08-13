// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::{
    db::tests::{MockFailingDatabase, MockSuccessDatabase},
    indexer_api::{element::Payload, Element},
};

fn test_blob_element() -> Element {
    let test_blob = Blob::new_data(b"test blob content".to_vec());
    let blob_data = bincode::serialize(&test_blob).unwrap();

    Element {
        payload: Some(Payload::Blob(crate::indexer_api::Blob { bytes: blob_data })),
    }
}

// Create a protobuf message that is not a valid ConfiredBlockCertificate instance.
fn invalid_block_element() -> Element {
    Element {
        payload: Some(Payload::Block(crate::indexer_api::Block {
            bytes: b"fake_block_certificate_data".to_vec(),
        })),
    }
}

// Create a valid block element with minimal data.
fn valid_block_element() -> Element {
    valid_block_element_with_chain_id("test_chain")
}

fn valid_block_element_with_chain_id(chain_suffix: &str) -> Element {
    use std::collections::BTreeMap;

    use linera_base::{
        crypto::CryptoHash,
        data_types::{BlockHeight, Epoch, Round, Timestamp},
        identifiers::{ChainId, StreamId},
    };
    use linera_chain::{
        block::{Block, ConfirmedBlock},
        data_types::{BlockExecutionOutcome, ProposedBlock},
    };
    // Create a simple test ChainId with unique suffix
    let chain_id = ChainId(CryptoHash::test_hash(chain_suffix));

    // Create a minimal proposed block (genesis block)
    let proposed_block = ProposedBlock {
        epoch: Epoch::ZERO,
        chain_id,
        transactions: vec![],
        previous_block_hash: None,
        height: BlockHeight::ZERO,
        authenticated_signer: None,
        timestamp: Timestamp::default(),
    };

    // Create a minimal block execution outcome with proper BTreeMap types
    let outcome = BlockExecutionOutcome {
        messages: vec![],
        state_hash: CryptoHash::default(),
        oracle_responses: vec![],
        events: vec![],
        blobs: vec![],
        operation_results: vec![],
        previous_event_blocks: BTreeMap::<StreamId, (CryptoHash, BlockHeight)>::new(),
        previous_message_blocks: BTreeMap::<ChainId, (CryptoHash, BlockHeight)>::new(),
    };

    let block = Block::new(proposed_block, outcome);
    let confirmed_block = ConfirmedBlock::new(block);
    let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
    let block_data = bincode::serialize(&certificate).unwrap();

    Element {
        payload: Some(Payload::Block(crate::indexer_api::Block {
            bytes: block_data,
        })),
    }
}

#[tokio::test]
async fn test_process_element_blob_success() {
    let database = MockSuccessDatabase::new();
    let mut pending_blobs = HashMap::new();
    let element = test_blob_element();

    let result = IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

    // Processing blob returns `Ok(None)` (no ACK).
    assert!(matches!(result, Ok(None)));
    // Blob should be added to pending blobs
    assert_eq!(pending_blobs.len(), 1);
}

#[tokio::test]
async fn test_process_element_invalid_block() {
    let database = MockFailingDatabase::new();

    let mut pending_blobs = HashMap::new();
    match IndexerGrpcServer::process_element(&database, &mut pending_blobs, test_blob_element())
        .await
    {
        Ok(None) => {}
        _ => panic!("Expected Ok(None)"),
    };

    let element = invalid_block_element();

    let result = IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

    // Should return an error due to deserialization failure
    assert!(result.is_err());
    match result.unwrap_err() {
        ProcessingError::BlockDeserialization(_) => {}
        _ => panic!("Expected BlockDeserialization error"),
    }

    assert_eq!(
        pending_blobs.len(),
        1,
        "Pending blobs should remain after block failure"
    );
}

#[tokio::test]
async fn test_process_element_empty_payload() {
    let database = MockFailingDatabase::new();
    let mut pending_blobs = HashMap::new();
    let element = Element { payload: None };

    match IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await {
        Err(ProcessingError::EmptyPayload) => {}
        _ => panic!("Expected EmptyPayload error"),
    }
}

#[tokio::test]
async fn test_process_element_invalid_blob() {
    let database = MockFailingDatabase::new();
    let mut pending_blobs = HashMap::new();

    // Create element with invalid blob data
    let element = Element {
        payload: Some(Payload::Blob(crate::indexer_api::Blob {
            bytes: vec![0x00, 0x01, 0x02], // Invalid blob data
        })),
    };

    match IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await {
        Err(ProcessingError::BlobDeserialization(_)) => {}
        _ => panic!("Expected BlobDeserialization error"),
    }
}

#[tokio::test]
async fn test_process_valid_block() {
    use std::sync::Arc;

    // Use MockSuccessDatabase to test successful paths and ACK behavior
    let database = Arc::new(MockSuccessDatabase::new());
    let mut pending_blobs = HashMap::new();

    // First add a blob to pending_blobs
    let blob_element = test_blob_element();
    let blob_result =
        IndexerGrpcServer::process_element(&*database, &mut pending_blobs, blob_element).await;

    assert!(
        matches!(blob_result, Ok(None)),
        "Blobs should return Ok(None)"
    );
    assert_eq!(pending_blobs.len(), 1, "Pending blobs should have 1 blob");

    let block_element = invalid_block_element();
    let block_result =
        IndexerGrpcServer::process_element(&*database, &mut pending_blobs, block_element).await;

    assert!(
        block_result.is_err(),
        "Invalid block should return an error"
    );
    match block_result.unwrap_err() {
        ProcessingError::BlockDeserialization(_) => {
            // This should fail due to invalid block deserialization
        }
        _ => panic!("Expected BlockDeserialization error"),
    }

    assert_eq!(
        pending_blobs.len(),
        1,
        "Pending blobs should remain after block failure"
    );

    // Valid block should produce ACK
    let block_element = valid_block_element();
    let block_result =
        IndexerGrpcServer::process_element(&*database, &mut pending_blobs, block_element).await;
    assert!(
        matches!(block_result, Ok(Some(()))),
        "Valid blocks should return Ok(Some(())) ACK"
    );
    assert!(
        pending_blobs.is_empty(),
        "Pending blobs should be cleared after block"
    );
}

// === STREAM PROCESSING TESTS (Integration Tests) ===

#[tokio::test]
async fn test_process_stream_end_to_end_mixed_elements() {
    use std::sync::Arc;

    use futures::StreamExt;
    use tokio_stream;
    use tonic::{Code, Status};

    let database = Arc::new(MockSuccessDatabase::new());

    // Create test elements - same blob content will have same BlobId
    let blob1 = test_blob_element();
    let blob2 = test_blob_element();
    let blob3 = test_blob_element();
    let valid_block1 = valid_block_element_with_chain_id("test_chain_1");
    let invalid_block = invalid_block_element();
    let valid_block2 = valid_block_element_with_chain_id("test_chain_2");

    // Create a mixed stream of elements: blobs and blocks
    let elements = vec![
        Ok(blob1),         // Blob #1 - should not produce ACK
        Ok(blob2),         // Blob #2 - should not produce ACK
        Ok(valid_block1),  // Valid Block #1 - should produce ACK and store blobs 1&2
        Ok(invalid_block), // Invalid Block - should produce ERROR, no storage
        Ok(blob3),         // Blob #3 - should not produce ACK
        Ok(valid_block2),  // Valid Block #2 - should produce ACK and store blob 3
    ];

    // Verify initial state - no data stored
    assert_eq!(database.blob_count(), 0, "Database should start empty");
    assert_eq!(database.block_count(), 0, "Database should start empty");

    // Create a BoxStream from the elements
    let input_stream = tokio_stream::iter(elements).boxed();

    // Call the process_stream method
    let output_stream = IndexerGrpcServer::process_stream(database.clone(), input_stream).await;

    // Collect all results from the output stream
    let results: Vec<Result<(), Status>> = output_stream.collect().await;

    // === VERIFY OUTPUT STREAM RESPONSES ===

    // Verify we get exactly 3 responses:
    // 1. ACK for first valid block (processes 2 blobs + block 1)
    // 2. ERROR for invalid block (deserialization fails)
    // 3. ACK for second valid block (processes 1 blob + block 2)
    // (No responses for the individual blobs)
    assert_eq!(results.len(), 3, "Expected exactly 3 responses from stream");

    // Verify the first result is a successful ACK
    assert!(
        matches!(results[0], Ok(())),
        "First valid block should produce successful ACK"
    );

    // Verify the second result is an error for invalid block
    // Verify the error details
    if let Err(status) = &results[1] {
        assert_eq!(
            status.code(),
            Code::InvalidArgument,
            "Invalid block should return InvalidArgument status"
        );
        assert!(
            status.message().contains("Invalid block"),
            "Error message should mention invalid block"
        );
    }

    // Verify the third result is a successful ACK
    assert!(
        matches!(results[2], Ok(())),
        "Second valid block should produce successful ACK"
    );

    // After processing the stream, we should have:
    // - 1 blob stored (all blobs have same content/BlobId, so they overwrite each other)
    // - 2 blocks stored (valid_block1 and valid_block2 with different chain IDs)
    // - invalid_block should NOT be stored (deserialization failed)

    assert_eq!(
        database.blob_count(),
        1,
        "Should have 1 unique blob stored (all blobs have same content/BlobId)"
    );

    assert_eq!(
        database.block_count(),
        2,
        "Should have 2 valid blocks stored (invalid block should not be stored)"
    );
}

#[tokio::test]
async fn test_process_stream_database_failure() {
    use std::sync::Arc;

    use futures::StreamExt;
    use tokio_stream;
    use tonic::{Code, Status};

    // Use MockFailingDatabase to simulate database failures during block storage
    let database = Arc::new(MockFailingDatabase::new());

    // Create test elements: blobs + valid block (which will fail at database level)
    let blob1 = test_blob_element();
    let blob2 = test_blob_element();
    let blob3 = test_blob_element();
    let valid_block = valid_block_element(); // This will fail when trying to store

    // Create a stream: blobs followed by a valid block that will fail at DB level
    let elements = vec![
        Ok(blob1),       // Blob #1 - should not produce ACK, stored in pending_blobs
        Ok(blob2),       // Blob #2 - should not produce ACK, stored in pending_blobs
        Ok(blob3),       // Blob #3 - should not produce ACK, stored in pending_blobs
        Ok(valid_block), // Valid Block - should produce ERROR due to database failure
    ];

    // Create a BoxStream from the elements
    let input_stream = tokio_stream::iter(elements).boxed();

    // Call the process_stream method
    let output_stream = IndexerGrpcServer::process_stream(database.clone(), input_stream).await;

    // Collect all results from the output stream
    let results: Vec<Result<(), Status>> = output_stream.collect().await;

    // === VERIFY OUTPUT STREAM RESPONSES ===

    // Verify we get exactly 1 response: ERROR for the block (database failure)
    // (No responses for the individual blobs, they just accumulate in pending_blobs)
    assert_eq!(results.len(), 1, "Expected exactly 1 response from stream");

    // Verify the result is an error due to database failure
    assert!(results[0].is_err(), "Block should produce database error");

    // Verify the error details
    if let Err(status) = &results[0] {
        assert_eq!(
            status.code(),
            Code::InvalidArgument,
            "MockFailingDatabase returns SqliteError::Serialization which maps to InvalidArgument"
        );
        assert!(
            status
                .message()
                .contains("Mock: Cannot create real transaction"),
            "Error message should contain database failure details, got: {}",
            status.message()
        );
    }
}
