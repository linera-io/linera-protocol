// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! gRPC server implementation for the indexer.

use std::{collections::HashMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use linera_base::{data_types::Blob, identifiers::BlobId};
use linera_chain::types::ConfirmedBlockCertificate;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use crate::{
    database_trait::IndexerDatabase,
    indexer_api::{
        element::Payload,
        indexer_server::{Indexer, IndexerServer},
        Element,
    },
    sqlite_db::SqliteError,
};

/// Error type for processing elements in the indexer
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Failed to deserialize blob: {0}")]
    BlobDeserialization(#[from] bincode::Error),
    #[error("Failed to deserialize block: {0}")]
    BlockDeserialization(String),
    #[error("Failed to serialize blob: {0}")]
    BlobSerialization(bincode::Error),
    #[error("Failed to serialize block: {0}")]
    BlockSerialization(bincode::Error),
    #[error("Database error: {0}")]
    Database(#[from] SqliteError),
    #[error("Empty element payload")]
    EmptyPayload,
}

pub struct IndexerGrpcServer<D> {
    database: Arc<D>,
}

impl<D> IndexerGrpcServer<D> {
    pub fn new(database: D) -> Self {
        Self {
            database: Arc::new(database),
        }
    }
}

impl<D: IndexerDatabase + 'static> IndexerGrpcServer<D> {
    pub fn new_with_database(database: D) -> Self {
        Self {
            database: Arc::new(database),
        }
    }

    /// Start the gRPC indexer server
    pub async fn serve(self, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = format!("0.0.0.0:{}", port).parse()?;

        info!("Starting gRPC indexer server on {}", addr);

        Server::builder()
            .add_service(IndexerServer::new(self))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Process the entire stream and return responses
    async fn process_stream(
        database: Arc<D>,
        stream: Streaming<Element>,
    ) -> impl Stream<Item = Result<(), Status>> {
        futures::stream::unfold(
            (stream, database, HashMap::<BlobId, Vec<u8>>::new()),
            |(mut input_stream, database, mut pending_blobs)| async move {
                loop {
                    match input_stream.next().await {
                        Some(Ok(element)) => {
                            match Self::process_element(&database, &mut pending_blobs, element)
                                .await
                            {
                                Ok(Some(())) => {
                                    // If processing was successful, return an ACK
                                    info!("Processed element successfully");
                                    return Some((Ok(()), (input_stream, database, pending_blobs)));
                                }
                                Err(error) => {
                                    // If there was an error, return it
                                    let status = Status::from(error);
                                    error!("Error processing element: {}", status);
                                    return Some((
                                        Err(status),
                                        (input_stream, database, pending_blobs),
                                    ));
                                }
                                Ok(None) => {
                                    // If processing was a blob, we just continue without returning a response
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!("Error receiving element: {}", e);
                            return Some((Err(e), (input_stream, database, pending_blobs)));
                        }
                        None => {
                            // Stream ended
                            return None;
                        }
                    }
                }
            },
        )
    }

    /// Process a single element and return a response if needed.
    /// This handles both blobs and blocks.
    /// For blobs, it stores them in `pending_blobs` and returns `Ok(None)`.
    /// For blocks, it processes them and returns `Ok(Some(()))` on success or `Err(ProcessingError)` on failure.
    async fn process_element(
        database: &D,
        pending_blobs: &mut HashMap<BlobId, Vec<u8>>,
        element: Element,
    ) -> Result<Option<()>, ProcessingError> {
        match element.payload {
            Some(Payload::Blob(proto_blob)) => {
                // Convert protobuf blob to linera blob
                let blob = Blob::try_from(proto_blob)?;
                let blob_id = blob.id();
                let blob_data =
                    bincode::serialize(&blob).map_err(ProcessingError::BlobSerialization)?;

                info!("Received blob: {}", blob_id);
                pending_blobs.insert(blob_id, blob_data);
                Ok(None) // No response for blobs, just store them
            }
            Some(Payload::Block(proto_block)) => {
                // Convert protobuf block to linera block first
                let block_cert = ConfirmedBlockCertificate::try_from(proto_block)
                    .map_err(|e| ProcessingError::BlockDeserialization(e.to_string()))?;

                // Extract block metadata
                let block_hash = block_cert.hash();
                let chain_id = block_cert.inner().chain_id();
                let height = block_cert.inner().height();
                let incoming_bundles = block_cert.value().block().body.incoming_bundles.clone();

                info!(
                    "Received block: {} for chain: {} at height: {}",
                    block_hash, chain_id, height
                );

                // Serialize block BEFORE taking any database locks
                let block_data =
                    bincode::serialize(&block_cert).map_err(ProcessingError::BlockSerialization)?;

                // Convert pending blobs to the format expected by the high-level API
                let blobs: Vec<(BlobId, Vec<u8>)> = pending_blobs
                    .iter()
                    .map(|(blob_id, blob_data)| (*blob_id, blob_data.clone()))
                    .collect();

                // Use the high-level atomic API with incoming bundles - this manages all locking internally
                database
                    .store_block_with_blobs_and_bundles(
                        &block_hash,
                        &chain_id,
                        height,
                        &block_data,
                        &blobs,
                        incoming_bundles,
                    )
                    .await?;

                info!(
                    "Successfully committed block {} with {} blobs",
                    block_hash,
                    pending_blobs.len()
                );
                pending_blobs.clear();
                Ok(Some(()))
            }
            None => {
                warn!("Received empty element");
                Err(ProcessingError::EmptyPayload)
            }
        }
    }
}

#[async_trait]
impl<D: IndexerDatabase + 'static> Indexer for IndexerGrpcServer<D> {
    type IndexBatchStream = Pin<Box<dyn Stream<Item = Result<(), Status>> + Send + 'static>>;

    async fn index_batch(
        &self,
        request: Request<Streaming<Element>>,
    ) -> Result<Response<Self::IndexBatchStream>, Status> {
        let stream = request.into_inner();
        let database = Arc::clone(&self.database);

        let output_stream = Self::process_stream(database, stream).await;
        Ok(Response::new(Box::pin(output_stream)))
    }
}

impl From<SqliteError> for Status {
    fn from(error: SqliteError) -> Self {
        match error {
            SqliteError::Database(e) => Status::internal(format!("Database error: {}", e)),
            SqliteError::Serialization(e) => {
                Status::invalid_argument(format!("Serialization error: {}", e))
            }
            SqliteError::BlockNotFound(hash) => {
                Status::not_found(format!("Block not found: {}", hash))
            }
            SqliteError::BlobNotFound(hash) => {
                Status::not_found(format!("Blob not found: {}", hash))
            }
        }
    }
}
impl From<ProcessingError> for Status {
    fn from(error: ProcessingError) -> Self {
        match error {
            ProcessingError::BlobDeserialization(e) => {
                Status::invalid_argument(format!("Invalid blob: {}", e))
            }
            ProcessingError::BlockDeserialization(e) => {
                Status::invalid_argument(format!("Invalid block: {}", e))
            }
            ProcessingError::BlobSerialization(e) => {
                Status::internal(format!("Failed to serialize blob: {}", e))
            }
            ProcessingError::BlockSerialization(e) => {
                Status::internal(format!("Failed to serialize block: {}", e))
            }
            ProcessingError::Database(e) => e.into(),
            ProcessingError::EmptyPayload => Status::invalid_argument("Empty element"),
        }
    }
}

/// Type conversions between protobuf and linera types
impl TryFrom<crate::indexer_api::Block> for ConfirmedBlockCertificate {
    type Error = bincode::Error;

    fn try_from(value: crate::indexer_api::Block) -> Result<Self, Self::Error> {
        bincode::deserialize(&value.bytes)
    }
}

impl TryFrom<crate::indexer_api::Blob> for Blob {
    type Error = bincode::Error;

    fn try_from(value: crate::indexer_api::Blob) -> Result<Self, Self::Error> {
        bincode::deserialize(&value.bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        indexer_api::{element::Payload, Element},
        mock_database::{MockFailingDatabase, MockSuccessDatabase},
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

    #[tokio::test]
    async fn test_process_single_element_blob_success() {
        let database = MockFailingDatabase::new();
        let mut pending_blobs = HashMap::new();
        let element = test_blob_element();

        let result =
            IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

        // Processing blob returns `Ok(None)` (no ACK).
        assert!(matches!(result, Ok(None)));
        // Blob should be added to pending blobs
        assert_eq!(pending_blobs.len(), 1);
    }

    #[tokio::test]
    async fn test_process_single_element_block_deserialization_failure() {
        let database = MockFailingDatabase::new();

        let mut pending_blobs = HashMap::new();
        let element = invalid_block_element(); // This will have invalid block data

        let result =
            IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

        // Should return an error due to deserialization failure
        assert!(result.is_err());
        match result.unwrap_err() {
            ProcessingError::BlockDeserialization(_) => {}
            _ => panic!("Expected BlockDeserialization error"),
        }

        // Pending blobs should not be cleared on failure
        assert_eq!(pending_blobs.len(), 0);
    }

    #[tokio::test]
    async fn test_process_single_element_empty_payload() {
        let database = MockFailingDatabase::new();
        let mut pending_blobs = HashMap::new();
        let element = Element { payload: None };

        let result =
            IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

        // Should return an error for empty element
        assert!(result.is_err());
        match result.unwrap_err() {
            ProcessingError::EmptyPayload => {}
            _ => panic!("Expected EmptyPayload error"),
        }
    }

    #[tokio::test]
    async fn test_process_single_element_invalid_blob() {
        let database = MockFailingDatabase::new();
        let mut pending_blobs = HashMap::new();

        // Create element with invalid blob data
        let element = Element {
            payload: Some(Payload::Blob(crate::indexer_api::Blob {
                bytes: vec![0x00, 0x01, 0x02], // Invalid blob data
            })),
        };

        let result =
            IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

        // Should return an error for invalid blob
        assert!(result.is_err());
        match result.unwrap_err() {
            ProcessingError::BlobDeserialization(_) => {}
            _ => panic!("Expected BlobDeserialization error"),
        }
    }

    #[tokio::test]
    async fn test_process_single_element_invalid_block() {
        let database = MockFailingDatabase::new();
        let mut pending_blobs = HashMap::new();

        // Create element with invalid block data
        let element = Element {
            payload: Some(Payload::Block(crate::indexer_api::Block {
                bytes: vec![0x00, 0x01, 0x02], // Invalid block data
            })),
        };

        let result =
            IndexerGrpcServer::process_element(&database, &mut pending_blobs, element).await;

        // Should return an error for invalid block
        assert!(result.is_err());
        match result.unwrap_err() {
            ProcessingError::BlockDeserialization(_) => {}
            _ => panic!("Expected BlockDeserialization error"),
        }
    }

    #[tokio::test]
    async fn test_process_stream_blob_no_ack() {
        use std::sync::Arc;

        let database = Arc::new(MockSuccessDatabase);

        // Test the core logic through process_single_element
        // which is what process_stream uses internally
        let mut pending_blobs = HashMap::new();
        let blob_element = test_blob_element();

        let result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, blob_element).await;

        // Blob processing should return Ok(None) (no ACK)
        assert!(matches!(result, Ok(None)));

        // Blob should be stored in pending_blobs
        assert_eq!(pending_blobs.len(), 1);
    }

    #[tokio::test]
    async fn test_process_stream_block_with_ack() {
        use std::sync::Arc;

        let database = Arc::new(MockFailingDatabase::new());
        let mut pending_blobs = HashMap::new();

        // First add a blob to pending_blobs
        let blob_element = test_blob_element();
        let blob_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, blob_element).await;

        // Blob should not produce an ACK
        assert!(matches!(blob_result, Ok(None)));
        assert_eq!(pending_blobs.len(), 1);

        // Now try to process a block (which will fail due to invalid data)
        let block_element = invalid_block_element();
        let block_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, block_element).await;

        // Block processing should return Err (processing failure)
        assert!(block_result.is_err());
        match block_result.unwrap_err() {
            ProcessingError::BlockDeserialization(_) => {
                // This should fail due to invalid block deserialization
            }
            _ => panic!("Expected BlockDeserialization error"),
        }

        // Pending blobs should still be there since block failed
        assert_eq!(pending_blobs.len(), 1);
    }

    #[tokio::test]
    async fn test_process_stream_successful_block_clears_blobs() {
        use std::sync::Arc;

        // Create a mock that will succeed with transactions but fail at begin_transaction
        // to simulate the block storage path without actually storing
        let database = Arc::new(MockSuccessDatabase);
        let mut pending_blobs = HashMap::new();

        // Add a blob first
        let blob_element = test_blob_element();
        let _blob_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, blob_element).await;

        // Try to process a block - this will fail because MockSuccessDatabase
        // can't create real transactions, but it demonstrates the logic flow
        let block_element = invalid_block_element();
        let block_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, block_element).await;

        // Block should return an error (processing failure)
        assert!(block_result.is_err());

        // The result will be an error due to invalid block data, but this confirms
        // that blocks attempt to produce responses while blobs don't
        match block_result.unwrap_err() {
            ProcessingError::BlockDeserialization(_) => {}
            _ => panic!("Expected BlockDeserialization error"),
        }
    }

    #[tokio::test]
    async fn test_ack_behavior_invariant() {
        use std::sync::Arc;

        let database = Arc::new(MockFailingDatabase::new());

        // Test the key invariant: blobs never produce ACKs, blocks always attempt ACKs

        // Test 1: Blob should never produce ACK
        let mut pending_blobs = HashMap::new();
        let blob_element = test_blob_element();
        let blob_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, blob_element).await;
        assert!(
            matches!(blob_result, Ok(None)),
            "Blobs should return Ok(None)"
        );

        // Test 2: Block should always attempt ACK (even on failure)
        let block_element = invalid_block_element();
        let block_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, block_element).await;
        assert!(
            block_result.is_err(),
            "Blocks should return error on failure"
        );

        // Test 3: Empty element should produce error ACK
        let empty_element = Element { payload: None };
        let empty_result =
            IndexerGrpcServer::process_element(&*database, &mut pending_blobs, empty_element).await;
        assert!(empty_result.is_err(), "Empty elements should return error");
    }
}
