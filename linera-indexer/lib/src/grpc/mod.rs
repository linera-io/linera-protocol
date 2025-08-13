// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! gRPC server implementation for the indexer.
#[cfg(test)]
mod tests;

use std::{collections::HashMap, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::{stream::BoxStream, Stream, StreamExt};
use linera_base::{data_types::Blob, identifiers::BlobId};
use linera_chain::types::ConfirmedBlockCertificate;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use crate::{
    db::{sqlite::SqliteError, IndexerDatabase},
    indexer_api::{
        element::Payload,
        indexer_server::{Indexer, IndexerServer},
        Element,
    },
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

impl<D: IndexerDatabase + 'static> IndexerGrpcServer<D>
where
    D::Error: Into<ProcessingError>,
{
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
        stream: BoxStream<'static, Result<Element, Status>>,
    ) -> impl Stream<Item = Result<(), Status>>
    where
        D::Error: Into<ProcessingError>,
    {
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
    ) -> Result<Option<()>, ProcessingError>
    where
        D::Error: Into<ProcessingError>,
    {
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
                let timestamp = block_cert.inner().timestamp();

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

                // Use the high-level atomic API - this manages all locking internally
                database
                    .store_block_with_blobs(
                        &block_hash,
                        &chain_id,
                        height,
                        timestamp,
                        &block_data,
                        &blobs,
                    )
                    .await
                    .map_err(Into::into)?;

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
impl<D: IndexerDatabase + 'static> Indexer for IndexerGrpcServer<D>
where
    D::Error: Into<ProcessingError>,
{
    type IndexBatchStream = Pin<Box<dyn Stream<Item = Result<(), Status>> + Send + 'static>>;

    async fn index_batch(
        &self,
        request: Request<Streaming<Element>>,
    ) -> Result<Response<Self::IndexBatchStream>, Status> {
        let stream = request.into_inner();
        let database = Arc::clone(&self.database);

        let output_stream = Self::process_stream(database, stream.boxed()).await;
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
