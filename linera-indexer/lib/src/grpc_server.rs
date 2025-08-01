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
    indexer_api::{
        element::Payload,
        indexer_server::{Indexer, IndexerServer},
        Element,
    },
    sqlite_db::{SqliteDatabase, SqliteError},
};

pub struct IndexerGrpcServer {
    database: Arc<SqliteDatabase>,
}

impl IndexerGrpcServer {
    pub fn new(database: SqliteDatabase) -> Self {
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
        database: Arc<SqliteDatabase>,
        stream: Streaming<Element>,
    ) -> impl Stream<Item = Result<(), Status>> {
        futures::stream::unfold(
            (stream, database, HashMap::<BlobId, Vec<u8>>::new()),
            |(mut input_stream, database, mut pending_blobs)| async move {
                loop {
                    match input_stream.next().await {
                        Some(Ok(element)) => {
                            let response = Self::process_single_element(
                                &database,
                                &mut pending_blobs,
                                element,
                            )
                            .await;
                            if let Some(resp) = response {
                                // Return the response and continue with the updated state
                                return Some((resp, (input_stream, database, pending_blobs)));
                            }
                            // If no response (blob case), continue to next element
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

    /// Process a single element and return a response if needed
    async fn process_single_element(
        database: &SqliteDatabase,
        pending_blobs: &mut HashMap<BlobId, Vec<u8>>,
        element: Element,
    ) -> Option<Result<(), Status>> {
        match element.payload {
            Some(Payload::Blob(proto_blob)) => {
                // Convert protobuf blob to linera blob
                match Blob::try_from(proto_blob) {
                    Ok(blob) => {
                        let blob_id = blob.id();
                        match bincode::serialize(&blob) {
                            Ok(blob_data) => {
                                info!("Received blob: {}", blob_id);
                                pending_blobs.insert(blob_id, blob_data);
                                None // No response for blobs, just store them
                            }
                            Err(e) => {
                                error!("Failed to serialize blob: {}", e);
                                Some(Err(Status::internal(format!(
                                    "Failed to serialize blob: {}",
                                    e
                                ))))
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize blob: {}", e);
                        Some(Err(Status::invalid_argument(format!(
                            "Invalid blob: {}",
                            e
                        ))))
                    }
                }
            }
            Some(Payload::Block(proto_block)) => {
                // Convert protobuf block to linera block first
                let block_cert = match ConfirmedBlockCertificate::try_from(proto_block) {
                    Ok(cert) => cert,
                    Err(e) => {
                        error!("Failed to deserialize block: {}", e);
                        return Some(Err(Status::invalid_argument(format!(
                            "Invalid block: {}",
                            e
                        ))));
                    }
                };

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
                let block_data = match bincode::serialize(&block_cert) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to serialize block: {}", e);
                        return Some(Err(Status::internal(format!(
                            "Failed to serialize block: {}",
                            e
                        ))));
                    }
                };

                // Convert pending blobs to the format expected by the high-level API
                let blobs: Vec<(BlobId, Vec<u8>)> = pending_blobs
                    .iter()
                    .map(|(blob_id, blob_data)| (*blob_id, blob_data.clone()))
                    .collect();

                // Use the high-level atomic API with incoming bundles - this manages all locking internally
                match database
                    .store_block_with_blobs_and_bundles(
                        &block_hash,
                        &chain_id,
                        height,
                        &block_data,
                        &blobs,
                        incoming_bundles,
                    )
                    .await
                {
                    Ok(()) => {
                        info!(
                            "Successfully committed block {} with {} blobs",
                            block_hash,
                            pending_blobs.len()
                        );
                        pending_blobs.clear();
                        Some(Ok(()))
                    }
                    Err(e) => {
                        error!("Failed to store block {} with blobs: {}", block_hash, e);
                        Some(Err(Status::internal(format!(
                            "Failed to store block with blobs: {}",
                            e
                        ))))
                    }
                }
            }
            None => {
                warn!("Received empty element");
                Some(Err(Status::invalid_argument("Empty element")))
            }
        }
    }
}

#[async_trait]
impl Indexer for IndexerGrpcServer {
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
