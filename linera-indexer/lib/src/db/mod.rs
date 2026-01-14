// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Database trait for the indexer.

#[cfg(test)]
pub(crate) mod tests;

pub mod common;
pub mod postgres;
pub mod sqlite;

use std::collections::HashMap;

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_service_graphql_client::MessageAction;

/// Trait defining the database operations for the indexer
#[async_trait]
pub trait IndexerDatabase: Send + Sync {
    type Error;

    type Transaction<'a>: Send + Sync;

    async fn store_block_with_blobs(
        &self,
        block_cert: &ConfirmedBlockCertificate,
        pending_blobs: &HashMap<BlobId, Vec<u8>>,
    ) -> Result<(), Self::Error>
    where
        Self::Error: From<bincode::Error>,
    {
        let block_hash = block_cert.hash();
        let chain_id = block_cert.inner().chain_id();
        let height = block_cert.inner().height();
        let timestamp = block_cert.inner().timestamp();

        let block_data = bincode::serialize(block_cert)?;

        // Extract blobs from the block body
        let block = block_cert.inner().block();
        let mut all_blobs: Vec<(BlobId, Vec<u8>)> = Vec::new();

        // Add standalone blobs
        for (blob_id, blob_data) in pending_blobs {
            all_blobs.push((*blob_id, blob_data.clone()));
        }

        // Add blobs from the block body
        for transaction_blobs in &block.body.blobs {
            for blob in transaction_blobs {
                let blob_id = blob.id();
                let blob_data = bincode::serialize(blob)?;
                all_blobs.push((blob_id, blob_data));
            }
        }

        // Start atomic transaction
        let mut tx = self.begin_transaction().await?;

        // Insert the block first to satisfy foreign key constraint
        self.insert_block_tx(
            &mut tx,
            &block_hash,
            &chain_id,
            height,
            timestamp,
            &block_data,
        )
        .await?;

        // Insert all blobs
        for (blob_id, blob_data) in &all_blobs {
            self.insert_blob_tx(&mut tx, blob_id, blob_data).await?;
        }

        // Commit transaction - this is the only point where data becomes visible
        self.commit_transaction(tx).await?;

        Ok(())
    }

    /// Starts a new transaction.
    async fn begin_transaction(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    /// Inserts a blob within a transaction.
    async fn insert_blob_tx(
        &self,
        tx: &mut Self::Transaction<'_>,
        blob_id: &BlobId,
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Inserts a block within a transaction.
    async fn insert_block_tx(
        &self,
        tx: &mut Self::Transaction<'_>,
        hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        timestamp: Timestamp,
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Commits a transaction.
    async fn commit_transaction(&self, tx: Self::Transaction<'_>) -> Result<(), Self::Error>;

    /// Gets a block by hash.
    async fn get_block(&self, hash: &CryptoHash) -> Result<Vec<u8>, Self::Error>;

    /// Gets a blob by blob ID.
    async fn get_blob(&self, blob_id: &BlobId) -> Result<Vec<u8>, Self::Error>;

    /// Gets the latest block for a chain.
    async fn get_latest_block_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error>;

    /// Gets blocks for a chain within a height range.
    async fn get_blocks_for_chain_range(
        &self,
        chain_id: &ChainId,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error>;

    /// Checks if a blob exists.
    async fn blob_exists(&self, blob_id: &BlobId) -> Result<bool, Self::Error>;

    /// Checks if a block exists.
    async fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Self::Error>;

    /// Gets incoming bundles for a specific block.
    async fn get_incoming_bundles_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, Self::Error>;

    /// Gets posted messages for a specific bundle.
    async fn get_posted_messages_for_bundle(
        &self,
        bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, Self::Error>;

    /// Gets all bundles from a specific origin chain.
    async fn get_bundles_from_origin_chain(
        &self,
        origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, Self::Error>;
}

/// Information about an incoming bundle (denormalized for queries)
#[derive(Debug, Clone)]
pub struct IncomingBundleInfo {
    pub bundle_index: usize,
    pub origin_chain_id: ChainId,
    pub action: MessageAction,
    pub source_height: BlockHeight,
    pub source_timestamp: Timestamp,
    pub source_cert_hash: CryptoHash,
    pub transaction_index: u32,
}

/// Information about a posted message (with serialized complex fields)
#[derive(Debug, Clone)]
pub struct PostedMessageInfo {
    pub message_index: u32,
    pub authenticated_signer_data: Option<String>,
    pub grant_amount: String,
    pub refund_grant_to_data: Option<String>,
    pub message_kind: String,
    pub message_data: Vec<u8>,
}
