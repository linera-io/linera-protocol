// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Database trait for the indexer.

#[cfg(test)]
pub(crate) mod tests;

pub mod common;
pub mod postgres;
pub mod sqlite;

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_service_graphql_client::MessageAction;

/// Trait defining the database operations for the indexer
#[async_trait]
pub trait IndexerDatabase: Send + Sync {
    type Error;

    type Transaction<'a>: Send + Sync;

    /// Atomically store a block with its required blobs
    /// This is the high-level API that can be implemented in terms of the other methods
    ///
    /// Blobs are tuples of (BlobId, data, Option<transaction_index>)
    /// - For blobs received as standalone elements: transaction_index is None
    /// - For blobs extracted from block.body.blobs: transaction_index indicates which transaction created them
    async fn store_block_with_blobs(
        &self,
        block_hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        timestamp: Timestamp,
        block_data: &[u8],
        blobs: &[(BlobId, Vec<u8>, Option<u32>)],
    ) -> Result<(), Self::Error> {
        // Start atomic transaction
        let mut tx = self.begin_transaction().await?;

        // Insert the block first to satisfy foreign key constraint
        self.insert_block_tx(&mut tx, block_hash, chain_id, height, timestamp, block_data)
            .await?;

        // Insert all blobs
        // Only blobs extracted from the block (those with transaction_index) get the block_hash
        for (blob_id, blob_data, transaction_index) in blobs {
            let block_hash_for_blob = if transaction_index.is_some() {
                Some(*block_hash)
            } else {
                None
            };
            self.insert_blob_tx(
                &mut tx,
                blob_id,
                blob_data,
                block_hash_for_blob,
                *transaction_index,
            )
            .await?;
        }

        // Commit transaction - this is the only point where data becomes visible
        self.commit_transaction(tx).await?;

        Ok(())
    }

    /// Start a new transaction
    async fn begin_transaction(&self) -> Result<Self::Transaction<'_>, Self::Error>;

    /// Insert a blob within a transaction
    async fn insert_blob_tx(
        &self,
        tx: &mut Self::Transaction<'_>,
        blob_id: &BlobId,
        data: &[u8],
        block_hash: Option<CryptoHash>,
        transaction_index: Option<u32>,
    ) -> Result<(), Self::Error>;

    /// Insert a block within a transaction
    async fn insert_block_tx(
        &self,
        tx: &mut Self::Transaction<'_>,
        hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        timestamp: Timestamp,
        data: &[u8],
    ) -> Result<(), Self::Error>;

    /// Commit a transaction
    async fn commit_transaction(&self, tx: Self::Transaction<'_>) -> Result<(), Self::Error>;

    /// Get a block by hash
    async fn get_block(&self, hash: &CryptoHash) -> Result<Vec<u8>, Self::Error>;

    /// Get a blob by blob_id
    async fn get_blob(&self, blob_id: &BlobId) -> Result<Vec<u8>, Self::Error>;

    /// Get the latest block for a chain
    async fn get_latest_block_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error>;

    /// Get blocks for a chain within a height range
    async fn get_blocks_for_chain_range(
        &self,
        chain_id: &ChainId,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error>;

    /// Check if a blob exists
    async fn blob_exists(&self, blob_id: &BlobId) -> Result<bool, Self::Error>;

    /// Check if a block exists
    async fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Self::Error>;

    /// Get incoming bundles for a specific block
    async fn get_incoming_bundles_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, Self::Error>;

    /// Get posted messages for a specific bundle
    async fn get_posted_messages_for_bundle(
        &self,
        bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, Self::Error>;

    /// Get all bundles from a specific origin chain
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
