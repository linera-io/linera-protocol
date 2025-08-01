// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Database trait for the indexer.

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::IncomingBundle;
use sqlx::{Sqlite, Transaction};

use crate::sqlite_db::{IncomingBundleInfo, PostedMessageInfo, SqliteError};

/// Transaction type for database operations
pub type DatabaseTransaction<'a> = Transaction<'a, Sqlite>;

/// Trait defining the database operations for the indexer
#[async_trait]
pub trait IndexerDatabase: Send + Sync {
    /// Atomically store a block with its required blobs and incoming bundles
    /// This is the high-level API that can be implemented in terms of the other methods
    async fn store_block_with_blobs_and_bundles(
        &self,
        block_hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        block_data: &[u8],
        blobs: &[(BlobId, Vec<u8>)],
        incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        // Start atomic transaction
        let mut tx = self.begin_transaction().await?;

        // Insert all blobs first
        for (blob_id, blob_data) in blobs {
            self.insert_blob_tx(&mut tx, blob_id, blob_data).await?;
        }

        // Insert the block
        self.insert_block_tx(&mut tx, block_hash, chain_id, height, block_data)
            .await?;

        // Store incoming bundles and their messages
        self.store_incoming_bundles_tx(&mut tx, block_hash, incoming_bundles)
            .await?;

        // Commit transaction - this is the only point where data becomes visible
        self.commit_transaction(tx).await?;

        Ok(())
    }

    /// Start a new transaction
    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, SqliteError>;

    /// Insert a blob within a transaction
    async fn insert_blob_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        blob_id: &BlobId,
        data: &[u8],
    ) -> Result<(), SqliteError>;

    /// Insert a block within a transaction
    async fn insert_block_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        data: &[u8],
    ) -> Result<(), SqliteError>;

    /// Store incoming bundles within a transaction
    async fn store_incoming_bundles_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        block_hash: &CryptoHash,
        incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError>;

    /// Commit a transaction
    async fn commit_transaction(&self, tx: DatabaseTransaction<'_>) -> Result<(), SqliteError>;

    /// Get a block by hash
    async fn get_block(&self, hash: &CryptoHash) -> Result<Vec<u8>, SqliteError>;

    /// Get a blob by blob_id
    async fn get_blob(&self, blob_id: &BlobId) -> Result<Vec<u8>, SqliteError>;

    /// Get the latest block for a chain
    async fn get_latest_block_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError>;

    /// Get blocks for a chain within a height range
    async fn get_blocks_for_chain_range(
        &self,
        chain_id: &ChainId,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError>;

    /// Check if a blob exists
    async fn blob_exists(&self, blob_id: &BlobId) -> Result<bool, SqliteError>;

    /// Check if a block exists
    async fn block_exists(&self, hash: &CryptoHash) -> Result<bool, SqliteError>;

    /// Get incoming bundles for a specific block
    async fn get_incoming_bundles_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, SqliteError>;

    /// Get posted messages for a specific bundle
    async fn get_posted_messages_for_bundle(
        &self,
        bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, SqliteError>;

    /// Get all bundles from a specific origin chain
    async fn get_bundles_from_origin_chain(
        &self,
        origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, SqliteError>;
}
