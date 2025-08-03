// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mock database implementations for testing.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        RwLock,
    },
};

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::IncomingBundle;

use crate::{
    database_trait::{DatabaseTransaction, IndexerDatabase},
    sqlite_db::{IncomingBundleInfo, PostedMessageInfo, SqliteError},
};

/// Mock database that can be configured to fail at different points
pub struct MockFailingDatabase {
    /// Whether to fail when beginning transactions
    pub fail_begin_transaction: AtomicBool,
    /// Whether to fail when inserting blobs
    pub fail_insert_blob: AtomicBool,
    /// Whether to fail when inserting blocks
    pub fail_insert_block: AtomicBool,
    /// Whether to fail when storing incoming bundles
    pub fail_store_bundles: AtomicBool,
    /// Whether to fail when committing transactions
    pub fail_commit: AtomicBool,
}

impl MockFailingDatabase {
    pub fn new() -> Self {
        Self {
            fail_begin_transaction: AtomicBool::new(false),
            fail_insert_blob: AtomicBool::new(false),
            fail_insert_block: AtomicBool::new(false),
            fail_store_bundles: AtomicBool::new(false),
            fail_commit: AtomicBool::new(false),
        }
    }

    pub fn fail_begin_transaction(&self) {
        self.fail_begin_transaction.store(true, Ordering::SeqCst);
    }

    pub fn fail_insert_blob(&self) {
        self.fail_insert_blob.store(true, Ordering::SeqCst);
    }

    pub fn fail_insert_block(&self) {
        self.fail_insert_block.store(true, Ordering::SeqCst);
    }

    pub fn fail_store_bundles(&self) {
        self.fail_store_bundles.store(true, Ordering::SeqCst);
    }

    pub fn fail_commit(&self) {
        self.fail_commit.store(true, Ordering::SeqCst);
    }
}

impl Default for MockFailingDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IndexerDatabase for MockFailingDatabase {
    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, SqliteError> {
        if self.fail_begin_transaction.load(Ordering::SeqCst) {
            return Err(SqliteError::Serialization(
                "Mock: Failed to begin transaction".to_string(),
            ));
        }
        // We can't actually create a real transaction for a mock, so this will fail
        // but it's sufficient for testing the error path
        Err(SqliteError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        if self.fail_insert_blob.load(Ordering::SeqCst) {
            return Err(SqliteError::Serialization(
                "Mock: Failed to insert blob".to_string(),
            ));
        }
        Ok(())
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        if self.fail_insert_block.load(Ordering::SeqCst) {
            return Err(SqliteError::Serialization(
                "Mock: Failed to insert block".to_string(),
            ));
        }
        Ok(())
    }

    async fn store_incoming_bundles_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _block_hash: &CryptoHash,
        _incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        if self.fail_store_bundles.load(Ordering::SeqCst) {
            return Err(SqliteError::Serialization(
                "Mock: Failed to store bundles".to_string(),
            ));
        }
        Ok(())
    }

    async fn commit_transaction(&self, _tx: DatabaseTransaction<'_>) -> Result<(), SqliteError> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(SqliteError::Serialization(
                "Mock: Failed to commit transaction".to_string(),
            ));
        }
        Ok(())
    }

    async fn get_block(&self, _hash: &CryptoHash) -> Result<Vec<u8>, SqliteError> {
        Err(SqliteError::Serialization(
            "Mock: get_block not implemented".to_string(),
        ))
    }

    async fn get_blob(&self, _blob_id: &BlobId) -> Result<Vec<u8>, SqliteError> {
        Err(SqliteError::Serialization(
            "Mock: get_blob not implemented".to_string(),
        ))
    }

    async fn get_latest_block_for_chain(
        &self,
        _chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        Err(SqliteError::Serialization(
            "Mock: get_latest_block_for_chain not implemented".to_string(),
        ))
    }

    async fn get_blocks_for_chain_range(
        &self,
        _chain_id: &ChainId,
        _start_height: BlockHeight,
        _end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        Err(SqliteError::Serialization(
            "Mock: get_blocks_for_chain_range not implemented".to_string(),
        ))
    }

    async fn blob_exists(&self, _blob_id: &BlobId) -> Result<bool, SqliteError> {
        Ok(false)
    }

    async fn block_exists(&self, _hash: &CryptoHash) -> Result<bool, SqliteError> {
        Ok(false)
    }

    async fn get_incoming_bundles_for_block(
        &self,
        _block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, SqliteError> {
        Ok(vec![])
    }

    async fn get_posted_messages_for_bundle(
        &self,
        _bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, SqliteError> {
        Ok(vec![])
    }

    async fn get_bundles_from_origin_chain(
        &self,
        _origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, SqliteError> {
        Ok(vec![])
    }
}

type Blocks = HashMap<CryptoHash, (ChainId, BlockHeight, Vec<u8>)>;

/// A more sophisticated mock that actually works for successful paths
/// and stores data in internal HashMaps for testing verification
pub struct MockSuccessDatabase {
    /// Storage for blobs: BlobId -> blob data
    blobs: RwLock<HashMap<BlobId, Vec<u8>>>,
    /// Storage for blocks: CryptoHash -> (ChainId, BlockHeight, block data)
    blocks: RwLock<Blocks>,
}

impl Default for MockSuccessDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSuccessDatabase {
    pub fn new() -> Self {
        Self {
            blobs: RwLock::new(HashMap::new()),
            blocks: RwLock::new(HashMap::new()),
        }
    }

    /// Get the count of stored blobs
    pub fn blob_count(&self) -> usize {
        self.blobs.read().unwrap().len()
    }

    /// Get the count of stored blocks
    pub fn block_count(&self) -> usize {
        self.blocks.read().unwrap().len()
    }
}

#[async_trait]
impl IndexerDatabase for MockSuccessDatabase {
    /// Override the high-level method to succeed and store data
    async fn store_block_with_blobs_and_bundles(
        &self,
        block_hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        block_data: &[u8],
        blobs: &[(BlobId, Vec<u8>)],
        _incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        // Store all blobs
        {
            let mut blob_storage = self.blobs.write().unwrap();
            for (blob_id, blob_data) in blobs {
                blob_storage.insert(*blob_id, blob_data.clone());
            }
        }

        // Store the block
        {
            let mut block_storage = self.blocks.write().unwrap();
            block_storage.insert(*block_hash, (*chain_id, height, block_data.to_vec()));
        }

        Ok(())
    }
    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, SqliteError> {
        // We can't create a real transaction, but for successful testing we can just
        // return an error that indicates we can't create a mock transaction
        Err(SqliteError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn store_incoming_bundles_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _block_hash: &CryptoHash,
        _incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: DatabaseTransaction<'_>) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn get_block(&self, _hash: &CryptoHash) -> Result<Vec<u8>, SqliteError> {
        Ok(vec![])
    }

    async fn get_blob(&self, _blob_id: &BlobId) -> Result<Vec<u8>, SqliteError> {
        Ok(vec![])
    }

    async fn get_latest_block_for_chain(
        &self,
        _chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        Ok(None)
    }

    async fn get_blocks_for_chain_range(
        &self,
        _chain_id: &ChainId,
        _start_height: BlockHeight,
        _end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        Ok(vec![])
    }

    async fn blob_exists(&self, _blob_id: &BlobId) -> Result<bool, SqliteError> {
        Ok(false)
    }

    async fn block_exists(&self, _hash: &CryptoHash) -> Result<bool, SqliteError> {
        Ok(false)
    }

    async fn get_incoming_bundles_for_block(
        &self,
        _block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, SqliteError> {
        Ok(vec![])
    }

    async fn get_posted_messages_for_bundle(
        &self,
        _bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, SqliteError> {
        Ok(vec![])
    }

    async fn get_bundles_from_origin_chain(
        &self,
        _origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, SqliteError> {
        Ok(vec![])
    }
}
