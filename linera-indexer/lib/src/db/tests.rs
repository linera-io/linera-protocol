// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mock database implementations for testing.

use std::{collections::HashMap, sync::RwLock};

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};

use crate::{
    db::{DatabaseTransaction, IncomingBundleInfo, IndexerDatabase, PostedMessageInfo},
    grpc::ProcessingError,
};

/// Mock database that fails on transaction operations for testing error paths
pub struct MockFailingDatabase;

impl MockFailingDatabase {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockFailingDatabase {
    fn default() -> Self {
        Self::new()
    }
}

pub enum MockDatabaseError {
    Serialization(String),
}

impl From<MockDatabaseError> for ProcessingError {
    fn from(error: MockDatabaseError) -> Self {
        match error {
            MockDatabaseError::Serialization(msg) => ProcessingError::BlockDeserialization(msg),
        }
    }
}

#[async_trait]
impl IndexerDatabase for MockFailingDatabase {
    type Error = MockDatabaseError;

    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, Self::Error> {
        // Always fail transaction creation for testing error paths
        Err(MockDatabaseError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _timestamp: Timestamp,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: DatabaseTransaction<'_>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get_block(&self, _hash: &CryptoHash) -> Result<Vec<u8>, Self::Error> {
        Err(MockDatabaseError::Serialization(
            "Mock: get_block not implemented".to_string(),
        ))
    }

    async fn get_blob(&self, _blob_id: &BlobId) -> Result<Vec<u8>, Self::Error> {
        Err(MockDatabaseError::Serialization(
            "Mock: get_blob not implemented".to_string(),
        ))
    }

    async fn get_latest_block_for_chain(
        &self,
        _chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error> {
        Err(MockDatabaseError::Serialization(
            "Mock: get_latest_block_for_chain not implemented".to_string(),
        ))
    }

    async fn get_blocks_for_chain_range(
        &self,
        _chain_id: &ChainId,
        _start_height: BlockHeight,
        _end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error> {
        Err(MockDatabaseError::Serialization(
            "Mock: get_blocks_for_chain_range not implemented".to_string(),
        ))
    }

    async fn blob_exists(&self, _blob_id: &BlobId) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn block_exists(&self, _hash: &CryptoHash) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn get_incoming_bundles_for_block(
        &self,
        _block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, Self::Error> {
        Ok(vec![])
    }

    async fn get_posted_messages_for_bundle(
        &self,
        _bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, Self::Error> {
        Ok(vec![])
    }

    async fn get_bundles_from_origin_chain(
        &self,
        _origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, Self::Error> {
        Ok(vec![])
    }
}

type Blocks = HashMap<CryptoHash, (ChainId, BlockHeight, Timestamp, Vec<u8>)>;

/// A more sophisticated mock that actually works for successful paths
/// and stores data in internal HashMaps for testing verification
pub struct MockSuccessDatabase {
    /// Storage for blobs: BlobId -> blob data
    blobs: RwLock<HashMap<BlobId, Vec<u8>>>,
    /// Storage for blocks: CryptoHash -> (ChainId, BlockHeight, Timestamp, block data)
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
    type Error = MockDatabaseError;

    /// Override the high-level method to succeed and store data
    async fn store_block_with_blobs(
        &self,
        block_hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        timestamp: Timestamp,
        block_data: &[u8],
        blobs: &[(BlobId, Vec<u8>)],
    ) -> Result<(), Self::Error> {
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
            block_storage.insert(
                *block_hash,
                (*chain_id, height, timestamp, block_data.to_vec()),
            );
        }

        Ok(())
    }
    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, Self::Error> {
        // We can't create a real transaction, but for successful testing we can just
        // return an error that indicates we can't create a mock transaction
        Err(MockDatabaseError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut DatabaseTransaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _timestamp: Timestamp,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: DatabaseTransaction<'_>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get_block(&self, _hash: &CryptoHash) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }

    async fn get_blob(&self, _blob_id: &BlobId) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }

    async fn get_latest_block_for_chain(
        &self,
        _chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error> {
        Ok(None)
    }

    async fn get_blocks_for_chain_range(
        &self,
        _chain_id: &ChainId,
        _start_height: BlockHeight,
        _end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, Self::Error> {
        Ok(vec![])
    }

    async fn blob_exists(&self, _blob_id: &BlobId) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn block_exists(&self, _hash: &CryptoHash) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn get_incoming_bundles_for_block(
        &self,
        _block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, Self::Error> {
        Ok(vec![])
    }

    async fn get_posted_messages_for_bundle(
        &self,
        _bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, Self::Error> {
        Ok(vec![])
    }

    async fn get_bundles_from_origin_chain(
        &self,
        _origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, Self::Error> {
        Ok(vec![])
    }
}
