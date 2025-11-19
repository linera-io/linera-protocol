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
use linera_chain::types::ConfirmedBlockCertificate;

use crate::db::{sqlite::SqliteError, IncomingBundleInfo, IndexerDatabase, PostedMessageInfo};

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

#[async_trait]
impl IndexerDatabase for MockFailingDatabase {
    type Error = SqliteError;
    type Transaction<'a> = ();

    async fn begin_transaction(&self) -> Result<Self::Transaction<'_>, SqliteError> {
        // Always fail transaction creation for testing error paths
        Err(SqliteError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _timestamp: Timestamp,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
        _block_hash: Option<CryptoHash>,
        _transaction_index: Option<u32>,
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: Self::Transaction<'_>) -> Result<(), SqliteError> {
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
    type Error = SqliteError;
    type Transaction<'a> = ();

    /// Override the high-level method to succeed and store data
    async fn store_block_with_blobs(
        &self,
        block_cert: &ConfirmedBlockCertificate,
        pending_blobs: &HashMap<BlobId, Vec<u8>>,
    ) -> Result<(), SqliteError>
    where
        SqliteError: From<bincode::Error>,
    {
        // Extract block metadata
        let block_hash = block_cert.hash();
        let chain_id = block_cert.inner().chain_id();
        let height = block_cert.inner().height();

        // Serialize the block certificate
        let block_data = bincode::serialize(block_cert).unwrap();

        // Store standalone blobs
        {
            let mut blob_storage = self.blobs.write().unwrap();
            for (blob_id, blob_data) in pending_blobs {
                blob_storage.insert(*blob_id, blob_data.clone());
            }
        }

        // Extract and store blobs from the block body
        let block = block_cert.inner().block();
        {
            let mut blob_storage = self.blobs.write().unwrap();
            for transaction_blobs in block.body.blobs.iter() {
                for blob in transaction_blobs {
                    let blob_id = blob.id();
                    let blob_data = bincode::serialize(blob).unwrap();
                    blob_storage.insert(blob_id, blob_data);
                }
            }
        }

        // Store the block
        {
            let mut block_storage = self.blocks.write().unwrap();
            block_storage.insert(block_hash, (chain_id, height, block_data));
        }

        Ok(())
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction<'_>, SqliteError> {
        // We can't create a real transaction, but for successful testing we can just
        // return an error that indicates we can't create a mock transaction
        Err(SqliteError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _timestamp: Timestamp,
        _data: &[u8],
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
        _block_hash: Option<CryptoHash>,
        _transaction_index: Option<u32>,
    ) -> Result<(), SqliteError> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: Self::Transaction<'_>) -> Result<(), SqliteError> {
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
