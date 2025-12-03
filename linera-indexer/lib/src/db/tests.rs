// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mock database implementations for testing.

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};
use sqlx::Sqlite;

use crate::{
    db::{IncomingBundleInfo, IndexerDatabase, PostedMessageInfo},
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

impl From<bincode::Error> for MockDatabaseError {
    fn from(err: bincode::Error) -> Self {
        MockDatabaseError::Serialization(err.to_string())
    }
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

    type Transaction<'a> = sqlx::Transaction<'a, Sqlite>;

    async fn begin_transaction(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        // Always fail transaction creation for testing error paths
        Err(MockDatabaseError::Serialization(
            "Mock: Cannot create real transaction".to_string(),
        ))
    }

    async fn insert_blob_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _blob_id: &BlobId,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn insert_block_tx(
        &self,
        _tx: &mut Self::Transaction<'_>,
        _hash: &CryptoHash,
        _chain_id: &ChainId,
        _height: BlockHeight,
        _timestamp: Timestamp,
        _data: &[u8],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn commit_transaction(&self, _tx: Self::Transaction<'_>) -> Result<(), Self::Error> {
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
