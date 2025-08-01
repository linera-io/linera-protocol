// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing blocks and blobs.

use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use sqlx::{
    sqlite::{SqlitePool, SqlitePoolOptions},
    Row, Sqlite, Transaction,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqliteError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bcs::Error),
    #[error("Block not found: {0}")]
    BlockNotFound(CryptoHash),
    #[error("Blob not found: {0}")]
    BlobNotFound(BlobId),
}

pub struct SqliteDatabase {
    pool: SqlitePool,
}

impl SqliteDatabase {
    /// Create a new SQLite database connection
    pub async fn new(database_url: &str) -> Result<Self, SqliteError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(SqliteError::Database)?;
        let db = Self { pool };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Initialize the database schema
    async fn initialize_schema(&self) -> Result<(), SqliteError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blocks (
                hash TEXT PRIMARY KEY NOT NULL,
                chain_id TEXT NOT NULL,
                height INTEGER NOT NULL,
                data BLOB NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_blocks_chain_height ON blocks(chain_id, height);
            CREATE INDEX IF NOT EXISTS idx_blocks_chain_id ON blocks(chain_id);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blobs (
                hash TEXT PRIMARY KEY NOT NULL,
                type TEXT NOT NULL,
                data BLOB NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Start a new transaction
    async fn begin_transaction(&self) -> Result<Transaction<'_, Sqlite>, SqliteError> {
        Ok(self.pool.begin().await?)
    }

    /// Insert a blob within a transaction
    async fn insert_blob_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        blob_id: &BlobId,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        let blob_id_str = blob_id.hash.to_string();
        let blob_type = blob_id.blob_type.to_string();
        sqlx::query("INSERT OR IGNORE INTO blobs (hash, type, data) VALUES (?1, ?2, ?3)")
            .bind(&blob_id_str)
            .bind(&blob_type)
            .bind(data)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Insert a block within a transaction
    async fn insert_block_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        let hash_str = hash.to_string();
        let chain_id_str = chain_id.to_string();
        sqlx::query(
            "INSERT OR REPLACE INTO blocks (hash, chain_id, height, data) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&hash_str)
        .bind(&chain_id_str)
        .bind(height.0 as i64)
        .bind(data)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Get a block by hash
    pub async fn get_block(&self, hash: &CryptoHash) -> Result<Vec<u8>, SqliteError> {
        let hash_str = hash.to_string();
        let row = sqlx::query("SELECT data FROM blocks WHERE hash = ?1")
            .bind(&hash_str)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => Ok(row.get("data")),
            None => Err(SqliteError::BlockNotFound(*hash)),
        }
    }

    /// Get a blob by blob_id
    pub async fn get_blob(&self, blob_id: &BlobId) -> Result<Vec<u8>, SqliteError> {
        let blob_id_str = blob_id.hash.to_string();
        let row = sqlx::query("SELECT data FROM blobs WHERE hash = ?1")
            .bind(&blob_id_str)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(row) => Ok(row.get("data")),
            None => Err(SqliteError::BlobNotFound(*blob_id)),
        }
    }

    /// Get the latest block for a chain
    pub async fn get_latest_block_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        let chain_id_str = chain_id.to_string();
        let row = sqlx::query(
            "SELECT hash, height, data FROM blocks WHERE chain_id = ?1 ORDER BY height DESC LIMIT 1"
        )
        .bind(&chain_id_str)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let hash_str: String = row.get("hash");
                let height: i64 = row.get("height");
                let data: Vec<u8> = row.get("data");
                let hash = hash_str.parse().map_err(|_| {
                    SqliteError::Serialization(bcs::Error::Custom(
                        "Invalid hash format".to_string(),
                    ))
                })?;
                Ok(Some((hash, BlockHeight(height as u64), data)))
            }
            None => Ok(None),
        }
    }

    /// Get blocks for a chain within a height range
    pub async fn get_blocks_for_chain_range(
        &self,
        chain_id: &ChainId,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        let chain_id_str = chain_id.to_string();
        let rows = sqlx::query(
            "SELECT hash, height, data FROM blocks WHERE chain_id = ?1 AND height >= ?2 AND height <= ?3 ORDER BY height ASC"
        )
        .bind(&chain_id_str)
        .bind(start_height.0 as i64)
        .bind(end_height.0 as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut result = Vec::new();
        for row in rows {
            let hash_str: String = row.get("hash");
            let height: i64 = row.get("height");
            let data: Vec<u8> = row.get("data");
            let hash = hash_str.parse().map_err(|_| {
                SqliteError::Serialization(bcs::Error::Custom("Invalid hash format".to_string()))
            })?;
            result.push((hash, BlockHeight(height as u64), data));
        }
        Ok(result)
    }

    /// Check if a blob exists
    pub async fn blob_exists(&self, blob_id: &BlobId) -> Result<bool, SqliteError> {
        let blob_id_str = blob_id.hash.to_string();
        let row = sqlx::query("SELECT 1 FROM blobs WHERE hash = ?1 LIMIT 1")
            .bind(&blob_id_str)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Check if a block exists
    pub async fn block_exists(&self, hash: &CryptoHash) -> Result<bool, SqliteError> {
        let hash_str = hash.to_string();
        let row = sqlx::query("SELECT 1 FROM blocks WHERE hash = ?1 LIMIT 1")
            .bind(&hash_str)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// Atomically store a block with its required blobs
    /// This is the high-level API that manages the transaction internally
    pub async fn store_block_with_blobs(
        &self,
        block_hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        block_data: &[u8],
        blobs: &[(BlobId, Vec<u8>)],
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

        // Commit transaction - this is the only point where data becomes visible
        tx.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use linera_base::data_types::Blob;

    use crate::{grpc_server::IndexerGrpcServer, sqlite_db::SqliteDatabase};

    async fn create_test_database() -> SqliteDatabase {
        SqliteDatabase::new("sqlite::memory:")
            .await
            .expect("Failed to create test database")
    }

    #[tokio::test]
    async fn test_sqlite_database_operations() {
        let db = create_test_database().await;

        // Test blob storage
        let blob = Blob::new_data(b"test blob content".to_vec());
        let blob_hash = blob.id();
        let blob_data = bincode::serialize(&blob).unwrap();

        let mut tx = db.begin_transaction().await.unwrap();
        db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Verify blob was stored
        let retrieved_blob_data = db.get_blob(&blob_hash).await.unwrap();
        assert_eq!(blob_data, retrieved_blob_data);

        // Test block storage (we'd need to create a proper ConfirmedBlockCertificate here)
        // For now, just test that the database operations work
    }

    #[tokio::test]
    async fn test_indexer_server_creation() {
        let db = create_test_database().await;
        let _server = IndexerGrpcServer::new(db);

        // Test that we can create the server with a non-cloneable database
        // This verifies the Arc<SqliteDatabase> approach works
    }

    #[tokio::test]
    async fn test_atomic_transaction_behavior() {
        let db = create_test_database().await;

        // Test that failed transactions are rolled back
        let blob = Blob::new_data(b"test content".to_vec());
        let blob_hash = blob.id();
        let blob_data = bincode::serialize(&blob).unwrap();

        // Start transaction but don't commit
        {
            let mut tx = db.begin_transaction().await.unwrap();
            db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
                .await
                .unwrap();
            // tx is dropped here without commit, should rollback
        }

        // Verify blob was not stored
        assert!(db.get_blob(&blob_hash).await.is_err());

        // Now test successful commit
        let mut tx = db.begin_transaction().await.unwrap();
        db.insert_blob_tx(&mut tx, &blob_hash, &blob_data)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Verify blob was stored
        let retrieved = db.get_blob(&blob_hash).await.unwrap();
        assert_eq!(blob_data, retrieved);
    }

    #[tokio::test]
    async fn test_high_level_atomic_api() {
        let db = create_test_database().await;

        // Create test data using simple hashes for testing
        let blob1 = Blob::new_data(b"test blob 1".to_vec());
        let blob2 = Blob::new_data(b"test blob 2".to_vec());
        let blob1_data = bincode::serialize(&blob1).unwrap();
        let blob2_data = bincode::serialize(&blob2).unwrap();

        // Use blob hashes for test IDs (simpler than creating proper ones)
        let block_hash = linera_base::crypto::CryptoHash::new(blob1.content());
        let chain_id = linera_base::identifiers::ChainId(linera_base::crypto::CryptoHash::new(
            blob2.content(),
        ));
        let height = linera_base::data_types::BlockHeight(1);
        let block_data = b"fake block data".to_vec();

        let blobs = vec![
            (blob1.id(), blob1_data.clone()),
            (blob2.id(), blob2_data.clone()),
        ];

        // Test atomic storage of block with blobs
        db.store_block_with_blobs(&block_hash, &chain_id, height, &block_data, &blobs)
            .await
            .unwrap();

        // Verify block was stored
        let retrieved_block = db.get_block(&block_hash).await.unwrap();
        assert_eq!(block_data, retrieved_block);

        // Verify blobs were stored
        let retrieved_blob1 = db.get_blob(&blob1.id()).await.unwrap();
        let retrieved_blob2 = db.get_blob(&blob2.id()).await.unwrap();
        assert_eq!(blob1_data, retrieved_blob1);
        assert_eq!(blob2_data, retrieved_blob2);
    }
}
