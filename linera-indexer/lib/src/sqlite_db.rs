// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing blocks and blobs.

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{IncomingBundle, MessageAction, PostedMessage};
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
    Serialization(String),
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

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS incoming_bundles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_hash TEXT NOT NULL,
                bundle_index INTEGER NOT NULL,
                origin_chain_id TEXT NOT NULL,
                action TEXT NOT NULL,
                source_height INTEGER NOT NULL,
                source_timestamp INTEGER NOT NULL,
                source_cert_hash TEXT NOT NULL,
                transaction_index INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_hash) REFERENCES blocks(hash)
            );
            
            CREATE INDEX IF NOT EXISTS idx_incoming_bundles_block_hash ON incoming_bundles(block_hash);
            CREATE INDEX IF NOT EXISTS idx_incoming_bundles_origin_chain ON incoming_bundles(origin_chain_id);
            CREATE INDEX IF NOT EXISTS idx_incoming_bundles_action ON incoming_bundles(action);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS posted_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bundle_id INTEGER NOT NULL,
                message_index INTEGER NOT NULL,
                authenticated_signer TEXT,
                grant_amount INTEGER NOT NULL,
                refund_grant_to TEXT,
                message_kind TEXT NOT NULL,
                message_data BLOB NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (bundle_id) REFERENCES incoming_bundles(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_posted_messages_bundle_id ON posted_messages(bundle_id);
            CREATE INDEX IF NOT EXISTS idx_posted_messages_kind ON posted_messages(message_kind);
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

    /// Insert an incoming bundle within a transaction and return the bundle ID
    async fn insert_incoming_bundle_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        bundle_index: usize,
        incoming_bundle: &IncomingBundle,
    ) -> Result<i64, SqliteError> {
        let block_hash_str = block_hash.to_string();
        let origin_chain_str = incoming_bundle.origin.to_string();
        let action_str = match incoming_bundle.action {
            MessageAction::Accept => "Accept",
            MessageAction::Reject => "Reject",
        };
        let source_cert_hash_str = incoming_bundle.bundle.certificate_hash.to_string();

        let result = sqlx::query(
            r#"
            INSERT INTO incoming_bundles 
            (block_hash, bundle_index, origin_chain_id, action, source_height, source_timestamp, source_cert_hash, transaction_index)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#
        )
        .bind(&block_hash_str)
        .bind(bundle_index as i64)
        .bind(&origin_chain_str)
        .bind(action_str)
        .bind(incoming_bundle.bundle.height.0 as i64)
        .bind(incoming_bundle.bundle.timestamp.micros() as i64)
        .bind(&source_cert_hash_str)
        .bind(incoming_bundle.bundle.transaction_index as i64)
        .execute(&mut **tx)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Insert a posted message within a transaction
    async fn insert_posted_message_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        bundle_id: i64,
        message: &PostedMessage,
    ) -> Result<(), SqliteError> {
        let authenticated_signer_data = message
            .authenticated_signer
            .as_ref()
            .map(bincode::serialize)
            .transpose()
            .map_err(|e| {
                SqliteError::Serialization(format!(
                    "Failed to serialize authenticated_signer: {}",
                    e
                ))
            })?;

        let refund_grant_to_data = message
            .refund_grant_to
            .as_ref()
            .map(bincode::serialize)
            .transpose()
            .map_err(|e| {
                SqliteError::Serialization(format!("Failed to serialize refund_grant_to: {}", e))
            })?;

        let message_kind_str = format!("{:?}", message.kind);
        let message_data = bincode::serialize(&message.message).map_err(|e| {
            SqliteError::Serialization(format!("Failed to serialize message: {}", e))
        })?;

        sqlx::query(
            r#"
            INSERT INTO posted_messages 
            (bundle_id, message_index, authenticated_signer, grant_amount, refund_grant_to, message_kind, message_data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#
        )
        .bind(bundle_id)
        .bind(message.index as i64)
        .bind(authenticated_signer_data)
        .bind(u128::from(message.grant) as i64)
        .bind(refund_grant_to_data)
        .bind(&message_kind_str)
        .bind(&message_data)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Extract and store incoming bundles from a ConfirmedBlockCertificate
    async fn store_incoming_bundles_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        for (bundle_index, incoming_bundle) in incoming_bundles.iter().enumerate() {
            let bundle_id = self
                .insert_incoming_bundle_tx(tx, block_hash, bundle_index, incoming_bundle)
                .await?;

            for message in &incoming_bundle.bundle.messages {
                self.insert_posted_message_tx(tx, bundle_id, message)
                    .await?;
            }
        }
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
                let hash = hash_str
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid hash format".to_string()))?;
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
            let hash = hash_str
                .parse()
                .map_err(|_| SqliteError::Serialization("Invalid hash format".to_string()))?;
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

    /// Atomically store a block with its required blobs and incoming bundles
    /// This is the high-level API that manages the transaction internally
    pub async fn store_block_with_blobs_and_bundles(
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
        tx.commit().await?;

        Ok(())
    }

    /// Get incoming bundles for a specific block
    pub async fn get_incoming_bundles_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, SqliteError> {
        let block_hash_str = block_hash.to_string();
        let rows = sqlx::query(
            r#"
            SELECT id, bundle_index, origin_chain_id, action, source_height, 
                   source_timestamp, source_cert_hash, transaction_index
            FROM incoming_bundles 
            WHERE block_hash = ?1 
            ORDER BY bundle_index ASC
            "#,
        )
        .bind(&block_hash_str)
        .fetch_all(&self.pool)
        .await?;

        let mut bundles = Vec::new();
        for row in rows {
            let bundle_id: i64 = row.get("id");
            let bundle_info = IncomingBundleInfo {
                bundle_index: row.get::<i64, _>("bundle_index") as usize,
                origin_chain_id: row
                    .get::<String, _>("origin_chain_id")
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid chain ID".to_string()))?,
                action: match row.get::<String, _>("action").as_str() {
                    "Accept" => MessageAction::Accept,
                    "Reject" => MessageAction::Reject,
                    _ => return Err(SqliteError::Serialization("Invalid action".to_string())),
                },
                source_height: BlockHeight(row.get::<i64, _>("source_height") as u64),
                source_timestamp: Timestamp::from(row.get::<i64, _>("source_timestamp") as u64),
                source_cert_hash: row
                    .get::<String, _>("source_cert_hash")
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid cert hash".to_string()))?,
                transaction_index: row.get::<i64, _>("transaction_index") as u32,
            };
            bundles.push((bundle_id, bundle_info));
        }
        Ok(bundles)
    }

    /// Get posted messages for a specific bundle
    pub async fn get_posted_messages_for_bundle(
        &self,
        bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, SqliteError> {
        let rows = sqlx::query(
            r#"
            SELECT message_index, authenticated_signer, grant_amount, refund_grant_to, 
                   message_kind, message_data
            FROM posted_messages 
            WHERE bundle_id = ?1 
            ORDER BY message_index ASC
            "#,
        )
        .bind(bundle_id)
        .fetch_all(&self.pool)
        .await?;

        let mut messages = Vec::new();
        for row in rows {
            let message_info = PostedMessageInfo {
                message_index: row.get::<i64, _>("message_index") as u32,
                authenticated_signer_data: row.get("authenticated_signer"),
                grant_amount: row.get::<i64, _>("grant_amount") as u64,
                refund_grant_to_data: row.get("refund_grant_to"),
                message_kind: row.get("message_kind"),
                message_data: row.get("message_data"),
            };
            messages.push(message_info);
        }
        Ok(messages)
    }

    /// Get all bundles from a specific origin chain
    pub async fn get_bundles_from_origin_chain(
        &self,
        origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, SqliteError> {
        let origin_chain_str = origin_chain_id.to_string();
        let rows = sqlx::query(
            r#"
            SELECT block_hash, id, bundle_index, origin_chain_id, action, source_height, 
                   source_timestamp, source_cert_hash, transaction_index
            FROM incoming_bundles 
            WHERE origin_chain_id = ?1 
            ORDER BY source_height ASC, bundle_index ASC
            "#,
        )
        .bind(&origin_chain_str)
        .fetch_all(&self.pool)
        .await?;

        let mut bundles = Vec::new();
        for row in rows {
            let block_hash: CryptoHash = row
                .get::<String, _>("block_hash")
                .parse()
                .map_err(|_| SqliteError::Serialization("Invalid block hash".to_string()))?;
            let bundle_id: i64 = row.get("id");
            let bundle_info = IncomingBundleInfo {
                bundle_index: row.get::<i64, _>("bundle_index") as usize,
                origin_chain_id: row
                    .get::<String, _>("origin_chain_id")
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid chain ID".to_string()))?,
                action: match row.get::<String, _>("action").as_str() {
                    "Accept" => MessageAction::Accept,
                    "Reject" => MessageAction::Reject,
                    _ => return Err(SqliteError::Serialization("Invalid action".to_string())),
                },
                source_height: BlockHeight(row.get::<i64, _>("source_height") as u64),
                source_timestamp: Timestamp::from(row.get::<i64, _>("source_timestamp") as u64),
                source_cert_hash: row
                    .get::<String, _>("source_cert_hash")
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid cert hash".to_string()))?,
                transaction_index: row.get::<i64, _>("transaction_index") as u32,
            };
            bundles.push((block_hash, bundle_id, bundle_info));
        }
        Ok(bundles)
    }
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
    pub grant_amount: u64,
    pub refund_grant_to_data: Option<String>,
    pub message_kind: String,
    pub message_data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::Blob,
        identifiers::ChainId,
    };
    use linera_chain::data_types::MessageAction;

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
        db.store_block_with_blobs_and_bundles(
            &block_hash,
            &chain_id,
            height,
            &block_data,
            &blobs,
            vec![],
        )
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

    #[tokio::test]
    async fn test_incoming_bundles_storage_and_query() {
        let db = create_test_database().await;

        // Test that we can create the database schema with the new tables
        // The tables should be created in initialize_schema()

        // Verify the new tables exist by trying to query them
        let bundles_result = sqlx::query("SELECT COUNT(*) FROM incoming_bundles")
            .fetch_one(&db.pool)
            .await;
        assert!(
            bundles_result.is_ok(),
            "incoming_bundles table should exist"
        );

        let messages_result = sqlx::query("SELECT COUNT(*) FROM posted_messages")
            .fetch_one(&db.pool)
            .await;
        assert!(
            messages_result.is_ok(),
            "posted_messages table should exist"
        );

        // Test manual insertion to verify the schema works using string parsing
        let block_hash = CryptoHash::new(&TestString::new("test_block_hash"));
        let origin_chain = ChainId(CryptoHash::new(&TestString::new("origin_chain_id")));
        let source_cert_hash = CryptoHash::new(&TestString::new("source_cert_hash"));

        let mut tx = db.begin_transaction().await.unwrap();

        // First insert a test block that the bundle can reference
        let test_chain_id = ChainId(CryptoHash::new(&TestString::new("test_chain_id")));
        let test_height = 100_i64;
        let test_block_data = b"test_block_data".to_vec();

        sqlx::query("INSERT INTO blocks (hash, chain_id, height, data) VALUES (?1, ?2, ?3, ?4)")
            .bind(block_hash.to_string())
            .bind(test_chain_id.to_string())
            .bind(test_height)
            .bind(&test_block_data)
            .execute(&mut *tx)
            .await
            .expect("Should be able to insert test block");

        // Insert a test incoming bundle
        let bundle_result = sqlx::query(
            r#"
            INSERT INTO incoming_bundles 
            (block_hash, bundle_index, origin_chain_id, action, source_height, source_timestamp, source_cert_hash, transaction_index)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#
        )
        .bind(block_hash.to_string())
        .bind(0_i64)
        .bind(origin_chain.to_string())
        .bind("Accept")
        .bind(10_i64)
        .bind(1234567890_i64)
        .bind(source_cert_hash.to_string())
        .bind(2_i64)
        .execute(&mut *tx)
        .await;

        let bundle_id = bundle_result
            .expect("Should be able to insert into incoming_bundles")
            .last_insert_rowid();

        // Insert a test posted message
        let message_result = sqlx::query(
            r#"
            INSERT INTO posted_messages 
            (bundle_id, message_index, authenticated_signer, grant_amount, refund_grant_to, message_kind, message_data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#
        )
        .bind(bundle_id)
        .bind(0_i64)
        .bind(None::<Vec<u8>>)
        .bind(1000_i64)
        .bind(None::<Vec<u8>>)
        .bind("Protected")
        .bind(b"test_message_data".to_vec())
        .execute(&mut *tx)
        .await;

        assert_matches!(
            message_result,
            Ok(_),
            "Should be able to insert into posted_messages"
        );

        tx.commit().await.unwrap();

        // Test the query methods
        let bundles = db
            .get_incoming_bundles_for_block(&block_hash)
            .await
            .unwrap();
        assert_eq!(bundles.len(), 1);

        let (queried_bundle_id, bundle_info) = &bundles[0];
        assert_eq!(bundle_info.bundle_index, 0);
        assert_eq!(bundle_info.origin_chain_id, origin_chain);
        assert_eq!(bundle_info.action, MessageAction::Accept);
        assert_eq!(
            bundle_info.source_height,
            linera_base::data_types::BlockHeight(10)
        );
        assert_eq!(bundle_info.transaction_index, 2);

        let messages = db
            .get_posted_messages_for_bundle(*queried_bundle_id)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        let message_info = &messages[0];
        assert_eq!(message_info.message_index, 0);
        assert_eq!(message_info.grant_amount, 1000);
        assert_eq!(message_info.message_kind, "Protected");
        assert!(message_info.authenticated_signer_data.is_none());
        assert!(message_info.refund_grant_to_data.is_none());
        assert_eq!(message_info.message_data, b"test_message_data");

        // Test querying by origin chain
        let origin_bundles = db
            .get_bundles_from_origin_chain(&origin_chain)
            .await
            .unwrap();
        assert_eq!(origin_bundles.len(), 1);
        assert_eq!(origin_bundles[0].0, block_hash);
        assert_eq!(origin_bundles[0].1, *queried_bundle_id);
    }
}
