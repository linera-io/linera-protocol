// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing blocks and blobs.

mod consts;
#[cfg(test)]
mod tests;

use async_trait::async_trait;
use consts::{
    CREATE_BLOBS_TABLE, CREATE_BLOCKS_TABLE, CREATE_INCOMING_BUNDLES_TABLE,
    CREATE_POSTED_MESSAGES_TABLE,
};
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

use crate::db::{DatabaseTransaction, IndexerDatabase};

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
        sqlx::query(CREATE_BLOCKS_TABLE).execute(&self.pool).await?;

        sqlx::query(CREATE_BLOBS_TABLE).execute(&self.pool).await?;

        sqlx::query(CREATE_INCOMING_BUNDLES_TABLE)
            .execute(&self.pool)
            .await?;

        sqlx::query(CREATE_POSTED_MESSAGES_TABLE)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Start a new transaction
    async fn begin_transaction(&self) -> Result<Transaction<'_, Sqlite>, SqliteError> {
        Ok(self.pool.begin().await?)
    }

    /// Commit a transaction
    async fn commit_transaction(&self, tx: Transaction<'_, Sqlite>) -> Result<(), SqliteError> {
        tx.commit().await.map_err(SqliteError::Database)
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

#[async_trait]
impl IndexerDatabase for SqliteDatabase {
    async fn begin_transaction(&self) -> Result<DatabaseTransaction<'_>, SqliteError> {
        self.begin_transaction().await
    }

    async fn insert_blob_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        blob_id: &BlobId,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        self.insert_blob_tx(tx, blob_id, data).await
    }

    async fn insert_block_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        hash: &CryptoHash,
        chain_id: &ChainId,
        height: BlockHeight,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        self.insert_block_tx(tx, hash, chain_id, height, data).await
    }

    async fn store_incoming_bundles_tx(
        &self,
        tx: &mut DatabaseTransaction<'_>,
        block_hash: &CryptoHash,
        incoming_bundles: Vec<IncomingBundle>,
    ) -> Result<(), SqliteError> {
        self.store_incoming_bundles_tx(tx, block_hash, incoming_bundles)
            .await
    }

    async fn commit_transaction(&self, tx: DatabaseTransaction<'_>) -> Result<(), SqliteError> {
        self.commit_transaction(tx).await
    }

    async fn get_block(&self, hash: &CryptoHash) -> Result<Vec<u8>, SqliteError> {
        self.get_block(hash).await
    }

    async fn get_blob(&self, blob_id: &BlobId) -> Result<Vec<u8>, SqliteError> {
        self.get_blob(blob_id).await
    }

    async fn get_latest_block_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        self.get_latest_block_for_chain(chain_id).await
    }

    async fn get_blocks_for_chain_range(
        &self,
        chain_id: &ChainId,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Vec<u8>)>, SqliteError> {
        self.get_blocks_for_chain_range(chain_id, start_height, end_height)
            .await
    }

    async fn blob_exists(&self, blob_id: &BlobId) -> Result<bool, SqliteError> {
        self.blob_exists(blob_id).await
    }

    async fn block_exists(&self, hash: &CryptoHash) -> Result<bool, SqliteError> {
        self.block_exists(hash).await
    }

    async fn get_incoming_bundles_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(i64, IncomingBundleInfo)>, SqliteError> {
        self.get_incoming_bundles_for_block(block_hash).await
    }

    async fn get_posted_messages_for_bundle(
        &self,
        bundle_id: i64,
    ) -> Result<Vec<PostedMessageInfo>, SqliteError> {
        self.get_posted_messages_for_bundle(bundle_id).await
    }

    async fn get_bundles_from_origin_chain(
        &self,
        origin_chain_id: &ChainId,
    ) -> Result<Vec<(CryptoHash, i64, IncomingBundleInfo)>, SqliteError> {
        self.get_bundles_from_origin_chain(origin_chain_id).await
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
