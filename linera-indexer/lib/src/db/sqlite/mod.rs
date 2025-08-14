// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing blocks and blobs.

mod consts;
#[cfg(test)]
mod tests;

use std::str::FromStr;

use async_trait::async_trait;
use consts::{
    CREATE_BLOBS_TABLE, CREATE_BLOCKS_TABLE, CREATE_EVENTS_TABLE, CREATE_INCOMING_BUNDLES_TABLE,
    CREATE_OPERATIONS_TABLE, CREATE_ORACLE_RESPONSES_TABLE, CREATE_OUTGOING_MESSAGES_TABLE,
    CREATE_POSTED_MESSAGES_TABLE,
};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Event, OracleResponse, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    block::Block,
    data_types::{IncomingBundle, MessageAction, PostedMessage},
};
use linera_execution::{
    Message, MessageKind, Operation, OutgoingMessage, SystemMessage, SystemOperation,
};
use sqlx::{
    sqlite::{SqlitePool, SqlitePoolOptions},
    Row, Sqlite, Transaction,
};
use thiserror::Error;

use crate::db::{DatabaseTransaction, IncomingBundleInfo, IndexerDatabase, PostedMessageInfo};

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

/// Classification result for a Message with denormalized SystemMessage fields
#[derive(Debug)]
struct MessageClassification {
    message_type: String,
    application_id: Option<String>,
    system_message_type: Option<String>,
    system_target: Option<String>,
    system_amount: Option<Amount>,
    system_source: Option<String>,
    system_owner: Option<String>,
    system_recipient: Option<String>,
}

impl SqliteDatabase {
    /// Create a new SQLite database connection
    pub async fn new(database_url: &str) -> Result<Self, SqliteError> {
        if !database_url.contains("memory") {
            match std::fs::exists(database_url) {
                Ok(true) => {
                    tracing::info!(?database_url, "opening existing SQLite database");
                }
                Ok(false) => {
                    tracing::info!(?database_url, "creating new SQLite database");
                    // Create the database file if it doesn't exist
                    std::fs::File::create(database_url).unwrap_or_else(|e| {
                        panic!(
                            "failed to create SQLite database file: {}, error: {}",
                            database_url, e
                        )
                    });
                }
                Err(e) => {
                    panic!(
                        "failed to check SQLite database existence. file: {}, error: {}",
                        database_url, e
                    )
                }
            }
        }
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
        // Create core tables
        sqlx::query(CREATE_BLOCKS_TABLE).execute(&self.pool).await?;
        sqlx::query(CREATE_BLOBS_TABLE).execute(&self.pool).await?;

        // Create denormalized tables for block data
        sqlx::query(CREATE_OPERATIONS_TABLE)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_OUTGOING_MESSAGES_TABLE)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_EVENTS_TABLE).execute(&self.pool).await?;
        sqlx::query(CREATE_ORACLE_RESPONSES_TABLE)
            .execute(&self.pool)
            .await?;

        // Create existing message-related tables
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
        let blob_type = format!("{:?}", blob_id.blob_type);

        // For now, we don't have block_hash and application_id context here
        // These could be passed as optional parameters in the future
        sqlx::query("INSERT OR IGNORE INTO blobs (hash, blob_type, data) VALUES (?1, ?2, ?3)")
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
        timestamp: Timestamp,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        // Deserialize the block to extract denormalized data
        let block: Block = bincode::deserialize(data).map_err(|e| {
            SqliteError::Serialization(format!("Failed to deserialize block: {}", e))
        })?;

        // Count aggregated data
        let operation_count = block.body.operations().count();
        let incoming_bundle_count = block.body.incoming_bundles().count();
        let message_count = block.body.messages.iter().map(|v| v.len()).sum::<usize>();
        let event_count = block.body.events.iter().map(|v| v.len()).sum::<usize>();
        let blob_count = block.body.blobs.len();

        // Insert main block record with denormalized fields
        let hash_str = hash.to_string();
        let chain_id_str = chain_id.to_string();
        let state_hash_str = block.header.state_hash.to_string();
        let previous_block_hash_str = block.header.previous_block_hash.map(|h| h.to_string());
        let authenticated_signer_str = block.header.authenticated_signer.map(|s| s.to_string());

        sqlx::query(
            r#"
            INSERT OR REPLACE INTO blocks 
            (hash, chain_id, height, timestamp, epoch, state_hash, previous_block_hash, 
             authenticated_signer, operation_count, incoming_bundle_count, message_count, 
             event_count, blob_count, data) 
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
        )
        .bind(&hash_str)
        .bind(&chain_id_str)
        .bind(height.0 as i64)
        .bind(timestamp.micros() as i64)
        .bind(block.header.epoch.0 as i64)
        .bind(&state_hash_str)
        .bind(&previous_block_hash_str)
        .bind(&authenticated_signer_str)
        .bind(operation_count as i64)
        .bind(incoming_bundle_count as i64)
        .bind(message_count as i64)
        .bind(event_count as i64)
        .bind(blob_count as i64)
        .bind(data)
        .execute(&mut **tx)
        .await?;

        // Insert operations
        for (index, transaction) in block.body.transactions.iter().enumerate() {
            match transaction {
                linera_chain::data_types::Transaction::ExecuteOperation(operation) => {
                    self.insert_operation_tx(
                        tx,
                        hash,
                        index,
                        operation,
                        block.header.authenticated_signer,
                    )
                    .await?;
                }
                linera_chain::data_types::Transaction::ReceiveMessages(bundle) => {
                    let bundle_id = self
                        .insert_incoming_bundle_tx(tx, hash, index, bundle)
                        .await?;

                    for message in &bundle.bundle.messages {
                        self.insert_bundle_message_tx(tx, bundle_id, message)
                            .await?;
                    }
                }
            }
        }

        // Insert outgoing messages
        for (txn_index, messages) in block.body.messages.iter().enumerate() {
            for (msg_index, message) in messages.iter().enumerate() {
                self.insert_outgoing_message_tx(tx, hash, txn_index, msg_index, message)
                    .await?;
            }
        }

        // Insert events
        for (txn_index, events) in block.body.events.iter().enumerate() {
            for (event_index, event) in events.iter().enumerate() {
                self.insert_event_tx(tx, hash, txn_index, event_index, event)
                    .await?;
            }
        }

        // Insert oracle responses
        for (txn_index, responses) in block.body.oracle_responses.iter().enumerate() {
            for (response_index, response) in responses.iter().enumerate() {
                self.insert_oracle_response_tx(tx, hash, txn_index, response_index, response)
                    .await?;
            }
        }

        Ok(())
    }

    /// Insert an operation within a transaction
    async fn insert_operation_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        operation_index: usize,
        operation: &Operation,
        authenticated_signer: Option<linera_base::identifiers::AccountOwner>,
    ) -> Result<(), SqliteError> {
        let block_hash_str = block_hash.to_string();
        let authenticated_signer_str = authenticated_signer.map(|s| s.to_string());

        let (operation_type, application_id, system_operation_type) = match operation {
            Operation::System(sys_op) => {
                let sys_op_type = match sys_op.as_ref() {
                    SystemOperation::Transfer { .. } => "Transfer",
                    SystemOperation::Claim { .. } => "Claim",
                    SystemOperation::OpenChain { .. } => "OpenChain",
                    SystemOperation::CloseChain => "CloseChain",
                    SystemOperation::ChangeApplicationPermissions { .. } => {
                        "ChangeApplicationPermissions"
                    }
                    SystemOperation::CreateApplication { .. } => "CreateApplication",
                    SystemOperation::PublishModule { .. } => "PublishModule",
                    SystemOperation::PublishDataBlob { .. } => "PublishDataBlob",
                    SystemOperation::Admin(_) => "Admin",
                    SystemOperation::ProcessNewEpoch(_) => "ProcessNewEpoch",
                    SystemOperation::ProcessRemovedEpoch(_) => "ProcessRemovedEpoch",
                    SystemOperation::UpdateStreams(_) => "UpdateStreams",
                    SystemOperation::ChangeOwnership { .. } => "ChangeOwnership",
                    SystemOperation::VerifyBlob { .. } => "VerifyBlob",
                };
                ("System", None, Some(sys_op_type))
            }
            Operation::User { application_id, .. } => {
                ("User", Some(application_id.to_string()), None)
            }
        };

        let data = bincode::serialize(operation).map_err(|e| {
            SqliteError::Serialization(format!("Failed to serialize operation: {}", e))
        })?;

        sqlx::query(
            r#"
            INSERT INTO operations 
            (block_hash, operation_index, operation_type, application_id, system_operation_type, authenticated_signer, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
        )
        .bind(&block_hash_str)
        .bind(operation_index as i64)
        .bind(operation_type)
        .bind(application_id)
        .bind(system_operation_type)
        .bind(&authenticated_signer_str)
        .bind(&data)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Insert an outgoing message within a transaction
    async fn insert_outgoing_message_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        transaction_index: usize,
        message_index: usize,
        message: &OutgoingMessage,
    ) -> Result<(), SqliteError> {
        let block_hash_str = block_hash.to_string();
        let destination_chain_id_str = message.destination.to_string();
        let authenticated_signer_str = message.authenticated_signer.map(|s| s.to_string());
        let message_kind_str = Self::message_kind_to_string(&message.kind);

        let classification = Self::classify_message(&message.message);
        let data = Self::serialize_message(&message.message)?;

        sqlx::query(
            r#"
            INSERT INTO outgoing_messages 
            (block_hash, transaction_index, message_index, destination_chain_id, authenticated_signer, 
             grant_amount, message_kind, message_type, application_id, system_message_type,
             system_target, system_amount, system_source, system_owner, system_recipient, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            "#,
        )
        .bind(&block_hash_str)
        .bind(transaction_index as i64)
        .bind(message_index as i64)
        .bind(&destination_chain_id_str)
        .bind(&authenticated_signer_str)
        .bind(message.grant.to_string())
        .bind(&message_kind_str)
        .bind(classification.message_type)
        .bind(classification.application_id)
        .bind(classification.system_message_type)
        .bind(classification.system_target)
        .bind(classification.system_amount.map(|a| a.to_string()))
        .bind(classification.system_source)
        .bind(classification.system_owner)
        .bind(classification.system_recipient)
        .bind(&data)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Insert an event within a transaction
    async fn insert_event_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        transaction_index: usize,
        event_index: usize,
        event: &Event,
    ) -> Result<(), SqliteError> {
        let block_hash_str = block_hash.to_string();
        let stream_id_str = event.stream_id.to_string();

        sqlx::query(
            r#"
            INSERT INTO events 
            (block_hash, transaction_index, event_index, stream_id, stream_index, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )
        .bind(&block_hash_str)
        .bind(transaction_index as i64)
        .bind(event_index as i64)
        .bind(&stream_id_str)
        .bind(event.index as i64)
        .bind(&event.value)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Insert an oracle response within a transaction
    async fn insert_oracle_response_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        block_hash: &CryptoHash,
        transaction_index: usize,
        response_index: usize,
        response: &OracleResponse,
    ) -> Result<(), SqliteError> {
        let block_hash_str = block_hash.to_string();

        let (response_type, blob_hash, data): (&str, Option<String>, Option<Vec<u8>>) =
            match response {
                OracleResponse::Service(service_data) => {
                    ("Service", None, Some(service_data.clone()))
                }
                OracleResponse::Blob(blob_id) => ("Blob", Some(blob_id.hash.to_string()), None),
                OracleResponse::Http(http_response) => {
                    let serialized = bincode::serialize(http_response).map_err(|e| {
                        SqliteError::Serialization(format!(
                            "Failed to serialize HTTP response: {}",
                            e
                        ))
                    })?;
                    ("Http", None, Some(serialized))
                }
                OracleResponse::Assert => ("Assert", None, None),
                OracleResponse::Round(round) => {
                    let serialized = bincode::serialize(round).map_err(|e| {
                        SqliteError::Serialization(format!("Failed to serialize round: {}", e))
                    })?;
                    ("Round", None, Some(serialized))
                }
                OracleResponse::Event(stream_id, index) => {
                    let serialized = bincode::serialize(&(stream_id, index)).map_err(|e| {
                        SqliteError::Serialization(format!("Failed to serialize event: {}", e))
                    })?;
                    ("Event", None, Some(serialized))
                }
                OracleResponse::EventExists(event_exists) => {
                    let serialized = bincode::serialize(event_exists).map_err(|e| {
                        SqliteError::Serialization(format!(
                            "Failed to serialize event exists: {}",
                            e
                        ))
                    })?;
                    ("EventExists", None, Some(serialized))
                }
            };

        sqlx::query(
            r#"
            INSERT INTO oracle_responses 
            (block_hash, transaction_index, response_index, response_type, blob_hash, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )
        .bind(&block_hash_str)
        .bind(transaction_index as i64)
        .bind(response_index as i64)
        .bind(response_type)
        .bind(blob_hash)
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
    async fn insert_bundle_message_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        bundle_id: i64,
        message: &PostedMessage,
    ) -> Result<(), SqliteError> {
        let authenticated_signer_str = message.authenticated_signer.map(|s| s.to_string());
        let refund_grant_to = message.refund_grant_to.as_ref().map(|s| format!("{s}"));
        let message_kind_str = Self::message_kind_to_string(&message.kind);

        let classification = Self::classify_message(&message.message);
        let message_data = Self::serialize_message(&message.message)?;

        sqlx::query(
            r#"
            INSERT INTO posted_messages 
            (bundle_id, message_index, authenticated_signer, grant_amount, refund_grant_to, 
             message_kind, message_type, application_id, system_message_type,
             system_target, system_amount, system_source, system_owner, system_recipient, message_data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            "#
        )
        .bind(bundle_id)
        .bind(message.index as i64)
        .bind(authenticated_signer_str)
        .bind(message.grant.to_string())
        .bind(refund_grant_to)
        .bind(&message_kind_str)
        .bind(classification.message_type)
        .bind(classification.application_id)
        .bind(classification.system_message_type)
        .bind(classification.system_target)
        .bind(classification.system_amount.map(|a| a.to_string()))
        .bind(classification.system_source)
        .bind(classification.system_owner)
        .bind(classification.system_recipient)
        .bind(&message_data)
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
                grant_amount: row.get("grant_amount"),
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

    /// Get operations for a specific block
    pub async fn get_operations_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<(usize, Operation)>, SqliteError> {
        let block_hash_str = block_hash.to_string();
        let rows = sqlx::query(
            r#"
            SELECT operation_index, data
            FROM operations 
            WHERE block_hash = ?1 
            ORDER BY operation_index ASC
            "#,
        )
        .bind(&block_hash_str)
        .fetch_all(&self.pool)
        .await?;

        let mut operations = Vec::new();
        for row in rows {
            let index = row.get::<i64, _>("operation_index") as usize;
            let data: Vec<u8> = row.get("data");
            let operation: Operation = bincode::deserialize(&data).map_err(|e| {
                SqliteError::Serialization(format!("Failed to deserialize operation: {}", e))
            })?;
            operations.push((index, operation));
        }
        Ok(operations)
    }

    /// Get outgoing messages for a specific block
    pub async fn get_outgoing_messages_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<OutgoingMessage>, SqliteError> {
        let block_hash_str = block_hash.to_string();
        let rows = sqlx::query(
            r#"
            SELECT destination_chain_id, authenticated_signer, grant_amount, message_kind, data
            FROM outgoing_messages 
            WHERE block_hash = ?1 
            ORDER BY transaction_index, message_index ASC
            "#,
        )
        .bind(&block_hash_str)
        .fetch_all(&self.pool)
        .await?;

        let mut messages = Vec::new();
        for row in rows {
            let destination_str: String = row.get("destination_chain_id");
            let destination = destination_str
                .parse()
                .map_err(|_| SqliteError::Serialization("Invalid chain ID".to_string()))?;
            let authenticated_signer_str: Option<String> = row.get("authenticated_signer");
            let authenticated_signer = authenticated_signer_str.and_then(|s| s.parse().ok());
            let grant_amount: String = row.get("grant_amount");
            let grant = linera_base::data_types::Amount::from_str(grant_amount.as_str())
                .map_err(|_| SqliteError::Serialization("Invalid grant amount".to_string()))?;
            let kind_str: String = row.get("message_kind");
            let kind = Self::parse_message_kind(kind_str.as_str())?;
            let message_bytes: Vec<u8> = row.get("data");
            let message = Self::deserialize_message(message_bytes.as_slice())?;

            messages.push(OutgoingMessage {
                destination,
                authenticated_signer,
                grant,
                refund_grant_to: None, // This would need to be stored separately
                kind,
                message,
            });
        }
        Ok(messages)
    }

    /// Get events for a specific block
    pub async fn get_events_for_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<Event>, SqliteError> {
        let block_hash_str = block_hash.to_string();
        let rows = sqlx::query(
            r#"
            SELECT stream_id, stream_index, data
            FROM events 
            WHERE block_hash = ?1 
            ORDER BY transaction_index, event_index ASC
            "#,
        )
        .bind(&block_hash_str)
        .fetch_all(&self.pool)
        .await?;

        let mut events = Vec::new();
        for row in rows {
            let stream_id_str: String = row.get("stream_id");
            let stream_id = stream_id_str
                .parse()
                .map_err(|_| SqliteError::Serialization("Invalid stream ID".to_string()))?;
            let stream_index = row.get::<i64, _>("stream_index") as u32;
            let value: Vec<u8> = row.get("data");

            events.push(Event {
                stream_id,
                index: stream_index,
                value,
            });
        }
        Ok(events)
    }

    /// Query blocks with filters
    pub async fn query_blocks_with_filters(
        &self,
        chain_id: Option<&ChainId>,
        epoch: Option<u64>,
        min_operations: Option<usize>,
        min_messages: Option<usize>,
    ) -> Result<Vec<(CryptoHash, BlockHeight, Timestamp)>, SqliteError> {
        let mut query = String::from("SELECT hash, height, timestamp FROM blocks WHERE 1=1");
        let mut bindings = Vec::new();

        if let Some(chain_id) = chain_id {
            query.push_str(" AND chain_id = ?");
            bindings.push(chain_id.to_string());
        }

        if let Some(epoch) = epoch {
            query.push_str(" AND epoch = ?");
            bindings.push(epoch.to_string());
        }

        if let Some(min_ops) = min_operations {
            query.push_str(" AND operation_count >= ?");
            bindings.push(min_ops.to_string());
        }

        if let Some(min_msgs) = min_messages {
            query.push_str(" AND message_count >= ?");
            bindings.push(min_msgs.to_string());
        }

        query.push_str(" ORDER BY height DESC");

        let mut sql_query = sqlx::query(&query);
        for binding in bindings {
            sql_query = sql_query.bind(binding);
        }

        let rows = sql_query.fetch_all(&self.pool).await?;

        let mut results = Vec::new();
        for row in rows {
            let hash_str: String = row.get("hash");
            let hash = hash_str
                .parse()
                .map_err(|_| SqliteError::Serialization("Invalid hash".to_string()))?;
            let height = BlockHeight(row.get::<i64, _>("height") as u64);
            let timestamp = Timestamp::from(row.get::<i64, _>("timestamp") as u64);
            results.push((hash, height, timestamp));
        }
        Ok(results)
    }

    /// Get block summary (header fields without full data)
    pub async fn get_block_summary(
        &self,
        hash: &CryptoHash,
    ) -> Result<Option<BlockSummary>, SqliteError> {
        let hash_str = hash.to_string();
        let row = sqlx::query(
            r#"
            SELECT chain_id, height, timestamp, epoch, state_hash, previous_block_hash,
                   authenticated_signer, operation_count, incoming_bundle_count, 
                   message_count, event_count, blob_count
            FROM blocks 
            WHERE hash = ?1
            "#,
        )
        .bind(&hash_str)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let chain_id_str: String = row.get("chain_id");
                let chain_id = chain_id_str
                    .parse()
                    .map_err(|_| SqliteError::Serialization("Invalid chain ID".to_string()))?;

                Ok(Some(BlockSummary {
                    hash: *hash,
                    chain_id,
                    height: BlockHeight(row.get::<i64, _>("height") as u64),
                    timestamp: Timestamp::from(row.get::<i64, _>("timestamp") as u64),
                    epoch: row.get::<i64, _>("epoch") as u64,
                    state_hash: row.get::<String, _>("state_hash").parse().map_err(|_| {
                        SqliteError::Serialization("Invalid state hash".to_string())
                    })?,
                    previous_block_hash: row
                        .get::<Option<String>, _>("previous_block_hash")
                        .and_then(|s| s.parse().ok()),
                    authenticated_signer: row.get::<Option<String>, _>("authenticated_signer"),
                    operation_count: row.get::<i64, _>("operation_count") as usize,
                    incoming_bundle_count: row.get::<i64, _>("incoming_bundle_count") as usize,
                    message_count: row.get::<i64, _>("message_count") as usize,
                    event_count: row.get::<i64, _>("event_count") as usize,
                    blob_count: row.get::<i64, _>("blob_count") as usize,
                }))
            }
            None => Ok(None),
        }
    }

    /// Classify a Message into database fields with denormalized SystemMessage fields
    fn classify_message(message: &Message) -> MessageClassification {
        match message {
            Message::System(sys_msg) => {
                let (
                    sys_msg_type,
                    system_target,
                    system_amount,
                    system_source,
                    system_owner,
                    system_recipient,
                ) = match sys_msg {
                    SystemMessage::Credit {
                        target,
                        amount,
                        source,
                    } => (
                        "Credit",
                        Some(target.to_string()),
                        Some(*amount),
                        Some(source.to_string()),
                        None,
                        None,
                    ),
                    SystemMessage::Withdraw {
                        owner,
                        amount,
                        recipient,
                    } => (
                        "Withdraw",
                        None,
                        Some(*amount),
                        None,
                        Some(owner.to_string()),
                        Some(recipient.to_string()),
                    ),
                };

                MessageClassification {
                    message_type: "System".to_string(),
                    application_id: None,
                    system_message_type: Some(sys_msg_type.to_string()),
                    system_target,
                    system_amount,
                    system_source,
                    system_owner,
                    system_recipient,
                }
            }
            Message::User { application_id, .. } => MessageClassification {
                message_type: "User".to_string(),
                application_id: Some(application_id.to_string()),
                system_message_type: None,
                system_target: None,
                system_amount: None,
                system_source: None,
                system_owner: None,
                system_recipient: None,
            },
        }
    }

    /// Serialize a Message with consistent error handling
    fn serialize_message(message: &Message) -> Result<Vec<u8>, SqliteError> {
        bincode::serialize(message)
            .map_err(|e| SqliteError::Serialization(format!("Failed to serialize message: {}", e)))
    }

    fn deserialize_message(data: &[u8]) -> Result<Message, SqliteError> {
        bincode::deserialize(data).map_err(|e| {
            SqliteError::Serialization(format!("Failed to deserialize message: {}", e))
        })
    }

    /// Parse MessageKind from string
    fn parse_message_kind(kind_str: &str) -> Result<MessageKind, SqliteError> {
        match kind_str {
            "Simple" => Ok(MessageKind::Simple),
            "Tracked" => Ok(MessageKind::Tracked),
            "Bouncing" => Ok(MessageKind::Bouncing),
            "Protected" => Ok(MessageKind::Protected),
            _ => Err(SqliteError::Serialization(format!(
                "Unknown message kind: {}",
                kind_str
            ))),
        }
    }

    /// Convert MessageKind to string
    fn message_kind_to_string(kind: &MessageKind) -> String {
        format!("{:?}", kind)
    }
}

/// Summary of a block without the full data
pub struct BlockSummary {
    pub hash: CryptoHash,
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub timestamp: Timestamp,
    pub epoch: u64,
    pub state_hash: CryptoHash,
    pub previous_block_hash: Option<CryptoHash>,
    pub authenticated_signer: Option<String>,
    pub operation_count: usize,
    pub incoming_bundle_count: usize,
    pub message_count: usize,
    pub event_count: usize,
    pub blob_count: usize,
}
#[async_trait]
impl IndexerDatabase for SqliteDatabase {
    type Error = SqliteError;

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
        timestamp: Timestamp,
        data: &[u8],
    ) -> Result<(), SqliteError> {
        self.insert_block_tx(tx, hash, chain_id, height, timestamp, data)
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
