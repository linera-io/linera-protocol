// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data models matching the PostgreSQL schema from linera-indexer/lib/src/db/postgres/consts.rs

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Block summary for list views (matches blocks table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct BlockSummary {
    pub hash: String,
    pub chain_id: String,
    pub height: i64,
    pub timestamp: i64,
    pub epoch: i64,
    pub state_hash: String,
    pub previous_block_hash: Option<String>,
    pub authenticated_signer: Option<String>,
    pub operation_count: i64,
    pub incoming_bundle_count: i64,
    pub message_count: i64,
    pub event_count: i64,
    pub blob_count: i64,
    pub size: i64,
}

/// Full block data (matches blocks table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Block {
    pub hash: String,
    pub chain_id: String,
    pub height: i64,
    pub timestamp: i64,
    pub epoch: i64,
    pub state_hash: String,
    pub previous_block_hash: Option<String>,
    pub authenticated_signer: Option<String>,
    pub operation_count: i64,
    pub incoming_bundle_count: i64,
    pub message_count: i64,
    pub event_count: i64,
    pub blob_count: i64,
    pub data: String, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Incoming bundle (matches incoming_bundles table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct IncomingBundle {
    pub id: i64,
    pub block_hash: String,
    pub bundle_index: i64,
    pub origin_chain_id: String,
    pub action: String,
    pub source_height: i64,
    pub source_timestamp: i64,
    pub source_cert_hash: String,
    pub transaction_index: i64,
    pub created_at: Option<DateTime<Utc>>,
}

/// Bundle with its messages (composite for API response)
#[derive(Debug, Serialize)]
pub struct BundleWithMessages {
    pub id: i64,
    pub block_hash: String,
    pub bundle_index: i64,
    pub origin_chain_id: String,
    pub action: String,
    pub source_height: i64,
    pub source_timestamp: i64,
    pub source_cert_hash: String,
    pub transaction_index: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub messages: Vec<PostedMessage>,
}

/// Posted message (matches posted_messages table schema)
#[derive(Debug, Serialize, sqlx::FromRow, Clone)]
pub struct PostedMessage {
    pub id: i64,
    pub bundle_id: i64,
    pub message_index: i64,
    pub authenticated_signer: Option<String>,
    pub grant_amount: Option<String>,
    pub refund_grant_to: Option<String>,
    pub message_kind: String,
    pub message_type: String,
    pub application_id: Option<String>,
    pub system_message_type: Option<String>,
    pub system_target: Option<String>,
    pub system_amount: Option<String>,
    pub system_source: Option<String>,
    pub system_owner: Option<String>,
    pub system_recipient: Option<String>,
    pub message_data: String, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Operation (matches operations table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Operation {
    pub id: i64,
    pub block_hash: String,
    pub operation_index: i64,
    pub operation_type: String,
    pub application_id: Option<String>,
    pub system_operation_type: Option<String>,
    pub authenticated_signer: Option<String>,
    pub data: String, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Outgoing message (matches outgoing_messages table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OutgoingMessage {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i64,
    pub message_index: i64,
    pub destination_chain_id: String,
    pub authenticated_signer: Option<String>,
    pub grant_amount: Option<String>,
    pub message_kind: String,
    pub message_type: String,
    pub application_id: Option<String>,
    pub system_message_type: Option<String>,
    pub system_target: Option<String>,
    pub system_amount: Option<String>,
    pub system_source: Option<String>,
    pub system_owner: Option<String>,
    pub system_recipient: Option<String>,
    pub data: String, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Event (matches events table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Event {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i64,
    pub event_index: i64,
    pub stream_id: String,
    pub stream_index: i64,
    pub data: String, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Oracle response (matches oracle_responses table schema)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OracleResponse {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i64,
    pub response_index: i64,
    pub response_type: String,
    pub blob_hash: Option<String>,
    pub data: Option<String>, // base64-encoded
    pub created_at: Option<DateTime<Utc>>,
}

/// Chain statistics (derived from blocks table)
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ChainStats {
    pub chain_id: String,
    pub block_count: i64,
    pub latest_height: Option<i64>,
    pub latest_block_hash: Option<String>,
}

/// API response types

#[derive(Debug, Serialize)]
pub struct Stats {
    #[serde(rename = "totalBlocks")]
    pub total_blocks: i64,
    #[serde(rename = "totalChains")]
    pub total_chains: i64,
    pub chains: Vec<ChainStats>,
}

#[derive(Debug, Serialize)]
pub struct CountResponse {
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
