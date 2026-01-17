use serde::Serialize;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct BlockSummary {
    pub hash: String,
    pub chain_id: String,
    pub height: i64,
    pub timestamp: i64,
    pub size: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Block {
    pub hash: String,
    pub chain_id: String,
    pub height: i64,
    pub timestamp: i64,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct IncomingBundle {
    pub id: i64,
    pub block_hash: String,
    pub bundle_index: i32,
    pub origin_chain_id: Option<String>,
    pub action: Option<String>,
    pub source_height: Option<i64>,
    pub source_timestamp: Option<i64>,
    pub source_cert_hash: Option<String>,
    pub transaction_index: Option<i32>,
    pub created_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BundleWithMessages {
    pub id: i64,
    pub block_hash: String,
    pub bundle_index: i32,
    pub origin_chain_id: Option<String>,
    pub action: Option<String>,
    pub source_height: Option<i64>,
    pub source_timestamp: Option<i64>,
    pub source_cert_hash: Option<String>,
    pub transaction_index: Option<i32>,
    pub created_at: Option<String>,
    pub messages: Vec<PostedMessage>,
}

#[derive(Debug, Serialize, Clone)]
pub struct PostedMessage {
    pub id: i64,
    pub bundle_id: i64,
    pub message_index: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticated_signer: Option<String>,
    pub grant_amount: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_grant_to: Option<String>,
    pub message_kind: Option<String>,
    pub message_type: Option<String>,
    pub application_id: Option<String>,
    pub system_message_type: Option<String>,
    pub system_target: Option<String>,
    pub system_amount: Option<String>,
    pub system_source: Option<String>,
    pub system_owner: Option<String>,
    pub system_recipient: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_data: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ChainStats {
    pub chain_id: String,
    pub block_count: i64,
    pub latest_height: Option<i64>,
    pub latest_block_hash: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Operation {
    pub id: i64,
    pub block_hash: String,
    pub operation_index: i32,
    pub transaction_index: Option<i32>,
    pub application_id: Option<String>,
    pub operation_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OutgoingMessage {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i32,
    pub message_index: i32,
    pub destination: Option<String>,
    pub authenticated_signer: Option<String>,
    pub grant_amount: Option<String>,
    pub refund_grant_to: Option<String>,
    pub message_kind: Option<String>,
    pub message_type: Option<String>,
    pub application_id: Option<String>,
    pub system_message_type: Option<String>,
    pub system_target: Option<String>,
    pub system_amount: Option<String>,
    pub system_source: Option<String>,
    pub system_owner: Option<String>,
    pub system_recipient: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Event {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i32,
    pub event_index: i32,
    pub stream_name: Option<String>,
    pub event_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct OracleResponse {
    pub id: i64,
    pub block_hash: String,
    pub transaction_index: i32,
    pub response_index: i32,
    pub response_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub created_at: Option<String>,
}

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
