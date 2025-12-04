// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL schema definitions and constants.

/// SQL schema for creating the blocks table with denormalized fields
pub const CREATE_BLOCKS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    hash TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,

    -- Denormalized fields from BlockHeader
    epoch BIGINT NOT NULL,
    state_hash TEXT NOT NULL,
    previous_block_hash TEXT,
    authenticated_owner TEXT,

    -- Aggregated counts for filtering and display
    operation_count BIGINT NOT NULL DEFAULT 0,
    incoming_bundle_count BIGINT NOT NULL DEFAULT 0,
    message_count BIGINT NOT NULL DEFAULT 0,
    event_count BIGINT NOT NULL DEFAULT 0,
    blob_count BIGINT NOT NULL DEFAULT 0,

    -- Original serialized block data for backward compatibility
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_blocks_chain_height ON blocks(chain_id, height);
CREATE INDEX IF NOT EXISTS idx_blocks_epoch ON blocks(epoch);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX IF NOT EXISTS idx_blocks_state_hash ON blocks(state_hash);
"#;

/// SQL schema for creating the operations table
pub const CREATE_OPERATIONS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS operations (
    id BIGSERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    operation_index BIGINT NOT NULL,
    operation_type TEXT NOT NULL, -- 'System' or 'User'
    application_id TEXT, -- For user operations
    system_operation_type TEXT, -- For system operations (Transfer, OpenChain, etc.)
    authenticated_owner TEXT,
    data BYTEA NOT NULL, -- Serialized operation
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_operations_block_hash ON operations(block_hash);
CREATE INDEX IF NOT EXISTS idx_operations_type ON operations(operation_type);
CREATE INDEX IF NOT EXISTS idx_operations_application_id ON operations(application_id);
CREATE INDEX IF NOT EXISTS idx_operations_system_type ON operations(system_operation_type);
"#;

/// SQL schema for creating the outgoing messages table
pub const CREATE_OUTGOING_MESSAGES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS outgoing_messages (
    id BIGSERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    message_index BIGINT NOT NULL,
    destination_chain_id TEXT NOT NULL,
    authenticated_owner TEXT,
    grant_amount TEXT,
    message_kind TEXT NOT NULL, -- 'Simple', 'Tracked', 'Bouncing', 'Protected'
    message_type TEXT NOT NULL, -- 'System' or 'User'
    application_id TEXT, -- For user messages
    system_message_type TEXT, -- For system messages (Credit, Withdraw, etc.)
    system_target TEXT, -- Credit target
    system_amount TEXT, -- Credit/Withdraw amount
    system_source TEXT, -- Credit source
    system_owner TEXT, -- Withdraw owner
    system_recipient TEXT, -- Withdraw recipient
    data BYTEA NOT NULL, -- Serialized message content
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_outgoing_messages_block_hash ON outgoing_messages(block_hash);
CREATE INDEX IF NOT EXISTS idx_outgoing_messages_destination ON outgoing_messages(destination_chain_id);
CREATE INDEX IF NOT EXISTS idx_outgoing_messages_type ON outgoing_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_outgoing_messages_application_id ON outgoing_messages(application_id);
CREATE INDEX IF NOT EXISTS idx_outgoing_messages_system_type ON outgoing_messages(system_message_type);
"#;

/// SQL schema for creating the events table
pub const CREATE_EVENTS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    event_index BIGINT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_index BIGINT NOT NULL,
    data BYTEA NOT NULL, -- Event payload
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_events_block_hash ON events(block_hash);
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);
"#;

/// SQL schema for creating the oracle responses table
pub const CREATE_ORACLE_RESPONSES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS oracle_responses (
    id BIGSERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    response_index BIGINT NOT NULL,
    response_type TEXT NOT NULL, -- 'Service' or 'Blob'
    blob_hash TEXT, -- For blob responses
    data BYTEA, -- For service responses
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_oracle_responses_block_hash ON oracle_responses(block_hash);
CREATE INDEX IF NOT EXISTS idx_oracle_responses_type ON oracle_responses(response_type);
"#;

/// SQL schema for creating the blobs table with enhanced metadata
pub const CREATE_BLOBS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY NOT NULL,
    blob_type TEXT NOT NULL, -- 'Data', 'ContractBytecode', 'ServiceBytecode', etc.
    application_id TEXT, -- If applicable
    block_hash TEXT, -- Block that created this blob
    transaction_index BIGINT, -- Transaction that created this blob
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_blobs_type ON blobs(blob_type);
CREATE INDEX IF NOT EXISTS idx_blobs_block_hash ON blobs(block_hash);
CREATE INDEX IF NOT EXISTS idx_blobs_application_id ON blobs(application_id);
"#;

/// SQL schema for creating the incoming_bundles table
pub const CREATE_INCOMING_BUNDLES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS incoming_bundles (
    id BIGSERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    bundle_index BIGINT NOT NULL,
    origin_chain_id TEXT NOT NULL,
    action TEXT NOT NULL,
    source_height BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    source_cert_hash TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_hash) REFERENCES blocks(hash)
);

CREATE INDEX IF NOT EXISTS idx_incoming_bundles_block_hash ON incoming_bundles(block_hash);
CREATE INDEX IF NOT EXISTS idx_incoming_bundles_origin_chain ON incoming_bundles(origin_chain_id);
CREATE INDEX IF NOT EXISTS idx_incoming_bundles_action ON incoming_bundles(action);
"#;

/// SQL schema for creating the posted_messages table
pub const CREATE_POSTED_MESSAGES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS posted_messages (
    id BIGSERIAL PRIMARY KEY,
    bundle_id BIGINT NOT NULL,
    message_index BIGINT NOT NULL,
    authenticated_owner TEXT,
    grant_amount TEXT,
    refund_grant_to TEXT,
    message_kind TEXT NOT NULL,
    message_type TEXT NOT NULL, -- 'System' or 'User'
    application_id TEXT, -- For user messages
    system_message_type TEXT, -- For system messages (Credit, Withdraw, etc.)
    system_target TEXT, -- Credit target
    system_amount TEXT, -- Credit/Withdraw amount
    system_source TEXT, -- Credit source
    system_owner TEXT, -- Withdraw owner
    system_recipient TEXT, -- Withdraw recipient
    message_data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bundle_id) REFERENCES incoming_bundles(id)
);

CREATE INDEX IF NOT EXISTS idx_posted_messages_bundle_id ON posted_messages(bundle_id);
CREATE INDEX IF NOT EXISTS idx_posted_messages_kind ON posted_messages(message_kind);
CREATE INDEX IF NOT EXISTS idx_posted_messages_type ON posted_messages(message_type);
CREATE INDEX IF NOT EXISTS idx_posted_messages_system_type ON posted_messages(system_message_type);
"#;
