// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite schema definitions and constants.

/// SQL schema for creating the blocks table
pub const CREATE_BLOCKS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    hash TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    height INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    data BLOB NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_blocks_chain_height ON blocks(chain_id, height);
CREATE INDEX IF NOT EXISTS idx_blocks_chain_id ON blocks(chain_id);
"#;

/// SQL schema for creating the blobs table
pub const CREATE_BLOBS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY NOT NULL,
    type TEXT NOT NULL,
    data BLOB NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"#;

/// SQL schema for creating the incoming_bundles table
pub const CREATE_INCOMING_BUNDLES_TABLE: &str = r#"
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
"#;

/// SQL schema for creating the posted_messages table
pub const CREATE_POSTED_MESSAGES_TABLE: &str = r#"
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
"#;
