// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing chain assignments and daily claim tracking.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Context as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_core::client::ChainClient;
use linera_storage::Storage;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use tracing::info;

/// SQLite database for persistent storage of chain assignments and daily claims.
pub struct FaucetDatabase {
    pool: SqlitePool,
}

/// Information about a previous chain creation.
#[derive(Clone, Debug)]
pub struct ClaimRecord {
    /// The chain ID that was created.
    pub chain_id: ChainId,
    /// The timestamp when the chain was created.
    pub timestamp: Timestamp,
}

/// Schema for creating the chains table.
const CREATE_CHAINS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS chains (
    owner TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chains_chain_id ON chains(chain_id);
"#;

/// Schema for creating the daily_claims table.
const CREATE_DAILY_CLAIMS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS daily_claims (
    owner TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    last_period INTEGER NOT NULL
);
"#;

impl FaucetDatabase {
    /// Creates a new SQLite database connection.
    pub async fn new(database_path: &PathBuf) -> anyhow::Result<Self> {
        // Create parent directory if it doesn't exist.
        if let Some(parent) = database_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("Failed to create database directory")?;
        }

        let database_url = format!("sqlite:{}", database_path.display());
        info!(?database_url, "Connecting to SQLite database");

        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .context("Failed to connect to SQLite database")?;

        let db = Self { pool };
        db.initialize_schema().await?;
        Ok(db)
    }

    /// Initializes the database schema.
    async fn initialize_schema(&self) -> anyhow::Result<()> {
        sqlx::query(CREATE_CHAINS_TABLE)
            .execute(&self.pool)
            .await
            .context("Failed to create chains table")?;
        sqlx::query(CREATE_DAILY_CLAIMS_TABLE)
            .execute(&self.pool)
            .await
            .context("Failed to create daily_claims table")?;
        info!("Database schema initialized");
        Ok(())
    }

    /// Synchronizes the database with the blockchain by traversing block history.
    pub async fn sync_with_blockchain<E>(&self, client: &ChainClient<E>) -> anyhow::Result<()>
    where
        E: linera_core::Environment,
        E::Storage: Storage,
    {
        info!("Starting database synchronization with blockchain");

        // Build height->hash map and find sync point in a single traversal.
        let height_to_hash = self.build_sync_map(client).await?;

        info!(
            "Found sync point at height {}, processing {} blocks",
            height_to_hash.keys().next().unwrap_or(&BlockHeight::ZERO),
            height_to_hash.len()
        );

        // Sync forward from the sync point using the pre-built map.
        self.sync_forward_with_map(client, height_to_hash).await?;

        info!("Database synchronization completed");
        Ok(())
    }

    /// Builds a height->hash map and finds the sync point in a single blockchain traversal.
    /// Returns (sync_point, height_to_hash_map).
    async fn build_sync_map<E>(
        &self,
        client: &ChainClient<E>,
    ) -> anyhow::Result<BTreeMap<BlockHeight, CryptoHash>>
    where
        E: linera_core::Environment,
        E::Storage: Storage,
    {
        let info = client.chain_info().await?;
        let end_height = info.next_block_height;

        if end_height == BlockHeight::ZERO {
            info!("Chain is empty, no synchronization needed");
            return Ok(BTreeMap::new());
        }

        let mut height_to_hash = BTreeMap::new();
        let mut current_hash = info.block_hash;

        // Traverse backwards to build the height -> hash mapping and find sync point
        while let Some(hash) = current_hash {
            let certificate = client
                .storage_client()
                .read_certificate(hash)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Certificate not found for hash {}", hash))?;
            let current_height = certificate.block().header.height;

            // Check if this block's chains are already in our database
            let chains_in_block = super::extract_opened_single_owner_chains(&certificate)?;

            if !chains_in_block.is_empty() {
                let mut all_chains_exist = true;
                for (owner, _description) in &chains_in_block {
                    if self.get_chain_id(owner).await?.is_none() {
                        all_chains_exist = false;
                        break;
                    }
                }

                if all_chains_exist {
                    // All chains from this block are already in the database.
                    break;
                }
            }

            // Add to our height->hash map
            height_to_hash.insert(current_height, hash);

            // Move to the previous block
            current_hash = certificate.block().header.previous_block_hash;
        }

        Ok(height_to_hash)
    }

    /// Syncs the database using a pre-built height->hash map.
    async fn sync_forward_with_map<E>(
        &self,
        client: &ChainClient<E>,
        height_to_hash: BTreeMap<BlockHeight, CryptoHash>,
    ) -> anyhow::Result<()>
    where
        E: linera_core::Environment,
        E::Storage: Storage,
    {
        if height_to_hash.is_empty() {
            return Ok(());
        }

        // Process blocks in chronological order (forward)
        for (height, hash) in height_to_hash {
            let certificate = client
                .storage_client()
                .read_certificate(hash)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Certificate not found for hash {}", hash))?;

            let block_timestamp = certificate.block().header.timestamp;
            let chains_to_store = super::extract_opened_single_owner_chains(&certificate)?
                .into_iter()
                .map(|(owner, description)| (owner, description.id()))
                .collect::<Vec<_>>();

            if !chains_to_store.is_empty() {
                info!(
                    "Processing block at height {height} with {} new chains",
                    chains_to_store.len()
                );
                self.store_chains_batch(chains_to_store, block_timestamp)
                    .await?;
            }
        }

        Ok(())
    }

    /// Gets the chain ID for an owner if it exists.
    pub async fn get_chain_id(&self, owner: &AccountOwner) -> anyhow::Result<Option<ChainId>> {
        let owner_str = owner.to_string();

        let Some(row) = sqlx::query("SELECT chain_id FROM chains WHERE owner = ?")
            .bind(&owner_str)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let chain_id_str: String = row.get("chain_id");
        let chain_id: ChainId = chain_id_str.parse()?;
        Ok(Some(chain_id))
    }

    /// Gets the initial claim record for an owner, if they have claimed a chain.
    /// Returns the chain ID and the block timestamp of the initial claim.
    pub async fn initial_claim(&self, owner: &AccountOwner) -> anyhow::Result<Option<ClaimRecord>> {
        let owner_str = owner.to_string();

        let Some(row) = sqlx::query("SELECT chain_id, created_at FROM chains WHERE owner = ?")
            .bind(&owner_str)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };

        let chain_id_str: String = row.get("chain_id");
        let chain_id: ChainId = chain_id_str.parse()?;

        let created_at_micros: i64 = row.get("created_at");
        let timestamp = Timestamp::from(created_at_micros as u64);

        Ok(Some(ClaimRecord {
            chain_id,
            timestamp,
        }))
    }

    /// Stores multiple chain mappings in a single transaction.
    /// The `timestamp` is the block timestamp when the chains were created.
    pub async fn store_chains_batch(
        &self,
        chains: Vec<(AccountOwner, ChainId)>,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let micros = timestamp.micros() as i64;

        for (owner, chain_id) in chains {
            let owner_str = owner.to_string();
            let chain_id_str = chain_id.to_string();

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO chains (owner, chain_id, created_at)
                VALUES (?, ?, ?)
                "#,
            )
            .bind(&owner_str)
            .bind(&chain_id_str)
            .bind(micros)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Gets the last daily claim period for an owner, if they have made a daily claim.
    pub async fn last_daily_claim_period(
        &self,
        owner: &AccountOwner,
    ) -> anyhow::Result<Option<u64>> {
        let owner_str = owner.to_string();

        let Some(row) = sqlx::query("SELECT last_period FROM daily_claims WHERE owner = ?")
            .bind(&owner_str)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };

        let last_period: i64 = row.get("last_period");
        Ok(Some(last_period as u64))
    }

    /// Stores multiple daily claim records in a single transaction.
    /// Uses `INSERT OR REPLACE` to update `last_period` on subsequent daily claims.
    pub async fn store_daily_claims_batch(
        &self,
        claims: Vec<(AccountOwner, ChainId, u64)>,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for (owner, chain_id, period) in claims {
            let owner_str = owner.to_string();
            let chain_id_str = chain_id.to_string();

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO daily_claims (owner, chain_id, last_period)
                VALUES (?, ?, ?)
                "#,
            )
            .bind(&owner_str)
            .bind(&chain_id_str)
            .bind(period as i64)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
