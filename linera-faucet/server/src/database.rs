// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing chain assignments.

use std::{collections::HashMap, path::PathBuf};

use anyhow::Context as _;
use linera_base::{data_types::ChainDescription, identifiers::AccountOwner};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use tracing::{debug, info, warn};

/// SQLite database for persistent storage of chain assignments.
pub struct FaucetDatabase {
    pool: SqlitePool,
}

/// Schema for creating the chains table.
const CREATE_CHAINS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS chains (
    owner TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    description_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chains_chain_id ON chains(chain_id);
"#;

impl FaucetDatabase {
    /// Creates a new SQLite database connection.
    pub async fn new(database_path: &PathBuf) -> anyhow::Result<Self> {
        // Create parent directory if it doesn't exist
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
        info!("Database schema initialized");
        Ok(())
    }

    /// Migrates data from a legacy JSON file to the database.
    pub async fn migrate_from_json(&self, json_path: &PathBuf) -> anyhow::Result<()> {
        // Check if migration is needed.
        let count: i64 = self.get_chain_count().await?;
        if count > 0 {
            info!("Database already contains {count} chains, skipping migration");
            return Ok(());
        }

        // Try to load the JSON file.
        let data = match tokio::fs::read(json_path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("No legacy JSON file found, starting with empty database");
                return Ok(());
            }
            Err(e) => {
                return Err(e).context("Failed to read legacy JSON file");
            }
        };

        let chain_map: HashMap<AccountOwner, ChainDescription> =
            serde_json::from_slice(&data).context("Failed to deserialize legacy JSON storage")?;

        info!("Migrating {} chains from JSON to SQLite", chain_map.len());

        // Start a transaction for atomic migration.
        let mut tx = self.pool.begin().await?;

        for (owner, description) in chain_map {
            let owner_str = serde_json::to_string(&owner)?;
            let chain_id = description.id().to_string();
            let description_json = serde_json::to_string(&description)?;

            sqlx::query("INSERT INTO chains (owner, chain_id, description_json) VALUES (?, ?, ?)")
                .bind(&owner_str)
                .bind(&chain_id)
                .bind(&description_json)
                .execute(&mut *tx)
                .await?;

            debug!("Migrated chain {chain_id} for owner {owner_str}");
        }

        tx.commit().await?;
        info!("Successfully migrated all chains to SQLite");

        // Rename the old file to indicate it's been migrated.
        let backup_path = json_path.with_extension("json.migrated");
        if let Err(e) = tokio::fs::rename(json_path, &backup_path).await {
            warn!(
                "Failed to rename migrated JSON file to {:?}: {}",
                backup_path, e
            );
        }

        Ok(())
    }

    /// Gets the chain description for an owner if it exists.
    pub async fn get_chain(
        &self,
        owner: &AccountOwner,
    ) -> anyhow::Result<Option<ChainDescription>> {
        let owner_str = serde_json::to_string(owner)?;

        let Some(row) = sqlx::query("SELECT description_json FROM chains WHERE owner = ?")
            .bind(&owner_str)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let description_json: String = row.get("description_json");
        let description: ChainDescription = serde_json::from_str(&description_json)?;
        Ok(Some(description))
    }

    /// Stores multiple chain mappings in a single transaction
    pub async fn store_chains_batch(
        &self,
        chains: Vec<(AccountOwner, ChainDescription)>,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for (owner, description) in chains {
            let owner_str = serde_json::to_string(&owner)?;
            let chain_id = description.id().to_string();
            let description_json = serde_json::to_string(&description)?;

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO chains (owner, chain_id, description_json)
                VALUES (?, ?, ?)
                "#,
            )
            .bind(&owner_str)
            .bind(&chain_id)
            .bind(&description_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Gets the total number of chains in the database.
    pub async fn get_chain_count(&self) -> anyhow::Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chains")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }
}
