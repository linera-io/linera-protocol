// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing chain assignments.

use std::{collections::HashMap, path::PathBuf};

use anyhow::Context as _;
use linera_base::{data_types::ChainDescription, identifiers::AccountOwner};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use tracing::{debug, info, warn};

/// SQLite database for persistent storage of chain assignments
pub struct FaucetDatabase {
    pool: SqlitePool,
}

/// Schema for creating the chains table
const CREATE_CHAINS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS chains (
    owner TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    description_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chains_chain_id ON chains(chain_id);
"#;

/// Persistent mapping of account owners to chain descriptions (legacy format)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LegacyFaucetStorage {
    /// Maps account owners to their corresponding chain descriptions
    pub owner_to_chain: HashMap<AccountOwner, ChainDescription>,
}

impl FaucetDatabase {
    /// Create a new SQLite database connection
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

    /// Initialize the database schema
    async fn initialize_schema(&self) -> anyhow::Result<()> {
        sqlx::query(CREATE_CHAINS_TABLE)
            .execute(&self.pool)
            .await
            .context("Failed to create chains table")?;
        info!("Database schema initialized");
        Ok(())
    }

    /// Migrate data from a legacy JSON file to the database
    pub async fn migrate_from_json(&self, json_path: &PathBuf) -> anyhow::Result<()> {
        // Check if migration is needed
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chains")
            .fetch_one(&self.pool)
            .await?;

        if count > 0 {
            info!(
                "Database already contains {} chains, skipping migration",
                count
            );
            return Ok(());
        }

        // Try to load the JSON file
        match tokio::fs::read(json_path).await {
            Ok(data) => {
                let legacy_storage: LegacyFaucetStorage = serde_json::from_slice(&data)
                    .context("Failed to deserialize legacy JSON storage")?;

                info!(
                    "Migrating {} chains from JSON to SQLite",
                    legacy_storage.owner_to_chain.len()
                );

                // Start a transaction for atomic migration
                let mut tx = self.pool.begin().await?;

                for (owner, description) in legacy_storage.owner_to_chain {
                    let owner_str = serde_json::to_string(&owner)?;
                    let chain_id = description.id().to_string();
                    let description_json = serde_json::to_string(&description)?;

                    sqlx::query(
                        "INSERT INTO chains (owner, chain_id, description_json) VALUES (?, ?, ?)",
                    )
                    .bind(&owner_str)
                    .bind(&chain_id)
                    .bind(&description_json)
                    .execute(&mut *tx)
                    .await?;

                    debug!("Migrated chain {} for owner {}", chain_id, owner_str);
                }

                tx.commit().await?;
                info!("Successfully migrated all chains to SQLite");

                // Optionally rename the old file to indicate it's been migrated
                let backup_path = json_path.with_extension("json.migrated");
                if let Err(e) = tokio::fs::rename(json_path, &backup_path).await {
                    warn!(
                        "Failed to rename migrated JSON file to {:?}: {}",
                        backup_path, e
                    );
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("No legacy JSON file found, starting with empty database");
            }
            Err(e) => {
                return Err(e).context("Failed to read legacy JSON file");
            }
        }

        Ok(())
    }

    /// Gets the chain description for an owner if it exists
    pub async fn get_chain(
        &self,
        owner: &AccountOwner,
    ) -> anyhow::Result<Option<ChainDescription>> {
        let owner_str = serde_json::to_string(owner)?;

        let row = sqlx::query("SELECT description_json FROM chains WHERE owner = ?")
            .bind(&owner_str)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            let description_json: String = row.get("description_json");
            let description: ChainDescription = serde_json::from_str(&description_json)?;
            Ok(Some(description))
        } else {
            Ok(None)
        }
    }

    /// Stores a new mapping from owner to chain description
    #[allow(dead_code)]
    pub async fn store_chain(
        &self,
        owner: AccountOwner,
        description: ChainDescription,
    ) -> anyhow::Result<()> {
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
        .execute(&self.pool)
        .await?;

        debug!("Stored chain {} for owner {}", chain_id, owner_str);
        Ok(())
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

    /// Gets the total number of chains in the database
    #[allow(dead_code)]
    pub async fn get_chain_count(&self) -> anyhow::Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chains")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    /// Export all chains to the legacy JSON format (for backward compatibility)
    #[allow(dead_code)]
    pub async fn export_to_json(&self) -> anyhow::Result<LegacyFaucetStorage> {
        let rows = sqlx::query("SELECT owner, description_json FROM chains")
            .fetch_all(&self.pool)
            .await?;

        let mut owner_to_chain = HashMap::new();

        for row in rows {
            let owner_str: String = row.get("owner");
            let description_json: String = row.get("description_json");

            let owner: AccountOwner = serde_json::from_str(&owner_str)?;
            let description: ChainDescription = serde_json::from_str(&description_json)?;

            owner_to_chain.insert(owner, description);
        }

        Ok(LegacyFaucetStorage { owner_to_chain })
    }
}
