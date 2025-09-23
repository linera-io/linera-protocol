// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database module for storing chain assignments.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Context as _;
use linera_base::{
    bcs,
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainDescription},
    identifiers::{AccountOwner, ChainId},
};
use linera_chain::{data_types::Transaction, types::ConfirmedBlockCertificate};
use linera_core::client::ChainClient;
use linera_execution::{system::SystemOperation, Operation};
use linera_storage::Storage;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use tracing::{debug, info};

/// SQLite database for persistent storage of chain assignments.
pub struct FaucetDatabase {
    pool: SqlitePool,
}

/// Schema for creating the chains table.
const CREATE_CHAINS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS chains (
    owner TEXT PRIMARY KEY NOT NULL,
    chain_id TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chains_chain_id ON chains(chain_id);
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
            let chains_in_block = self.extract_opened_chains(&certificate).await?;

            if !chains_in_block.is_empty() {
                let mut all_chains_exist = true;
                for (owner, _chain_id) in &chains_in_block {
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

            let chains_to_store = self.extract_opened_chains(&certificate).await?;

            if !chains_to_store.is_empty() {
                info!(
                    "Processing block at height {height} with {} new chains",
                    chains_to_store.len()
                );
                self.store_chains_batch(chains_to_store).await?;
            }
        }

        Ok(())
    }

    /// Extracts OpenChain operations from a certificate and returns (owner, chain_id) pairs.
    async fn extract_opened_chains(
        &self,
        certificate: &ConfirmedBlockCertificate,
    ) -> anyhow::Result<Vec<(AccountOwner, ChainId)>> {
        let mut chains = Vec::new();
        let block = certificate.block();

        // Parse chain descriptions from the block's blobs
        let blobs = block.body.blobs.iter().flatten();
        let chain_descriptions = blobs
            .map(|blob| bcs::from_bytes::<ChainDescription>(blob.bytes()))
            .collect::<Result<Vec<ChainDescription>, _>>()?;

        let mut chain_desc_iter = chain_descriptions.into_iter();

        // Examine each transaction in the block
        for transaction in &block.body.transactions {
            if let Transaction::ExecuteOperation(Operation::System(system_op)) = transaction {
                if let SystemOperation::OpenChain(config) = system_op.as_ref() {
                    // Extract the owner from the OpenChain operation
                    // We expect single-owner chains from the faucet
                    let mut owners = config.ownership.all_owners();
                    if let Some(owner) = owners.next() {
                        // Verify it's a single-owner chain (faucet only creates these)
                        if owners.next().is_none() {
                            // Get the corresponding chain description from the blobs
                            if let Some(description) = chain_desc_iter.next() {
                                chains.push((*owner, description.id()));
                                debug!(
                                    "Found OpenChain operation for owner {} creating chain {}",
                                    owner,
                                    description.id()
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(chains)
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

    /// Stores multiple chain mappings in a single transaction
    pub async fn store_chains_batch(
        &self,
        chains: Vec<(AccountOwner, ChainId)>,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for (owner, chain_id) in chains {
            let owner_str = owner.to_string();
            let chain_id_str = chain_id.to_string();

            sqlx::query(
                r#"
                INSERT OR REPLACE INTO chains (owner, chain_id)
                VALUES (?, ?)
                "#,
            )
            .bind(&owner_str)
            .bind(&chain_id_str)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
