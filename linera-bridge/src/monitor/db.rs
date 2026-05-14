// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite persistent storage for bridge relayer deposit/burn requests.

// SQLite (via sqlx) has no native unsigned integer types, so this module
// routinely casts `u64` to/from `i64` when binding parameters and reading
// rows. The casts are by design at the SQL boundary.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]
//!
//! This is a write-through layer alongside the in-memory `MonitorState`.
//! It persists request metadata and raw operation bytes so they can be
//! queried and replayed without the relayer running.
//!
//! Pending and finished requests live in separate tables (`pending_deposits`/
//! `finished_deposits`, `pending_burns`/`finished_burns`) so that the hot
//! `load_pending_*` queries scan only live work and stay fast as historical
//! volume grows. Completion or failure moves a row from the pending table to
//! the finished table atomically.

use std::path::Path;

use alloy::primitives::{Address, B256, U256};
use anyhow::{Context as _, Result};
use linera_base::data_types::BlockHeight;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};

use super::{PendingBurn, PendingDeposit};
use crate::proof::DepositKey;

/// Persistent SQLite store for bridging requests.
pub struct BridgeDb {
    pool: SqlitePool,
}

impl BridgeDb {
    /// Opens (or creates) the SQLite database at `path` and runs migrations.
    pub async fn open(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let db = Self { pool };
        db.create_tables().await?;
        Ok(db)
    }

    /// Opens an in-memory database for testing.
    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let db = Self { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pending_deposits (
                source_chain_id   INTEGER NOT NULL,
                block_hash        BLOB NOT NULL,
                tx_index          INTEGER NOT NULL,
                log_index         INTEGER NOT NULL,
                tx_hash           BLOB NOT NULL,
                depositor         BLOB NOT NULL,
                amount            TEXT NOT NULL,
                nonce             TEXT NOT NULL,
                raw_operation     BLOB,
                created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (source_chain_id, block_hash, tx_index, log_index)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS finished_deposits (
                source_chain_id   INTEGER NOT NULL,
                block_hash        BLOB NOT NULL,
                tx_index          INTEGER NOT NULL,
                log_index         INTEGER NOT NULL,
                tx_hash           BLOB NOT NULL,
                depositor         BLOB NOT NULL,
                amount            TEXT NOT NULL,
                nonce             TEXT NOT NULL,
                raw_operation     BLOB,
                status            TEXT NOT NULL,
                created_at        TEXT NOT NULL,
                finished_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (source_chain_id, block_hash, tx_index, log_index)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pending_burns (
                linera_height     INTEGER NOT NULL,
                event_index        INTEGER NOT NULL,
                evm_recipient     TEXT NOT NULL,
                amount            TEXT NOT NULL,
                raw_cert          BLOB,
                created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (linera_height, event_index)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS finished_burns (
                linera_height     INTEGER NOT NULL,
                event_index        INTEGER NOT NULL,
                evm_recipient     TEXT NOT NULL,
                amount            TEXT NOT NULL,
                raw_cert          BLOB,
                status            TEXT NOT NULL,
                created_at        TEXT NOT NULL,
                finished_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (linera_height, event_index)
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserts a new pending deposit. Ignores duplicates (idempotent).
    pub async fn insert_deposit(&self, deposit: &PendingDeposit) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO pending_deposits
                (source_chain_id, block_hash, tx_index, log_index, tx_hash, depositor, amount, nonce)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(deposit.key.source_chain_id as i64)
        .bind(deposit.key.block_hash.as_slice())
        .bind(deposit.key.tx_index as i64)
        .bind(deposit.key.log_index as i64)
        .bind(deposit.tx_hash.as_slice())
        .bind(deposit.depositor.as_slice())
        .bind(deposit.amount.to_string())
        .bind(deposit.nonce.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically moves a deposit from `pending_deposits` to `finished_deposits`
    /// with the given terminal `status` (`"completed"` or `"failed"`).
    ///
    /// If `finished_deposits` already has a row for this key (replay scenario),
    /// the existing record is preserved and a warning is logged. The matching
    /// pending row is still deleted so the deposit does not stay queued.
    pub async fn update_deposit_status(&self, key: &DepositKey, status: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO finished_deposits
                (source_chain_id, block_hash, tx_index, log_index,
                 tx_hash, depositor, amount, nonce, raw_operation,
                 status, created_at)
             SELECT source_chain_id, block_hash, tx_index, log_index,
                    tx_hash, depositor, amount, nonce, raw_operation,
                    ?, created_at
             FROM pending_deposits
             WHERE source_chain_id = ? AND block_hash = ? AND tx_index = ? AND log_index = ?",
        )
        .bind(status)
        .bind(key.source_chain_id as i64)
        .bind(key.block_hash.as_slice())
        .bind(key.tx_index as i64)
        .bind(key.log_index as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let deleted = sqlx::query(
            "DELETE FROM pending_deposits
             WHERE source_chain_id = ? AND block_hash = ? AND tx_index = ? AND log_index = ?",
        )
        .bind(key.source_chain_id as i64)
        .bind(key.block_hash.as_slice())
        .bind(key.tx_index as i64)
        .bind(key.log_index as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if deleted > 0 && inserted == 0 {
            tracing::warn!(
                ?key,
                "Replay detected: deposit already in finished_deposits, original record kept"
            );
        }
        Ok(())
    }

    /// Stores raw BCS-serialized operation bytes on the pending deposit row.
    pub async fn store_deposit_raw(&self, key: &DepositKey, raw: &[u8]) -> Result<()> {
        sqlx::query(
            "UPDATE pending_deposits SET raw_operation = ?
             WHERE source_chain_id = ? AND block_hash = ? AND tx_index = ? AND log_index = ?",
        )
        .bind(raw)
        .bind(key.source_chain_id as i64)
        .bind(key.block_hash.as_slice())
        .bind(key.tx_index as i64)
        .bind(key.log_index as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts a new pending burn. Ignores duplicates (idempotent).
    pub async fn insert_burn(&self, burn: &PendingBurn) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO pending_burns (linera_height, event_index, evm_recipient, amount)
             VALUES (?, ?, ?, ?)",
        )
        .bind(burn.height.0 as i64)
        .bind(burn.event_index as i64)
        .bind(format!("{:#x}", burn.evm_recipient))
        .bind(burn.amount.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically moves a burn from `pending_burns` to `finished_burns` with
    /// the given terminal `status` (`"completed"` or `"failed"`).
    ///
    /// If `finished_burns` already has a row for this key (replay scenario),
    /// the existing record is preserved and a warning is logged. The matching
    /// pending row is still deleted so the burn does not stay queued.
    pub async fn update_burn_status(
        &self,
        height: BlockHeight,
        index: u32,
        status: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO finished_burns
                (linera_height, event_index, evm_recipient, amount, raw_cert,
                 status, created_at)
             SELECT linera_height, event_index, evm_recipient, amount, raw_cert,
                    ?, created_at
             FROM pending_burns
             WHERE linera_height = ? AND event_index = ?",
        )
        .bind(status)
        .bind(height.0 as i64)
        .bind(index as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let deleted =
            sqlx::query("DELETE FROM pending_burns WHERE linera_height = ? AND event_index = ?")
                .bind(height.0 as i64)
                .bind(index as i64)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        tx.commit().await?;
        if deleted > 0 && inserted == 0 {
            tracing::warn!(
                ?height,
                event_index = index,
                "Replay detected: burn already in finished_burns, original record kept"
            );
        }
        Ok(())
    }

    /// Loads every pending deposit, used at relay startup to repopulate the
    /// in-memory `MonitorState` so that work in flight at the time of the
    /// previous shutdown is not lost.
    pub async fn load_pending_deposits(&self) -> Result<Vec<PendingDeposit>> {
        let rows = sqlx::query(
            "SELECT source_chain_id, block_hash, tx_index, log_index, tx_hash, depositor, amount, nonce
             FROM pending_deposits",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let source_chain_id: i64 = row.get(0);
            let block_hash: Vec<u8> = row.get(1);
            let tx_index: i64 = row.get(2);
            let log_index: i64 = row.get(3);
            let tx_hash: Vec<u8> = row.get(4);
            let depositor: Vec<u8> = row.get(5);
            let amount: String = row.get(6);
            let nonce: String = row.get(7);

            out.push(PendingDeposit {
                key: DepositKey {
                    source_chain_id: source_chain_id as u64,
                    block_hash: B256::try_from(block_hash.as_slice())
                        .context("invalid block_hash in deposits row")?,
                    tx_index: tx_index as u64,
                    log_index: log_index as u64,
                },
                tx_hash: B256::try_from(tx_hash.as_slice())
                    .context("invalid tx_hash in deposits row")?,
                depositor: Address::try_from(depositor.as_slice())
                    .context("invalid depositor in deposits row")?,
                amount: U256::from_str_radix(&amount, 10)
                    .context("invalid amount in deposits row")?,
                nonce: U256::from_str_radix(&nonce, 10).context("invalid nonce in deposits row")?,
            });
        }
        Ok(out)
    }

    /// Loads every pending burn, used at relay startup to repopulate the
    /// in-memory `MonitorState`.
    pub async fn load_pending_burns(&self) -> Result<Vec<PendingBurn>> {
        let rows = sqlx::query(
            "SELECT linera_height, event_index, evm_recipient, amount
             FROM pending_burns",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let height: i64 = row.get(0);
            let event_index: i64 = row.get(1);
            let evm_recipient: String = row.get(2);
            let amount: String = row.get(3);

            out.push(PendingBurn {
                height: BlockHeight(height as u64),
                event_index: event_index as u32,
                evm_recipient: evm_recipient
                    .parse()
                    .context("invalid evm_recipient in burns row")?,
                amount: amount.parse().context("invalid amount in burns row")?,
            });
        }
        Ok(out)
    }

    /// Returns the number of rows currently in `pending_deposits`.
    pub async fn pending_deposits_count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_deposits")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    /// Returns the number of rows currently in `pending_burns`.
    pub async fn pending_burns_count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_burns")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    /// Stores raw BCS-serialized certificate bytes on the pending burn row.
    pub async fn store_burn_raw(&self, height: BlockHeight, index: u32, raw: &[u8]) -> Result<()> {
        sqlx::query(
            "UPDATE pending_burns SET raw_cert = ?
             WHERE linera_height = ? AND event_index = ?",
        )
        .bind(raw)
        .bind(height.0 as i64)
        .bind(index as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy::primitives::{Address, B256, U256};
    use linera_base::data_types::Amount;
    use test_case::test_case;

    use super::*;

    static FILE_COUNTER: AtomicU32 = AtomicU32::new(0);

    async fn open_db(use_file: bool) -> Result<BridgeDb> {
        if use_file {
            let n = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::path::PathBuf::from(format!("/tmp/bridge_db_test_{n}.sqlite3"));
            std::fs::remove_file(&path).ok(); // Best-effort cleanup; NotFound is the common case.
            BridgeDb::open(&path).await
        } else {
            BridgeDb::open_in_memory().await
        }
    }

    fn test_deposit_key() -> DepositKey {
        DepositKey {
            source_chain_id: 8453,
            block_hash: B256::from([0xAA; 32]),
            tx_index: 5,
            log_index: 0,
        }
    }

    fn test_deposit() -> PendingDeposit {
        PendingDeposit {
            key: test_deposit_key(),
            tx_hash: B256::from([0xBB; 32]),
            depositor: Address::from([0xCC; 20]),
            amount: U256::from(1_000_000u64),
            nonce: U256::from(42u64),
        }
    }

    fn test_burn() -> PendingBurn {
        PendingBurn {
            height: BlockHeight(100),
            event_index: 0,
            evm_recipient: "0xabcdef1234567890abcdef1234567890abcdef12"
                .parse()
                .unwrap(),
            amount: Amount::from_attos(500_000),
        }
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_insert_and_query_deposit(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;

        let row: (String, Vec<u8>) = sqlx::query_as(
            "SELECT amount, depositor FROM pending_deposits WHERE source_chain_id = 8453",
        )
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(row.0, "1000000");
        assert_eq!(row.1, Address::from([0xCC; 20]).as_slice());
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_insert_deposit_idempotent(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;
        db.insert_deposit(&test_deposit()).await?;

        assert_eq!(db.pending_deposits_count().await?, 1);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_partial_processing_splits_pending_and_finished(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;

        let mut keys = Vec::new();
        for log_index in 0..5 {
            let mut deposit = test_deposit();
            deposit.key.log_index = log_index;
            db.insert_deposit(&deposit).await?;
            keys.push(deposit.key);
        }
        assert_eq!(db.pending_deposits_count().await?, 5);

        for key in &keys[..2] {
            db.update_deposit_status(key, "completed").await?;
        }

        assert_eq!(db.pending_deposits_count().await?, 3);
        let (finished_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM finished_deposits")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(finished_count, 2);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_store_and_retrieve_deposit_raw(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;

        let raw_bytes = vec![1, 2, 3, 4, 5];
        db.store_deposit_raw(&test_deposit_key(), &raw_bytes)
            .await?;

        let (raw,): (Vec<u8>,) = sqlx::query_as(
            "SELECT raw_operation FROM pending_deposits WHERE source_chain_id = 8453",
        )
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(raw, raw_bytes);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_insert_and_query_burn(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;

        let burn = test_burn();
        let row: (String, String) = sqlx::query_as(
            "SELECT evm_recipient, amount FROM pending_burns WHERE linera_height = 100",
        )
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(row.0, format!("{:#x}", burn.evm_recipient));
        assert_eq!(row.1, burn.amount.to_string());
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_insert_burn_idempotent(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;
        db.insert_burn(&test_burn()).await?;

        assert_eq!(db.pending_burns_count().await?, 1);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_store_and_retrieve_burn_raw(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;

        let cert_bytes = vec![10, 20, 30, 40, 50];
        db.store_burn_raw(BlockHeight(100), 0, &cert_bytes).await?;

        let (raw,): (Vec<u8>,) =
            sqlx::query_as("SELECT raw_cert FROM pending_burns WHERE linera_height = 100")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(raw, cert_bytes);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_complete_deposit_moves_to_finished(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;
        db.update_deposit_status(&test_deposit_key(), "completed")
            .await?;

        assert_eq!(
            db.pending_deposits_count().await?,
            0,
            "row should be removed from pending"
        );

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_deposits WHERE source_chain_id = 8453")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(status, "completed");
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_fail_deposit_moves_to_finished(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;
        db.update_deposit_status(&test_deposit_key(), "failed")
            .await?;

        assert_eq!(db.pending_deposits_count().await?, 0);

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_deposits WHERE source_chain_id = 8453")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(status, "failed");
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_complete_burn_moves_to_finished(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;
        db.update_burn_status(BlockHeight(100), 0, "completed")
            .await?;

        assert_eq!(db.pending_burns_count().await?, 0);

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_burns WHERE linera_height = 100")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(status, "completed");
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_fail_burn_moves_to_finished(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;
        db.update_burn_status(BlockHeight(100), 0, "failed").await?;

        assert_eq!(db.pending_burns_count().await?, 0);

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_burns WHERE linera_height = 100")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(status, "failed");
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_replay_preserves_original_finished_deposit(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_deposit(&test_deposit()).await?;
        db.update_deposit_status(&test_deposit_key(), "completed")
            .await?;

        // Replay: re-insert the same deposit and try to mark it failed.
        db.insert_deposit(&test_deposit()).await?;
        db.update_deposit_status(&test_deposit_key(), "failed")
            .await?;

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_deposits WHERE source_chain_id = 8453")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(
            status, "completed",
            "original terminal status must be preserved on replay"
        );
        assert_eq!(
            db.pending_deposits_count().await?,
            0,
            "replayed pending row must be cleared"
        );
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_replay_preserves_original_finished_burn(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        db.insert_burn(&test_burn()).await?;
        db.update_burn_status(BlockHeight(100), 0, "completed")
            .await?;

        db.insert_burn(&test_burn()).await?;
        db.update_burn_status(BlockHeight(100), 0, "failed").await?;

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM finished_burns WHERE linera_height = 100")
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(status, "completed");
        assert_eq!(db.pending_burns_count().await?, 0);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_load_pending_excludes_finished_deposits(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        let first = test_deposit();
        let mut second = test_deposit();
        second.key.log_index = 1;
        db.insert_deposit(&first).await?;
        db.insert_deposit(&second).await?;
        db.update_deposit_status(&first.key, "completed").await?;

        let pending = db.load_pending_deposits().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key.log_index, 1);
        Ok(())
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "file_backed")]
    #[tokio::test]
    async fn test_load_pending_excludes_finished_burns(use_file: bool) -> Result<()> {
        let db = open_db(use_file).await?;
        let mut second = test_burn();
        second.event_index = 1;
        db.insert_burn(&test_burn()).await?;
        db.insert_burn(&second).await?;
        db.update_burn_status(BlockHeight(100), 0, "completed")
            .await?;

        let pending = db.load_pending_burns().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event_index, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_persistence_survives_reopen() -> Result<()> {
        let path = std::path::PathBuf::from("/tmp/bridge_db_test_reopen.sqlite3");
        std::fs::remove_file(&path).ok(); // Best-effort pre-cleanup; NotFound is the common case.

        {
            let db = BridgeDb::open(&path).await?;
            db.insert_deposit(&test_deposit()).await?;
            db.update_deposit_status(&test_deposit_key(), "completed")
                .await?;
        }

        {
            let db = BridgeDb::open(&path).await?;
            let (status,): (String,) =
                sqlx::query_as("SELECT status FROM finished_deposits WHERE source_chain_id = 8453")
                    .fetch_one(&db.pool)
                    .await?;
            assert_eq!(status, "completed");
        }

        std::fs::remove_file(&path).ok(); // Best-effort post-test cleanup.
        Ok(())
    }
}
