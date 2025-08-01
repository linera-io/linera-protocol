// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQLite database configuration and initialization utilities.

use crate::{common::IndexerError, sqlite_db::SqliteDatabase};

/// SQLite database configuration
#[derive(Clone, Debug)]
pub struct SqliteConfig {
    /// SQLite database file path
    pub database_path: String,
}

impl SqliteConfig {
    pub fn new(database_path: String) -> Self {
        Self { database_path }
    }

    /// Initialize SQLite database
    pub async fn initialize(&self) -> Result<SqliteDatabase, IndexerError> {
        let database_url = self.database_path.to_string();
        SqliteDatabase::new(&database_url)
            .await
            .map_err(|e| IndexerError::Other(e.into()))
    }
}
