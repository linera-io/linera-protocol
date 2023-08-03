// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides a KeyValueStoreClient for the ScyllaDB database.
//! The code is functional but some aspects are missing.
//!
//! The current connection is done via a Session and a corresponding
//! primary key that we name `namespace`. The maximum number of
//! concurrent queries is controlled by max_concurrent_queries.
//!
//! Support for ScyllaDb is experimental and is still missing important features:
//! TODO(#936): Enable all tests using ScyllaDB
//! TODO(#935): Read several keys at once
//! TODO(#934): Journaling operations
//! TODO(#933): Enable the CI.

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_upper_bound_option, KeyValueStoreClient},
};
use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::{ops::Deref, sync::Arc};
use thiserror::Error;

/// The creation of a ScyllaDb client that can be used for accessing it.
/// The `Vec<u8>`is a primary key.
type ScyllaDbClientPair = (Session, Vec<u8>);

/// We limit the number of connections that can be done.
const SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connection in the stream is limited.
const SCYLLA_DB_MAX_STREAM_QUERIES: usize = 10;

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbClient {
    client: Arc<ScyllaDbClientPair>,
    count: Arc<Semaphore>,
    max_concurrent_queries: Option<usize>,
    max_stream_queries: usize,
}

/// The error type for [`ScyllaDbClient`]
#[derive(Error, Debug)]
pub enum ScyllaDbContextError {
    /// BCS serialization error.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// A query error in ScyllaDb
    #[error(transparent)]
    ScyllaDbQueryError(#[from] scylla::transport::errors::QueryError),

    /// A query error in ScyllaDb
    #[error(transparent)]
    ScyllaDbNewSessionError(#[from] scylla::transport::errors::NewSessionError),
}

#[async_trait]
impl KeyValueStoreClient for ScyllaDbClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = ScyllaDbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::read_key_internal(client, key.to_vec()).await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        // There is probably a better way in ScyllaDB for downloading several keys than this one.
        let client = self.client.deref();
        let _guard = self.acquire().await;
        let mut values = Vec::new();
        for key in keys {
            let value = Self::read_key_internal(client, key.to_vec()).await?;
            values.push(value);
        }
        Ok(values)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::find_keys_by_prefix_internal(client, key_prefix.to_vec()).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::find_key_values_by_prefix_internal(client, key_prefix.to_vec()).await
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::write_batch_internal(client, batch).await
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ScyllaDbClient {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match self.max_concurrent_queries {
            None => None,
            Some(_max_concurrent_queries) => Some(self.count.acquire().await),
        }
    }

    async fn read_key_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        // Read the value of a key
        let values = (namespace.to_vec(), key);
        let rows = session
            .query(
                "SELECT v FROM kv.pairs WHERE namespace = ? AND k = ?",
                values,
            )
            .await?;
        if let Some(rows) = rows.rows {
            if let Some(row) = rows.into_typed::<(Vec<u8>,)>().next() {
                let value = row.unwrap();
                return Ok(Some(value.0));
            }
        }
        Ok(None)
    }

    async fn insert_key_value_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        let query = "INSERT INTO kv.pairs (namespace, k, v) VALUES (?, ?, ?)";
        let values = (namespace.to_vec(), key, value);
        session.query(query, values).await?;
        Ok(())
    }

    async fn delete_key_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        let values = (namespace.to_vec(), key);
        let query = "DELETE FROM kv.pairs WHERE namespace = ? AND k = ?";
        session.query(query, values).await?;
        Ok(())
    }

    async fn delete_key_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (namespace.to_vec(), key_prefix);
                let query = "DELETE FROM kv.pairs WHERE namespace = ? AND k >= ?";
                session.query(query, values).await?;
            }
            Some(upper_bound) => {
                let values = (namespace.to_vec(), key_prefix, upper_bound);
                let query = "DELETE FROM kv.pairs WHERE namespace = ? AND k >= ? AND k < ?";
                session.query(query, values).await?;
            }
        }
        Ok(())
    }

    async fn write_batch_internal(
        client: &ScyllaDbClientPair,
        batch: Batch,
    ) -> Result<(), ScyllaDbContextError> {
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => {
                    Self::insert_key_value_internal(client, key, value).await?;
                }
                WriteOperation::Delete { key } => {
                    Self::delete_key_internal(client, key).await?;
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    Self::delete_key_prefix_internal(client, key_prefix).await?;
                }
            }
        }
        Ok(())
    }

    async fn find_keys_by_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        // Read the value of a key
        let len = key_prefix.len();
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (namespace.to_vec(), key_prefix);
                let query = "SELECT k FROM kv.pairs WHERE namespace = ? AND k >= ?";
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (namespace.to_vec(), key_prefix, upper_bound);
                let query = "SELECT k FROM kv.pairs WHERE namespace = ? AND k >= ? AND k < ?";
                session.query(query, values).await?
            }
        };
        let mut keys = Vec::new();
        if let Some(rows) = rows.rows {
            for row in rows.into_typed::<(Vec<u8>,)>() {
                let key = row.unwrap();
                let short_key = key.0[len..].to_vec();
                keys.push(short_key);
            }
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbContextError> {
        let session = &client.0;
        let namespace = &client.1;
        // Read the value of a key
        let len = key_prefix.len();
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (namespace.to_vec(), key_prefix);
                let query = "SELECT k FROM kv.pairs WHERE namespace = ? AND k >= ?";
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (namespace.to_vec(), key_prefix, upper_bound);
                let query = "SELECT k,v FROM kv.pairs WHERE namespace = ? AND k >= ? AND k < ?";
                session.query(query, values).await?
            }
        };
        let mut key_values = Vec::new();
        if let Some(rows) = rows.rows {
            for row in rows.into_typed::<(Vec<u8>, Vec<u8>)>() {
                let key = row.unwrap();
                let short_key = key.0[len..].to_vec();
                key_values.push((short_key, key.1));
            }
        }
        Ok(key_values)
    }

    async fn new(
        uri: &str,
        namespace: Vec<u8>,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
    ) -> Result<Self, ScyllaDbContextError> {
        // Create a session builder and specify the ScyllaDB contact points
        let session = SessionBuilder::new().known_node(uri).build().await?;

        // Create a keyspace if it doesn't exist
        session
            .query(
                "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                &[],
            )
            .await?;

        // Dropping the table
        session.query("DROP TABLE IF EXISTS kv.pairs;", &[]).await?;

        // Create a table if it doesn't exist
        session
            .query(
                "CREATE TABLE kv.pairs (namespace blob, k blob, v blob, primary key (namespace, k))",
                &[],
            )
            .await?;
        let client = (session, namespace);
        let client = Arc::new(client);
        let n = max_concurrent_queries.unwrap_or(1);
        let count = Arc::new(Semaphore::new(n));
        Ok(ScyllaDbClient {
            client,
            count,
            max_concurrent_queries,
            max_stream_queries,
        })
    }
}

/// Creates a ScyllaDb test client
pub async fn create_scylla_db_test_client() -> ScyllaDbClient {
    let uri = "localhost:9042";
    let dummy_namespace = vec![0];
    let max_concurrent_queries = Some(SCYLLA_DB_MAX_CONCURRENT_QUERIES);
    let max_stream_queries = SCYLLA_DB_MAX_STREAM_QUERIES;
    ScyllaDbClient::new(
        uri,
        dummy_namespace,
        max_concurrent_queries,
        max_stream_queries,
    )
    .await
    .expect("client")
}
