// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the IndexedDB Web database.

use std::{convert::Infallible, future::Future, ops::Bound, rc::Rc};

use futures::future;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use web_sys::{js_sys, wasm_bindgen::JsValue};

use crate::{
    batch::{Batch, WriteOperation},
    common::get_upper_bound_option,
    store::{
        KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};

/// The initial configuration of the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDbStoreConfig {
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
}

/// The prefixes being used in the system
static ROOT_KEY_DOMAIN: [u8; 1] = [0];
static STORED_ROOT_KEYS_PREFIX: [u8; 1] = [1];

/// The number of streams for the test
pub const TEST_INDEX_DB_MAX_STREAM_QUERIES: usize = 10;

const OBJECT_STORE_NAME: &str = "linera";

/// A browser implementation of a key-value store using the [IndexedDB
/// API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API#:~:text=IndexedDB%20is%20a%20low%2Dlevel,larger%20amounts%20of%20structured%20data.).
#[derive(Clone)]
pub struct IndexedDbDatabase {
    /// The database used for storing the data.
    pub database: Rc<indexed_db::Database<Infallible>>,
    /// The maximum number of queries used for the stream.
    pub max_stream_queries: usize,
    /// The database name used for storing the data.
    pub namespace: String,
}

/// A logical partition of [`IndexedDbDatabase`]
#[derive(Clone)]
pub struct IndexedDbStore {
    /// The database used for storing the data.
    pub database: IndexedDbDatabase,
    /// The key being used at the start of the writing
    start_key: Vec<u8>,
}

impl IndexedDbStore {
    async fn with_object_store<Fut: Future<Output: 'static>>(
        &self,
        f: impl FnOnce(indexed_db::ObjectStore<Infallible>) -> Fut + 'static,
    ) -> Result<Fut::Output> {
        self.database.database.transaction(&[OBJECT_STORE_NAME]).run(|transaction| async move {
            Ok(f(transaction.object_store(OBJECT_STORE_NAME)?).await)
        }).await.map_err(Into::into)
    }

    fn full_key(&self, key: &[u8]) -> Vec<u8> {
        let mut full_key = self.start_key.clone();
        full_key.extend(key);
        full_key
    }
}

impl IndexedDbDatabase {
    fn open_internal(&self, start_key: Vec<u8>) -> IndexedDbStore {
        IndexedDbStore {
            database: self.clone(),
            start_key,
        }
    }
}

fn database_name(namespace: &str) -> String {
    format!("linera/{namespace}")
}

fn prefix_to_range(prefix: &[u8]) -> (Bound<JsValue>, Bound<JsValue>) {
    let lower = Bound::Included(js_sys::Uint8Array::from(prefix).into());
    let upper = if let Some(upper) = get_upper_bound_option(prefix) {
        Bound::Excluded(js_sys::Uint8Array::from(&upper[..]).into())
    } else {
        Bound::Unbounded
    };

    (lower, upper)
}

impl WithError for IndexedDbStore {
    type Error = IndexedDbStoreError;
}

impl WithError for IndexedDbDatabase {
    type Error = IndexedDbStoreError;
}

impl ReadableKeyValueStore for IndexedDbStore {
    const MAX_KEY_SIZE: usize = usize::MAX;

    fn max_stream_queries(&self) -> usize {
        self.database.max_stream_queries
    }

    fn root_key(&self) -> Result<Vec<u8>> {
        assert!(self.start_key.starts_with(&ROOT_KEY_DOMAIN));
        let root_key = bcs::from_bytes(&self.start_key[ROOT_KEY_DOMAIN.len()..])?;
        Ok(root_key)
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = self.full_key(key);
        let value = self
            .with_object_store(move |o| o.get(&js_sys::Uint8Array::from(key.as_slice())))
            .await??;
        Ok(value.map(|v| js_sys::Uint8Array::new(&v).to_vec()))
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let key = self.full_key(key);
        Ok(self
            .with_object_store(move |o| o.contains(&js_sys::Uint8Array::from(key.as_slice())))
            .await??)
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>> {
        future::try_join_all(
            keys.iter()
                .map(|key| async move { self.contains_key(key).await }),
        )
        .await
    }

    async fn read_multi_values_bytes(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        future::try_join_all(
            keys.iter()
                .map(|key| async move { self.read_value_bytes(key).await }),
        )
        .await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let key_prefix = self.full_key(key_prefix);
        let range = prefix_to_range(&key_prefix);
        Ok(self
            .with_object_store(|o| o.get_all_keys_in(range, None))
            .await??
            .into_iter()
            .map(|key| {
                let key = js_sys::Uint8Array::new(&key);
                key.subarray(key_prefix.len() as u32, key.length()).to_vec()
            })
            .collect())
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let key_prefix = self.full_key(key_prefix);
        let range = prefix_to_range(&key_prefix);
        self.with_object_store(|object_store| async move {
            let mut key_values = vec![];
            let mut cursor = object_store.cursor().range(range)?.open().await?;

            while let Some(key) = cursor.primary_key() {
                let key = js_sys::Uint8Array::new(&key);
                key_values.push((
                    key.subarray(key_prefix.len() as u32, key.length()).to_vec(),
                    js_sys::Uint8Array::new(
                        &cursor
                            .value()
                            .expect("we should have a value because we have a key"),
                    )
                    .to_vec(),
                ));
                cursor.advance(1).await?;
            }

            Ok(key_values)
        })
        .await?
    }
}

impl WritableKeyValueStore for IndexedDbStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<()> {
        let mut start_key = self.start_key.clone();
        self.database
            .database
            .transaction(&[OBJECT_STORE_NAME])
            .rw()
            .run(move |transaction| async move {
                let object_store = transaction.object_store(OBJECT_STORE_NAME)?;

                for ent in batch.operations {
                    match ent {
                        WriteOperation::Put { key, value } => {
                            let key = [start_key.as_slice(), key.as_slice()].concat();
                            object_store
                                .put_kv(
                                    &js_sys::Uint8Array::from(&key[..]),
                                    &js_sys::Uint8Array::from(&value[..]),
                                )
                                .await?;
                        }
                        WriteOperation::Delete { key } => {
                            let key = [start_key.as_slice(), key.as_slice()].concat();
                            object_store
                                .delete(&js_sys::Uint8Array::from(&key[..]))
                                .await?;
                        }
                        WriteOperation::DeletePrefix { key_prefix } => {
                            let key_prefix = [start_key.as_slice(), key_prefix.as_slice()].concat();
                            object_store
                                .delete_range(prefix_to_range(&key_prefix[..]))
                                .await?;
                        }
                    }
                }
                start_key[0] = STORED_ROOT_KEYS_PREFIX[0];
                object_store
                    .put_kv(
                        &js_sys::Uint8Array::from(&start_key[..]),
                        &js_sys::Uint8Array::default(),
                    )
                    .await?;

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn clear_journal(&self) -> Result<()> {
        Ok(())
    }
}

impl KeyValueDatabase for IndexedDbDatabase {
    type Config = IndexedDbStoreConfig;

    type Store = IndexedDbStore;

    fn get_name() -> String {
        "indexed db".to_string()
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self> {
        Ok(Self {
            database: indexed_db::Factory::<Infallible>::get()?
                .open(
                    &database_name(namespace),
                    1,
                    |event: indexed_db::VersionChangeEvent<Infallible>| async move {
                        event
                            .database()
                            .build_object_store(OBJECT_STORE_NAME)
                            .create()?;
                        Ok(())
                    },
                )
                .await?
                .into(),
            namespace: namespace.to_string(),
            max_stream_queries: config.max_stream_queries,
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store> {
        let mut start_key = ROOT_KEY_DOMAIN.to_vec();
        start_key.extend(bcs::to_bytes(&root_key)?);
        Ok(self.open_internal(start_key))
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store> {
        self.open_shared(root_key)
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>> {
        // This would be supported if we assume IndexedDB v3, which has good support but
        // is still considered a draft at time of writing.
        tracing::warn!("`list_all` is not currently supported for IndexedDB: listing databases is only possible in IndexedDB v3");
        Ok(vec![])
    }

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>> {
        let start_key = STORED_ROOT_KEYS_PREFIX.to_vec();
        let store = self.open_internal(start_key);
        store.find_keys_by_prefix(&[]).await
    }

    async fn exists(_config: &Self::Config, _namespace: &str) -> Result<bool> {
        // IndexedDB will create the database if it doesn't exist, so let's pretend it
        // always exists.
        Ok(true)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<()> {
        let Self { .. } = Self::connect(config, namespace).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<()> {
        Ok(Self::connect(config, namespace)
            .await?
            .database
            .delete_object_store(namespace)?)
    }
}

#[cfg(with_testing)]
mod testing {
    use super::*;
    use crate::random::generate_test_namespace;

    /// Creates a test IndexedDB client for working.
    pub async fn create_indexed_db_store_stream_queries(
        max_stream_queries: usize,
    ) -> IndexedDbStore {
        let config = IndexedDbStoreConfig { max_stream_queries };
        let namespace = generate_test_namespace();
        let database = IndexedDbDatabase::connect(&config, &namespace)
            .await
            .unwrap();
        database.open_shared(&[]).unwrap()
    }

    /// Creates a test IndexedDB store for working.
    #[cfg(with_testing)]
    pub async fn create_indexed_db_test_store() -> IndexedDbStore {
        create_indexed_db_store_stream_queries(TEST_INDEX_DB_MAX_STREAM_QUERIES).await
    }
}

#[cfg(with_testing)]
pub use testing::*;

type Result<T, E = IndexedDbStoreError> = ::std::result::Result<T, E>;

/// Errors thrown by the IndexedDB backend.
#[derive(Error, Debug)]
pub enum IndexedDbStoreError {
    /// Serialization error with BCS.
    #[error(transparent)]
    Bcs(#[from] bcs::Error),

    /// A DOM exception occurred in the IndexedDB operations
    // #[error("DOM exception: {0}")]
    // Dom(gloo_utils::errors::JsError),

    // /// JavaScript threw an exception whilst handling IndexedDB operations
    // #[error("JavaScript exception: {0}")]
    // Js(gloo_utils::errors::JsError),

    #[error("IndexedDB error: {0:?}")]
    IndexedDb(#[from] indexed_db::Error<Infallible>),
}

// impl From<web_sys::DomException> for Error {
//     fn from(dom_exception: web_sys::DomException) -> Self {
//         let value: &wasm_bindgen::JsValue = dom_exception.as_ref();
//         Self::Dom(value.clone().try_into().unwrap())
//     }
// }

// impl From<wasm_bindgen::JsValue> for IndexedDbStoreError {
//     fn from(js_value: wasm_bindgen::JsValue) -> Self {
//         Self::Js(js_value.try_into().unwrap())
//     }
// }

impl KeyValueStoreError for IndexedDbStoreError {
    const BACKEND: &'static str = "indexed_db";
}
