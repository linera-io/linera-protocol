// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the IndexedDB Web database.

use std::rc::Rc;

use futures::future;
use indexed_db_futures::{js_sys, prelude::*, web_sys};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

const DATABASE_NAME: &str = "linera";

/// A browser implementation of a key-value store using the [IndexedDB
/// API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API#:~:text=IndexedDB%20is%20a%20low%2Dlevel,larger%20amounts%20of%20structured%20data.).
pub struct IndexedDbDatabase {
    /// The database used for storing the data.
    pub database: Rc<IdbDatabase>,
    /// The object store name used for storing the data.
    pub object_store_name: String,
    /// The maximum number of queries used for the stream.
    pub max_stream_queries: usize,
}

/// A logical partition of [`IndexedDbDatabase`]
pub struct IndexedDbStore {
    /// The database used for storing the data.
    pub database: Rc<IdbDatabase>,
    /// The object store name used for storing the data.
    pub object_store_name: String,
    /// The maximum number of queries used for the stream.
    pub max_stream_queries: usize,
    /// The key being used at the start of the writing
    start_key: Vec<u8>,
}

impl IndexedDbStore {
    fn with_object_store<R>(
        &self,
        f: impl FnOnce(IdbObjectStore) -> R,
    ) -> Result<R, IndexedDbStoreError> {
        let transaction = self.database.transaction_on_one(&self.object_store_name)?;
        let object_store = transaction.object_store(&self.object_store_name)?;
        Ok(f(object_store))
    }

    fn full_key(&self, key: &[u8]) -> Vec<u8> {
        let mut full_key = self.start_key.clone();
        full_key.extend(key);
        full_key
    }
}

impl IndexedDbDatabase {
    fn open_internal(&self, start_key: Vec<u8>) -> Result<IndexedDbStore, IndexedDbStoreError> {
        let database = self.database.clone();
        let object_store_name = self.object_store_name.clone();
        let max_stream_queries = self.max_stream_queries;
        Ok(IndexedDbStore {
            database,
            object_store_name,
            max_stream_queries,
            start_key,
        })
    }
}

fn prefix_to_range(prefix: &[u8]) -> Result<web_sys::IdbKeyRange, wasm_bindgen::JsValue> {
    let lower = js_sys::Uint8Array::from(prefix);
    if let Some(upper) = get_upper_bound_option(prefix) {
        let upper = js_sys::Uint8Array::from(&upper[..]);
        web_sys::IdbKeyRange::bound_with_lower_open_and_upper_open(
            &lower.into(),
            &upper.into(),
            false,
            true,
        )
    } else {
        web_sys::IdbKeyRange::lower_bound(&lower.into())
    }
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
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexedDbStoreError> {
        let key = self.full_key(key);
        let key = js_sys::Uint8Array::from(key.as_slice());
        let value = self.with_object_store(|o| o.get(&key))??.await?;
        Ok(value.map(|v| js_sys::Uint8Array::new(&v).to_vec()))
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, IndexedDbStoreError> {
        let key = self.full_key(key);
        let key = js_sys::Uint8Array::from(key.as_slice());
        let count = self.with_object_store(|o| o.count_with_key(&key))??.await?;
        assert!(count < 2);
        Ok(count == 1)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, IndexedDbStoreError> {
        future::try_join_all(
            keys.into_iter()
                .map(|key| async move { self.contains_key(&key).await }),
        )
        .await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, IndexedDbStoreError> {
        future::try_join_all(
            keys.into_iter()
                .map(|key| async move { self.read_value_bytes(&key).await }),
        )
        .await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, IndexedDbStoreError> {
        let key_prefix = self.full_key(key_prefix);
        let range = prefix_to_range(&key_prefix)?;
        Ok(self
            .with_object_store(|o| o.get_all_keys_with_key(&range))??
            .await?
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, IndexedDbStoreError> {
        let mut key_values = vec![];
        let key_prefix = self.full_key(key_prefix);
        let range = prefix_to_range(&key_prefix)?;
        let transaction = self.database.transaction_on_one(&self.object_store_name)?;
        let object_store = transaction.object_store(&self.object_store_name)?;
        let Some(cursor) = object_store.open_cursor_with_range_owned(range)?.await? else {
            return Ok(key_values);
        };

        loop {
            let Some(key) = cursor.primary_key() else {
                break;
            };
            let key = js_sys::Uint8Array::new(&key);
            key_values.push((
                key.subarray(key_prefix.len() as u32, key.length()).to_vec(),
                js_sys::Uint8Array::new(&cursor.value()).to_vec(),
            ));
            if !cursor.continue_cursor()?.await? {
                break;
            }
        }

        Ok(key_values)
    }
}

impl WritableKeyValueStore for IndexedDbStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), IndexedDbStoreError> {
        let transaction = self
            .database
            .transaction_on_one_with_mode(&self.object_store_name, IdbTransactionMode::Readwrite)?;
        let object_store = transaction.object_store(&self.object_store_name)?;

        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => {
                    let key = self.full_key(&key);
                    object_store
                        .put_key_val_owned(
                            js_sys::Uint8Array::from(&key[..]),
                            &js_sys::Uint8Array::from(&value[..]),
                        )?
                        .await?;
                }
                WriteOperation::Delete { key } => {
                    let key = self.full_key(&key);
                    object_store
                        .delete_owned(js_sys::Uint8Array::from(&key[..]))?
                        .await?;
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let key_prefix = self.full_key(&key_prefix);
                    object_store
                        .delete_owned(prefix_to_range(&key_prefix[..])?)?
                        .await?;
                }
            }
        }
        let mut key = self.start_key.clone();
        key[0] = STORED_ROOT_KEYS_PREFIX[0];
        object_store
            .put_key_val_owned(
                js_sys::Uint8Array::from(&key[..]),
                &js_sys::Uint8Array::default(),
            )?
            .await?;
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), IndexedDbStoreError> {
        Ok(())
    }
}

impl KeyValueDatabase for IndexedDbDatabase {
    type Config = IndexedDbStoreConfig;

    type Store = IndexedDbStore;

    fn get_name() -> String {
        "indexed db".to_string()
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, IndexedDbStoreError> {
        let namespace = namespace.to_string();
        let object_store_name = namespace.clone();
        let mut database = IdbDatabase::open(DATABASE_NAME)?.await?;

        if !database.object_store_names().any(|n| n == namespace) {
            let version = database.version();
            database.close();
            let mut db_req = IdbDatabase::open_f64(DATABASE_NAME, version + 1.0)?;
            db_req.set_on_upgrade_needed(Some(move |event: &IdbVersionChangeEvent| {
                event.db().create_object_store(&namespace)?;
                Ok(())
            }));
            database = db_req.await?;
        }
        let database = Rc::new(database);
        Ok(Self {
            database,
            object_store_name,
            max_stream_queries: config.max_stream_queries,
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, IndexedDbStoreError> {
        let mut start_key = ROOT_KEY_DOMAIN.to_vec();
        start_key.extend(root_key);
        self.open_internal(start_key)
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, IndexedDbStoreError> {
        self.open_shared(root_key)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, IndexedDbStoreError> {
        Ok(Self::connect(config, "")
            .await?
            .database
            .object_store_names()
            .collect())
    }

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, IndexedDbStoreError> {
        let database = Self::connect(config, namespace).await?;
        let start_key = STORED_ROOT_KEYS_PREFIX.to_vec();
        let store = database.open_internal(start_key)?;
        store.find_keys_by_prefix(&[]).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, IndexedDbStoreError> {
        Ok(Self::connect(config, "")
            .await?
            .database
            .object_store_names()
            .any(|x| x == namespace))
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), IndexedDbStoreError> {
        Self::connect(config, "")
            .await?
            .database
            .create_object_store(namespace)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), IndexedDbStoreError> {
        Ok(Self::connect(config, "")
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

/// The error type for [`IndexedDbStore`].
#[derive(Error, Debug)]
pub enum IndexedDbStoreError {
    /// Serialization error with BCS.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// A DOM exception occurred in the IndexedDB operations
    #[error("DOM exception: {0:?}")]
    Dom(gloo_utils::errors::JsError),

    /// JavaScript threw an exception whilst handling IndexedDB operations
    #[error("JavaScript exception: {0:?}")]
    Js(gloo_utils::errors::JsError),
}

impl From<web_sys::DomException> for IndexedDbStoreError {
    fn from(dom_exception: web_sys::DomException) -> Self {
        let value: &wasm_bindgen::JsValue = dom_exception.as_ref();
        Self::Dom(value.clone().try_into().unwrap())
    }
}

impl From<wasm_bindgen::JsValue> for IndexedDbStoreError {
    fn from(js_value: wasm_bindgen::JsValue) -> Self {
        Self::Js(js_value.try_into().unwrap())
    }
}

impl KeyValueStoreError for IndexedDbStoreError {
    const BACKEND: &'static str = "indexed_db";
}
