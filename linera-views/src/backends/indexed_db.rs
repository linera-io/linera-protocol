// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the IndexedDB Web database.

use std::rc::Rc;

use futures::future;
use indexed_db_futures::{js_sys, prelude::*, web_sys};
use thiserror::Error;

use crate::{
    batch::{Batch, WriteOperation},
    common::get_upper_bound_option,
    store::{
        CommonStoreConfig, KeyValueStoreError, LocalAdminKeyValueStore, LocalReadableKeyValueStore,
        LocalWritableKeyValueStore, WithError,
    },
};

/// The initial configuration of the system
#[derive(Debug)]
pub struct IndexedDbStoreConfig {
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl IndexedDbStoreConfig {
    /// Creates a `IndexedDbStoreConfig`. `max_concurrent_queries` and `cache_size` are not used.
    pub fn new(max_stream_queries: usize) -> Self {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries,
            cache_size: 1000,
        };
        Self { common_config }
    }
}

/// The number of streams for the test
pub const TEST_INDEX_DB_MAX_STREAM_QUERIES: usize = 10;

const DATABASE_NAME: &str = "linera";

/// A browser implementation of a key-value store using the [IndexedDB
/// API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API#:~:text=IndexedDB%20is%20a%20low%2Dlevel,larger%20amounts%20of%20structured%20data.).
pub struct IndexedDbStore {
    /// The database used for storing the data.
    pub database: Rc<IdbDatabase>,
    /// The object store name used for storing the data.
    pub object_store_name: String,
    /// The maximum number of queries used for the stream.
    pub max_stream_queries: usize,
    /// The used root key
    root_key: Vec<u8>,
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
        let mut full_key = self.root_key.clone();
        full_key.extend(key);
        full_key
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

impl LocalReadableKeyValueStore for IndexedDbStore {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

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

impl LocalWritableKeyValueStore for IndexedDbStore {
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

        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), IndexedDbStoreError> {
        Ok(())
    }
}

impl LocalAdminKeyValueStore for IndexedDbStore {
    type Config = IndexedDbStoreConfig;

    fn get_name() -> String {
        "indexed db".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, IndexedDbStoreError> {
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
        let root_key = root_key.to_vec();
        Ok(IndexedDbStore {
            database,
            object_store_name,
            max_stream_queries: config.common_config.max_stream_queries,
            root_key,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, IndexedDbStoreError> {
        let database = self.database.clone();
        let object_store_name = self.object_store_name.clone();
        let max_stream_queries = self.max_stream_queries;
        let root_key = root_key.to_vec();
        Ok(Self {
            database,
            object_store_name,
            max_stream_queries,
            root_key,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, IndexedDbStoreError> {
        let root_key = &[];
        Ok(Self::connect(config, "", root_key)
            .await?
            .database
            .object_store_names()
            .collect())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, IndexedDbStoreError> {
        let root_key = &[];
        Ok(Self::connect(config, "", root_key)
            .await?
            .database
            .object_store_names()
            .any(|x| x == namespace))
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), IndexedDbStoreError> {
        let root_key = &[];
        Self::connect(config, "", root_key)
            .await?
            .database
            .create_object_store(namespace)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), IndexedDbStoreError> {
        let root_key = &[];
        Ok(Self::connect(config, "", root_key)
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
        let config = IndexedDbStoreConfig::new(max_stream_queries);
        let namespace = generate_test_namespace();
        let root_key = &[];
        IndexedDbStore::connect(&config, &namespace, root_key)
            .await
            .unwrap()
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

    /// The value is too large for the IndexedDbStore
    #[error("The value is too large for the IndexedDbStore")]
    TooLargeValue,

    /// A DOM exception occurred in the IndexedDB operations
    #[error("DOM exception: {0:?}")]
    Dom(web_sys::DomException),

    /// JavaScript threw an exception whilst handling IndexedDB operations
    #[error("JavaScript exception: {0:?}")]
    Js(wasm_bindgen::JsValue),
}

impl From<web_sys::DomException> for IndexedDbStoreError {
    fn from(dom_exception: web_sys::DomException) -> Self {
        Self::Dom(dom_exception)
    }
}

impl From<wasm_bindgen::JsValue> for IndexedDbStoreError {
    fn from(js_value: wasm_bindgen::JsValue) -> Self {
        Self::Js(js_value)
    }
}

impl KeyValueStoreError for IndexedDbStoreError {
    const BACKEND: &'static str = "indexed_db";
}
