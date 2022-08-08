// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::Storage;
use async_trait::async_trait;
use linera_base::{
    chain::ChainState,
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId},
};
use moka::sync::Cache;
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(test)]
#[path = "unit_tests/rocksdb_storage_tests.rs"]
mod rocksdb_storage_tests;

/// Rocksdb-based store.
#[derive(Debug)]
pub struct RocksdbStore {
    /// RocksDB handle.
    db: rocksdb::DB,
    cache: Cache<Vec<u8>, Arc<dyn Any + Send + Sync + 'static>>,
}

#[derive(Clone, Copy)]
pub struct ColumnHandle<'db> {
    db: &'db rocksdb::DB,
    column: &'db rocksdb::ColumnFamily,
}

impl<'db> ColumnHandle<'db> {
    pub fn new(db: &'db rocksdb::DB, column: &'db rocksdb::ColumnFamily) -> Self {
        ColumnHandle { db, column }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.db.get_cf(self.column, key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.put_cf(self.column, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.delete_cf(self.column, key)
    }
}

fn open_db(path: &Path) -> Result<rocksdb::DB, rocksdb::Error> {
    let cfs = match rocksdb::DB::list_cf(&rocksdb::Options::default(), path) {
        Ok(cfs) => cfs,
        // Need to declare all the potential Column Families in advance to keep RocksDB immutable
        Err(_e) => vec![
            "default".to_string(),
            serde_name::trace_name::<ChainState>().unwrap().to_string(),
            serde_name::trace_name::<Certificate>().unwrap().to_string(),
        ],
    };

    let mut v_cf: Vec<rocksdb::ColumnFamilyDescriptor> = Vec::new();
    for i in &cfs {
        v_cf.push(rocksdb::ColumnFamilyDescriptor::new(
            i,
            rocksdb::Options::default(),
        ));
    }

    let mut db_opts = rocksdb::Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    rocksdb::DB::open_cf_descriptors(&db_opts, path, v_cf)
}

impl RocksdbStore {
    pub fn new(path: PathBuf, cache_size: u64) -> Result<Self, rocksdb::Error> {
        assert!(path.is_dir());
        Ok(Self {
            db: open_db(&path)?,
            cache: Cache::<std::vec::Vec<u8>, Arc<dyn Send + Sync + Any + 'static>>::new(
                cache_size,
            ),
        })
    }

    fn get_db_handler(&self, kind: &str) -> Result<ColumnHandle<'_>, rocksdb::Error> {
        let handle = self
            .db
            .cf_handle(kind)
            .expect("Unable to access a Rocksdb ColumnFamily");

        Ok(ColumnHandle::new(&self.db, handle))
    }

    async fn read_value(
        &self,
        kind: &str,
        key: &[u8],
    ) -> Result<std::option::Option<Vec<u8>>, rocksdb::Error> {
        self.get_db_handler(kind)?.get(key)
    }

    async fn write_value(
        &self,
        kind: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), rocksdb::Error> {
        self.get_db_handler(kind)?.put(key, value)
    }

    async fn remove_value(&self, kind: &str, key: &[u8]) -> Result<(), rocksdb::Error> {
        self.get_db_handler(kind)?.delete(key)
    }

    async fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: serde::Serialize + std::fmt::Debug,
        V: serde::de::DeserializeOwned + Send + Clone + Sync + Any + 'static,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let result = match self.cache.get(&key) {
            Some(v) => Some(V::clone(&v.downcast().expect("wrong bytes from cache"))),
            None => {
                let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
                let value =
                    self.read_value(kind, &key)
                        .await
                        .map_err(|e| Error::StorageIoError {
                            error: format!("{}: {}", kind, e),
                        })?;
                match value {
                    Some(v) => {
                        let from_db =
                            ron::de::from_bytes::<V>(&v).map_err(|e| Error::StorageBcsError {
                                error: format!("{}: {}", kind, e),
                            })?;
                        self.cache.insert(key.clone(), Arc::new(from_db.clone()));
                        Some(from_db)
                    }
                    None => None,
                }
            }
        };
        Ok(result)
    }

    async fn write<'b, K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: serde::Serialize + std::fmt::Debug + 'static,
        V: serde::Serialize
            + serde::Deserialize<'b>
            + std::fmt::Debug
            + std::marker::Sync
            + Send
            + Clone
            + 'static,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        self.cache.insert(key.clone(), Arc::new(value.clone()));
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        let value = ron::to_string(&value).expect("should not fail");
        self.write_value(kind, &key, value.as_bytes())
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("write {}", e),
            })?;
        Ok(())
    }

    async fn remove<K, V>(&self, key: &K) -> Result<(), Error>
    where
        K: serde::Serialize + std::fmt::Debug,
        V: serde::de::DeserializeOwned,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        self.cache.invalidate(&key);
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        self.remove_value(kind, &key)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("remove {}", e),
            })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RocksdbStoreClient(Arc<RocksdbStore>);

impl RocksdbStoreClient {
    pub fn new(path: PathBuf, cache_size: u64) -> Result<Self, rocksdb::Error> {
        Ok(RocksdbStoreClient(Arc::new(RocksdbStore::new(
            path, cache_size,
        )?)))
    }
}

#[async_trait]
impl Storage for RocksdbStoreClient {
    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainState, Error> {
        Ok(self
            .0
            .read(&id)
            .await?
            .unwrap_or_else(|| ChainState::new(id)))
    }

    async fn write_chain(&mut self, state: ChainState) -> Result<(), Error> {
        self.0.write(&state.state.system.chain_id, &state).await
    }

    async fn remove_chain(&mut self, id: ChainId) -> Result<(), Error> {
        self.0.remove::<_, ChainState>(&id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        self.0
            .read(&hash)
            .await?
            .ok_or(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        self.0.write(&certificate.hash, &certificate).await
    }
}
