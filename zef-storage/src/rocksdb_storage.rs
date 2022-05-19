// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::Storage;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use zef_base::{
    chain::ChainState,
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId},
};

#[cfg(test)]
#[path = "unit_tests/rocksdb_storage_tests.rs"]
mod rocksdb_storage_tests;

/// Rocksdb-based store.
#[derive(Debug)]
pub struct RocksdbStore {
    /// RockdbDB handle.
    db: rocksdb::DB,
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
        Err(_e) => vec![String::from("None")],
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
    pub fn new(path: PathBuf) -> Result<Self, rocksdb::Error> {
        assert!(path.is_dir());
        Ok(Self {
            db: open_db(&path)?,
        })
    }

    fn get_db_handler(&mut self, kind: &str) -> Result<ColumnHandle<'_>, rocksdb::Error> {
        if self.db.cf_handle(kind).is_none() {
            self.db.create_cf(kind, &rocksdb::Options::default())?;
        }

        let handle = self
            .db
            .cf_handle(kind)
            .expect("Unable to create Rocksdb ColumnFamily");

        Ok(ColumnHandle::new(&self.db, handle))
    }

    async fn read_value(
        &mut self,
        kind: &str,
        key: &[u8],
    ) -> Result<std::option::Option<Vec<u8>>, rocksdb::Error> {
        self.get_db_handler(kind)?.get(key)
    }

    async fn write_value(
        &mut self,
        kind: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), rocksdb::Error> {
        self.get_db_handler(kind)?.put(key, value)
    }

    async fn remove_value(&mut self, kind: &str, key: &[u8]) -> Result<(), rocksdb::Error> {
        self.get_db_handler(kind)?.delete(key)
    }

    async fn read<K, V>(&mut self, key: &K) -> Result<Option<V>, Error>
    where
        K: serde::Serialize + std::fmt::Debug,
        V: serde::de::DeserializeOwned,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        let value = self
            .read_value(kind, &key)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("{}: {}", kind, e),
            })?;
        let result = match value {
            Some(v) => Some(ron::de::from_bytes(&v).map_err(|e| Error::StorageBcsError {
                error: format!("{}: {}", kind, e),
            })?),
            None => None,
        };
        Ok(result)
    }

    async fn write<'b, K, V>(&mut self, key: &K, value: &V) -> Result<(), Error>
    where
        K: serde::Serialize + std::fmt::Debug,
        V: serde::Serialize + serde::Deserialize<'b> + std::fmt::Debug,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        let value = ron::to_string(&value).expect("should not fail");
        self.write_value(kind, &key, value.as_bytes())
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("write {}", e),
            })?;
        Ok(())
    }

    async fn remove<K, V>(&mut self, key: &K) -> Result<(), Error>
    where
        K: serde::Serialize + std::fmt::Debug,
        V: serde::de::DeserializeOwned,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
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
pub struct RocksdbStoreClient(Arc<Mutex<RocksdbStore>>);

impl RocksdbStoreClient {
    pub fn new(path: PathBuf) -> Result<Self, rocksdb::Error> {
        Ok(RocksdbStoreClient(Arc::new(Mutex::new(RocksdbStore::new(
            path,
        )?))))
    }
}

#[async_trait]
impl Storage for RocksdbStoreClient {
    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainState, Error> {
        let mut store = self.0.lock().await;
        Ok(store
            .read(&id)
            .await?
            .unwrap_or_else(|| ChainState::new(id)))
    }

    async fn write_chain(&mut self, state: ChainState) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.write(&state.state.chain_id, &state).await
    }

    async fn remove_chain(&mut self, id: ChainId) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.remove::<_, ChainState>(&id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        let mut store = self.0.lock().await;
        store
            .read(&hash)
            .await?
            .ok_or(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.write(&certificate.hash, &certificate).await
    }
}
