// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::Storage;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    path::{Path, PathBuf},
    sync::Arc,
};
use zef_base::{
    base_types::HashValue,
    chain::{ChainId, ChainState},
    error::Error,
    messages::Certificate,
};

#[cfg(test)]
#[path = "unit_tests/rocksdb_storage_tests.rs"]
mod rocksdb_storage_tests;

/// Rocksdb-based store.
#[derive(Debug)]
pub struct RocksdbStore {
    /// Base path.
    path: PathBuf,
    /// Open DB connections indexed by kind.
    dbs: HashMap<String, rocksdb::DB>,
}

fn open_db(path: &Path, kind: &str) -> Result<rocksdb::DB, rocksdb::Error> {
    let cf_opts = rocksdb::Options::default();
    let cf = rocksdb::ColumnFamilyDescriptor::new(kind, cf_opts);

    let mut db_opts = rocksdb::Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    rocksdb::DB::open_cf_descriptors(&db_opts, path, vec![cf])
}

impl RocksdbStore {
    pub fn new(path: PathBuf) -> Self {
        assert!(path.is_dir());
        Self {
            path,
            dbs: HashMap::new(),
        }
    }

    fn get_db(&mut self, kind: &str) -> Result<&mut rocksdb::DB, rocksdb::Error> {
        match self.dbs.entry(String::from(kind)) {
            Vacant(entry) => {
                let db = open_db(&self.path, kind)?;
                Ok(entry.insert(db))
            }
            Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn read_value(
        &mut self,
        kind: &str,
        key: &[u8],
    ) -> Result<std::option::Option<Vec<u8>>, rocksdb::Error> {
        let db = self.get_db(kind)?;
        match db.get(key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn write_value(
        &mut self,
        kind: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), rocksdb::Error> {
        let db = self.get_db(kind)?;
        db.put(key, value)?;
        Ok(())
    }

    async fn remove_value(&mut self, kind: &str, key: &[u8]) -> Result<(), rocksdb::Error> {
        let db = self.get_db(kind)?;
        db.delete(key)?;
        Ok(())
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
    pub fn new(path: PathBuf) -> Self {
        RocksdbStoreClient(Arc::new(Mutex::new(RocksdbStore::new(path))))
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
