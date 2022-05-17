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
    account::AccountState,
    base_types::{AccountId, HashValue},
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
    /// RockdbDB handle.
    db: Mutex<rocksdb::DB>,
}

fn open_db(path: &PathBuf) -> Result<rocksdb::DB, rocksdb::Error> {
    let cfs = match rocksdb::DB::list_cf(&rocksdb::Options::default(), path){
        Ok(cfs) => cfs,
        Err(_e) => vec![String::from("None")],
    };

    let mut v_cf: Vec<rocksdb::ColumnFamilyDescriptor> = Vec::new();
    for i in &cfs {
        v_cf.push(rocksdb::ColumnFamilyDescriptor::new(i, rocksdb::Options::default()));
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
            path: path.clone(),
            db : Mutex::new(open_db(&path)?),
        })
    }

    fn get_db_handler(&mut self, kind: &str) -> Result<&rocksdb::ColumnFamily, rocksdb::Error> {
        let tmp = self.db.lock();
        match tmp.cf_handle(kind){
            Some(handle) => Ok(handle),
            None => {
                tmp.create_cf(kind, &rocksdb::Options::default())?;
                Ok(tmp.cf_handle(kind).expect("Unable to create Rocksdb ColumnFamily"))
            }
        }
    }

    async fn read_value(
        &mut self,
        kind: &str,
        key: &[u8],
    ) -> Result<std::option::Option<Vec<u8>>, rocksdb::Error> {
        let cf_handle = self.get_db_handler(kind)?;
        match self.db.get_cf(cf_handle, key) {
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
        let cf_handle = self.get_db_handler(kind)?;
        self.db.put_cf(&cf_handle, key, value)?;
        Ok(())
    }

    async fn remove_value(&mut self, kind: &str, key: &[u8]) -> Result<(), rocksdb::Error> {
        let cf_handle = self.get_db_handler(kind)?;
        self.db.delete_cf(&cf_handle, key)?;
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
            Some(v) => Some(
                serde_json::from_slice(&v).map_err(|e| Error::StorageBcsError {
                    error: format!("{}: {}", kind, e),
                })?,
            ),
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
        let value = serde_json::to_vec(&value).expect("should not fail");
        self.write_value(kind, &key, &value)
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
        Ok(RocksdbStoreClient(Arc::new(Mutex::new(RocksdbStore::new(path)?))))
    }
}

#[async_trait]
impl Storage for RocksdbStoreClient {
    async fn read_account_or_default(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        let mut store = self.0.lock().await;
        Ok(store
            .read(&id)
            .await?
            .unwrap_or_else(|| AccountState::new(id.clone())))
    }

    async fn write_account(&mut self, state: AccountState) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.write(&state.id, &state).await
    }

    async fn remove_account(&mut self, id: &AccountId) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.remove::<_, AccountState>(&id).await
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
