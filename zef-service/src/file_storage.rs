// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use futures::lock::Mutex;
use rand::{Rng, SeedableRng};
use sha2::Digest;
use std::{path::PathBuf, sync::Arc};
use tokio::fs;
use zef_core::{
    account::AccountState,
    base_types::{AccountId, HashValue, InstanceId},
    consensus::ConsensusState,
    error::Error,
    messages::Certificate,
    storage::StorageClient,
};

#[cfg(test)]
#[path = "unit_tests/file_storage_tests.rs"]
mod file_storage_tests;

/// File-based store.
#[derive(Debug, Clone)]
pub struct FileStore {
    /// Base path.
    path: PathBuf,
    /// For generation of temporary names.
    rng: rand::rngs::SmallRng,
}

impl FileStore {
    pub fn new(path: PathBuf) -> Self {
        assert!(path.is_dir());
        let rng = rand::rngs::SmallRng::from_entropy();
        Self { path, rng }
    }

    fn get_path(&self, kind: &str, key: &[u8]) -> PathBuf {
        let mut hasher = sha2::Sha512::default();
        hasher.update(key);
        let hash = hasher.finalize();
        // Only encode the first 40 bytes to ensure that the resulting
        // filename is well below 128 bytes.
        let key = hex::encode(&hash[0..40]);
        self.path.join(format!("{}_{}.json", kind, key))
    }

    async fn read_value(&self, kind: &str, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let path = self.get_path(kind, key);
        match fs::read(path).await {
            Ok(value) => Ok(Some(value)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn write_value(&mut self, kind: &str, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let path = self.get_path(kind, key);
        let seed: u64 = self.rng.gen();
        let seed = format!("_{}", seed);
        let mut tmp_path = path.clone();
        tmp_path.pop();
        tmp_path.push(&seed);
        fs::write(&tmp_path, value).await?;
        fs::rename(&tmp_path, &path).await?;
        Ok(())
    }

    async fn remove_value(&self, kind: &str, key: &[u8]) -> std::io::Result<()> {
        let path = self.get_path(kind, key);
        if path.is_file() {
            fs::remove_file(path).await.unwrap();
        }
        Ok(())
    }

    async fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: serde::Serialize,
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

    async fn write<'a, K, V>(&mut self, key: &K, value: &V) -> Result<(), Error>
    where
        K: serde::Serialize,
        V: serde::Serialize + serde::Deserialize<'a>,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        let value = serde_json::to_vec(&value).expect("should not fail");
        self.write_value(kind, &key, &value)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("write {} {:?}", e, self.get_path(kind, &key)),
            })?;
        Ok(())
    }

    async fn remove<K, V>(&self, key: &K) -> Result<(), Error>
    where
        K: serde::Serialize,
        V: serde::de::DeserializeOwned,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        self.remove_value(kind, &key)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("{} {:?}", e, self.get_path(kind, &key)),
            })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FileStoreClient(Arc<Mutex<FileStore>>);

impl FileStoreClient {
    pub fn new(path: PathBuf) -> Self {
        FileStoreClient(Arc::new(Mutex::new(FileStore::new(path))))
    }
}

#[async_trait]
impl StorageClient for FileStoreClient {
    async fn read_account_or_default(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        let store = self.0.lock().await;
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
        let store = self.0.lock().await;
        store.remove::<_, AccountState>(&id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        let store = self.0.lock().await;
        store
            .read(&hash)
            .await?
            .ok_or(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.write(&certificate.hash, &certificate).await
    }

    async fn has_consensus(&mut self, id: &InstanceId) -> Result<bool, Error> {
        let store = self.0.lock().await;
        Ok(store.read::<_, ConsensusState>(&id).await?.is_some())
    }

    async fn read_consensus(&mut self, id: &InstanceId) -> Result<ConsensusState, Error> {
        let store = self.0.lock().await;
        store
            .read(&id)
            .await?
            .ok_or(Error::MissingConsensusInstance { id: id.clone() })
    }

    async fn write_consensus(&mut self, state: ConsensusState) -> Result<(), Error> {
        let mut store = self.0.lock().await;
        store.write(&state.id, &state).await
    }

    async fn remove_consensus(&mut self, id: &InstanceId) -> Result<(), Error> {
        let store = self.0.lock().await;
        store.remove::<_, ConsensusState>(&id).await
    }
}
