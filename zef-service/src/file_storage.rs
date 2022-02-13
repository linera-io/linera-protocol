// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use futures::lock::Mutex;
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

/// File-based store.
#[derive(Debug, Clone)]
pub struct FileStore {
    /// Base path.
    path: PathBuf,
}

impl FileStore {
    pub fn new(path: PathBuf) -> Self {
        assert!(path.is_dir());
        Self { path }
    }

    fn get_path(&self, kind: &str, key: &[u8]) -> PathBuf {
        let key = hex::encode(key);
        self.path.join(format!("{}_{}", kind, key))
    }

    async fn read_value(&self, kind: &str, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let path = self.get_path(kind, key);
        if !path.is_file() {
            return Ok(None);
        }
        let value = fs::read(path).await?;
        Ok(Some(value))
    }

    async fn write_value(&self, kind: &str, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let path = self.get_path(kind, key);
        fs::write(&path, value).await?;
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
            Some(v) => Some(bcs::from_bytes(&v).map_err(|e| Error::StorageBcsError {
                error: format!("{}: {}", kind, e),
            })?),
            None => None,
        };
        Ok(result)
    }

    async fn write<'a, K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: serde::Serialize,
        V: serde::Serialize + serde::Deserialize<'a>,
    {
        let key = bcs::to_bytes(&key).expect("should not fail");
        let kind = serde_name::trace_name::<V>().expect("V must be a struct or an enum");
        let value = bcs::to_bytes(&value).expect("should not fail");
        self.write_value(kind, &key, &value)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("{}", e),
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
                error: format!("{}", e),
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
    async fn read_active_account(&mut self, id: AccountId) -> Result<AccountState, Error> {
        let store = self.0.lock().await;
        store.read(&id).await?.ok_or(Error::InactiveAccount(id))
    }

    async fn read_account_or_default(&mut self, id: AccountId) -> Result<AccountState, Error> {
        let store = self.0.lock().await;
        Ok(store.read(&id).await?.unwrap_or_default())
    }

    async fn write_account(&mut self, id: AccountId, state: AccountState) -> Result<(), Error> {
        let store = self.0.lock().await;
        store.write(&id, &state).await
    }

    async fn remove_account(&mut self, id: AccountId) -> Result<(), Error> {
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

    async fn write_certificate(
        &mut self,
        hash: HashValue,
        certificate: Certificate,
    ) -> Result<(), Error> {
        let store = self.0.lock().await;
        store.write(&hash, &certificate).await
    }

    async fn has_consensus(&mut self, id: InstanceId) -> Result<bool, Error> {
        let store = self.0.lock().await;
        Ok(store.read::<_, ConsensusState>(&id).await?.is_some())
    }

    async fn read_consensus(&mut self, id: InstanceId) -> Result<ConsensusState, Error> {
        let store = self.0.lock().await;
        store
            .read(&id)
            .await?
            .ok_or(Error::MissingConsensusInstance { id })
    }

    async fn write_consensus(
        &mut self,
        id: InstanceId,
        state: ConsensusState,
    ) -> Result<(), Error> {
        let store = self.0.lock().await;
        store.write(&id, &state).await
    }

    async fn remove_consensus(&mut self, id: InstanceId) -> Result<(), Error> {
        let store = self.0.lock().await;
        store.remove::<_, ConsensusState>(&id).await
    }
}
