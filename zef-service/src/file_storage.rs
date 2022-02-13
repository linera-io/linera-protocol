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
#[derive(Debug, Default, Clone)]
pub struct FileStorage {
    /// Base path.
    path: PathBuf,
}

impl FileStorage {
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
            .read_value(&kind, &key)
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
        self.write_value(&kind, &key, &value)
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
        self.remove_value(&kind, &key)
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("{}", e),
            })?;
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct FileStorageClient(Arc<Mutex<FileStorage>>);

impl FileStorageClient {
    pub fn new(path: PathBuf) -> Self {
        FileStorageClient(Arc::new(Mutex::new(FileStorage::new(path))))
    }
}

#[async_trait]
impl StorageClient for FileStorageClient {
    async fn read_active_account(&mut self, id: AccountId) -> Result<AccountState, Error> {
        let storage = self.0.lock().await;
        storage
            .read(&id)
            .await?
            .ok_or_else(|| Error::InactiveAccount(id))
    }

    async fn read_account_or_default(&mut self, id: AccountId) -> Result<AccountState, Error> {
        let storage = self.0.lock().await;
        Ok(storage.read(&id).await?.unwrap_or_default())
    }

    async fn write_account(&mut self, id: AccountId, state: AccountState) -> Result<(), Error> {
        let storage = self.0.lock().await;
        storage.write(&id, &state).await
    }

    async fn remove_account(&mut self, id: AccountId) -> Result<(), Error> {
        let storage = self.0.lock().await;
        storage.remove::<_, AccountState>(&id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        let storage = self.0.lock().await;
        storage
            .read(&hash)
            .await?
            .ok_or_else(|| Error::MissingCertificate { hash })
    }

    async fn write_certificate(
        &mut self,
        hash: HashValue,
        certificate: Certificate,
    ) -> Result<(), Error> {
        let storage = self.0.lock().await;
        storage.write(&hash, &certificate).await
    }

    async fn has_consensus(&mut self, id: InstanceId) -> Result<bool, Error> {
        let storage = self.0.lock().await;
        Ok(storage.read::<_, ConsensusState>(&id).await?.is_some())
    }

    async fn read_consensus(&mut self, id: InstanceId) -> Result<ConsensusState, Error> {
        let storage = self.0.lock().await;
        storage
            .read(&id)
            .await?
            .ok_or_else(|| Error::MissingConsensusInstance { id })
    }

    async fn write_consensus(
        &mut self,
        id: InstanceId,
        state: ConsensusState,
    ) -> Result<(), Error> {
        let storage = self.0.lock().await;
        storage.write(&id, &state).await
    }

    async fn remove_consensus(&mut self, id: InstanceId) -> Result<(), Error> {
        let storage = self.0.lock().await;
        storage.remove::<_, ConsensusState>(&id).await
    }
}
