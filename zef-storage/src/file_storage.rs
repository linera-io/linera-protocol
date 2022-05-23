// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::Storage;
use async_trait::async_trait;
use futures::lock::Mutex;
use sha2::Digest;
use std::{path::PathBuf, sync::Arc};
use tokio::fs;
use zef_base::{
    chain::{ChainId, ChainState},
    crypto::HashValue,
    error::Error,
    messages::Certificate,
};

#[cfg(test)]
#[path = "unit_tests/file_storage_tests.rs"]
mod file_storage_tests;

/// File-based store.
#[derive(Debug, Clone)]
pub struct FileStore {
    /// Base path.
    path: PathBuf,
    /// Configuration for RON serialization.
    ron_config: ron::ser::PrettyConfig,
}

impl FileStore {
    pub fn new(path: PathBuf) -> Self {
        assert!(path.is_dir());
        let ron_config = ron::ser::PrettyConfig::default().depth_limit(8);
        Self { path, ron_config }
    }

    fn get_path(&self, kind: &str, key: &[u8]) -> PathBuf {
        let mut hasher = sha2::Sha512::default();
        hasher.update(key);
        let hash = hasher.finalize();
        // Only encode the first 40 bytes to ensure that the resulting
        // filename is well below 128 bytes.
        let key = hex::encode(&hash[0..40]);
        self.path.join(format!("{}_{}.ron", kind, key))
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
        let dir_path = self.path.clone();
        let temp_file =
            tokio::task::spawn_blocking(|| tempfile::NamedTempFile::new_in(dir_path)).await??;
        fs::write(&temp_file, value).await?;
        // persist atomically replaces the destination.
        tokio::task::spawn_blocking(|| temp_file.persist(path)).await??;
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
            Some(v) => Some(ron::de::from_bytes(&v).map_err(|e| Error::StorageBcsError {
                error: format!("{}: {}", kind, e),
            })?),
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
        let value =
            ron::ser::to_string_pretty(&value, self.ron_config.clone()).expect("should not fail");
        self.write_value(kind, &key, value.as_bytes())
            .await
            .map_err(|e| Error::StorageIoError {
                error: format!("write {}", e),
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
                error: format!("remove {}", e),
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
impl Storage for FileStoreClient {
    async fn read_chain_or_default(&mut self, id: ChainId) -> Result<ChainState, Error> {
        let store = self.0.lock().await;
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
        let store = self.0.lock().await;
        store.remove::<_, ChainState>(&id).await
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
}
