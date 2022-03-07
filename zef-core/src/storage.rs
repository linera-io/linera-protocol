// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountState,
    base_types::{AccountId, HashValue, InstanceId},
    consensus::ConsensusState,
    ensure,
    error::Error,
    messages::Certificate,
};
use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::{future, lock::Mutex};
use std::{collections::HashMap, ops::DerefMut, sync::Arc};

#[cfg(test)]
use crate::account::AccountManager;
#[cfg(test)]
use crate::base_types::{dbg_account, dbg_addr};

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
#[async_trait]
pub trait StorageClient: DynClone + Send + Sync {
    async fn read_active_account(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        let account = self.read_account_or_default(id).await?;
        ensure!(account.manager.is_active(), Error::InactiveAccount(id.clone()));
        Ok(account)
    }

    async fn read_account_or_default(
        &mut self,
        account_id: &AccountId,
    ) -> Result<AccountState, Error>;

    async fn write_account(&mut self, state: AccountState) -> Result<(), Error>;

    async fn remove_account(&mut self, account_id: &AccountId) -> Result<(), Error>;

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error>;

    async fn read_certificates<I: Iterator<Item = HashValue> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Certificate>, Error>
    where
        Self: Clone + Send + 'static,
    {
        let mut handles = Vec::new();
        for hash in hashes {
            let mut client = self.clone();
            handles.push(async move { client.read_certificate(hash).await });
        }
        future::join_all(handles).await.into_iter().collect()
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error>;

    async fn has_consensus(&mut self, instance_id: &InstanceId) -> Result<bool, Error>;

    async fn read_consensus(&mut self, instance_id: &InstanceId) -> Result<ConsensusState, Error>;

    async fn write_consensus(&mut self, state: ConsensusState) -> Result<(), Error>;

    async fn remove_consensus(&mut self, id: &InstanceId) -> Result<(), Error>;
}

dyn_clone::clone_trait_object!(StorageClient);

/// Vanilla in-memory key-value store.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    accounts: HashMap<AccountId, AccountState>,
    certificates: HashMap<HashValue, Certificate>,
    instances: HashMap<InstanceId, ConsensusState>,
}

/// The corresponding vanilla client.
#[derive(Clone, Default)]
pub struct InMemoryStoreClient(Arc<Mutex<InMemoryStore>>);

#[async_trait]
impl StorageClient for InMemoryStoreClient {
    async fn read_account_or_default(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        let store = self.0.clone();
        let account = store
            .lock()
            .await
            .accounts
            .get(id)
            .cloned()
            .unwrap_or_else(|| AccountState::new(id.clone()));
        Ok(account)
    }

    async fn write_account(&mut self, value: AccountState) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.accounts.insert(value.id.clone(), value);
        Ok(())
    }

    async fn remove_account(&mut self, id: &AccountId) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.accounts.remove(id);
        Ok(())
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        let store = self.0.clone();
        let value = store.lock().await.certificates.get(&hash).cloned();
        value.ok_or(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, value: Certificate) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.certificates.insert(value.hash, value);
        Ok(())
    }

    async fn has_consensus(&mut self, id: &InstanceId) -> Result<bool, Error> {
        let store = self.0.clone();
        let result = store.lock().await.instances.contains_key(id);
        Ok(result)
    }

    async fn read_consensus(&mut self, id: &InstanceId) -> Result<ConsensusState, Error> {
        let store = self.0.clone();
        let value = store.lock().await.instances.get(id).cloned();
        value.ok_or(Error::MissingConsensusInstance { id: id.clone() })
    }

    async fn write_consensus(&mut self, value: ConsensusState) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.instances.insert(value.id.clone(), value);
        Ok(())
    }

    async fn remove_consensus(&mut self, id: &InstanceId) -> Result<(), Error> {
        let store = self.0.clone();
        store.lock().await.instances.remove(id);
        Ok(())
    }
}

#[async_trait]
impl StorageClient for Box<dyn StorageClient> {
    async fn read_account_or_default(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        self.deref_mut().read_account_or_default(id).await
    }

    async fn write_account(&mut self, value: AccountState) -> Result<(), Error> {
        self.deref_mut().write_account(value).await
    }

    async fn remove_account(&mut self, id: &AccountId) -> Result<(), Error> {
        self.deref_mut().remove_account(id).await
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        self.deref_mut().read_certificate(hash).await
    }

    async fn write_certificate(&mut self, value: Certificate) -> Result<(), Error> {
        self.deref_mut().write_certificate(value).await
    }

    async fn has_consensus(&mut self, id: &InstanceId) -> Result<bool, Error> {
        self.deref_mut().has_consensus(id).await
    }

    async fn read_consensus(&mut self, id: &InstanceId) -> Result<ConsensusState, Error> {
        self.deref_mut().read_consensus(id).await
    }

    async fn write_consensus(&mut self, value: ConsensusState) -> Result<(), Error> {
        self.deref_mut().write_consensus(value).await
    }

    async fn remove_consensus(&mut self, id: &InstanceId) -> Result<(), Error> {
        self.deref_mut().remove_consensus(id).await
    }
}

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut account = store
        .read_account_or_default(&dbg_account(1))
        .await
        .unwrap();
    account.manager = AccountManager::single(dbg_addr(2));
    store.write_account(account).await.unwrap();
    store
        .clone()
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
}
