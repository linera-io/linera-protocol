// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountState,
    base_types::{AccountId, HashValue},
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
        ensure!(
            account.manager.is_active(),
            Error::InactiveAccount(id.clone())
        );
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
        keys: I,
    ) -> Result<Vec<Certificate>, Error>
    where
        Self: Clone + Send + 'static,
    {
        let mut tasks = Vec::new();
        for key in keys {
            let mut client = self.clone();
            tasks.push(tokio::task::spawn(async move {
                client.read_certificate(key).await
            }));
        }
        let results = future::join_all(tasks).await;
        let mut certs = Vec::new();
        for result in results {
            certs.push(result.expect("storage access should not cancel or crash")?);
        }
        Ok(certs)
    }

    async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), Error>;
}

dyn_clone::clone_trait_object!(StorageClient);

/// Vanilla in-memory key-value store.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    accounts: HashMap<AccountId, AccountState>,
    certificates: HashMap<HashValue, Certificate>,
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
