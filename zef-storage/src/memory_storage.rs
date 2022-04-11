// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::StorageClient;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::{collections::HashMap, ops::DerefMut, sync::Arc};
use zef_base::{
    account::AccountState,
    base_types::{AccountId, HashValue},
    error::Error,
    messages::Certificate,
};

#[cfg(test)]
use zef_base::{
    account::AccountManager,
    base_types::{dbg_account, dbg_addr},
    committee::Committee,
};

/// Vanilla in-memory key-value store.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    accounts: HashMap<AccountId, AccountState>,
    certificates: HashMap<HashValue, Certificate>,
}

/// The corresponding vanilla client.
#[derive(Clone, Default)]
pub struct InMemoryStoreClient(Arc<Mutex<InMemoryStore>>);

impl InMemoryStoreClient {
    /// Create a distinct copy of the data.
    pub async fn copy(&self) -> Self {
        let store = self.0.clone().lock().await.clone();
        Self(Arc::new(Mutex::new(store)))
    }
}

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
    account.committee = Some(Committee::make_simple(Vec::new()));
    account.manager = AccountManager::single(dbg_addr(2));
    store.write_account(account).await.unwrap();
    store
        .clone()
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
}
