// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountState,
    base_types::{AccountId, HashValue, InstanceId},
    consensus::ConsensusState,
    ensure,
    error::Error,
    messages::Certificate,
    AsyncResult,
};
use futures::lock::Mutex;
use std::{collections::HashMap, sync::Arc};

#[cfg(test)]
use crate::base_types::{dbg_account, dbg_addr};

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
pub trait StorageClient {
    fn read_active_account(&mut self, account_id: AccountId) -> AsyncResult<AccountState, Error>;

    fn read_account_or_default(
        &mut self,
        account_id: AccountId,
    ) -> AsyncResult<AccountState, Error>;

    fn write_account(
        &mut self,
        account_id: AccountId,
        state: AccountState,
    ) -> AsyncResult<(), Error>;

    fn remove_account(&mut self, account_id: AccountId) -> AsyncResult<(), Error>;

    fn read_certificate(&mut self, value_hash: HashValue) -> AsyncResult<Certificate, Error>;

    fn write_certificate(
        &mut self,
        value_hash: HashValue,
        certificate: Certificate,
    ) -> AsyncResult<(), Error>;

    fn has_consensus(&mut self, instance_id: InstanceId) -> AsyncResult<bool, Error>;

    fn read_consensus(&mut self, instance_id: InstanceId) -> AsyncResult<ConsensusState, Error>;

    fn write_consensus(
        &mut self,
        instance_id: InstanceId,
        state: ConsensusState,
    ) -> AsyncResult<(), Error>;

    fn remove_consensus(&mut self, id: InstanceId) -> AsyncResult<(), Error>;
}

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

impl StorageClient for InMemoryStoreClient {
    fn read_active_account(&mut self, id: AccountId) -> AsyncResult<AccountState, Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let store = store.lock().await;
            let account = store.accounts.get(&id).cloned().unwrap_or_default();
            ensure!(account.owner.is_some(), Error::InactiveAccount(id));
            Ok(account)
        })
    }

    fn read_account_or_default(&mut self, id: AccountId) -> AsyncResult<AccountState, Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let store = store.lock().await;
            let account = store.accounts.get(&id).cloned().unwrap_or_default();
            Ok(account)
        })
    }

    fn write_account(&mut self, id: AccountId, value: AccountState) -> AsyncResult<(), Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let mut store = store.lock().await;
            store.accounts.insert(id, value);
            Ok(())
        })
    }

    fn remove_account(&mut self, id: AccountId) -> AsyncResult<(), Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let mut store = store.lock().await;
            store.accounts.remove(&id);
            Ok(())
        })
    }

    fn read_certificate(&mut self, hash: HashValue) -> AsyncResult<Certificate, Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let store = store.lock().await;
            store
                .certificates
                .get(&hash)
                .cloned()
                .ok_or(Error::MissingCertificate { hash })
        })
    }

    fn write_certificate(&mut self, hash: HashValue, value: Certificate) -> AsyncResult<(), Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let mut store = store.lock().await;
            store.certificates.insert(hash, value);
            Ok(())
        })
    }

    fn has_consensus(&mut self, id: InstanceId) -> AsyncResult<bool, Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let store = store.lock().await;
            Ok(store.instances.contains_key(&id))
        })
    }

    fn read_consensus(&mut self, id: InstanceId) -> AsyncResult<ConsensusState, Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let store = store.lock().await;
            store
                .instances
                .get(&id)
                .cloned()
                .ok_or(Error::MissingConsensusInstance { id })
        })
    }

    fn write_consensus(&mut self, id: InstanceId, value: ConsensusState) -> AsyncResult<(), Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let mut store = store.lock().await;
            store.instances.insert(id, value);
            Ok(())
        })
    }

    fn remove_consensus(&mut self, id: InstanceId) -> AsyncResult<(), Error> {
        let store = self.0.clone();
        Box::pin(async move {
            let mut store = store.lock().await;
            store.instances.remove(&id);
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_read_write() {
    let mut store = InMemoryStoreClient::default();
    let mut account = store.read_account_or_default(dbg_account(1)).await.unwrap();
    account.owner = Some(dbg_addr(2));
    store.write_account(dbg_account(1), account).await.unwrap();
    store
        .clone()
        .read_active_account(dbg_account(1))
        .await
        .unwrap();
}
