// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod file_storage;
mod memory_storage;

use async_trait::async_trait;
use dyn_clone::DynClone;
use futures::future;
use zef_base::{
    account::AccountState,
    base_types::{AccountId, HashValue},
    ensure,
    error::Error,
    messages::Certificate,
};

pub use file_storage::*;
pub use memory_storage::*;

/// How to communicate with a persistent storage.
/// * Writes should be blocking until they are completed.
/// * Reads should be optimized to hit a local cache.
#[async_trait]
pub trait StorageClient: DynClone + Send + Sync {
    async fn read_active_account(&mut self, id: &AccountId) -> Result<AccountState, Error> {
        let account = self.read_account_or_default(id).await?;
        ensure!(
            account.manager.is_active() && account.committee.is_some(),
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
