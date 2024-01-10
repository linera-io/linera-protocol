// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types for the exported futures for the contract endpoints.
//!
//! Each type is called by the code generated by [`wit-bindgen-guest-rust`] when the host calls the guest
//! Wasm module's respective endpoint. This module contains the code to forward the call to the
//! contract type that implements [`Contract`].

use crate::{
    contract::system_api,
    views::{AppStateStore, ViewStorageContext},
    Contract, SimpleStateStorage, ViewStateStorage,
};
use async_trait::async_trait;
use futures::TryFutureExt;
use linera_views::{
    batch::Batch,
    common::{ReadableKeyValueStore, WritableKeyValueStore},
    views::RootView,
};
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

/// The storage APIs used by a contract.
#[async_trait]
pub trait ContractStateStorage<Application> {
    /// Loads the `Application` state.
    async fn load() -> Application;

    /// Stores the `Application` state.
    async fn store(state: Application);

    /// Executes an `operation` with the `Application` state.
    ///
    /// The state is only stored back in storage if the `operation` succeeds. Otherwise, the error
    /// is returned as a [`String`].
    async fn execute_with_state<Operation, AsyncOperation, Success, Error>(
        operation: Operation,
    ) -> Result<Success, String>
    where
        Operation: FnOnce(Application) -> AsyncOperation,
        AsyncOperation: Future<Output = Result<(Application, Success), Error>> + Send,
        Application: Send,
        Operation: Send,
        Success: Send + 'static,
        Error: ToString + 'static,
    {
        let application = Self::load().await;

        operation(application)
            .and_then(|(application, result)| async move {
                Self::store(application).await;
                Ok(result)
            })
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl<Application> ContractStateStorage<Application> for SimpleStateStorage<Application>
where
    Application: Contract + Default + DeserializeOwned + Serialize + Send + 'static,
{
    async fn load() -> Application {
        let maybe_bytes = AppStateStore
            .read_value_bytes(&[])
            .await
            .expect("Failed to read application state bytes");

        if let Some(bytes) = maybe_bytes {
            bcs::from_bytes(&bytes).expect("Failed to deserialize application state")
        } else {
            Application::default()
        }
    }

    async fn store(state: Application) {
        let mut batch = Batch::new();

        batch
            .put_key_value(vec![], &state)
            .expect("Failed to serialize application state");

        AppStateStore
            .write_batch(batch, &[])
            .await
            .expect("Failed to store application state bytes");
    }
}

#[async_trait]
impl<Application> ContractStateStorage<Application> for ViewStateStorage<Application>
where
    Application: Contract + RootView<ViewStorageContext> + Send + 'static,
{
    async fn load() -> Application {
        system_api::load_view().await
    }

    async fn store(state: Application) {
        system_api::store_view(state).await;
    }
}
