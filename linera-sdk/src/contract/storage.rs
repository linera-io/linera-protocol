// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types for the exported futures for the contract endpoints.
//!
//! Each type is called by the code generated by [`wit-bindgen-guest-rust`] when the host calls the guest
//! Wasm module's respective endpoint. This module contains the code to forward the call to the
//! contract type that implements [`Contract`].

use async_trait::async_trait;
use linera_views::{
    batch::Batch,
    common::{ReadableKeyValueStore, WritableKeyValueStore},
    views::{RootView, View},
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    views::{AppStateStore, ViewStorageContext},
    Contract, SimpleStateStorage, ViewStateStorage,
};

/// The storage APIs used by a contract.
#[async_trait]
pub trait ContractStateStorage<Application>
where
    Application: Contract,
{
    /// Loads the `Application` state.
    async fn load() -> Application::State;

    /// Stores the `Application` state.
    async fn store(state: &mut Application::State);
}

#[async_trait]
impl<Application> ContractStateStorage<Application> for SimpleStateStorage<Application>
where
    Application: Contract,
    Application::State: Default + DeserializeOwned + Serialize + Send + 'static,
{
    async fn load() -> Application::State {
        let maybe_bytes = AppStateStore
            .read_value_bytes(&[])
            .await
            .expect("Failed to read application state bytes");

        if let Some(bytes) = maybe_bytes {
            bcs::from_bytes(&bytes).expect("Failed to deserialize application state")
        } else {
            Application::State::default()
        }
    }

    async fn store(state: &mut Application::State) {
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
    Application: Contract,
    Application::State: RootView<ViewStorageContext> + Send + 'static,
{
    async fn load() -> Application::State {
        Application::State::load(ViewStorageContext::default())
            .await
            .expect("Failed to load application state")
    }

    async fn store(state: &mut Application::State) {
        state
            .save()
            .await
            .expect("Failed to store application state")
    }
}
