// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use futures::stream::StreamExt;
use linera_base::identifiers::AccountOwner;
use linera_client::chain_listener::ClientContext as _;
use linera_core::{
    client::ChainClient,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

use crate::{Client, Environment, JsResult};

pub mod application;
pub use application::Application;

#[wasm_bindgen]
pub struct Chain {
    pub(crate) client: Client,
    pub(crate) chain_client: ChainClient<Environment>,
}

#[derive(serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
pub struct TransferParams {
    #[serde(default)]
    pub donor: Option<AccountOwner>,
    pub amount: u64,
    pub recipient: linera_base::identifiers::Account,
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
pub struct AddOwnerOptions {
    #[serde(default)]
    pub weight: u64,
}

#[wasm_bindgen]
impl Chain {
    /// Sets a callback to be called when a notification is received
    /// from the network.
    ///
    /// # Errors
    /// If we fail to subscribe to the notification stream.
    ///
    /// # Panics
    /// If the handler function fails.
    #[wasm_bindgen(js_name = onNotification)]
    pub fn on_notification(&self, handler: js_sys::Function) -> JsResult<()> {
        let mut notifications = self.chain_client.subscribe()?;
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(notification) = notifications.next().await {
                tracing::debug!("received notification: {notification:?}");
                handler
                    .call1(
                        &JsValue::null(),
                        &serde_wasm_bindgen::to_value(&notification).unwrap(),
                    )
                    .unwrap_throw();
            }
        });
        Ok(())
    }

    /// Transfers funds from one account to another.
    ///
    /// `options` should be an options object of the form `{ donor,
    /// recipient, amount }`; omitting `donor` will cause the funds to
    /// come from the chain balance.
    ///
    /// # Errors
    /// - if the options object is of the wrong form
    /// - if the transfer fails
    #[wasm_bindgen]
    pub async fn transfer(&self, params: TransferParams) -> JsResult<()> {
        let _hash = self
            .client
            .client_context
            .lock()
            .await
            .apply_client_command(&self.chain_client, |_chain_client| {
                self.chain_client.transfer(
                    params.donor.unwrap_or(AccountOwner::CHAIN),
                    linera_base::data_types::Amount::from_tokens(params.amount.into()),
                    params.recipient,
                )
            })
            .await?;

        Ok(())
    }

    /// Gets the balance of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn balance(&self) -> JsResult<String> {
        Ok(self.chain_client.query_balance().await?.to_string())
    }

    /// Gets the identity of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn identity(&self) -> JsResult<AccountOwner> {
        Ok(self.chain_client.identity().await?)
    }

    /// Adds a new owner to the default chain.
    ///
    /// # Errors
    ///
    /// If the owner is in the wrong format, or the chain client can't be instantiated.
    #[wasm_bindgen(js_name = addOwner)]
    pub async fn add_owner(
        &self,
        owner: AccountOwner,
        options: Option<AddOwnerOptions>,
    ) -> JsResult<()> {
        let AddOwnerOptions { weight } = options.unwrap_or_default();
        self.client
            .client_context
            .lock()
            .await
            .apply_client_command(&self.chain_client, |_chain_client| {
                self.chain_client.share_ownership(owner, weight)
            })
            .await?;
        Ok(())
    }

    /// Gets the version information of the validators of the current network.
    ///
    /// # Errors
    /// If a validator is unreachable.
    #[wasm_bindgen(js_name = validatorVersionInfo)]
    pub async fn validator_version_info(&self) -> JsResult<JsValue> {
        self.chain_client.synchronize_from_validators().await?;
        let result = self.chain_client.local_committee().await;
        let mut client = self.client.client_context.lock().await;
        client.update_wallet(&self.chain_client).await?;
        let committee = result?;
        let node_provider = client.make_node_provider();

        let mut validator_versions = HashMap::new();

        for (name, state) in committee.validators() {
            match node_provider
                .make_node(&state.network_address)?
                .get_version_info()
                .await
            {
                Ok(version_info) => {
                    if validator_versions
                        .insert(name, version_info.clone())
                        .is_some()
                    {
                        tracing::warn!("duplicate validator entry for validator {name:?}");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to get version information for validator {name:?}:\n{e:?}"
                    );
                }
            }
        }

        Ok(validator_versions.serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_large_number_types_as_bigints(true)
                .serialize_maps_as_objects(true),
        )?)
    }

    /// Retrieves an application for querying.
    ///
    /// # Errors
    /// If the application ID is invalid.
    #[wasm_bindgen]
    pub async fn application(&self, id: &str) -> JsResult<Application> {
        web_sys::console::debug_1(&format!("connecting to Linera application {id}").into());
        Ok(Application {
            client: self.client.clone(),
            chain_client: self.chain_client.clone(),
            id: id.parse()?,
        })
    }
}
