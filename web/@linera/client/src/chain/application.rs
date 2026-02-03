// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::{AccountOwner, ApplicationId};
use linera_core::client::ChainClient;
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::{Client, Environment, JsResult};

#[wasm_bindgen]
pub struct Application {
    pub(crate) client: Client,
    pub(crate) chain_client: ChainClient<Environment>,
    pub(crate) id: ApplicationId,
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(from_wasm_abi)]
pub struct QueryOptions {
    #[serde(default)]
    pub block_hash: Option<String>,
    #[serde(default)]
    pub owner: Option<AccountOwner>,
}

#[wasm_bindgen]
impl Application {
    /// Performs a query against an application's service.
    ///
    /// If `block_hash` is non-empty, it specifies the block at which to
    /// perform the query; otherwise, the latest block is used.
    ///
    /// # Errors
    /// If the application ID is invalid, the query is incorrect, or
    /// the response isn't valid UTF-8.
    ///
    /// # Panics
    /// On internal protocol errors.
    #[wasm_bindgen]
    // TODO(#5253) allow passing bytes here rather than just strings
    // TODO(#5152) a lot of this logic is shared with `linera_service::node_service`
    pub async fn query(&self, query: &str, options: Option<QueryOptions>) -> JsResult<String> {
        tracing::debug!("querying application: {query}");
        let QueryOptions { block_hash, owner } = options.unwrap_or_default();
        let mut chain_client = self.chain_client.clone();
        if let Some(owner) = owner {
            chain_client.set_preferred_owner(owner);
        }
        let block_hash = if let Some(hash) = block_hash {
            Some(hash.as_str().parse()?)
        } else {
            None
        };
        let linera_execution::QueryOutcome {
            response: linera_execution::QueryResponse::User(response),
            operations,
        } = chain_client
            .query_application(
                linera_execution::Query::User {
                    application_id: self.id,
                    bytes: query.as_bytes().to_vec(),
                },
                block_hash,
            )
            .await?
        else {
            panic!("system response to user query")
        };

        if !operations.is_empty() {
            let _hash = self
                .client
                .client_context
                .lock()
                .await
                .apply_client_command(&chain_client, |_chain_client| {
                    chain_client.execute_operations(operations.clone(), vec![])
                })
                .await?;
        }

        Ok(String::from_utf8(response)?)
    }
}
