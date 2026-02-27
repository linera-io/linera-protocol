// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The client component of the Linera faucet.

// TODO(#3362): generate this code

use std::collections::BTreeMap;

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{ArithmeticError, ChainDescription},
};
use linera_client::config::GenesisConfig;
use linera_execution::{committee::ValidatorState, Committee, ResourceControlPolicy};
use linera_version::VersionInfo;
use thiserror_context::Context;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorInner {
    #[error("JSON parsing error: {0:?}")]
    Json(#[from] serde_json::Error),
    #[error("GraphQL error: {0:?}")]
    GraphQl(Vec<serde_json::Value>),
    #[error("HTTP error: {0:?}")]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
}

thiserror_context::impl_context!(Error(ErrorInner));

/// A faucet instance that can be queried.
#[derive(Debug, Clone)]
pub struct Faucet {
    url: String,
}

impl Faucet {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    async fn query<Response: serde::de::DeserializeOwned>(
        &self,
        query: impl AsRef<str>,
    ) -> Result<Response, Error> {
        let query = query.as_ref();

        #[derive(serde::Deserialize)]
        struct GraphQlResponse<T> {
            data: Option<T>,
            errors: Option<Vec<serde_json::Value>>,
        }

        let builder = reqwest::ClientBuilder::new();

        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder.timeout(linera_base::time::Duration::from_secs(30));

        let response: GraphQlResponse<Response> = builder
            .build()
            .unwrap()
            .post(&self.url)
            .json(&serde_json::json!({
                "query": query,
            }))
            .send()
            .await
            .with_context(|| format!("executing query {query:?}"))?
            .error_for_status()?
            .json()
            .await?;

        if let Some(errors) = response.errors {
            // Extract just the error messages, ignore locations and path
            let messages = errors
                .iter()
                .filter_map(|error| {
                    error
                        .get("message")
                        .and_then(|msg| msg.as_str())
                        .map(|s| s.to_string())
                })
                .collect::<Vec<_>>();

            if messages.is_empty() {
                Err(ErrorInner::GraphQl(errors).into())
            } else {
                Err(
                    ErrorInner::GraphQl(vec![serde_json::Value::String(messages.join("; "))])
                        .into(),
                )
            }
        } else {
            Ok(response
                .data
                .expect("no errors present but no data returned"))
        }
    }

    pub async fn genesis_config(&self) -> Result<GenesisConfig, Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            genesis_config: GenesisConfig,
        }

        Ok(self
            .query::<Response>("query { genesisConfig }")
            .await?
            .genesis_config)
    }

    pub async fn version_info(&self) -> Result<VersionInfo, Error> {
        #[derive(serde::Deserialize)]
        struct Response {
            version: VersionInfo,
        }

        Ok(self.query::<Response>("query { version }").await?.version)
    }

    pub async fn claim(
        &self,
        owner: &linera_base::identifiers::AccountOwner,
    ) -> Result<ChainDescription, Error> {
        #[derive(serde::Deserialize)]
        struct Response {
            claim: ChainDescription,
        }
        Ok(self
            .query::<Response>(format!("mutation {{ claim(owner: \"{owner}\") }}"))
            .await?
            .claim)
    }

    pub async fn current_validators(&self) -> Result<Vec<(ValidatorPublicKey, String)>, Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Validator {
            public_key: ValidatorPublicKey,
            network_address: String,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            current_validators: Vec<Validator>,
        }

        Ok(self
            .query::<Response>("query { currentValidators { publicKey networkAddress } }")
            .await?
            .current_validators
            .into_iter()
            .map(|validator| (validator.public_key, validator.network_address))
            .collect())
    }

    pub async fn current_committee(&self) -> Result<Committee, Error> {
        #[derive(serde::Deserialize)]
        struct CommitteeResponse {
            validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
            policy: ResourceControlPolicy,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            current_committee: CommitteeResponse,
        }

        let response = self
            .query::<Response>(
                "query { currentCommittee { \
                    validators \
                    policy \
                } }",
            )
            .await?;

        let committee_response = response.current_committee;

        Ok(Committee::new(
            committee_response.validators,
            committee_response.policy,
        )?)
    }
}
