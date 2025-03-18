// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The client component of the Linera faucet.

// TODO(#3362): generate this code

use linera_base::crypto::ValidatorPublicKey;
use linera_client::config::GenesisConfig;
use linera_faucet::ClaimOutcome;
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
        query: &str,
    ) -> Result<Response, Error> {
        #[derive(serde::Deserialize)]
        struct GraphQlResponse<T> {
            data: Option<T>,
            errors: Option<Vec<serde_json::Value>>,
        }

        let builder = reqwest::ClientBuilder::new();

        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder.timeout(std::time::Duration::from_secs(30));

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
            Err(ErrorInner::GraphQl(errors).into())
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
        owner: &linera_base::identifiers::Address,
    ) -> Result<ClaimOutcome, Error> {
        let query = format!(
            "mutation {{ claim(owner: \"{owner}\") {{ \
                messageId chainId certificateHash \
            }} }}"
        );

        #[derive(serde::Deserialize)]
        struct Response {
            claim: ClaimOutcome,
        }

        Ok(self.query::<Response>(&query).await?.claim)
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
}
