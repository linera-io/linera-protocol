// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The client component of the Linera faucet.

#![deny(missing_docs)]

// TODO(#3362): generate this code

use std::collections::BTreeMap;

use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Amount, ChainDescription, Timestamp},
    identifiers::ChainId,
};
use linera_client::config::GenesisConfig;
use linera_execution::{committee::ValidatorState, Committee, ResourceControlPolicy};
use linera_version::VersionInfo;
use thiserror_context::Context;

/// The kinds of error that the faucet client can return.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorInner {
    /// A response from the faucet could not be parsed as JSON.
    #[error("JSON parsing error: {0:?}")]
    Json(#[from] serde_json::Error),
    /// The faucet returned one or more GraphQL errors.
    #[error("GraphQL error: {0:?}")]
    GraphQl(Vec<serde_json::Value>),
    /// An HTTP request to the faucet failed.
    #[error("HTTP error: {0:?}")]
    Http(#[from] reqwest::Error),
}

pub use error::Error;

mod error {
    // `impl_context!` generates a public `Error` newtype (with accessors) that cannot carry
    // doc comments, so this wrapper module is exempt from the crate's `missing_docs` policy.
    #![allow(missing_docs)]

    use thiserror_context::Context;

    use super::ErrorInner;

    thiserror_context::impl_context!(Error(ErrorInner));
}

/// The result of a successful claim mutation.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimOutcome {
    /// The ID of the chain.
    pub chain_id: ChainId,
    /// The hash of the certificate containing the operation.
    pub certificate_hash: CryptoHash,
    /// The amount of tokens transferred.
    pub amount: Amount,
}

/// Information about the initial chain claim.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitialClaim {
    /// The chain ID that was created.
    pub chain_id: ChainId,
    /// The block timestamp when the chain was created.
    pub timestamp: Timestamp,
}

/// A faucet instance that can be queried.
#[derive(Debug, Clone)]
pub struct Faucet {
    url: String,
}

impl Faucet {
    /// Creates a faucet client querying the faucet service at the given URL.
    pub fn new(url: String) -> Self {
        Self { url }
    }

    /// Returns the URL of the faucet service.
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
            Err(ErrorInner::GraphQl(errors).into())
        } else {
            Ok(response
                .data
                .expect("no errors present but no data returned"))
        }
    }

    /// Fetches the network's genesis configuration from the faucet.
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

    /// Fetches the faucet's version information.
    pub async fn version_info(&self) -> Result<VersionInfo, Error> {
        #[derive(serde::Deserialize)]
        struct Response {
            version: VersionInfo,
        }

        Ok(self.query::<Response>("query { version }").await?.version)
    }

    /// Claims a new chain for the given owner, returning its chain description.
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

    /// Claims daily tokens for the given owner.
    /// The user must have already claimed a chain. Each user can claim once per
    /// 24-hour period.
    pub async fn daily_claim(
        &self,
        owner: &linera_base::identifiers::AccountOwner,
    ) -> Result<ClaimOutcome, Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            daily_claim: ClaimOutcome,
        }

        Ok(self
            .query::<Response>(format!("mutation {{ dailyClaim(owner: \"{owner}\") }}"))
            .await?
            .daily_claim)
    }

    /// Returns the initial claim for the given owner, if any.
    pub async fn initial_claim(
        &self,
        owner: &linera_base::identifiers::AccountOwner,
    ) -> Result<Option<InitialClaim>, Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            initial_claim: Option<InitialClaim>,
        }

        Ok(self
            .query::<Response>(format!(
                "query {{ initialClaim(owner: \"{owner}\") {{ chainId timestamp }} }}"
            ))
            .await?
            .initial_claim)
    }

    /// Returns the earliest time at which the owner can make a daily claim.
    /// If the returned timestamp is in the past (or now), the user can claim immediately.
    /// Returns `None` if the user has not yet completed the initial claim.
    pub async fn next_daily_claim(
        &self,
        owner: &linera_base::identifiers::AccountOwner,
    ) -> Result<Option<Timestamp>, Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            next_daily_claim: Option<Timestamp>,
        }

        Ok(self
            .query::<Response>(format!("query {{ nextDailyClaim(owner: \"{owner}\") }}"))
            .await?
            .next_daily_claim)
    }

    /// Returns the current validators' public keys and network addresses.
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

    /// Returns the current committee: its validators and resource-control policy.
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
        ))
    }
}
