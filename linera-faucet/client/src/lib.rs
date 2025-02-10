// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The client component of the Linera faucet.

use anyhow::{bail, Context, Result};
use linera_base::identifiers::Owner;
use linera_client::config::GenesisConfig;
use linera_execution::committee::ValidatorName;
use linera_faucet::ClaimOutcome;
use linera_version::VersionInfo;
use serde_json::{json, Value};

fn truncate_query_output(input: &str) -> String {
    let max_len = 200;
    if input.len() < max_len {
        input.to_string()
    } else {
        format!("{} ...", input.get(..max_len).unwrap())
    }
}

fn reqwest_client() -> reqwest::Client {
    let builder = reqwest::ClientBuilder::new();

    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder.timeout(std::time::Duration::from_secs(30));

    builder.build().unwrap()
}

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

    pub async fn genesis_config(&self) -> Result<GenesisConfig> {
        let query = "query { genesisConfig }";
        let client = reqwest_client();
        let response = client
            .post(&self.url)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("genesis_config: failed to post query")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Query \"{}\" failed: {}",
            query,
            response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not get response text: {error}"))
        );
        let mut value: Value = response.json().await.context("invalid JSON")?;
        if let Some(errors) = value.get("errors") {
            bail!("Query \"{}\" failed: {}", query, errors);
        }
        serde_json::from_value(value["data"]["genesisConfig"].take())
            .context("could not parse genesis config")
    }

    pub async fn version_info(&self) -> Result<VersionInfo> {
        let query =
            "query { version { crateVersion gitCommit gitDirty rpcHash graphqlHash witHash } }";
        let client = reqwest_client();
        let response = client
            .post(&self.url)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("version_info: failed to post query")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Query \"{}\" failed: {}",
            query,
            response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not get response text: {error}"))
        );
        let mut value: Value = response.json().await.context("invalid JSON")?;
        if let Some(errors) = value.get("errors") {
            bail!("Query \"{}\" failed: {}", query, errors);
        }
        let crate_version = serde_json::from_value(value["data"]["version"]["crateVersion"].take())
            .context("could not parse crate version")?;
        let git_commit = serde_json::from_value(value["data"]["version"]["gitCommit"].take())
            .context("could not parse git commit")?;
        let git_dirty = serde_json::from_value(value["data"]["version"]["gitDirty"].take())
            .context("could not parse git dirty")?;
        let rpc_hash = serde_json::from_value(value["data"]["version"]["rpcHash"].take())
            .context("could not parse rpc hash")?;
        let graphql_hash = serde_json::from_value(value["data"]["version"]["graphqlHash"].take())
            .context("could not parse graphql hash")?;
        let wit_hash = serde_json::from_value(value["data"]["version"]["witHash"].take())
            .context("could not parse wit hash")?;
        Ok(VersionInfo {
            crate_version,
            git_commit,
            git_dirty,
            rpc_hash,
            graphql_hash,
            wit_hash,
        })
    }

    pub async fn claim(&self, owner: &Owner) -> Result<ClaimOutcome> {
        let query = format!(
            "mutation {{ claim(owner: \"{owner}\") {{ \
                messageId chainId certificateHash \
            }} }}"
        );
        let client = reqwest_client();
        let response = client
            .post(&self.url)
            .json(&json!({ "query": &query }))
            .send()
            .await
            .with_context(|| {
                format!(
                    "claim: failed to post query={}",
                    truncate_query_output(&query)
                )
            })?;
        anyhow::ensure!(
            response.status().is_success(),
            "Query \"{}\" failed: {}",
            query,
            response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not get response text: {error}"))
        );
        let value: Value = response.json().await.context("invalid JSON")?;
        if let Some(errors) = value.get("errors") {
            bail!("Query \"{}\" failed: {}", query, errors);
        }
        let data = &value["data"]["claim"];
        let message_id = data["messageId"]
            .as_str()
            .context("message ID not found")?
            .parse()
            .context("could not parse message ID")?;
        let chain_id = data["chainId"]
            .as_str()
            .context("chain ID not found")?
            .parse()
            .context("could not parse chain ID")?;
        let certificate_hash = data["certificateHash"]
            .as_str()
            .context("Certificate hash not found")?
            .parse()
            .context("could not parse certificate hash")?;
        let outcome = ClaimOutcome {
            message_id,
            chain_id,
            certificate_hash,
        };
        Ok(outcome)
    }

    pub async fn current_validators(&self) -> Result<Vec<(ValidatorName, String)>> {
        let query = "query { currentValidators { name networkAddress } }";
        let client = reqwest_client();
        let response = client
            .post(&self.url)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("current_validators: failed to post query")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Query \"{}\" failed: {}",
            query,
            response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not get response text: {error}"))
        );
        let mut value: Value = response.json().await.context("invalid JSON")?;
        if let Some(errors) = value.get("errors") {
            bail!("Query \"{}\" failed: {}", query, errors);
        }
        let validators = match value["data"]["currentValidators"].take() {
            serde_json::Value::Array(validators) => validators,
            validators => bail!("{validators} is not an array"),
        };
        validators
            .into_iter()
            .map(|mut validator| {
                let name = serde_json::from_value::<ValidatorName>(validator["name"].take())
                    .context("could not parse current validators: invalid name")?;
                let addr = validator["networkAddress"]
                    .as_str()
                    .context("could not parse current validators: invalid address")?
                    .to_string();
                Ok((name, addr))
            })
            .collect()
    }
}
