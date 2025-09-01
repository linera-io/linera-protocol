// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The client component of the Linera faucet.

// TODO(#3362): generate this code

use std::collections::{BTreeMap, BTreeSet};

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Amount, ChainDescription},
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
        #[serde(rename_all = "camelCase")]
        struct PolicyResponse {
            wasm_fuel_unit: Amount,
            evm_fuel_unit: Amount,
            read_operation: Amount,
            write_operation: Amount,
            byte_runtime: Amount,
            byte_read: Amount,
            byte_written: Amount,
            blob_read: Amount,
            blob_published: Amount,
            blob_byte_read: Amount,
            blob_byte_published: Amount,
            byte_stored: Amount,
            operation: Amount,
            operation_byte: Amount,
            message: Amount,
            message_byte: Amount,
            service_as_oracle_query: Amount,
            http_request: Amount,
            maximum_wasm_fuel_per_block: u64,
            maximum_evm_fuel_per_block: u64,
            maximum_service_oracle_execution_ms: u64,
            maximum_block_size: u64,
            maximum_bytecode_size: u64,
            maximum_blob_size: u64,
            maximum_published_blobs: u64,
            maximum_block_proposal_size: u64,
            maximum_bytes_read_per_block: u64,
            maximum_bytes_written_per_block: u64,
            maximum_oracle_response_bytes: u64,
            maximum_http_response_bytes: u64,
            http_request_timeout_ms: u64,
            http_request_allow_list: BTreeSet<String>,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct CommitteeResponse {
            validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
            policy: PolicyResponse,
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
                    policy { \
                        wasmFuelUnit \
                        evmFuelUnit \
                        readOperation \
                        writeOperation \
                        byteRuntime \
                        byteRead \
                        byteWritten \
                        blobRead \
                        blobPublished \
                        blobByteRead \
                        blobBytePublished \
                        byteStored \
                        operation \
                        operationByte \
                        message \
                        messageByte \
                        serviceAsOracleQuery \
                        httpRequest \
                        maximumWasmFuelPerBlock \
                        maximumEvmFuelPerBlock \
                        maximumServiceOracleExecutionMs \
                        maximumBlockSize \
                        maximumBytecodeSize \
                        maximumBlobSize \
                        maximumPublishedBlobs \
                        maximumBlockProposalSize \
                        maximumBytesReadPerBlock \
                        maximumBytesWrittenPerBlock \
                        maximumOracleResponseBytes \
                        maximumHttpResponseBytes \
                        httpRequestTimeoutMs \
                        httpRequestAllowList \
                    } \
                } }",
            )
            .await?;

        let committee_response = response.current_committee;
        let policy = ResourceControlPolicy {
            wasm_fuel_unit: committee_response.policy.wasm_fuel_unit,
            evm_fuel_unit: committee_response.policy.evm_fuel_unit,
            read_operation: committee_response.policy.read_operation,
            write_operation: committee_response.policy.write_operation,
            byte_runtime: committee_response.policy.byte_runtime,
            byte_read: committee_response.policy.byte_read,
            byte_written: committee_response.policy.byte_written,
            blob_read: committee_response.policy.blob_read,
            blob_published: committee_response.policy.blob_published,
            blob_byte_read: committee_response.policy.blob_byte_read,
            blob_byte_published: committee_response.policy.blob_byte_published,
            byte_stored: committee_response.policy.byte_stored,
            operation: committee_response.policy.operation,
            operation_byte: committee_response.policy.operation_byte,
            message: committee_response.policy.message,
            message_byte: committee_response.policy.message_byte,
            service_as_oracle_query: committee_response.policy.service_as_oracle_query,
            http_request: committee_response.policy.http_request,
            maximum_wasm_fuel_per_block: committee_response.policy.maximum_wasm_fuel_per_block,
            maximum_evm_fuel_per_block: committee_response.policy.maximum_evm_fuel_per_block,
            maximum_service_oracle_execution_ms: committee_response
                .policy
                .maximum_service_oracle_execution_ms,
            maximum_block_size: committee_response.policy.maximum_block_size,
            maximum_bytecode_size: committee_response.policy.maximum_bytecode_size,
            maximum_blob_size: committee_response.policy.maximum_blob_size,
            maximum_published_blobs: committee_response.policy.maximum_published_blobs,
            maximum_block_proposal_size: committee_response.policy.maximum_block_proposal_size,
            maximum_bytes_read_per_block: committee_response.policy.maximum_bytes_read_per_block,
            maximum_bytes_written_per_block: committee_response
                .policy
                .maximum_bytes_written_per_block,
            maximum_oracle_response_bytes: committee_response.policy.maximum_oracle_response_bytes,
            maximum_http_response_bytes: committee_response.policy.maximum_http_response_bytes,
            http_request_timeout_ms: committee_response.policy.http_request_timeout_ms,
            http_request_allow_list: committee_response.policy.http_request_allow_list,
        };

        Ok(Committee::new(committee_response.validators, policy))
    }
}
