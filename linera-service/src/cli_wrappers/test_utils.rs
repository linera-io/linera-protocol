// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{ClientWrapper, Database, LocalNetwork, Network};
use anyhow::Result;
use async_graphql::InputType;
use linera_base::{
    abi::ContractAbi,
    identifiers::{ApplicationId, BytecodeId, ChainId},
};
use linera_execution::Bytecode;
use linera_views::test_utils::get_table_name;
use serde::de::DeserializeOwned;
use serde_json::{json, value::Value};
use std::{collections::HashMap, env, marker::PhantomData, path::PathBuf, time::Duration};
use tokio::process::Child;
use tracing::{info, warn};

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the node-service command of the client.
const CLIENT_SERVICE_ENV: &str = "LINERA_CLIENT_SERVICE_PARAMS";

impl Network {
    fn external_short(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Simple => "tcp",
        }
    }
}

impl LocalNetwork {
    pub fn new_for_testing(database: Database, network: Network) -> Result<Self> {
        let seed = 37;
        let table_name = get_table_name();
        let num_validators = 4;
        let num_shards = match database {
            Database::RocksDb => 1,
            Database::DynamoDb => 4,
            Database::ScyllaDb => 4,
        };
        Self::new(
            database,
            network,
            Some(seed),
            table_name,
            num_validators,
            num_shards,
        )
    }
}

impl ClientWrapper {
    pub async fn publish_and_create<A: ContractAbi>(
        &self,
        contract: PathBuf,
        service: PathBuf,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: &[ApplicationId],
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_parameters = serde_json::to_string(parameters)?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.run().await?;
        command
            .arg("publish-and-create")
            .args([contract, service])
            .args(publisher.into().iter().map(ChainId::to_string))
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument]);
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(
                required_application_ids
                    .iter()
                    .map(ApplicationId::to_string),
            );
        }
        let stdout = Self::run_command(&mut command).await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    pub async fn publish_bytecode(
        &self,
        contract: PathBuf,
        service: PathBuf,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<BytecodeId> {
        let stdout = Self::run_command(
            self.run()
                .await?
                .arg("publish-bytecode")
                .args([contract, service])
                .args(publisher.into().iter().map(ChainId::to_string)),
        )
        .await?;
        Ok(stdout.trim().parse()?)
    }

    pub async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: &BytecodeId,
        argument: &A::InitializationArgument,
        creator: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_argument = serde_json::to_string(argument)?;
        let stdout = Self::run_command(
            self.run()
                .await?
                .arg("create-application")
                .arg(bytecode_id.to_string())
                .args(["--json-argument", &json_argument])
                .args(creator.into().iter().map(ChainId::to_string)),
        )
        .await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    pub async fn run_node_service(&self, port: impl Into<Option<u16>>) -> Result<NodeService> {
        let port = port.into().unwrap_or(8080);
        let mut command = self.run().await?;
        command.arg("service");
        if let Ok(var) = env::var(CLIENT_SERVICE_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--port".to_string(), port.to_string()])
            .spawn()?;
        let client = reqwest::Client::new();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let request = client
                .get(format!("http://localhost:{}/", port))
                .send()
                .await;
            if request.is_ok() {
                info!("Node service has started");
                return Ok(NodeService { port, child });
            } else {
                warn!("Waiting for node service to start");
            }
        }
        panic!("Failed to start node service");
    }

    pub async fn query_validators(&self, chain_id: Option<ChainId>) -> Result<()> {
        let mut command = self.run().await?;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        Self::run_command(&mut command).await?;
        Ok(())
    }

    pub async fn query_balance(&self, chain_id: ChainId) -> Result<String> {
        let stdout = Self::run_command(
            self.run()
                .await?
                .arg("query-balance")
                .arg(&chain_id.to_string()),
        )
        .await?;
        let amount = stdout.trim().to_string();
        Ok(amount)
    }

    pub async fn transfer(&self, amount: &str, from: ChainId, to: ChainId) -> Result<()> {
        Self::run_command(
            self.run()
                .await?
                .arg("transfer")
                .arg(amount)
                .args(["--from", &from.to_string()])
                .args(["--to", &to.to_string()]),
        )
        .await?;
        Ok(())
    }

    pub async fn set_validator(&self, name: &str, port: usize, votes: usize) -> Result<()> {
        let address = format!("{}:127.0.0.1:{}", self.network.external_short(), port);
        Self::run_command(
            self.run()
                .await?
                .arg("set-validator")
                .args(["--name", name])
                .args(["--address", &address])
                .args(["--votes", &votes.to_string()]),
        )
        .await?;
        Ok(())
    }

    pub async fn remove_validator(&self, name: &str) -> Result<()> {
        Self::run_command(
            self.run()
                .await?
                .arg("remove-validator")
                .args(["--name", name]),
        )
        .await?;
        Ok(())
    }

    pub async fn synchronize_balance(&self, chain_id: ChainId) -> Result<()> {
        Self::run_command(
            self.run()
                .await?
                .arg("sync-balance")
                .arg(&chain_id.to_string()),
        )
        .await?;
        Ok(())
    }
}

pub struct NodeService {
    pub(crate) port: u16,
    pub(crate) child: Child,
}

impl NodeService {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn assert_is_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            assert!(status.success());
        }
    }

    pub async fn process_inbox(&self, chain_id: &ChainId) {
        let query = format!("mutation {{ processInbox(chainId: \"{chain_id}\") }}");
        self.query_node(&query).await;
    }

    pub async fn make_application<A: ContractAbi>(
        &self,
        chain_id: &ChainId,
        application_id: &ApplicationId<A>,
    ) -> ApplicationWrapper<A> {
        let application_id = application_id.forget_abi().to_string();
        let n_try = 30;
        for i in 0..n_try {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let values = self.try_get_applications_uri(chain_id).await;
            if let Some(link) = values.get(&application_id) {
                return ApplicationWrapper::from(link.to_string());
            }
            warn!("Waiting for application {application_id:?} to be visible on chain {chain_id:?}");
        }
        panic!("Could not find application URI: {application_id} after {n_try} tries");
    }

    pub async fn try_get_applications_uri(&self, chain_id: &ChainId) -> HashMap<String, String> {
        let query = format!("query {{ applications(chainId: \"{chain_id}\") {{ id link }}}}");
        let data = self.query_node(&query).await;
        data["applications"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| {
                let id = a["id"].as_str().unwrap().to_string();
                let link = a["link"].as_str().unwrap().to_string();
                (id, link)
            })
            .collect()
    }

    pub async fn publish_bytecode(
        &self,
        chain_id: &ChainId,
        contract: PathBuf,
        service: PathBuf,
    ) -> BytecodeId {
        let contract_code = Bytecode::load_from_file(&contract).await.unwrap();
        let service_code = Bytecode::load_from_file(&service).await.unwrap();
        let query = format!(
            "mutation {{ publishBytecode(chainId: {}, contract: {}, service: {}) }}",
            chain_id.to_value(),
            contract_code.to_value(),
            service_code.to_value(),
        );
        let data = self.query_node(&query).await;
        let bytecode_str = data["publishBytecode"].as_str().unwrap();
        bytecode_str.parse().unwrap()
    }

    pub async fn query_node(&self, query: &str) -> Value {
        let n_try = 30;
        for i in 0..n_try {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let url = format!("http://localhost:{}/", self.port);
            let client = reqwest::Client::new();
            let response = client
                .post(url)
                .json(&json!({ "query": query }))
                .send()
                .await
                .unwrap();
            if !response.status().is_success() {
                panic!(
                    "Query \"{}\" failed: {}",
                    query.get(..200).unwrap_or(query),
                    response.text().await.unwrap()
                );
            }
            let value: Value = response.json().await.unwrap();
            if let Some(errors) = value.get("errors") {
                warn!(
                    "Query \"{}\" failed: {}",
                    query.get(..200).unwrap_or(query),
                    errors
                );
            } else {
                return value["data"].clone();
            }
        }
        panic!(
            "Query \"{}\" failed after {} retries.",
            query.get(..200).unwrap_or(query),
            n_try
        );
    }

    pub async fn create_application<A: ContractAbi>(
        &self,
        chain_id: &ChainId,
        bytecode_id: &BytecodeId,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: &[ApplicationId],
    ) -> ApplicationId<A> {
        let json_required_applications_ids = required_application_ids
            .iter()
            .map(ApplicationId::to_string)
            .collect::<Vec<_>>()
            .to_value();
        // Convert to `serde_json::Value` then `async_graphql::Value` via the trait `InputType`.
        let new_parameters = serde_json::to_value(parameters).unwrap().to_value();
        let new_argument = serde_json::to_value(argument).unwrap().to_value();
        let query = format!(
            "mutation {{ createApplication(\
                     chainId: \"{chain_id}\",
                     bytecodeId: \"{bytecode_id}\", \
                     parameters: {new_parameters}, \
                     initializationArgument: {new_argument}, \
                     requiredApplicationIds: {json_required_applications_ids}) \
                 }}"
        );
        let data = self.query_node(&query).await;
        let app_id_str = data["createApplication"].as_str().unwrap().trim();
        app_id_str.parse::<ApplicationId>().unwrap().with_abi()
    }

    pub async fn request_application<A: ContractAbi>(
        &self,
        chain_id: &ChainId,
        application_id: &ApplicationId<A>,
    ) -> String {
        let application_id = application_id.forget_abi();
        let query = format!(
            "mutation {{ requestApplication(\
                     chainId: \"{chain_id}\", \
                     applicationId: \"{application_id}\") \
                 }}"
        );
        let data = self.query_node(&query).await;
        serde_json::from_value(data["requestApplication"].clone()).unwrap()
    }
}
pub struct ApplicationWrapper<A> {
    uri: String,
    _phantom: PhantomData<A>,
}

impl<A> ApplicationWrapper<A> {
    pub async fn raw_query(&self, query: impl AsRef<str>) -> Value {
        let query = query.as_ref();
        let client = reqwest::Client::new();
        let response = client
            .post(&self.uri)
            .json(&json!({ "query": query }))
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            panic!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                response.text().await.unwrap()
            );
        }
        let value: Value = response.json().await.unwrap();
        if let Some(errors) = value.get("errors") {
            panic!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                errors
            );
        }
        value["data"].clone()
    }

    pub async fn query(&self, query: impl AsRef<str>) -> Value {
        let query = query.as_ref();
        self.raw_query(&format!("query {{ {query} }}")).await
    }

    pub async fn query_json<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> T {
        let query = query.as_ref().trim();
        let name = query
            .split_once(|ch: char| !ch.is_alphanumeric())
            .map_or(query, |(name, _)| name);
        let data = self.query(query).await;
        serde_json::from_value(data[name].clone()).unwrap()
    }

    pub async fn mutate(&self, mutation: impl AsRef<str>) -> Value {
        let mutation = mutation.as_ref();
        self.raw_query(&format!("mutation {{ {mutation} }}")).await
    }
}

impl<A> From<String> for ApplicationWrapper<A> {
    fn from(uri: String) -> ApplicationWrapper<A> {
        ApplicationWrapper {
            uri,
            _phantom: PhantomData,
        }
    }
}
