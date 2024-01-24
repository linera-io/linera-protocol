// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cli_wrappers::Network,
    config::{GenesisConfig, WalletState},
    faucet::ClaimOutcome,
    util,
    util::{ChildExt, CommandExt},
};
use anyhow::{bail, Context, Result};
use async_graphql::InputType;
use linera_base::{
    abi::ContractAbi,
    crypto::{CryptoHash, PublicKey},
    data_types::Amount,
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId, Owner},
};
use linera_execution::{committee::ValidatorName, system::SystemChannel, Bytecode};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    env,
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tracing::{info, warn};

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the node-service command of the client.
const CLIENT_SERVICE_ENV: &str = "LINERA_CLIENT_SERVICE_PARAMS";

fn reqwest_client() -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

/// Wrapper to run a Linera client command.
pub struct ClientWrapper {
    testing_prng_seed: Option<u64>,
    storage: String,
    wallet: String,
    max_pending_messages: usize,
    network: Network,
    pub tmp_dir: Arc<TempDir>,
}

impl ClientWrapper {
    pub fn new(
        tmp_dir: Arc<TempDir>,
        network: Network,
        testing_prng_seed: Option<u64>,
        id: usize,
    ) -> Self {
        let storage = format!("rocksdb:{}/client_{}.db", tmp_dir.path().display(), id);
        let wallet = format!("wallet_{}.json", id);
        Self {
            testing_prng_seed,
            storage,
            wallet,
            max_pending_messages: 10_000,
            network,
            tmp_dir,
        }
    }

    /// Runs `linera project new`.
    pub async fn project_new(&self, project_name: &str, linera_root: &Path) -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let mut command = self.command().await?;
        command
            .current_dir(tmp.path())
            .arg("project")
            .arg("new")
            .arg(project_name)
            .arg("--linera-root")
            .arg(linera_root)
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(tmp)
    }

    /// Runs `linera project publish`.
    pub async fn project_publish<T: Serialize>(
        &self,
        path: PathBuf,
        required_application_ids: Vec<String>,
        publisher: impl Into<Option<ChainId>>,
        argument: &T,
    ) -> Result<String> {
        let json_parameters = serde_json::to_string(&())?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.command().await?;
        command
            .arg("project")
            .arg("publish-and-create")
            .arg(path)
            .args(publisher.into().iter().map(ChainId::to_string))
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument]);
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(required_application_ids);
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().to_string())
    }

    /// Runs `linera project test`.
    pub async fn project_test(&self, path: &Path) -> Result<()> {
        self.command()
            .await
            .context("failed to create project test command")?
            .current_dir(path)
            .arg("project")
            .arg("test")
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    async fn command(&self) -> Result<Command> {
        let path = util::resolve_binary("linera", env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command
            .current_dir(self.tmp_dir.path())
            .args(["--wallet", &self.wallet])
            .args(["--storage", &self.storage])
            .args([
                "--max-pending-messages",
                &self.max_pending_messages.to_string(),
            ])
            .args(["--send-timeout-ms", "10000"])
            .args(["--recv-timeout-ms", "10000"])
            .arg("--wait-for-outgoing-messages");
        Ok(command)
    }

    /// Runs `linera create-genesis-config`.
    pub async fn create_genesis_config(
        &self,
        num_other_initial_chains: u32,
        initial_funding: Amount,
    ) -> Result<()> {
        let mut command = self.command().await?;
        command
            .args([
                "create-genesis-config",
                &num_other_initial_chains.to_string(),
            ])
            .args(["--initial-funding", &initial_funding.to_string()])
            .args(["--committee", "committee.json"])
            .args(["--genesis", "genesis.json"]);
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    /// Runs `linera wallet init`.
    pub async fn wallet_init(
        &self,
        chain_ids: &[ChainId],
        faucet: FaucetOption<'_>,
    ) -> Result<Option<ClaimOutcome>> {
        let mut command = self.command().await?;
        command.args(["wallet", "init"]);
        match faucet {
            FaucetOption::None => {
                command.args(["--genesis", "genesis.json"]);
            }
            FaucetOption::GenesisOnly(url) => {
                command.args(["--faucet", url]);
            }
            FaucetOption::NewChain(url) => {
                command.args(["--with-new-chain", "--faucet", url]);
            }
        }
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        if !chain_ids.is_empty() {
            let ids = chain_ids.iter().map(ChainId::to_string);
            command.arg("--with-other-chains").args(ids);
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        if matches!(faucet, FaucetOption::NewChain(_)) {
            let mut lines = stdout.split_whitespace();
            let chain_id_str = lines.next().context("missing chain ID")?;
            let message_id_str = lines.next().context("missing message ID")?;
            let certificate_hash_str = lines.next().context("missing certificate hash")?;
            let outcome = ClaimOutcome {
                chain_id: chain_id_str.parse().context("invalid chain ID")?,
                message_id: message_id_str.parse().context("invalid message ID")?,
                certificate_hash: certificate_hash_str
                    .parse()
                    .context("invalid certificate hash")?,
            };
            Ok(Some(outcome))
        } else {
            Ok(None)
        }
    }

    /// Runs `linera wallet publish-and-create`.
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
        let mut command = self.command().await?;
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
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    /// Runs `linera publish-bytecode`.
    pub async fn publish_bytecode(
        &self,
        contract: PathBuf,
        service: PathBuf,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<BytecodeId> {
        let stdout = self
            .command()
            .await?
            .arg("publish-bytecode")
            .args([contract, service])
            .args(publisher.into().iter().map(ChainId::to_string))
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(stdout.trim().parse()?)
    }

    /// Runs `linera create-application`.
    pub async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: &BytecodeId,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: &[ApplicationId],
        creator: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_parameters = serde_json::to_string(parameters)?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.command().await?;
        command
            .arg("create-application")
            .arg(bytecode_id.to_string())
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument])
            .args(creator.into().iter().map(ChainId::to_string));
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(
                required_application_ids
                    .iter()
                    .map(ApplicationId::to_string),
            );
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    /// Runs `linera request-application`
    pub async fn request_application(
        &self,
        application_id: ApplicationId,
        requester_chain_id: ChainId,
        target_chain_id: Option<ChainId>,
    ) -> Result<BytecodeId> {
        let mut command = self.command().await?;
        command
            .arg("request-application")
            .arg(application_id.to_string())
            .args(["--requester-chain-id", &requester_chain_id.to_string()]);
        if let Some(target_chain_id) = target_chain_id {
            command.args(["--target-chain-id", &target_chain_id.to_string()]);
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().parse()?)
    }

    /// Runs `linera service`.
    pub async fn run_node_service(&self, port: impl Into<Option<u16>>) -> Result<NodeService> {
        let port = port.into().unwrap_or(8080);
        let mut command = self.command().await?;
        command.arg("service");
        if let Ok(var) = env::var(CLIENT_SERVICE_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--port".to_string(), port.to_string()])
            .spawn_into()?;
        let client = reqwest_client();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let request = client
                .get(format!("http://localhost:{}/", port))
                .send()
                .await;
            if request.is_ok() {
                info!("Node service has started");
                return Ok(NodeService::new(port, child));
            } else {
                warn!("Waiting for node service to start");
            }
        }
        bail!("Failed to start node service");
    }

    /// Runs `linera query-validators`.
    pub async fn query_validators(&self, chain_id: Option<ChainId>) -> Result<()> {
        let mut command = self.command().await?;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    /// Runs `linera faucet`.
    pub async fn run_faucet(
        &self,
        port: impl Into<Option<u16>>,
        chain_id: ChainId,
        amount: Amount,
    ) -> Result<Faucet> {
        let port = port.into().unwrap_or(8080);
        let mut command = self.command().await?;
        let child = command
            .arg("faucet")
            .arg(chain_id.to_string())
            .args(["--port".to_string(), port.to_string()])
            .args(["--amount".to_string(), amount.to_string()])
            .spawn_into()?;
        let client = reqwest_client();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let request = client
                .get(format!("http://localhost:{}/", port))
                .send()
                .await;
            if request.is_ok() {
                info!("Faucet has started");
                return Ok(Faucet::new(port, child));
            } else {
                warn!("Waiting for faucet to start");
            }
        }
        bail!("Failed to start faucet");
    }

    /// Runs `linera query-balance`.
    pub async fn query_balance(&self, chain_id: ChainId) -> Result<Amount> {
        let stdout = self
            .command()
            .await?
            .arg("query-balance")
            .arg(&chain_id.to_string())
            .spawn_and_wait_for_stdout()
            .await?;
        let amount = stdout
            .trim()
            .parse()
            .context("error while parsing the result of `linera query-balance`")?;
        Ok(amount)
    }

    /// Runs `linera transfer`.
    pub async fn transfer(&self, amount: Amount, from: ChainId, to: ChainId) -> Result<()> {
        self.command()
            .await?
            .arg("transfer")
            .arg(amount.to_string())
            .args(["--from", &from.to_string()])
            .args(["--to", &to.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    /// Runs `linera benchmark`.
    #[cfg(feature = "benchmark")]
    pub async fn benchmark(&self, max_in_flight: usize, num_chains: usize) -> Result<()> {
        self.command()
            .await?
            .arg("benchmark")
            .args(["--max-in-flight", &max_in_flight.to_string()])
            .args(["--num-chains", &num_chains.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    /// Runs `linera open-chain`.
    pub async fn open_chain(
        &self,
        from: ChainId,
        to_public_key: Option<PublicKey>,
        initial_balance: Amount,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.command().await?;
        command
            .arg("open-chain")
            .args(["--from", &from.to_string()])
            .args(["--initial-balance", &initial_balance.to_string()]);

        if let Some(public_key) = to_public_key {
            command.args(["--to-public-key", &public_key.to_string()]);
        }

        let stdout = command.spawn_and_wait_for_stdout().await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("no message ID in output")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("no chain ID in output")?)?;

        Ok((message_id, chain_id))
    }

    /// Runs `linera open-chain` then `linera assign`.
    pub async fn open_and_assign(
        &self,
        client: &ClientWrapper,
        initial_balance: Amount,
    ) -> Result<ChainId> {
        let our_chain = self
            .get_wallet()?
            .default_chain()
            .context("no default chain found")?;
        let key = client.keygen().await?;
        let (message_id, new_chain) = self
            .open_chain(our_chain, Some(key), initial_balance)
            .await?;
        assert_eq!(new_chain, client.assign(key, message_id).await?);
        Ok(new_chain)
    }

    pub async fn open_multi_owner_chain(
        &self,
        from: ChainId,
        to_public_keys: Vec<PublicKey>,
        weights: Vec<u64>,
        multi_leader_rounds: u32,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.command().await?;
        command
            .arg("open-multi-owner-chain")
            .args(["--from", &from.to_string()])
            .arg("--to-public-keys")
            .args(to_public_keys.iter().map(PublicKey::to_string))
            .arg("--weights")
            .args(weights.iter().map(u64::to_string))
            .args(["--multi-leader-rounds", &multi_leader_rounds.to_string()]);

        let stdout = command.spawn_and_wait_for_stdout().await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("no message ID in output")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("no chain ID in output")?)?;

        Ok((message_id, chain_id))
    }

    pub async fn retry_pending_block(
        &self,
        chain_id: Option<ChainId>,
    ) -> Result<Option<CryptoHash>> {
        let mut command = self.command().await?;
        command.arg("retry-pending-block");
        if let Some(chain_id) = chain_id {
            command.arg(chain_id.to_string());
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        let stdout = stdout.trim();
        if stdout.is_empty() {
            Ok(None)
        } else {
            Ok(Some(CryptoHash::from_str(stdout)?))
        }
    }

    pub fn get_wallet(&self) -> Result<WalletState> {
        WalletState::from_file(self.wallet_path().as_path())
    }

    pub fn wallet_path(&self) -> PathBuf {
        self.tmp_dir.path().join(&self.wallet)
    }

    pub fn storage_path(&self) -> &str {
        &self.storage
    }

    pub fn get_owner(&self) -> Option<Owner> {
        let wallet = self.get_wallet().ok()?;
        let chain_id = wallet.default_chain()?;
        let public_key = wallet.get(chain_id)?.key_pair.as_ref()?.public();
        Some(public_key.into())
    }

    pub async fn is_chain_present_in_wallet(&self, chain: ChainId) -> bool {
        self.get_wallet()
            .ok()
            .map_or(false, |wallet| wallet.get(chain).is_some())
    }

    pub async fn set_validator(&self, name: &str, port: usize, votes: usize) -> Result<()> {
        let address = format!("{}:127.0.0.1:{}", self.network.external_short(), port);
        self.command()
            .await?
            .arg("set-validator")
            .args(["--name", name])
            .args(["--address", &address])
            .args(["--votes", &votes.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn remove_validator(&self, name: &str) -> Result<()> {
        self.command()
            .await?
            .arg("remove-validator")
            .args(["--name", name])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    /// Runs `linera keygen`.
    pub async fn keygen(&self) -> Result<PublicKey> {
        let stdout = self
            .command()
            .await?
            .arg("keygen")
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(PublicKey::from_str(stdout.trim())?)
    }

    /// Returns the default chain.
    pub fn default_chain(&self) -> Option<ChainId> {
        self.get_wallet().ok()?.default_chain()
    }

    /// Runs `linera assign`.
    pub async fn assign(&self, key: PublicKey, message_id: MessageId) -> Result<ChainId> {
        let stdout = self
            .command()
            .await?
            .arg("assign")
            .args(["--key", &key.to_string()])
            .args(["--message-id", &message_id.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;

        let chain_id = ChainId::from_str(stdout.trim())?;

        Ok(chain_id)
    }

    /// Runs `linera sync-balance`.
    pub async fn synchronize_balance(&self, chain_id: ChainId) -> Result<Amount> {
        let stdout = self
            .command()
            .await?
            .arg("sync-balance")
            .arg(&chain_id.to_string())
            .spawn_and_wait_for_stdout()
            .await?;
        let amount = stdout
            .trim()
            .parse()
            .context("error while parsing the result of `linera sync-balance`")?;
        Ok(amount)
    }

    pub async fn build_application(
        &self,
        path: &Path,
        name: &str,
        is_workspace: bool,
    ) -> Result<(PathBuf, PathBuf)> {
        Command::new("cargo")
            .current_dir(self.tmp_dir.path())
            .arg("build")
            .arg("--release")
            .args(["--target", "wasm32-unknown-unknown"])
            .arg("--manifest-path")
            .arg(path.join("Cargo.toml"))
            .spawn_and_wait_for_stdout()
            .await?;

        let release_dir = match is_workspace {
            true => path.join("../target/wasm32-unknown-unknown/release"),
            false => path.join("target/wasm32-unknown-unknown/release"),
        };

        let contract = release_dir.join(format!("{}_contract.wasm", name.replace('-', "_")));
        let service = release_dir.join(format!("{}_service.wasm", name.replace('-', "_")));

        let contract_size = fs_err::tokio::metadata(&contract).await?.len();
        let service_size = fs_err::tokio::metadata(&service).await?.len();
        info!("Done building application {name}: contract_size={contract_size}, service_size={service_size}");

        Ok((contract, service))
    }
}

/// Whether `wallet_init` should use a faucet.
#[derive(Clone, Copy, Debug)]
pub enum FaucetOption<'a> {
    None,
    GenesisOnly(&'a str),
    NewChain(&'a str),
}

#[cfg(any(test, feature = "test"))]
impl ClientWrapper {
    pub async fn build_example(&self, name: &str) -> Result<(PathBuf, PathBuf)> {
        self.build_application(Self::example_path(name)?.as_path(), name, true)
            .await
    }

    pub fn example_path(name: &str) -> Result<PathBuf> {
        Ok(env::current_dir()?.join("../examples/").join(name))
    }
}

/// A running node service.
pub struct NodeService {
    port: u16,
    child: Child,
}

impl NodeService {
    fn new(port: u16, child: Child) -> Self {
        Self { port, child }
    }

    pub async fn terminate(mut self) -> Result<()> {
        self.child.kill().await.context("terminating node service")
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn ensure_is_running(&mut self) -> Result<()> {
        self.child.ensure_is_running()
    }

    pub async fn process_inbox(&self, chain_id: &ChainId) -> Result<()> {
        let query = format!("mutation {{ processInbox(chainId: \"{chain_id}\") }}");
        self.query_node(query).await?;
        Ok(())
    }

    pub async fn make_application<A: ContractAbi>(
        &self,
        chain_id: &ChainId,
        application_id: &ApplicationId<A>,
    ) -> Result<ApplicationWrapper<A>> {
        let application_id = application_id.forget_abi().to_string();
        let n_try = 30;
        for i in 0..n_try {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let values = self.try_get_applications_uri(chain_id).await?;
            if let Some(link) = values.get(&application_id) {
                return Ok(ApplicationWrapper::from(link.to_string()));
            }
            warn!("Waiting for application {application_id:?} to be visible on chain {chain_id:?}");
        }
        bail!("Could not find application URI: {application_id} after {n_try} tries");
    }

    pub async fn try_get_applications_uri(
        &self,
        chain_id: &ChainId,
    ) -> Result<HashMap<String, String>> {
        let query = format!("query {{ applications(chainId: \"{chain_id}\") {{ id link }}}}");
        let data = self.query_node(query).await?;
        data["applications"]
            .as_array()
            .context("missing applications in response")?
            .iter()
            .map(|a| {
                let id = a["id"]
                    .as_str()
                    .context("missing id field in response")?
                    .to_string();
                let link = a["link"]
                    .as_str()
                    .context("missing link field in response")?
                    .to_string();
                Ok((id, link))
            })
            .collect()
    }

    pub async fn publish_bytecode(
        &self,
        chain_id: &ChainId,
        contract: PathBuf,
        service: PathBuf,
    ) -> Result<BytecodeId> {
        let contract_code = Bytecode::load_from_file(&contract).await?;
        let service_code = Bytecode::load_from_file(&service).await?;
        let query = format!(
            "mutation {{ publishBytecode(chainId: {}, contract: {}, service: {}) }}",
            chain_id.to_value(),
            contract_code.to_value(),
            service_code.to_value(),
        );
        let data = self.query_node(query).await?;
        let bytecode_str = data["publishBytecode"]
            .as_str()
            .context("bytecode ID not found")?;
        bytecode_str.parse().context("could not parse bytecode ID")
    }

    pub async fn query_node(&self, query: impl AsRef<str>) -> Result<Value> {
        let n_try = 30;
        let query = query.as_ref();
        for i in 0..n_try {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let url = format!("http://localhost:{}/", self.port);
            let client = reqwest_client();
            let response = client
                .post(url)
                .json(&json!({ "query": query }))
                .send()
                .await
                .context("failed to post query")?;
            anyhow::ensure!(
                response.status().is_success(),
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("Could not get response text: {error}"))
            );
            let value: Value = response.json().await.context("invalid JSON")?;
            if let Some(errors) = value.get("errors") {
                warn!(
                    "Query \"{}\" failed: {}",
                    query.get(..200).unwrap_or(query),
                    errors
                );
            } else {
                return Ok(value["data"].clone());
            }
        }
        bail!(
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
    ) -> Result<ApplicationId<A>> {
        let json_required_applications_ids = required_application_ids
            .iter()
            .map(ApplicationId::to_string)
            .collect::<Vec<_>>()
            .to_value();
        // Convert to `serde_json::Value` then `async_graphql::Value` via the trait `InputType`.
        let new_parameters = serde_json::to_value(parameters)
            .context("could not create parameters JSON")?
            .to_value();
        let new_argument = serde_json::to_value(argument)
            .context("could not create argument JSON")?
            .to_value();
        let query = format!(
            "mutation {{ createApplication(\
                 chainId: \"{chain_id}\",
                 bytecodeId: \"{bytecode_id}\", \
                 parameters: {new_parameters}, \
                 initializationArgument: {new_argument}, \
                 requiredApplicationIds: {json_required_applications_ids}) \
             }}"
        );
        let data = self.query_node(query).await?;
        let app_id_str = data["createApplication"]
            .as_str()
            .context("missing createApplication string in response")?
            .trim();
        Ok(app_id_str
            .parse::<ApplicationId>()
            .context("invalid application ID")?
            .with_abi())
    }

    pub async fn request_application<A: ContractAbi>(
        &self,
        chain_id: &ChainId,
        application_id: &ApplicationId<A>,
    ) -> Result<String> {
        let application_id = application_id.forget_abi();
        let query = format!(
            "mutation {{ requestApplication(\
                 chainId: \"{chain_id}\", \
                 applicationId: \"{application_id}\") \
             }}"
        );
        let data = self.query_node(query).await?;
        serde_json::from_value(data["requestApplication"].clone())
            .context("missing requestApplication field in response")
    }

    pub async fn subscribe(
        &self,
        subscriber_chain_id: ChainId,
        publisher_chain_id: ChainId,
        channel: SystemChannel,
    ) -> Result<()> {
        let query = format!(
            "mutation {{ subscribe(\
                 subscriberChainId: \"{subscriber_chain_id}\", \
                 publisherChainId: \"{publisher_chain_id}\", \
                 channel: \"{}\") \
             }}",
            channel.to_value(),
        );
        self.query_node(query).await?;
        Ok(())
    }
}

/// A running faucet service.
pub struct Faucet {
    port: u16,
    child: Child,
}

impl Faucet {
    fn new(port: u16, child: Child) -> Self {
        Self { port, child }
    }

    pub async fn terminate(mut self) -> Result<()> {
        self.child
            .kill()
            .await
            .context("terminating faucet service")
    }

    pub fn ensure_is_running(&mut self) -> Result<()> {
        self.child.ensure_is_running()
    }

    pub fn url(&self) -> String {
        format!("http://localhost:{}/", self.port)
    }

    pub async fn claim(&self, public_key: &PublicKey) -> Result<ClaimOutcome> {
        Self::claim_url(public_key, &self.url()).await
    }

    pub async fn request_genesis_config(url: &str) -> Result<GenesisConfig> {
        let query = "query { genesisConfig }";
        let client = reqwest_client();
        let response = client
            .post(url)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("failed to post query")?;
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

    pub async fn claim_url(public_key: &PublicKey, url: &str) -> Result<ClaimOutcome> {
        let query = format!(
            "mutation {{ claim(publicKey: \"{public_key}\") {{ \
                messageId chainId certificateHash \
            }} }}"
        );
        let client = reqwest_client();
        let response = client
            .post(url)
            .json(&json!({ "query": &query }))
            .send()
            .await
            .context("failed to post query")?;
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

    pub async fn current_validators(url: &str) -> Result<Vec<(ValidatorName, String)>> {
        let query = "query { currentValidators { name networkAddress } }";
        let client = reqwest_client();
        let response = client
            .post(url)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("failed to post query")?;
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

/// A running `Application` to be queried in GraphQL.
pub struct ApplicationWrapper<A> {
    uri: String,
    _phantom: PhantomData<A>,
}

impl<A> ApplicationWrapper<A> {
    pub async fn raw_query(&self, query: impl AsRef<str>) -> Result<Value> {
        let query = query.as_ref();
        let client = reqwest_client();
        let response = client
            .post(&self.uri)
            .json(&json!({ "query": query }))
            .send()
            .await
            .context("failed to post query")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Query \"{}\" failed: {}",
            query.get(..200).unwrap_or(query),
            response
                .text()
                .await
                .unwrap_or_else(|error| format!("Could not get response text: {error}"))
        );
        let value: Value = response.json().await.context("invalid JSON")?;
        if let Some(errors) = value.get("errors") {
            bail!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                errors
            );
        }
        Ok(value["data"].clone())
    }

    pub async fn query(&self, query: impl AsRef<str>) -> Result<Value> {
        let query = query.as_ref();
        self.raw_query(&format!("query {{ {query} }}")).await
    }

    pub async fn query_json<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> Result<T> {
        let query = query.as_ref().trim();
        let name = query
            .split_once(|ch: char| !ch.is_alphanumeric())
            .map_or(query, |(name, _)| name);
        let data = self.query(query).await?;
        serde_json::from_value(data[name].clone())
            .with_context(|| format!("{name} field missing in response"))
    }

    pub async fn mutate(&self, mutation: impl AsRef<str>) -> Result<Value> {
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
