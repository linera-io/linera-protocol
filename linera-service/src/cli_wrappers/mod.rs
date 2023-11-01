// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper module to call the binaries of `linera-service` with appropriate command-line
//! arguments.

use crate::{
    config::WalletState,
    util,
    util::{ChildExt, CommandExt},
};
use anyhow::{anyhow, bail, Context, Result};
use async_graphql::InputType;
use async_trait::async_trait;
use linera_base::{
    abi::ContractAbi,
    crypto::PublicKey,
    data_types::{Amount, RoundNumber},
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId, Owner},
};
use linera_execution::Bytecode;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{json, value::Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    marker::PhantomData,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tempfile::{tempdir, TempDir};
use tokio::process::{Child, Command};
use tonic_health::proto::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::{info, warn};

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the binary when starting a server.
const SERVER_ENV: &str = "LINERA_SERVER_PARAMS";

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the node-service command of the client.
const CLIENT_SERVICE_ENV: &str = "LINERA_CLIENT_SERVICE_PARAMS";

#[derive(Copy, Clone)]
pub enum Network {
    Grpc,
    Simple,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Database {
    RocksDb,
    DynamoDb,
    ScyllaDb,
}

impl Network {
    fn internal(&self) -> &'static str {
        match self {
            Network::Grpc => "\"Grpc\"",
            Network::Simple => "{ Simple = \"Tcp\" }",
        }
    }

    fn external(&self) -> &'static str {
        match self {
            Network::Grpc => "\"Grpc\"",
            Network::Simple => "{ Simple = \"Tcp\" }",
        }
    }

    fn external_short(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Simple => "tcp",
        }
    }
}

pub struct ClientWrapper {
    testing_prng_seed: Option<u64>,
    storage: String,
    wallet: String,
    max_pending_messages: usize,
    network: Network,
    pub tmp_dir: Arc<TempDir>,
}

impl ClientWrapper {
    fn new(
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
            .args(["--send-timeout-us", "10000000"])
            .args(["--recv-timeout-us", "10000000"])
            .arg("--wait-for-outgoing-messages");
        Ok(command)
    }

    pub async fn create_genesis_config(&self) -> Result<()> {
        let mut command = self.command().await?;
        command
            .args(["create-genesis-config", "10"])
            .args(["--initial-funding", "10"])
            .args(["--committee", "committee.json"])
            .args(["--genesis", "genesis.json"]);
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    pub async fn wallet_init(&self, chain_ids: &[ChainId]) -> Result<()> {
        let mut command = self.command().await?;
        command
            .args(["wallet", "init"])
            .args(["--genesis", "genesis.json"]);
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        if !chain_ids.is_empty() {
            let ids = chain_ids.iter().map(ChainId::to_string);
            command.arg("--with-other-chains").args(ids);
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

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

    pub async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: &BytecodeId,
        argument: &A::InitializationArgument,
        creator: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_argument = serde_json::to_string(argument)?;
        let stdout = self
            .command()
            .await?
            .arg("create-application")
            .arg(bytecode_id.to_string())
            .args(["--json-argument", &json_argument])
            .args(creator.into().iter().map(ChainId::to_string))
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

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
        let client = reqwest::Client::new();
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

    pub async fn query_validators(&self, chain_id: Option<ChainId>) -> Result<()> {
        let mut command = self.command().await?;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

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

    #[cfg(benchmark)]
    async fn benchmark(&self, max_in_flight: usize) -> Result<()> {
        self.command()
            .await
            .arg("benchmark")
            .args(["--max-in-flight", &max_in_flight.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn open_chain(
        &self,
        from: ChainId,
        to_public_key: Option<PublicKey>,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.command().await?;
        command
            .arg("open-chain")
            .args(["--from", &from.to_string()]);

        if let Some(public_key) = to_public_key {
            command.args(["--to-public-key", &public_key.to_string()]);
        }

        let stdout = command.spawn_and_wait_for_stdout().await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("no message ID in output")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("no chain ID in output")?)?;

        Ok((message_id, chain_id))
    }

    pub async fn open_and_assign(&self, client: &ClientWrapper) -> Result<ChainId> {
        let our_chain = self
            .get_wallet()?
            .default_chain()
            .context("no default chain found")?;
        let key = client.keygen().await?;
        let (message_id, new_chain) = self.open_chain(our_chain, Some(key)).await?;
        assert_eq!(new_chain, client.assign(key, message_id).await?);
        Ok(new_chain)
    }

    pub async fn open_multi_owner_chain(
        &self,
        from: ChainId,
        to_public_keys: Vec<PublicKey>,
        weights: Vec<u64>,
        multi_leader_rounds: RoundNumber,
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

    pub async fn keygen(&self) -> Result<PublicKey> {
        let stdout = self
            .command()
            .await?
            .arg("keygen")
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(PublicKey::from_str(stdout.trim())?)
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.get_wallet().ok()?.default_chain()
    }

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
}

struct Validator {
    proxy: Child,
    servers: Vec<Child>,
}

impl Validator {
    fn new(proxy: Child) -> Self {
        Self {
            proxy,
            servers: vec![],
        }
    }

    async fn terminate(mut self) -> Result<()> {
        self.proxy
            .kill()
            .await
            .context("terminating validator proxy")?;
        for mut server in self.servers {
            server
                .kill()
                .await
                .context("terminating validator server")?;
        }
        Ok(())
    }

    fn add_server(&mut self, server: Child) {
        self.servers.push(server)
    }

    #[cfg(any(test, feature = "test"))]
    async fn terminate_server(&mut self, index: usize) -> Result<()> {
        let mut server = self.servers.remove(index);
        server
            .kill()
            .await
            .context("terminating validator server")?;
        Ok(())
    }

    fn ensure_is_running(&mut self) -> Result<()> {
        self.proxy.ensure_is_running()?;
        for child in &mut self.servers {
            child.ensure_is_running()?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait LineraNetConfig {
    type Net: LineraNet + Sized + Send + Sync + 'static;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)>;
}

#[async_trait]
pub trait LineraNet {
    fn ensure_is_running(&mut self) -> Result<()>;

    fn make_client(&mut self) -> ClientWrapper;

    async fn terminate(mut self) -> Result<()>;
}

pub struct LocalNetConfig {
    pub database: Database,
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub table_name: String,
    pub num_initial_validators: usize,
    pub num_shards: usize,
}

#[cfg(any(test, feature = "test"))]
pub struct LocalNetTestingConfig {
    pub database: Database,
    pub network: Network,
}

#[cfg(any(test, feature = "test"))]
impl LocalNetTestingConfig {
    pub fn new(database: Database, network: Network) -> Self {
        Self { database, network }
    }
}

pub struct LocalNet {
    database: Database,
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    num_initial_validators: usize,
    num_shards: usize,
    validator_names: BTreeMap<usize, String>,
    running_validators: BTreeMap<usize, Validator>,
    table_name: String,
    set_init: HashSet<(usize, usize)>,
    tmp_dir: Arc<TempDir>,
}

#[async_trait]
impl LineraNetConfig for LocalNetConfig {
    type Net = LocalNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let mut net = LocalNet::new(
            self.database,
            self.network,
            self.testing_prng_seed,
            self.table_name,
            self.num_initial_validators,
            self.num_shards,
        )?;
        let client = net.make_client();
        if self.num_initial_validators > 0 {
            net.generate_initial_validator_config().await.unwrap();
            client.create_genesis_config().await.unwrap();
            net.run().await.unwrap();
        }
        Ok((net, client))
    }
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl LineraNetConfig for LocalNetTestingConfig {
    type Net = LocalNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let seed = 37;
        let table_name = linera_views::test_utils::get_table_name();
        let num_validators = 4;
        let num_shards = match self.database {
            Database::RocksDb => 1,
            Database::DynamoDb => 4,
            Database::ScyllaDb => 4,
        };
        let mut net = LocalNet::new(
            self.database,
            self.network,
            Some(seed),
            table_name,
            num_validators,
            num_shards,
        )?;
        let client = net.make_client();
        if num_validators > 0 {
            net.generate_initial_validator_config().await.unwrap();
            client.create_genesis_config().await.unwrap();
            net.run().await.unwrap();
        }
        Ok((net, client))
    }
}

#[async_trait]
impl LineraNet for LocalNet {
    fn ensure_is_running(&mut self) -> Result<()> {
        for validator in self.running_validators.values_mut() {
            validator.ensure_is_running().context("in local network")?
        }
        Ok(())
    }

    fn make_client(&mut self) -> ClientWrapper {
        let client = ClientWrapper::new(
            self.tmp_dir.clone(),
            self.network,
            self.testing_prng_seed,
            self.next_client_id,
        );
        if let Some(seed) = self.testing_prng_seed {
            self.testing_prng_seed = Some(seed + 1);
        }
        self.next_client_id += 1;
        client
    }

    async fn terminate(mut self) -> Result<()> {
        let validators = std::mem::take(&mut self.running_validators);
        for (_, validator) in validators {
            validator.terminate().await.context("in local network")?
        }
        Ok(())
    }
}

impl LocalNet {
    fn new(
        database: Database,
        network: Network,
        testing_prng_seed: Option<u64>,
        table_name: String,
        num_initial_validators: usize,
        num_shards: usize,
    ) -> Result<Self> {
        Ok(Self {
            database,
            network,
            testing_prng_seed,
            next_client_id: 0,
            num_initial_validators,
            num_shards,
            validator_names: BTreeMap::new(),
            running_validators: BTreeMap::new(),
            table_name,
            set_init: HashSet::new(),
            tmp_dir: Arc::new(tempdir()?),
        })
    }

    async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = util::resolve_binary(name, env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command.current_dir(self.tmp_dir.path());
        Ok(command)
    }

    pub fn proxy_port(i: usize) -> usize {
        9000 + i * 100
    }

    fn shard_port(i: usize, j: usize) -> usize {
        9000 + i * 100 + j + 1
    }

    fn internal_port(i: usize) -> usize {
        10000 + i * 100
    }

    fn proxy_metrics_port(i: usize) -> usize {
        11000 + i * 100
    }

    fn shard_metrics_port(i: usize, j: usize) -> usize {
        11000 + i * 100 + j + 1
    }

    fn configuration_string(&self, server_number: usize) -> Result<String> {
        let n = server_number;
        let path = self.tmp_dir.path().join(format!("validator_{n}.toml"));
        let port = Self::proxy_port(n);
        let internal_port = Self::internal_port(n);
        let metrics_port = Self::proxy_metrics_port(n);
        let external_protocol = self.network.external();
        let internal_protocol = self.network.internal();
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "127.0.0.1"
                port = {port}
                internal_host = "127.0.0.1"
                internal_port = {internal_port}
                metrics_host = "127.0.0.1"
                metrics_port = {metrics_port}
                external_protocol = {external_protocol}
                internal_protocol = {internal_protocol}
            "#
        );
        for k in 0..self.num_shards {
            let shard_port = Self::shard_port(n, k);
            let shard_metrics_port = Self::shard_metrics_port(n, k);
            content.push_str(&format!(
                r#"

                [[shards]]
                host = "127.0.0.1"
                port = {shard_port}
                metrics_host = "127.0.0.1"
                metrics_port = {shard_metrics_port}
                "#
            ));
        }
        fs::write(&path, content)?;
        path.into_os_string().into_string().map_err(|error| {
            anyhow!(
                "could not parse string into OS string: {}",
                error.to_string_lossy()
            )
        })
    }

    async fn generate_initial_validator_config(&mut self) -> Result<()> {
        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("generate");
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
            self.testing_prng_seed = Some(seed + 1);
        }
        command.arg("--validators");
        for i in 0..self.num_initial_validators {
            command.arg(&self.configuration_string(i)?);
        }
        let output = command
            .args(["--committee", "committee.json"])
            .spawn_and_wait_for_stdout()
            .await?;
        self.validator_names = output
            .split_whitespace()
            .map(str::to_string)
            .enumerate()
            .collect();
        Ok(())
    }

    async fn run_proxy(&self, i: usize) -> Result<Child> {
        let child = self
            .command_for_binary("linera-proxy")
            .await?
            .arg(format!("server_{}.json", i))
            .spawn_into()?;

        match self.network {
            Network::Grpc => {
                let port = Self::proxy_port(i);
                let nickname = format!("validator proxy {i}");
                Self::ensure_grpc_server_has_started(&nickname, port).await?;
            }
            Network::Simple => {
                info!("Letting validator proxy {i} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        Ok(child)
    }

    async fn ensure_grpc_server_has_started(nickname: &str, port: usize) -> Result<()> {
        let connection = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{port}"))
            .context("endpoint should always parse")?
            .connect_lazy();
        let mut client = HealthClient::new(connection);
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let result = client.check(HealthCheckRequest::default()).await;
            if result.is_ok() && result.unwrap().get_ref().status() == ServingStatus::Serving {
                info!("Successfully started {nickname}");
                return Ok(());
            } else {
                warn!("Waiting for {nickname} to start");
            }
        }
        bail!("Failed to start {nickname}");
    }

    async fn run_server(&mut self, i: usize, j: usize) -> Result<Child> {
        let (storage, key) = match self.database {
            Database::RocksDb => (format!("rocksdb:server_{}_{}.db", i, j), (i, j)),
            Database::DynamoDb => (
                format!("dynamodb:{}_server_{}.db:localstack", self.table_name, i),
                (i, 0),
            ),
            Database::ScyllaDb => (
                format!("scylladb:{}_server_{}_db", self.table_name, i),
                (i, 0),
            ),
        };
        if !self.set_init.contains(&key) {
            let max_try = 4;
            let mut i_try = 0;
            loop {
                let mut command = self.command_for_binary("linera-server").await?;
                command.arg("initialize");
                if let Ok(var) = env::var(SERVER_ENV) {
                    command.args(var.split_whitespace());
                }
                let result = command
                    .args(["--storage", &storage])
                    .args(["--genesis", "genesis.json"])
                    .spawn_and_wait_for_stdout()
                    .await;
                if result.is_ok() {
                    break;
                }
                warn!(
                    "Failed to initialize storage={} using linera-server, i_try={}, error={:?}",
                    storage, i_try, result
                );
                i_try += 1;
                if i_try == max_try {
                    bail!("Failed to initialize after {} attempts", max_try);
                }
                let one_second = std::time::Duration::from_millis(1000);
                std::thread::sleep(one_second);
            }
            self.set_init.insert(key);
        }

        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("run");
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--storage", &storage])
            .args(["--server", &format!("server_{}.json", i)])
            .args(["--shard", &j.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn_into()?;

        match self.network {
            Network::Grpc => {
                let port = Self::shard_port(i, j);
                let nickname = format!("validator server {i}:{j}");
                Self::ensure_grpc_server_has_started(&nickname, port).await?;
            }
            Network::Simple => {
                info!("Letting validator server {i}:{j} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        Ok(child)
    }

    async fn run(&mut self) -> Result<()> {
        for i in 0..self.num_initial_validators {
            self.start_validator(i).await?;
        }
        Ok(())
    }

    pub async fn start_validator(&mut self, i: usize) -> Result<()> {
        let proxy = self.run_proxy(i).await?;
        let mut validator = Validator::new(proxy);
        for j in 0..self.num_shards {
            let server = self.run_server(i, j).await?;
            validator.add_server(server);
        }
        self.running_validators.insert(i, validator);
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl LocalNet {
    pub fn validator_name(&self, i: usize) -> Option<&String> {
        self.validator_names.get(&i)
    }

    pub async fn generate_validator_config(&mut self, i: usize) -> Result<()> {
        let stdout = self
            .command_for_binary("linera-server")
            .await?
            .arg("generate")
            .arg("--validators")
            .arg(&self.configuration_string(i)?)
            .spawn_and_wait_for_stdout()
            .await?;
        self.validator_names.insert(i, stdout.trim().to_string());
        Ok(())
    }

    pub async fn terminate_server(&mut self, i: usize, j: usize) -> Result<()> {
        self.running_validators
            .get_mut(&i)
            .context("server not found")?
            .terminate_server(j)
            .await?;
        Ok(())
    }

    pub fn remove_validator(&mut self, i: usize) -> Result<()> {
        self.running_validators
            .remove(&i)
            .context("validator not found")?;
        Ok(())
    }

    pub async fn start_server(&mut self, i: usize, j: usize) -> Result<()> {
        let server = self.run_server(i, j).await?;
        self.running_validators
            .get_mut(&i)
            .context("could not find server")?
            .add_server(server);
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl ClientWrapper {
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

        Ok((contract, service))
    }

    pub async fn build_example(&self, name: &str) -> Result<(PathBuf, PathBuf)> {
        self.build_application(Self::example_path(name)?.as_path(), name, true)
            .await
    }

    pub fn example_path(name: &str) -> Result<PathBuf> {
        Ok(env::current_dir()?.join("../examples/").join(name))
    }
}

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
            let client = reqwest::Client::new();
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
}

pub struct ApplicationWrapper<A> {
    uri: String,
    _phantom: PhantomData<A>,
}

impl<A> ApplicationWrapper<A> {
    pub async fn raw_query(&self, query: impl AsRef<str>) -> Result<Value> {
        let query = query.as_ref();
        let client = reqwest::Client::new();
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
