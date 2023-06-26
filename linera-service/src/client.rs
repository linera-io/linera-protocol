use crate::config::WalletState;
use anyhow::{bail, Context, Result};
use async_graphql::InputType;
use linera_base::{
    abi::ContractAbi,
    identifiers::{ChainId, MessageId, Owner},
};
use linera_execution::Bytecode;
use once_cell::sync::OnceCell;
use serde_json::{json, value::Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    process::Stdio,
    rc::Rc,
    str::FromStr,
    time::Duration,
};
use tempfile::{tempdir, TempDir};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use tonic_health::proto::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::{info, warn};

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to `cargo` when starting client, server and proxy processes.
const CARGO_ENV: &str = "INTEGRATION_TEST_CARGO_PARAMS";

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the binary when starting a server.
const SERVER_ENV: &str = "INTEGRATION_TEST_SERVER_PARAMS";

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the node-service command of the client.
const CLIENT_SERVICE_ENV: &str = "INTEGRATION_TEST_CLIENT_SERVICE_PARAMS";

#[derive(Copy, Clone)]
pub enum Network {
    Grpc,
    Simple,
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

#[allow(unused_mut)]
fn detect_current_features() -> Vec<&'static str> {
    let mut features = vec![];
    #[cfg(benchmark)]
    {
        features.push("benchmark");
    }
    #[cfg(wasmer)]
    {
        features.push("wasmer");
    }
    #[cfg(wasmtime)]
    {
        features.push("wasmtime");
    }
    #[cfg(rocksdb)]
    {
        features.push("rocksdb");
    }
    #[cfg(aws)]
    {
        features.push("aws");
    }
    features
}

pub async fn cargo_force_build_binary(name: &'static str) -> Result<PathBuf> {
    let mut build_command = Command::new("cargo");
    build_command.arg("build");
    let is_release = if let Ok(var) = env::var(CARGO_ENV) {
        let extra_args = var.split_whitespace();
        build_command.args(extra_args.clone());
        let extra_args: HashSet<_> = extra_args.into_iter().map(str::trim).collect();
        extra_args.contains("-r") || extra_args.contains("--release")
    } else {
        false
    };
    // Use the same features as the current environment so that we don't rebuild as often.
    let features = detect_current_features();
    if !features.is_empty() {
        build_command
            .arg("--no-default-features")
            .arg("--features")
            .args(features);
    }
    build_command.args(["--bin", name]);
    info!("Running compiler: {:?}", build_command);
    assert!(build_command.spawn()?.wait().await?.success());
    if is_release {
        Ok(env::current_dir()?
            .join("../target/release")
            .join(name)
            .canonicalize()?)
    } else {
        Ok(env::current_dir()?
            .join("../target/debug")
            .join(name)
            .canonicalize()?)
    }
}

pub async fn cargo_build_binary(name: &'static str) -> Result<PathBuf> {
    static COMPILED_BINARIES: OnceCell<Mutex<HashMap<&'static str, PathBuf>>> = OnceCell::new();
    let mut binaries = COMPILED_BINARIES.get_or_init(Default::default).lock().await;
    match binaries.get(name) {
        Some(path) => Ok(path.clone()),
        None => {
            let path = cargo_force_build_binary(name).await?;
            binaries.insert(name, path.clone());
            Ok(path)
        }
    }
}

pub struct Client {
    tmp_dir: Rc<TempDir>,
    storage: String,
    wallet: String,
    max_pending_messages: usize,
    network: Network,
}

impl Client {
    fn new(tmp_dir: Rc<TempDir>, network: Network, id: usize) -> Self {
        Self {
            tmp_dir,
            storage: format!("rocksdb:client_{}.db", id),
            wallet: format!("wallet_{}.json", id),
            max_pending_messages: 10_000,
            network,
        }
    }

    pub async fn project_new(&self, project_name: &str) -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let mut command = self.run().await?;
        assert!(command
            .current_dir(tmp.path().canonicalize()?)
            .kill_on_drop(true)
            .arg("project")
            .arg("new")
            .arg(project_name)
            .spawn()?
            .wait()
            .await?
            .success());
        Ok(tmp)
    }

    pub async fn project_publish(
        &self,
        path: PathBuf,
        required_application_ids: Vec<String>,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<String> {
        let json_parameters = serde_json::to_string(&())?;
        let json_argument = serde_json::to_string(&())?;
        let mut command = self.run_with_storage().await?;
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
        let stdout = Self::run_command(&mut command).await?;
        Ok(stdout.trim().to_string())
    }

    pub async fn project_test(&self, path: &Path) -> Result<()> {
        let mut command = self.run().await?;
        assert!(command
            .current_dir(path)
            .kill_on_drop(true)
            .arg("project")
            .arg("test")
            .spawn()?
            .wait()
            .await?
            .success());
        Ok(())
    }

    pub async fn run(&self) -> Result<Command> {
        let path = cargo_build_binary("linera").await?;
        let mut command = Command::new(path);
        command
            .current_dir(&self.tmp_dir.path().canonicalize()?)
            .kill_on_drop(true)
            .args(["--wallet", &self.wallet])
            .args(["--send-timeout-us", "10000000"])
            .args(["--recv-timeout-us", "10000000"])
            .arg("--wait-for-outgoing-messages");
        Ok(command)
    }

    pub async fn run_with_storage(&self) -> Result<Command> {
        let mut command = self.run().await?;
        command
            .args(["--storage", &self.storage.to_string()])
            .args([
                "--max-pending-messages",
                &self.max_pending_messages.to_string(),
            ]);
        Ok(command)
    }

    pub async fn create_genesis_config(&self) -> Result<()> {
        assert!(self
            .run()
            .await?
            .args(["create-genesis-config", "10"])
            .args(["--initial-funding", "10"])
            .args(["--committee", "committee.json"])
            .args(["--genesis", "genesis.json"])
            .spawn()?
            .wait()
            .await?
            .success());
        Ok(())
    }

    pub async fn wallet_init(&self, chain_ids: &[ChainId]) -> Result<()> {
        let mut command = self.run().await?;
        command
            .args(["wallet", "init"])
            .args(["--genesis", "genesis.json"]);
        if !chain_ids.is_empty() {
            let ids = chain_ids.iter().map(ChainId::to_string);
            command.arg("--with-other-chains").args(ids);
        }
        assert!(command.spawn()?.wait().await?.success());
        Ok(())
    }

    pub async fn run_command(command: &mut Command) -> Result<String> {
        let output = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        assert!(
            output.status.success(),
            "Command {:?} failed; stderr:\n{}\n(end stderr)",
            command,
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    pub async fn publish_and_create<A: ContractAbi>(
        &self,
        contract: PathBuf,
        service: PathBuf,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: Vec<String>,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<String> {
        let json_parameters = serde_json::to_string(parameters)?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.run_with_storage().await?;
        command
            .arg("publish-and-create")
            .args([contract, service])
            .args(publisher.into().iter().map(ChainId::to_string))
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument]);
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(required_application_ids);
        }
        let stdout = Self::run_command(&mut command).await?;
        Ok(stdout.trim().to_string())
    }

    pub async fn publish_bytecode(
        &self,
        contract: PathBuf,
        service: PathBuf,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<String> {
        let stdout = Self::run_command(
            self.run_with_storage()
                .await?
                .arg("publish-bytecode")
                .args([contract, service])
                .args(publisher.into().iter().map(ChainId::to_string)),
        )
        .await?;
        Ok(stdout.trim().to_string())
    }

    pub async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: String,
        argument: &A::InitializationArgument,
        creator: impl Into<Option<ChainId>>,
    ) -> Result<String> {
        let json_argument = serde_json::to_string(argument)?;
        let stdout = Self::run_command(
            self.run_with_storage()
                .await?
                .arg("create-application")
                .arg(bytecode_id)
                .args(["--json-argument", &json_argument])
                .args(creator.into().iter().map(ChainId::to_string)),
        )
        .await?;
        Ok(stdout.trim().to_string())
    }

    pub async fn run_node_service(
        &self,
        chain_id: impl Into<Option<ChainId>>,
        port: impl Into<Option<u16>>,
    ) -> Result<NodeService> {
        let chain_id = chain_id.into();
        let port = port.into().unwrap_or(8080);
        let mut command = self.run_with_storage().await?;
        command.arg("service");
        if let Ok(var) = env::var(CLIENT_SERVICE_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(chain_id.as_ref().map(ChainId::to_string))
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
                return Ok(NodeService {
                    port,
                    chain_id,
                    child,
                });
            } else {
                warn!("Waiting for node service to start");
            }
        }
        bail!("Failed to start node service");
    }

    pub async fn query_validators(&self, chain_id: Option<ChainId>) -> Result<()> {
        let mut command = self.run_with_storage().await?;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        Self::run_command(&mut command).await?;
        Ok(())
    }

    pub async fn query_balance(&self, chain_id: ChainId) -> Result<String> {
        let stdout = Self::run_command(
            self.run_with_storage()
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
            self.run_with_storage()
                .await?
                .arg("transfer")
                .arg(amount)
                .args(["--from", &from.to_string()])
                .args(["--to", &to.to_string()]),
        )
        .await?;
        Ok(())
    }

    #[cfg(benchmark)]
    pub async fn benchmark(&self, max_in_flight: usize) {
        assert!(self
            .run_with_storage()
            .await
            .arg("benchmark")
            .args(["--max-in-flight", &max_in_flight.to_string()])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .success());
    }

    pub async fn open_chain(
        &self,
        from: ChainId,
        to_owner: Option<Owner>,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.run_with_storage().await?;
        command
            .arg("open-chain")
            .args(["--from", &from.to_string()]);

        if let Some(owner) = to_owner {
            command.args(["--to-public-key", &owner.to_string()]);
        }

        let stdout = Self::run_command(&mut command).await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("failed to split stdout")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("failed to split stdout")?)?;

        Ok((message_id, chain_id))
    }

    pub async fn open_and_assign(&self, client: &Client) -> Result<ChainId> {
        let our_chain = self
            .get_wallet()?
            .default_chain()
            .context("no default chain specified in wallet")?;
        let key = client.keygen().await?;
        let (message_id, new_chain) = self.open_chain(our_chain, Some(key)).await?;
        assert_eq!(new_chain, client.assign(key, message_id).await?);
        Ok(new_chain)
    }

    pub fn get_wallet(&self) -> Result<WalletState> {
        WalletState::from_file(self.tmp_dir.path().join(&self.wallet).as_path())
    }

    pub fn get_owner(&self) -> Option<Owner> {
        let wallet = self.get_wallet().expect("failed to get wallet");
        let chain_id = wallet.default_chain()?;
        let public_key = wallet.get(chain_id)?.key_pair.as_ref()?.public();
        Some(public_key.into())
    }

    pub async fn is_chain_present_in_wallet(&self, chain: ChainId) -> Result<bool> {
        Ok(self.get_wallet()?.get(chain).is_some())
    }

    pub async fn set_validator(&self, name: &str, port: usize, votes: usize) -> Result<()> {
        let address = format!("{}:127.0.0.1:{}", self.network.external_short(), port);
        Self::run_command(
            self.run_with_storage()
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
            self.run_with_storage()
                .await?
                .arg("remove-validator")
                .args(["--name", name]),
        )
        .await?;
        Ok(())
    }

    pub async fn keygen(&self) -> Result<Owner> {
        let stdout = Self::run_command(self.run().await?.arg("keygen")).await?;
        Ok(Owner::from_str(stdout.trim())?)
    }

    pub async fn assign(&self, owner: Owner, message_id: MessageId) -> Result<ChainId> {
        let stdout = Self::run_command(
            self.run_with_storage()
                .await?
                .arg("assign")
                .args(["--key", &owner.to_string()])
                .args(["--message-id", &message_id.to_string()]),
        )
        .await?;

        let chain_id = ChainId::from_str(stdout.trim())?;

        Ok(chain_id)
    }

    pub async fn synchronize_balance(&self, chain_id: ChainId) -> Result<()> {
        Self::run_command(
            self.run_with_storage()
                .await?
                .arg("sync-balance")
                .arg(&chain_id.to_string()),
        )
        .await?;
        Ok(())
    }
}

pub struct Validator {
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

    fn add_server(&mut self, server: Child) {
        self.servers.push(server)
    }

    fn kill_server(&mut self, index: usize) {
        self.servers.remove(index);
    }

    fn assert_is_running(&mut self) -> Result<()> {
        if let Some(status) = self.proxy.try_wait()? {
            assert!(status.success());
        }
        for child in &mut self.servers {
            if let Some(status) = child.try_wait()? {
                assert!(status.success());
            }
        }
        Ok(())
    }
}

pub struct LocalNet {
    network: Network,
    tmp_dir: Rc<TempDir>,
    next_client_id: usize,
    num_initial_validators: usize,
    local_net: BTreeMap<usize, Validator>,
}

impl Drop for LocalNet {
    fn drop(&mut self) {
        for validator in self.local_net.values_mut() {
            let _ = validator.assert_is_running();
        }
    }
}

impl LocalNet {
    pub fn new(network: Network, num_initial_validators: usize) -> Result<Self> {
        Ok(Self {
            tmp_dir: Rc::new(tempdir()?),
            network,
            next_client_id: 0,
            num_initial_validators,
            local_net: BTreeMap::new(),
        })
    }

    pub fn make_client(&mut self, network: Network) -> Client {
        let client = Client::new(self.tmp_dir.clone(), network, self.next_client_id);
        self.next_client_id += 1;
        client
    }

    pub async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = cargo_build_binary(name).await?;
        let mut command = Command::new(path);
        command
            .current_dir(&self.tmp_dir.path().canonicalize()?)
            .kill_on_drop(true);
        Ok(command)
    }

    fn proxy_port(i: usize) -> usize {
        9000 + i * 100
    }

    fn shard_port(i: usize, j: usize) -> usize {
        9000 + i * 100 + j
    }

    fn internal_port(i: usize) -> usize {
        10000 + i * 100
    }

    fn metrics_port(i: usize) -> usize {
        11000 + i * 100
    }

    fn configuration_string(&self, server_number: usize) -> Result<String> {
        let n = server_number;
        let path = self
            .tmp_dir
            .path()
            .canonicalize()?
            .join(format!("validator_{n}.toml"));
        let port = Self::proxy_port(n);
        let internal_port = Self::internal_port(n);
        let metrics_port = Self::metrics_port(n);
        let external_protocol = self.network.external();
        let internal_protocol = self.network.internal();
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "127.0.0.1"
                port = {port}
                internal_host = "127.0.0.1"
                internal_port = {internal_port}
                external_protocol = {external_protocol}
                internal_protocol = {internal_protocol}
            "#
        );
        for k in 1..=4 {
            let shard_port = Self::shard_port(n, k);
            let shard_metrics_port = metrics_port + k;
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
        Ok(path.into_os_string().into_string().expect("TODO"))
    }

    pub async fn generate_initial_validator_config(&self) -> Result<Vec<String>> {
        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("generate").arg("--validators");
        for i in 1..=self.num_initial_validators {
            command.arg(&self.configuration_string(i)?);
        }
        let output = command
            .args(["--committee", "committee.json"])
            .stdout(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        assert!(output.status.success());
        let output_str = String::from_utf8_lossy(output.stdout.as_slice());
        Ok(output_str.split_whitespace().map(str::to_string).collect())
    }

    pub async fn generate_validator_config(&self, i: usize) -> Result<String> {
        let output = self
            .command_for_binary("linera-server")
            .await?
            .arg("generate")
            .arg("--validators")
            .arg(&self.configuration_string(i)?)
            .stdout(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        assert!(output.status.success());
        Ok(String::from_utf8_lossy(output.stdout.as_slice())
            .trim()
            .to_string())
    }

    pub async fn run_proxy(&self, i: usize) -> Result<Child> {
        let child = self
            .command_for_binary("linera-proxy")
            .await?
            .arg(format!("server_{}.json", i))
            .spawn()?;

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

    pub async fn ensure_grpc_server_has_started(nickname: &str, port: usize) -> Result<()> {
        let connection =
            tonic::transport::Endpoint::new(format!("http://127.0.0.1:{port}"))?.connect_lazy();
        let mut client = HealthClient::new(connection);
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let result = client.check(HealthCheckRequest::default()).await;
            if result.is_ok() && result?.get_ref().status() == ServingStatus::Serving {
                info!("Successfully started {nickname}");
                return Ok(());
            } else {
                warn!("Waiting for {nickname} to start");
            }
        }
        bail!("Failed to start {nickname}");
    }

    pub async fn run_server(&self, i: usize, j: usize) -> Result<Child> {
        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("run");
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--storage", &format!("rocksdb:server_{}_{}.db", i, j)])
            .args(["--server", &format!("server_{}.json", i)])
            .args(["--shard", &j.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn()?;

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

    pub async fn run_local_net(&mut self) -> Result<()> {
        self.start_validators(1..=self.num_initial_validators).await
    }

    pub fn net_path(&self) -> &Path {
        self.tmp_dir.path()
    }

    pub fn kill_server(&mut self, i: usize, j: usize) {
        if let Some(validator) = self.local_net.get_mut(&i) {
            validator.kill_server(j)
        };
    }

    pub fn remove_validator(&mut self, i: usize) {
        if self.local_net.remove(&i).is_none() {
            warn!(
                "Tried to remove validator at index {} which does not exist.",
                i
            )
        }
    }

    pub async fn start_server(&mut self, i: usize, j: usize) -> Result<()> {
        let server = self.run_server(i, j).await?;
        self.local_net
            .get_mut(&i)
            .context("failed to validator")?
            .add_server(server);
        Ok(())
    }

    pub async fn start_validators(&mut self, validator_range: RangeInclusive<usize>) -> Result<()> {
        for i in validator_range {
            let proxy = self.run_proxy(i).await?;
            let mut validator = Validator::new(proxy);
            for j in 0..4 {
                let server = self.run_server(i, j).await?;
                validator.add_server(server);
            }
            self.local_net.insert(i, validator);
        }
        Ok(())
    }

    pub async fn build_example(&self, name: &str) -> Result<(PathBuf, PathBuf)> {
        self.build_application(Self::example_path(name).as_path(), name, true)
            .await
    }

    pub fn example_path(name: &str) -> PathBuf {
        env::current_dir().unwrap().join("../examples/").join(name)
    }

    pub async fn build_application(
        &self,
        path: &Path,
        name: &str,
        is_workspace: bool,
    ) -> Result<(PathBuf, PathBuf)> {
        assert!(Command::new("cargo")
            .current_dir(self.tmp_dir.path().canonicalize()?)
            .arg("build")
            .arg("--release")
            .args(["--target", "wasm32-unknown-unknown"])
            .arg("--manifest-path")
            .arg(path.join("Cargo.toml"))
            .stdout(Stdio::piped())
            .spawn()?
            .wait()
            .await?
            .success());

        let release_dir = match is_workspace {
            true => path.join("../target/wasm32-unknown-unknown/release"),
            false => path.join("target/wasm32-unknown-unknown/release"),
        };

        let contract = release_dir.join(format!("{}_contract.wasm", name.replace('-', "_")));
        let service = release_dir.join(format!("{}_service.wasm", name.replace('-', "_")));

        Ok((contract, service))
    }
}

pub struct NodeService {
    chain_id: Option<ChainId>,
    port: u16,
    child: Child,
}

impl NodeService {
    pub fn assert_is_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            assert!(status.success());
        }
    }

    pub async fn process_inbox(&self) {
        self.query_node("mutation { processInbox }").await.unwrap();
    }

    pub async fn make_application(&self, application_id: &str) -> Result<String> {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let values = self.try_get_applications_uri().await?;
            if let Some(link) = values.get(application_id) {
                return Ok(link.to_string());
            }
            warn!(
                "Waiting for application {application_id:?} to be visible on chain {:?}",
                self.chain_id
            );
        }
        bail!("Could not find application URI: {application_id}");
    }

    pub async fn try_get_applications_uri(&self) -> Result<HashMap<String, String>> {
        let query = if let Some(chain_id) = self.chain_id {
            format!(
                "query {{ applications(chainId: \"{}\") {{ id link }}}}",
                chain_id
            )
        } else {
            "query { applications { id link }}".to_string()
        };
        let data = self.query_node(&query).await?;
        Ok(data["applications"]
            .as_array()
            .context("could cast to array")?
            .iter()
            .map(|a| {
                let id = a["id"]
                    .as_str()
                    .expect("could not cast id to string")
                    .to_string();
                let link = a["link"]
                    .as_str()
                    .expect("could nto cast link to string")
                    .to_string();
                (id, link)
            })
            .collect())
    }

    pub async fn publish_bytecode(&self, contract: PathBuf, service: PathBuf) -> Result<String> {
        let contract_code = Bytecode::load_from_file(&contract).await?;
        let service_code = Bytecode::load_from_file(&service).await?;
        let query = format!(
            "mutation {{ publishBytecode(contract: {}, service: {}) }}",
            contract_code.to_value(),
            service_code.to_value(),
        );
        let data = self.query_node(&query).await?;
        Ok(serde_json::from_value(data["publishBytecode"].clone())?)
    }

    pub async fn query_node(&self, query: &str) -> Result<Value> {
        let url = format!("http://localhost:{}/", self.port);
        let client = reqwest::Client::new();
        let response = client
            .post(url)
            .json(&json!({ "query": query }))
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                response.text().await?
            );
        }
        let value: Value = response.json().await.unwrap();
        if let Some(errors) = value.get("errors") {
            bail!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                errors
            );
        }
        Ok(value["data"].clone())
    }

    pub async fn create_application(&self, bytecode_id: &str) -> Result<String> {
        let query = format!(
            "mutation {{ createApplication(\
                bytecodeId: \"{bytecode_id}\", \
                parameters: \"null\", \
                initializationArgument: \"null\", \
                requiredApplicationIds: []) \
            }}"
        );
        let data = self.query_node(&query).await?;
        Ok(serde_json::from_value(data["createApplication"].clone())?)
    }

    pub async fn request_application(&self, application_id: &str) -> Result<String> {
        let query = format!("mutation {{ requestApplication(applicationId: {application_id}) }}");
        let data = self.query_node(&query).await?;
        Ok(serde_json::from_value(data["requestApplication"].clone())?)
    }
}
