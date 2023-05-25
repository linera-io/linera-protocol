// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(not(any(feature = "wasmer", feature = "wasmtime")), allow(dead_code))]

use async_graphql::InputType;
use linera_base::{
    abi::ContractAbi,
    identifiers::{ChainId, EffectId, Owner},
};
use linera_execution::Bytecode;
use linera_service::config::WalletState;
use once_cell::sync::{Lazy, OnceCell};
use serde_json::{json, Value};
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

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::ApplicationId,
};

/// A static lock to prevent integration tests from running in parallel.
static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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
enum Network {
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
    #[cfg(aws)]
    {
        features.push("aws");
    }
    features
}

async fn cargo_force_build_binary(name: &'static str) -> PathBuf {
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
    assert!(build_command
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    if is_release {
        env::current_dir()
            .unwrap()
            .join("../target/release")
            .join(name)
            .canonicalize()
            .unwrap()
    } else {
        env::current_dir()
            .unwrap()
            .join("../target/debug")
            .join(name)
            .canonicalize()
            .unwrap()
    }
}

async fn cargo_build_binary(name: &'static str) -> PathBuf {
    static COMPILED_BINARIES: OnceCell<Mutex<HashMap<&'static str, PathBuf>>> = OnceCell::new();
    let mut binaries = COMPILED_BINARIES.get_or_init(Default::default).lock().await;
    match binaries.get(name) {
        Some(path) => path.clone(),
        None => {
            let path = cargo_force_build_binary(name).await;
            binaries.insert(name, path.clone());
            path
        }
    }
}

struct Client {
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

    async fn project_new(&self, project_name: &str) -> TempDir {
        let tmp = TempDir::new().unwrap();
        let mut command = self.run().await;
        assert!(command
            .current_dir(tmp.path().canonicalize().unwrap())
            .kill_on_drop(true)
            .arg("project")
            .arg("new")
            .arg(project_name)
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .success());
        tmp
    }

    async fn test_project(&self, path: &Path) {
        let mut command = self.run().await;
        assert!(command
            .current_dir(path)
            .kill_on_drop(true)
            .arg("project")
            .arg("test")
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .success());
    }

    async fn run(&self) -> Command {
        let path = cargo_build_binary("linera").await;
        let mut command = Command::new(path);
        command
            .current_dir(&self.tmp_dir.path().canonicalize().unwrap())
            .kill_on_drop(true)
            .args(["--wallet", &self.wallet])
            .args(["--send-timeout-us", "10000000"])
            .args(["--recv-timeout-us", "10000000"])
            .arg("--wait-for-outgoing-messages");
        command
    }

    async fn run_with_storage(&self) -> Command {
        let mut command = self.run().await;
        command
            .args(["--storage", &self.storage.to_string()])
            .args([
                "--max-pending-messages",
                &self.max_pending_messages.to_string(),
            ]);
        command
    }

    async fn create_genesis_config(&self) {
        assert!(self
            .run()
            .await
            .args(["create-genesis-config", "10"])
            .args(["--initial-funding", "10"])
            .args(["--committee", "committee.json"])
            .args(["--genesis", "genesis.json"])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .success());
    }

    async fn wallet_init(&self, chain_ids: &[ChainId]) {
        let mut command = self.run().await;
        command
            .args(["wallet", "init"])
            .args(["--genesis", "genesis.json"]);
        if !chain_ids.is_empty() {
            let ids = chain_ids.iter().map(ChainId::to_string);
            command.arg("--with-other-chains").args(ids);
        }
        assert!(command.spawn().unwrap().wait().await.unwrap().success());
    }

    async fn run_command(command: &mut Command) -> String {
        let output = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "Command {:?} failed; stderr:\n{}\n(end stderr)",
            command,
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8(output.stdout).unwrap()
    }

    async fn publish_and_create<A: ContractAbi>(
        &self,
        contract: PathBuf,
        service: PathBuf,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: Vec<String>,
        publisher: impl Into<Option<ChainId>>,
    ) -> String {
        let json_parameters = serde_json::to_string(parameters).unwrap();
        let json_argument = serde_json::to_string(argument).unwrap();
        let mut command = self.run_with_storage().await;
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
        let stdout = Self::run_command(&mut command).await;
        stdout.trim().to_string()
    }

    async fn publish_bytecode(
        &self,
        contract: PathBuf,
        service: PathBuf,
        publisher: impl Into<Option<ChainId>>,
    ) -> String {
        let stdout = Self::run_command(
            self.run_with_storage()
                .await
                .arg("publish-bytecode")
                .args([contract, service])
                .args(publisher.into().iter().map(ChainId::to_string)),
        )
        .await;
        stdout.trim().to_string()
    }

    async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: String,
        argument: &A::InitializationArgument,
        creator: impl Into<Option<ChainId>>,
    ) -> String {
        let json_argument = serde_json::to_string(argument).unwrap();
        let stdout = Self::run_command(
            self.run_with_storage()
                .await
                .arg("create-application")
                .arg(bytecode_id)
                .args(["--json-argument", &json_argument])
                .args(creator.into().iter().map(ChainId::to_string)),
        )
        .await;
        stdout.trim().to_string()
    }

    async fn run_node_service(
        &self,
        chain_id: impl Into<Option<ChainId>>,
        port: impl Into<Option<u16>>,
    ) -> NodeService {
        let chain_id = chain_id.into();
        let port = port.into().unwrap_or(8080);
        let mut command = self.run_with_storage().await;
        command.arg("service");
        if let Ok(var) = env::var(CLIENT_SERVICE_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(chain_id.as_ref().map(ChainId::to_string))
            .args(["--port".to_string(), port.to_string()])
            .spawn()
            .unwrap();
        let client = reqwest::Client::new();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let request = client
                .get(format!("http://localhost:{}/", port))
                .send()
                .await;
            if request.is_ok() {
                info!("Node service has started");
                return NodeService {
                    port,
                    chain_id,
                    child,
                };
            } else {
                warn!("Waiting for node service to start");
            }
        }
        panic!("Failed to start node service");
    }

    async fn query_validators(&self, chain_id: Option<ChainId>) {
        let mut command = self.run_with_storage().await;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        Self::run_command(&mut command).await;
    }

    async fn query_balance(&self, chain_id: ChainId) -> anyhow::Result<String> {
        let stdout = Self::run_command(
            self.run_with_storage()
                .await
                .arg("query-balance")
                .arg(&chain_id.to_string()),
        )
        .await;
        let amount = stdout.trim().to_string();
        Ok(amount)
    }

    async fn transfer(&self, amount: &str, from: ChainId, to: ChainId) {
        Self::run_command(
            self.run_with_storage()
                .await
                .arg("transfer")
                .arg(amount)
                .args(["--from", &from.to_string()])
                .args(["--to", &to.to_string()]),
        )
        .await;
    }

    #[cfg(benchmark)]
    async fn benchmark(&self, max_in_flight: usize) {
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

    async fn open_chain(
        &self,
        from: ChainId,
        to_owner: Option<Owner>,
    ) -> anyhow::Result<(EffectId, ChainId)> {
        let mut command = self.run_with_storage().await;
        command
            .arg("open-chain")
            .args(["--from", &from.to_string()]);

        if let Some(owner) = to_owner {
            command.args(["--to-public-key", &owner.to_string()]);
        }

        let stdout = Self::run_command(&mut command).await;
        let mut split = stdout.split('\n');
        let effect_id: EffectId = split.next().unwrap().parse()?;
        let chain_id = ChainId::from_str(split.next().unwrap())?;

        Ok((effect_id, chain_id))
    }

    async fn open_and_assign(&self, client: &Client) -> ChainId {
        let our_chain = self.get_wallet().default_chain().unwrap();
        let key = client.keygen().await.unwrap();
        let (effect_id, new_chain) = self.open_chain(our_chain, Some(key)).await.unwrap();
        assert_eq!(new_chain, client.assign(key, effect_id).await.unwrap());
        new_chain
    }

    fn get_wallet(&self) -> WalletState {
        WalletState::from_file(self.tmp_dir.path().join(&self.wallet).as_path()).unwrap()
    }

    fn get_owner(&self) -> Option<Owner> {
        let wallet = self.get_wallet();
        let chain_id = wallet.default_chain()?;
        let public_key = wallet.get(chain_id)?.key_pair.as_ref()?.public();
        Some(public_key.into())
    }

    async fn is_chain_present_in_wallet(&self, chain: ChainId) -> bool {
        self.get_wallet().get(chain).is_some()
    }

    async fn set_validator(&self, name: &str, port: usize, votes: usize) {
        let address = format!("{}:127.0.0.1:{}", self.network.external_short(), port);
        Self::run_command(
            self.run_with_storage()
                .await
                .arg("set-validator")
                .args(["--name", name])
                .args(["--address", &address])
                .args(["--votes", &votes.to_string()]),
        )
        .await;
    }

    async fn remove_validator(&self, name: &str) {
        Self::run_command(
            self.run_with_storage()
                .await
                .arg("remove-validator")
                .args(["--name", name]),
        )
        .await;
    }

    async fn keygen(&self) -> anyhow::Result<Owner> {
        let stdout = Self::run_command(self.run().await.arg("keygen")).await;
        Ok(Owner::from_str(stdout.trim())?)
    }

    async fn assign(&self, owner: Owner, effect_id: EffectId) -> anyhow::Result<ChainId> {
        let stdout = Self::run_command(
            self.run_with_storage()
                .await
                .arg("assign")
                .args(["--key", &owner.to_string()])
                .args(["--effect-id", &effect_id.to_string()]),
        )
        .await;

        let chain_id = ChainId::from_str(stdout.trim())?;

        Ok(chain_id)
    }

    async fn synchronize_balance(&self, chain_id: ChainId) {
        Self::run_command(
            self.run_with_storage()
                .await
                .arg("sync-balance")
                .arg(&chain_id.to_string()),
        )
        .await;
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

    fn add_server(&mut self, server: Child) {
        self.servers.push(server)
    }

    fn kill_server(&mut self, index: usize) {
        self.servers.remove(index);
    }

    fn assert_is_running(&mut self) {
        if let Some(status) = self.proxy.try_wait().unwrap() {
            assert!(status.success());
        }
        for child in &mut self.servers {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
            }
        }
    }
}

struct TestRunner {
    network: Network,
    tmp_dir: Rc<TempDir>,
    next_client_id: usize,
    num_initial_validators: usize,
    local_net: BTreeMap<usize, Validator>,
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        for validator in self.local_net.values_mut() {
            validator.assert_is_running();
        }
    }
}

impl TestRunner {
    fn new(network: Network, num_initial_validators: usize) -> Self {
        Self {
            tmp_dir: Rc::new(tempdir().unwrap()),
            network,
            next_client_id: 0,
            num_initial_validators,
            local_net: BTreeMap::new(),
        }
    }

    fn make_client(&mut self, network: Network) -> Client {
        let client = Client::new(self.tmp_dir.clone(), network, self.next_client_id);
        self.next_client_id += 1;
        client
    }

    async fn command_for_binary(&self, name: &'static str) -> Command {
        let path = cargo_build_binary(name).await;
        let mut command = Command::new(path);
        command
            .current_dir(&self.tmp_dir.path().canonicalize().unwrap())
            .kill_on_drop(true);
        command
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

    fn configuration_string(&self, server_number: usize) -> String {
        let n = server_number;
        let path = self
            .tmp_dir
            .path()
            .canonicalize()
            .unwrap()
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
        fs::write(&path, content).unwrap();
        path.into_os_string().into_string().unwrap()
    }

    async fn generate_initial_validator_config(&self) -> Vec<String> {
        let mut command = self.command_for_binary("server").await;
        command.arg("generate").arg("--validators");
        for i in 1..=self.num_initial_validators {
            command.arg(&self.configuration_string(i));
        }
        let output = command
            .args(["--committee", "committee.json"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .await
            .unwrap();
        assert!(output.status.success());
        let output_str = String::from_utf8_lossy(output.stdout.as_slice());
        output_str.split_whitespace().map(str::to_string).collect()
    }

    async fn generate_validator_config(&self, i: usize) -> String {
        let output = self
            .command_for_binary("server")
            .await
            .arg("generate")
            .arg("--validators")
            .arg(&self.configuration_string(i))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .await
            .unwrap();
        assert!(output.status.success());
        String::from_utf8_lossy(output.stdout.as_slice())
            .trim()
            .to_string()
    }

    async fn run_proxy(&self, i: usize) -> Child {
        let child = self
            .command_for_binary("proxy")
            .await
            .arg(format!("server_{}.json", i))
            .spawn()
            .unwrap();

        match self.network {
            Network::Grpc => {
                let port = Self::proxy_port(i);
                let nickname = format!("validator proxy {i}");
                Self::ensure_grpc_server_has_started(&nickname, port).await;
            }
            Network::Simple => {
                info!("Letting validator proxy {i} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        child
    }

    async fn ensure_grpc_server_has_started(nickname: &str, port: usize) {
        let connection = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_lazy();
        let mut client = HealthClient::new(connection);
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let result = client.check(HealthCheckRequest::default()).await;
            if result.is_ok() && result.unwrap().get_ref().status() == ServingStatus::Serving {
                info!("Successfully started {nickname}");
                return;
            } else {
                warn!("Waiting for {nickname} to start");
            }
        }
        panic!("Failed to start {nickname}");
    }

    async fn run_server(&self, i: usize, j: usize) -> Child {
        let mut command = self.command_for_binary("server").await;
        command.arg("run");
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--storage", &format!("rocksdb:server_{}_{}.db", i, j)])
            .args(["--server", &format!("server_{}.json", i)])
            .args(["--shard", &j.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn()
            .unwrap();

        match self.network {
            Network::Grpc => {
                let port = Self::shard_port(i, j);
                let nickname = format!("validator server {i}:{j}");
                Self::ensure_grpc_server_has_started(&nickname, port).await;
            }
            Network::Simple => {
                info!("Letting validator server {i}:{j} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        child
    }

    async fn run_local_net(&mut self) {
        self.start_validators(1..=self.num_initial_validators).await
    }

    fn kill_server(&mut self, i: usize, j: usize) {
        self.local_net.get_mut(&i).unwrap().kill_server(j);
    }

    fn remove_validator(&mut self, i: usize) {
        self.local_net.remove(&i).unwrap();
    }

    async fn start_server(&mut self, i: usize, j: usize) {
        let server = self.run_server(i, j).await;
        self.local_net.get_mut(&i).unwrap().add_server(server);
    }

    async fn start_validators(&mut self, validator_range: RangeInclusive<usize>) {
        for i in validator_range {
            let proxy = self.run_proxy(i).await;
            let mut validator = Validator::new(proxy);
            for j in 0..4 {
                let server = self.run_server(i, j).await;
                validator.add_server(server);
            }
            self.local_net.insert(i, validator);
        }
    }

    async fn build_example(&self, name: &str) -> (PathBuf, PathBuf) {
        let examples_dir = env::current_dir().unwrap().join("../examples/");
        self.build_application(examples_dir.join(name).as_path(), name, true)
            .await
    }

    async fn build_application(
        &self,
        path: &Path,
        name: &str,
        is_workspace: bool,
    ) -> (PathBuf, PathBuf) {
        assert!(Command::new("cargo")
            .current_dir(self.tmp_dir.path().canonicalize().unwrap())
            .arg("build")
            .arg("--release")
            .args(["--target", "wasm32-unknown-unknown"])
            .arg("--manifest-path")
            .arg(path.join("Cargo.toml"))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .success());

        let release_dir = match is_workspace {
            true => path.join("../target/wasm32-unknown-unknown/release"),
            false => path.join("target/wasm32-unknown-unknown/release"),
        };

        let contract = release_dir.join(format!("{}_contract.wasm", name.replace('-', "_")));
        let service = release_dir.join(format!("{}_service.wasm", name.replace('-', "_")));

        (contract, service)
    }
}

struct NodeService {
    chain_id: Option<ChainId>,
    port: u16,
    child: Child,
}

impl NodeService {
    fn assert_is_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            assert!(status.success());
        }
    }

    async fn process_inbox(&self) {
        self.query_node("mutation { processInbox }").await;
    }

    async fn make_application(&self, application_id: &str) -> Application {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let values = self.try_get_applications_uri().await;
            if let Some(link) = values.get(application_id) {
                return Application {
                    uri: link.to_string(),
                };
            }
            warn!(
                "Waiting for application {application_id:?} to be visible on chain {:?}",
                self.chain_id
            );
        }
        panic!("Could not find application URI: {application_id}");
    }

    async fn try_get_applications_uri(&self) -> HashMap<String, String> {
        let query = if let Some(chain_id) = self.chain_id {
            format!(
                "query {{ applications(chainId: \"{}\") {{ id link }}}}",
                chain_id
            )
        } else {
            "query { applications { id link }}".to_string()
        };
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

    async fn publish_bytecode(&self, contract: PathBuf, service: PathBuf) -> String {
        let contract_code = Bytecode::load_from_file(&contract).await.unwrap();
        let service_code = Bytecode::load_from_file(&service).await.unwrap();
        let query = format!(
            "mutation {{ publishBytecode(contract: {}, service: {}) }}",
            contract_code.to_value(),
            service_code.to_value(),
        );
        let data = self.query_node(&query).await;
        serde_json::from_value(data["publishBytecode"].clone()).unwrap()
    }

    async fn query_node(&self, query: &str) -> Value {
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
            panic!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                errors
            );
        }
        value["data"].clone()
    }

    async fn create_application(&self, bytecode_id: &str) -> String {
        let query = format!(
            "mutation {{ createApplication(\
                bytecodeId: \"{bytecode_id}\", \
                parameters: \"null\", \
                initializationArgument: \"null\", \
                requiredApplicationIds: []) \
            }}"
        );
        let data = self.query_node(&query).await;
        serde_json::from_value(data["createApplication"].clone()).unwrap()
    }

    async fn request_application(&self, application_id: &str) -> String {
        let query = format!("mutation {{ requestApplication(applicationId: {application_id}) }}");
        let data = self.query_node(&query).await;
        serde_json::from_value(data["requestApplication"].clone()).unwrap()
    }
}

struct Application {
    uri: String,
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl Application {
    async fn get_fungible_account_owner_amount(
        &self,
        account_owner: &fungible::AccountOwner,
    ) -> Amount {
        let query = format!(
            "query {{ accounts(accountOwner: {} ) }}",
            account_owner.to_value()
        );
        let response_body = self.query_application(&query).await;
        serde_json::from_value(response_body["accounts"].clone()).unwrap_or_default()
    }

    async fn assert_fungible_account_balances(
        &self,
        accounts: impl IntoIterator<Item = (fungible::AccountOwner, Amount)>,
    ) {
        for (account_owner, amount) in accounts {
            let value = self.get_fungible_account_owner_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn get_counter_value(&self) -> u64 {
        let data = self.query_application("query { value }").await;
        serde_json::from_value(data["value"].clone()).unwrap()
    }

    async fn query_application(&self, query: &str) -> Value {
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

    async fn increment_counter_value(&self, increment: u64) {
        let query = format!("mutation {{ increment(value: {})}}", increment);
        self.query_application(&query).await;
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 4);
    let client = runner.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    runner.generate_initial_validator_config().await;
    client.create_genesis_config().await;
    runner.run_local_net().await;
    let (contract, service) = runner.build_example("counter").await;

    let application_id = client
        .publish_and_create::<CounterAbi>(
            contract,
            service,
            &(),
            &original_counter_value,
            vec![],
            None,
        )
        .await;
    let mut node_service = client.run_node_service(None, None).await;

    let application = node_service.make_application(&application_id).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_counter_value(increment).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter_publish_create() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 4);
    let client = runner.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    runner.generate_initial_validator_config().await;
    client.create_genesis_config().await;
    runner.run_local_net().await;
    let (contract, service) = runner.build_example("counter").await;

    let bytecode_id = client.publish_bytecode(contract, service, None).await;
    let application_id = client
        .create_application::<CounterAbi>(bytecode_id, &original_counter_value, None)
        .await;
    let mut node_service = client.run_node_service(None, None).await;

    let application = node_service.make_application(&application_id).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_counter_value(increment).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let mut runner = TestRunner::new(Network::Grpc, 4);
    let client_1 = runner.make_client(Network::Grpc);
    let client_2 = runner.make_client(Network::Grpc);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await;
    client_1.create_genesis_config().await;
    client_2.wallet_init(&[]).await;

    // Start local network.
    runner.run_local_net().await;

    // Get some chain owned by Client 1.
    let chain_1 = *client_1.get_wallet().chain_ids().first().unwrap();

    // Generate a key for Client 2.
    let client_2_key = client_2.keygen().await.unwrap();

    // Open chain on behalf of Client 2.
    let (effect_id, chain_2) = client_1
        .open_chain(chain_1, Some(client_2_key))
        .await
        .unwrap();

    // Assign chain_2 to client_2_key.
    assert_eq!(
        chain_2,
        client_2.assign(client_2_key, effect_id).await.unwrap()
    );

    // Check initial balance of Chain 1.
    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "10.");

    // Transfer 5 units from Chain 1 to Chain 2.
    client_1.transfer("5", chain_1, chain_2).await;
    client_2.synchronize_balance(chain_2).await;

    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "5.");
    assert_eq!(client_2.query_balance(chain_2).await.unwrap(), "5.");

    // Transfer 2 units from Chain 2 to Chain 1.
    client_2.transfer("2", chain_2, chain_1).await;
    client_1.synchronize_balance(chain_1).await;

    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "7.");
    assert_eq!(client_2.query_balance(chain_2).await.unwrap(), "3.");
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration_grpc() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    test_reconfiguration(Network::Grpc).await;
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration_simple() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    test_reconfiguration(Network::Simple).await;
}

async fn test_reconfiguration(network: Network) {
    let mut runner = TestRunner::new(network, 4);
    let client = runner.make_client(network);
    let client_2 = runner.make_client(network);

    let servers = runner.generate_initial_validator_config().await;
    client.create_genesis_config().await;
    client_2.wallet_init(&[]).await;
    runner.run_local_net().await;

    let chain_1 = client.get_wallet().default_chain().unwrap();

    let (node_service_2, chain_2) = match network {
        Network::Grpc => {
            let chain_2 = client.open_and_assign(&client_2).await;
            let node_service_2 = client_2.run_node_service(chain_2, 8081).await;
            (Some(node_service_2), chain_2)
        }
        Network::Simple => {
            client
                .transfer("10", ChainId::root(9), ChainId::root(8))
                .await;
            (None, ChainId::root(9))
        }
    };

    client.query_validators(None).await;

    // Query balance for first and last user chain
    assert_eq!(client.query_balance(chain_1).await.unwrap(), "10.");
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "0.");

    // Transfer 3 units
    client.transfer("3", chain_1, chain_2).await;

    // Restart last server (dropping it kills the process)
    runner.kill_server(4, 3);
    runner.start_server(4, 3).await;

    // Query balances again
    assert_eq!(client.query_balance(chain_1).await.unwrap(), "7.");
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "3.");

    #[cfg(benchmark)]
    {
        // Launch local benchmark using all user chains
        client.benchmark(500).await;
    }

    // Create derived chain
    let (_, chain_3) = client.open_chain(chain_1, None).await.unwrap();

    // Inspect state of derived chain
    assert!(client.is_chain_present_in_wallet(chain_3).await);

    // Create configurations for two more validators
    let server_5 = runner.generate_validator_config(5).await;
    let server_6 = runner.generate_validator_config(6).await;

    // Start the validators
    runner.start_validators(5..=6).await;

    // Add validator 5
    client.set_validator(&server_5, 9500, 100).await;

    client.query_validators(None).await;
    client.query_validators(Some(chain_1)).await;

    // Add validator 6
    client.set_validator(&server_6, 9600, 100).await;

    // Remove validator 5
    client.remove_validator(&server_5).await;
    runner.remove_validator(5);

    client.query_validators(None).await;
    client.query_validators(Some(chain_1)).await;

    // Remove validators 1, 2, 3 and 4, so only 6 remains.
    for (i, server) in servers.into_iter().enumerate() {
        client.remove_validator(&server).await;
        runner.remove_validator(i + 1);
    }

    client.transfer("5", chain_1, chain_2).await;
    client.synchronize_balance(chain_2).await;
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "8.");

    if let Some(node_service_2) = node_service_2 {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = node_service_2
                .query_node("query { chain { executionState { system { balance } } } }")
                .await;
            if response["chain"]["executionState"]["system"]["balance"].as_str() == Some("8.") {
                return;
            }
        }
        panic!("Failed to receive new block");
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_social_user_pub_sub() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 4);
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await;
    client1.create_genesis_config().await;
    client2.wallet_init(&[]).await;

    // Start local network.
    runner.run_local_net().await;
    let (contract, service) = runner.build_example("social").await;

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await;

    let mut node_service1 = client1.run_node_service(chain1, 8080).await;
    let mut node_service2 = client2.run_node_service(chain2, 8081).await;

    let bytecode_id = node_service1.publish_bytecode(contract, service).await;
    node_service1.process_inbox().await;
    let application_id = node_service1.create_application(&bytecode_id).await;

    // Request the application so chain 2 has it, too.
    node_service2.request_application(&application_id).await;

    let app2 = node_service2.make_application(&application_id).await;
    let subscribe = format!("mutation {{ subscribe(chainId: \"{chain1}\") }}");
    let hash = app2.query_application(&subscribe).await;

    // The returned hash should now be the latest one.
    let query = format!("query {{ chain(chainId: \"{chain2}\") {{ tipState {{ blockHash }} }} }}");
    let response = node_service2.query_node(&query).await;
    assert_eq!(hash, response["chain"]["tipState"]["blockHash"]);

    let app1 = node_service1.make_application(&application_id).await;
    let post = "mutation { post(text: \"Linera Social is the new Mastodon!\") }";
    app1.query_application(post).await;

    // Instead of retrying, we could call `node_service1.process_inbox().await` here.
    // However, we prefer to test the notification system for a change.
    let query = "query { receivedPostsKeys(count: 5) { author, index } }";
    let expected_response = json!({ "receivedPostsKeys": [
        { "author": chain1, "index": 0 }
    ]});
    'success: {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = app2.query_application(query).await;
            if response == expected_response {
                info!("Confirmed post");
                break 'success;
            }
            warn!("Waiting to confirm post: {}", response);
        }
        panic!("Failed to confirm post");
    }

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_notification_stream() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 1);
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await;
    client1.create_genesis_config().await;
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain]).await;

    // Start local network.
    runner.run_local_net().await;

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let mut node_service2 = client2.run_node_service(chain, 8081).await;
    let response = node_service2
        .query_node("query { chain { tipState { nextBlockHeight } } }")
        .await;
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The validator has an outage and gets restarted!
    runner.remove_validator(1);
    runner.start_validators(1..=1).await;

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1.transfer("1", chain, ChainId::root(9)).await;
            tokio::time::sleep(Duration::from_secs(i)).await;
            height += 1;
            let response = node_service2
                .query_node("query { chain { tipState { nextBlockHeight } } }")
                .await;
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_fungible() {
    use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 4);
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    runner.generate_initial_validator_config().await;
    client1.create_genesis_config().await;
    client2.wallet_init(&[]).await;

    // Create initial server and client config.
    runner.run_local_net().await;
    let (contract, service) = runner.build_example("fungible").await;

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await;

    // The players
    let owner1 = client1.get_owner().unwrap();
    let account_owner1 = AccountOwner::User(owner1);
    let owner2 = client2.get_owner().unwrap();
    let account_owner2 = AccountOwner::User(owner2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi>(contract, service, &(), &state, vec![], None)
        .await;

    let mut node_service1 = client1.run_node_service(chain1, 8080).await;
    let mut node_service2 = client2.run_node_service(chain2, 8081).await;

    let app1 = node_service1.make_application(&application_id).await;
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Transferring
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ transfer(owner: {}, amount: \"{}\", targetAccount: {}) }}",
        account_owner1.to_value(),
        amount_transfer,
        destination.to_value(),
    );
    app1.query_application(&query).await;

    // Checking the final values on chain1 and chain2.
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2 = node_service2.make_application(&application_id).await;

    app2.assert_fungible_account_balances(BTreeMap::from([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::ONE),
    ]))
    .await;

    // Claiming more money from chain1 to chain2.
    let source = Account {
        chain_id: chain1,
        owner: account_owner2,
    };
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer: Amount = Amount::from_tokens(2);
    let query = format!(
        "mutation {{ claim(sourceAccount: {}, amount: \"{}\", targetAccount: {}) }}",
        source.to_value(),
        amount_transfer,
        destination.to_value()
    );
    app2.query_application(&query).await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox().await;
    node_service2.process_inbox().await;

    // Checking the final value
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::ZERO),
    ])
    .await;
    app2.assert_fungible_account_balances([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::from_tokens(3)),
    ])
    .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_crowd_funding() {
    use crowd_funding::{CrowdFundingAbi, InitializationArgument};
    use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 4);
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    runner.generate_initial_validator_config().await;
    client1.create_genesis_config().await;
    client2.wallet_init(&[]).await;

    // Create initial server and client config.
    runner.run_local_net().await;
    let (contract_fungible, service_fungible) = runner.build_example("fungible").await;

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await;

    // The players
    let owner1 = client1.get_owner().unwrap();
    let account_owner1 = AccountOwner::User(owner1);
    let owner2 = client2.get_owner().unwrap();
    let account_owner2 = AccountOwner::User(owner2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([(account_owner1, Amount::from_tokens(6))]);
    let state_fungible = InitialState { accounts };

    // Setting up the application fungible
    let application_id_fungible = client1
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible,
            service_fungible,
            &(),
            &state_fungible,
            vec![],
            None,
        )
        .await;

    // Setting up the application crowd funding
    let deadline = Timestamp::from(std::u64::MAX);
    let target = Amount::ONE;
    let state_crowd = InitializationArgument {
        owner: account_owner1,
        deadline,
        target,
    };
    let (contract_crowd, service_crowd) = runner.build_example("crowd-funding").await;
    let application_id_crowd = client1
        .publish_and_create::<CrowdFundingAbi>(
            contract_crowd,
            service_crowd,
            // TODO(#723): This hack will disappear soon.
            &application_id_fungible
                .parse::<ApplicationId>()
                .unwrap()
                .with_abi(),
            &state_crowd,
            vec![application_id_fungible.clone()],
            None,
        )
        .await;

    let mut node_service1 = client1.run_node_service(chain1, 8080).await;
    let mut node_service2 = client2.run_node_service(chain2, 8081).await;

    let app_fungible1 = node_service1
        .make_application(&application_id_fungible)
        .await;

    let app_crowd1 = node_service1.make_application(&application_id_crowd).await;

    // Transferring tokens to user2 on chain2
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ transfer(owner: {}, amount: \"{}\", targetAccount: {}) }}",
        account_owner1.to_value(),
        amount_transfer,
        destination.to_value(),
    );
    app_fungible1.query_application(&query).await;

    // Register the campaign on chain2.
    node_service2
        .request_application(&application_id_crowd)
        .await;

    let app_crowd2 = node_service2.make_application(&application_id_crowd).await;

    // Transferring
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ pledgeWithTransfer(owner: {}, amount: \"{}\") }}",
        account_owner2.to_value(),
        amount_transfer,
    );
    app_crowd2.query_application(&query).await;

    // Make sure that the pledge is processed fast enough by client1.
    node_service1.process_inbox().await;

    // Ending the campaign.
    app_crowd1.query_application("mutation { collect }").await;

    // The rich gets their money back.
    app_fungible1
        .assert_fungible_account_balances([(account_owner1, Amount::from_tokens(6))])
        .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_project_new() {
    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 0);
    let client = runner.make_client(network);

    let tmp_dir = client.project_new("init-test").await;
    let project_dir = tmp_dir.path().join("init-test");
    runner
        .build_application(project_dir.as_path(), "init-test", false)
        .await;
}

#[test_log::test(tokio::test)]
async fn test_project_test() {
    let network = Network::Grpc;
    let mut runner = TestRunner::new(network, 0);
    let client = runner.make_client(network);
    client
        .test_project(&PathBuf::from_str("../examples/counter").unwrap())
        .await;
}
