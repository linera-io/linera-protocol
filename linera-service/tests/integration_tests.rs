// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::data_types::ChainId;
use linera_service::config::WalletState;
#[cfg(feature = "aws")]
use linera_views::test_utils::LocalStackTestContext;
use serde_json::{json, Value};
use std::{
    env,
    io::Write,
    ops::Range,
    path::PathBuf,
    process::{Command, Stdio},
    rc::Rc,
    str::FromStr,
    sync::Mutex,
    time::Duration,
};
use tempfile::{tempdir, TempDir};
use tokio::process::Child;

/// A static lock to prevent README examples from running in parallel.
static README_GUARD: Mutex<()> = Mutex::new(());

#[test]
fn test_examples_in_readme_simple() -> std::io::Result<()> {
    let _guard = README_GUARD.lock().unwrap();

    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);
    let quote = quotes.pop().unwrap();

    let mut test_script = std::fs::File::create(dir.path().join("test.sh"))?;
    write!(&mut test_script, "{}", quote)?;

    let status = Command::new("bash")
        .current_dir("..") // root of the repo
        .arg("-e")
        .arg("-x")
        .arg(dir.path().join("test.sh"))
        .status()?;
    assert!(status.success());
    Ok(())
}

#[allow(clippy::while_let_on_iterator)]
fn get_bash_quotes<R>(reader: R) -> std::io::Result<Vec<String>>
where
    R: std::io::BufRead,
{
    let mut result = Vec::new();
    let mut lines = reader.lines();

    while let Some(line) = lines.next() {
        let line = line?;
        if line.starts_with("```bash") {
            let mut quote = String::new();
            while let Some(line) = lines.next() {
                let line = line?;
                if line.starts_with("```") {
                    break;
                }
                quote += &line;
                quote += "\n";
            }
            result.push(quote);
        }
    }

    Ok(result)
}

#[cfg(feature = "aws")]
mod aws_test {
    use super::*;

    const ROCKSDB_STORAGE: &str = "--storage rocksdb:server_\"$I\"_\"$J\".db";
    const DYNAMO_DB_STORAGE: &str = "--storage dynamodb:server-\"$I\":localstack";

    const BUILD: &str = "cargo build";
    const AWS_BUILD: &str = "cargo build --features aws";

    #[tokio::test]
    async fn test_examples_in_readme_with_dynamo_db() -> anyhow::Result<()> {
        let _localstack_guard = LocalStackTestContext::new().await?;
        let dir = tempdir().unwrap();
        let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
        let mut quotes = get_bash_quotes(file)?;
        // Check that we have the expected number of examples starting with "```bash".
        assert_eq!(quotes.len(), 1);
        let quote = quotes.pop().unwrap();
        assert_eq!(quote.matches(ROCKSDB_STORAGE).count(), 3);
        let quote = quote.replace(ROCKSDB_STORAGE, DYNAMO_DB_STORAGE);
        let quote = quote.replace(BUILD, AWS_BUILD);

        let mut test_script = std::fs::File::create(dir.path().join("test.sh"))?;
        write!(&mut test_script, "{}", quote)?;

        let status = Command::new("bash")
            .current_dir("..") // root of the repo
            .arg("-e")
            .arg("-x")
            .arg(dir.path().join("test.sh"))
            .status()?;
        assert!(status.success());
        Ok(())
    }
}

#[derive(Copy, Clone)]
enum Network {
    Grpc,
    Simple,
}

impl Network {
    fn internal(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Simple => "udp",
        }
    }

    fn external(&self) -> &'static str {
        match self {
            Network::Grpc => "grpc",
            Network::Simple => "tcp",
        }
    }
}

struct Client {
    tmp_dir: Rc<TempDir>,
    storage: String,
    wallet: String,
    genesis: String,
    max_pending_messages: usize,
    network: Network,
}

impl Client {
    fn new(tmp_dir: Rc<TempDir>, network: Network) -> Self {
        Self {
            tmp_dir,
            storage: "rocksdb:client.db".to_string(),
            wallet: "wallet.json".to_string(),
            genesis: "genesis.json".to_string(),
            max_pending_messages: 10_000,
            network,
        }
    }

    fn client_run(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("cargo");
        command
            .current_dir(&self.tmp_dir.path().canonicalize().unwrap())
            .kill_on_drop(true)
            .env("RUST_LOG", "ERROR")
            .arg("run")
            .arg("--manifest-path")
            .arg(env::current_dir().unwrap().join("Cargo.toml"))
            .args(["--bin", "client"])
            .arg("--")
            .args(["--wallet", &self.wallet])
            .args(["--genesis", &self.genesis]);
        command
    }

    fn client_run_with_storage(&self) -> tokio::process::Command {
        let mut command = self.client_run();
        command
            .args(["--storage", &self.storage.to_string()])
            .args([
                "--max-pending-messages",
                &self.max_pending_messages.to_string(),
            ]);
        command
    }

    async fn generate_client_config(&self) {
        self.client_run()
            .args(["create_genesis_config", "10"])
            .args(["--initial-funding", "10"])
            .args(["--committee", "committee.json"])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn publish_application(&self, contract: PathBuf, service: PathBuf, arg: u64) {
        self.client_run_with_storage()
            .arg("publish")
            .args([contract, service])
            .arg(arg.to_string())
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn run_node_service(&self) -> Child {
        self.client_run_with_storage()
            .arg("service")
            .spawn()
            .unwrap()
    }

    async fn query_validators(&self, chain_id: Option<ChainId>) {
        let mut command = self.client_run_with_storage();
        command.arg("query_validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        command.spawn().unwrap().wait().await.unwrap();
    }

    async fn query_balance(&self, chain_id: ChainId) -> anyhow::Result<usize> {
        let output = self
            .client_run_with_storage()
            .arg("query_balance")
            .arg(&chain_id.to_string())
            .stdout(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        let amount = String::from_utf8_lossy(output.stdout.as_slice()).to_string();
        Ok(amount.trim().parse()?)
    }

    async fn transfer(&self, amount: usize, from: ChainId, to: ChainId) {
        self.client_run_with_storage()
            .arg("transfer")
            .arg(&amount.to_string())
            .args(["--from", &from.to_string()])
            .args(["--to", &to.to_string()])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn benchmark(&self, max_in_flight: usize) {
        self.client_run_with_storage()
            .arg("benchmark")
            .args(["--max-in-flight", &max_in_flight.to_string()])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn open_chain(&self, from: ChainId) -> anyhow::Result<ChainId> {
        let output = self
            .client_run_with_storage()
            .arg("open_chain")
            .args(["--from", &from.to_string()])
            .stdout(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        Ok(ChainId::from_str(
            String::from_utf8_lossy(output.stdout.as_slice()).trim(),
        )?)
    }

    async fn check_for_chain_in_wallet(&self, chain: ChainId) -> bool {
        let wallet =
            WalletState::read_or_create(self.tmp_dir.path().join(&self.wallet).as_path()).unwrap();
        wallet.get(chain).is_some()
    }

    async fn set_validator(&self, name: &str, port: usize, votes: usize) {
        let address = format!("{}:127.0.0.1:{}", self.network.external(), port);
        self.client_run_with_storage()
            .arg("set_validator")
            .args(["--name", name])
            .args(["--address", &address])
            .args(["--votes", &votes.to_string()])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn remove_validator(&self, name: &str) {
        self.client_run_with_storage()
            .arg("remove_validator")
            .args(["--name", name])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
}

struct Validator {
    _proxy: Child,
    servers: Vec<Child>,
}

impl Validator {
    fn new(proxy: Child) -> Self {
        Self {
            _proxy: proxy,
            servers: vec![],
        }
    }

    fn add_server(&mut self, server: Child) {
        self.servers.push(server)
    }

    fn kill_server(&mut self, index: usize) {
        self.servers.remove(index);
    }
}

struct TestRunner {
    tmp_dir: Rc<TempDir>,
    network: Network,
}

impl TestRunner {
    fn new(network: Network) -> Self {
        Self {
            tmp_dir: Rc::new(tempdir().unwrap()),
            network,
        }
    }

    fn tmp_dir(&self) -> Rc<TempDir> {
        self.tmp_dir.clone()
    }

    fn cargo_run(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("cargo");
        command
            .current_dir(&self.tmp_dir.path().canonicalize().unwrap())
            .kill_on_drop(true)
            .arg("run")
            .arg("--manifest-path")
            .arg(env::current_dir().unwrap().join("Cargo.toml"));
        command
    }

    async fn generate_initial_server_config(&self) {
        let server_1 = format!("server_1.json:{}:127.0.0.1:9100:{}:127.0.0.1:10100:127.0.0.1:9101:127.0.0.1:9102:127.0.0.1:9103:127.0.0.1:9104", self.network.external(), self.network.internal());
        let server_2 = format!("server_2.json:{}:127.0.0.1:9200:{}:127.0.0.1:10200:127.0.0.1:9201:127.0.0.1:9202:127.0.0.1:9203:127.0.0.1:9204", self.network.external(), self.network.internal());
        let server_3 = format!("server_3.json:{}:127.0.0.1:9300:{}:127.0.0.1:10300:127.0.0.1:9301:127.0.0.1:9302:127.0.0.1:9303:127.0.0.1:9304", self.network.external(), self.network.internal());
        let server_4 = format!("server_4.json:{}:127.0.0.1:9400:{}:127.0.0.1:10400:127.0.0.1:9401:127.0.0.1:9402:127.0.0.1:9403:127.0.0.1:9404", self.network.external(), self.network.internal());
        self.cargo_run()
            .args(["--bin", "server"])
            .arg("generate")
            .arg("--validators")
            .arg(&server_1)
            .arg(&server_2)
            .arg(&server_3)
            .arg(&server_4)
            .args(["--committee", "committee.json"])
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn generate_server_config(&self, server_number: usize) -> anyhow::Result<String> {
        let config = format!("server_X.json:{}:127.0.0.1:9X00:{}:127.0.0.1:10X00:127.0.0.1:9X01:127.0.0.1:9X02:127.0.0.1:9X03:127.0.0.1:9X04", self.network.external(), self.network.internal());
        let output = self
            .cargo_run()
            .env("RUST_LOG", "ERROR")
            .args(["--bin", "server"])
            .arg("generate")
            .arg("--validators")
            .arg(&config.replace('X', &server_number.to_string()))
            .stdout(Stdio::piped())
            .spawn()?
            .wait_with_output()
            .await?;
        Ok(String::from_utf8_lossy(output.stdout.as_slice())
            .to_string()
            .trim()
            .to_string())
    }

    fn run_proxy(&self, i: usize) -> Child {
        self.cargo_run()
            .args(["--bin", "proxy"])
            .arg("--")
            .arg(format!("server_{}.json", i))
            .spawn()
            .unwrap()
    }

    fn run_server(&self, i: usize, j: usize) -> Child {
        self.cargo_run()
            .args(["--bin", "server"])
            .arg("run")
            .args(["--storage", &format!("rocksdb:server_{}_{}.db", i, j)])
            .args(["--server", &format!("server_{}.json", i)])
            .args(["--shard", &j.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn()
            .unwrap()
    }

    fn run_local_net(&self) -> Vec<Validator> {
        self.start_validators(1..5)
    }

    fn start_validators(&self, validator_range: Range<usize>) -> Vec<Validator> {
        let mut validators = vec![];
        for i in validator_range {
            let mut validator = Validator::new(self.run_proxy(i));
            for j in 0..4 {
                validator.add_server(self.run_server(i, j));
            }
            validators.push(validator);
        }
        validators
    }

    async fn build_application(&self) -> (PathBuf, PathBuf) {
        let examples_dir = env::current_dir().unwrap().join("../linera-examples/");
        tokio::process::Command::new("cargo")
            .current_dir(self.tmp_dir.path().canonicalize().unwrap())
            .arg("build")
            .arg("--release")
            .args(["--target", "wasm32-unknown-unknown"])
            .arg("--manifest-path")
            .arg(examples_dir.join("counter-graphql/Cargo.toml"))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();

        let contract = examples_dir
            .join("target/wasm32-unknown-unknown/release/counter_graphql_contract.wasm");
        let service =
            examples_dir.join("target/wasm32-unknown-unknown/release/counter_graphql_service.wasm");

        (contract, service)
    }
}

async fn get_application_uri() -> String {
    let query = json!({ "query": "query {  applications {    link    }}" });
    let client = reqwest::Client::new();
    let res = client
        .post("http://localhost:8080/")
        .json(&query)
        .send()
        .await
        .unwrap();
    let response_body: Value = res.json().await.unwrap();
    let application_uri = response_body
        .get("data")
        .unwrap()
        .get("applications")
        .unwrap()
        .as_array()
        .unwrap()
        .get(0)
        .unwrap()
        .get("link")
        .unwrap();
    application_uri.as_str().unwrap().to_string()
}

async fn get_counter_value(application_uri: &str) -> u64 {
    let query = json!({ "query": "query { value }" });
    let client = reqwest::Client::new();
    let res = client
        .post(application_uri)
        .json(&query)
        .send()
        .await
        .unwrap();
    let response_body: Value = res.json().await.unwrap();
    response_body
        .get("data")
        .unwrap()
        .get("value")
        .unwrap()
        .as_u64()
        .unwrap()
}

async fn increment_counter_value(application_uri: &str, increment: u64) {
    let query_string = format!(
        "mutation {{  executeOperation(operation: {{ increment: {} }})}}",
        increment
    );
    let query = json!({ "query": query_string });
    let client = reqwest::Client::new();
    client
        .post(application_uri)
        .json(&query)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn end_to_end() {
    let _guard = README_GUARD.lock().unwrap();

    let network = Network::Grpc;
    let runner = TestRunner::new(network);
    let client = Client::new(runner.tmp_dir(), network);

    let original_counter_value = 35;
    let increment = 5;

    runner.generate_initial_server_config().await;
    client.generate_client_config().await;
    let _local_net = runner.run_local_net();
    let (contract, service) = runner.build_application().await;

    // wait for net to start
    tokio::time::sleep(Duration::from_millis(10_000)).await;

    client
        .publish_application(contract, service, original_counter_value)
        .await;
    let _node_service = client.run_node_service().await;

    // wait for node service to start
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    let application_uri = get_application_uri().await;

    let counter_value = get_counter_value(&application_uri).await;
    assert_eq!(counter_value, original_counter_value);

    increment_counter_value(&application_uri, increment).await;

    let counter_value = get_counter_value(&application_uri).await;
    assert_eq!(counter_value, original_counter_value + increment);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn reconfiguration_test_grpc() {
    let _guard = README_GUARD.lock().unwrap();
    test_reconfiguration(Network::Grpc).await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn reconfiguration_test_simple() {
    let _guard = README_GUARD.lock().unwrap();
    test_reconfiguration(Network::Simple).await;
}

async fn test_reconfiguration(network: Network) {
    let runner = TestRunner::new(network);
    let client = Client::new(runner.tmp_dir(), network);

    runner.generate_initial_server_config().await;
    client.generate_client_config().await;
    let mut local_net = runner.run_local_net();

    tokio::time::sleep(Duration::from_millis(5_000)).await;

    client.query_validators(None).await;

    // Query balance for first and last user chain
    let chain_1 =
        ChainId::from_str("91c7b394ef500cd000e365807b770d5b76a6e8c9c2f2af8e58c205e521b5f646")
            .unwrap();
    let chain_2 =
        ChainId::from_str("170883d704512b1682064639bdda0aab27756727af8e0dc5732bae70b2e15997")
            .unwrap();
    assert_eq!(client.query_balance(chain_1).await.unwrap(), 10);
    assert_eq!(client.query_balance(chain_2).await.unwrap(), 10);

    // Transfer 10 units then 5 back
    client.transfer(10, chain_1, chain_2).await;
    client.transfer(5, chain_2, chain_1).await;

    // Restart last server (dropping it kills the process)
    let validator_4 = local_net.get_mut(3).unwrap();
    validator_4.kill_server(3);
    validator_4.add_server(runner.run_server(4, 3));
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // Query balances again
    assert_eq!(client.query_balance(chain_1).await.unwrap(), 5);
    assert_eq!(client.query_balance(chain_2).await.unwrap(), 15);

    // Launch local benchmark using all user chains
    client.benchmark(500).await;

    // Create derived chain
    let chain_3 = client.open_chain(chain_1).await.unwrap();

    // Inspect state of derived chain
    assert!(client.check_for_chain_in_wallet(chain_3).await);

    // Create configurations for two more validators
    let server_5 = runner.generate_server_config(5).await.unwrap();
    let server_6 = runner.generate_server_config(6).await.unwrap();

    // Start the validators
    local_net.extend(runner.start_validators(5..7));

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // Add validator 5
    client.set_validator(&server_5, 9500, 100).await;

    assert_eq!(client.query_balance(chain_1).await.unwrap(), 5);
    client.query_validators(None).await;
    client.query_validators(Some(chain_1)).await;

    // Add validator 6
    client.set_validator(&server_6, 9600, 100).await;

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // Remove validator 5
    client.remove_validator(&server_5).await;
    local_net.remove(4);

    assert_eq!(client.query_balance(chain_1).await.unwrap(), 5);
    client.query_validators(None).await;
    client.query_validators(Some(chain_1)).await;
}
