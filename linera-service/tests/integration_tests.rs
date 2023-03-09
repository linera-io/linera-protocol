// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "aws")]
use linera_views::test_utils::LocalStackTestContext;
use serde_json::{json, Value};
use std::{
    env,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::Mutex,
    time::Duration,
};
use tempfile::{tempdir, TempDir};
use tokio::process::Child;

/// A static lock to prevent README examples from running in parallel.
static README_GUARD: Mutex<()> = Mutex::new(());

#[test]
fn test_examples_in_readme() -> std::io::Result<()> {
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

#[test]
fn test_examples_in_readme_grpc() -> std::io::Result<()> {
    let _guard = README_GUARD.lock().unwrap();

    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);
    let mut quote = quotes.pop().unwrap();

    quote = quote.replace("tcp", "grpc");
    quote = quote.replace("udp", "grpc");

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

fn cargo_run(tmp_dir: &TempDir) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("cargo");
    command
        .current_dir(tmp_dir.path().canonicalize().unwrap())
        .kill_on_drop(true)
        .arg("run")
        .arg("--manifest-path")
        .arg(env::current_dir().unwrap().join("Cargo.toml"));
    command
}

async fn generate_server_config(tmp_dir: &TempDir) {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("server")
        .arg("generate")
        .arg("--validators")
        .arg("server_1.json:grpc:127.0.0.1:9100:grpc:127.0.0.1:10100:127.0.0.1:9101:127.0.0.1:9102:127.0.0.1:9103:127.0.0.1:9104")
        .arg("server_2.json:grpc:127.0.0.1:9200:grpc:127.0.0.1:10200:127.0.0.1:9201:127.0.0.1:9202:127.0.0.1:9203:127.0.0.1:9204")
        .arg("server_3.json:grpc:127.0.0.1:9300:grpc:127.0.0.1:10300:127.0.0.1:9301:127.0.0.1:9302:127.0.0.1:9303:127.0.0.1:9304")
        .arg("server_4.json:grpc:127.0.0.1:9400:grpc:127.0.0.1:10400:127.0.0.1:9401:127.0.0.1:9402:127.0.0.1:9403:127.0.0.1:9404")
        .arg("--committee")
        .arg("committee.json")
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap();
}

async fn generate_client_config(tmp_dir: &TempDir) {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("client")
        .arg("--")
        .arg("--wallet")
        .arg("wallet.json")
        .arg("--genesis")
        .arg("genesis.json")
        .arg("create_genesis_config")
        .arg("10")
        .arg("--initial-funding")
        .arg("10")
        .arg("--committee")
        .arg("committee.json")
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap();
}

fn run_proxy(i: usize, tmp_dir: &TempDir) -> Child {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("proxy")
        .arg("--")
        .arg(format!("server_{}.json", i))
        .spawn()
        .unwrap()
}

fn run_server(i: usize, j: usize, tmp_dir: &TempDir) -> Child {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("server")
        .arg("run")
        .arg("--storage")
        .arg(format!("rocksdb:server_{}_{}.db", i, j))
        .arg("--server")
        .arg(format!("server_{}.json", i))
        .arg("--shard")
        .arg(j.to_string())
        .arg("--genesis")
        .arg("genesis.json")
        .spawn()
        .unwrap()
}

fn run_local_net(tmp_dir: &TempDir) -> Vec<Child> {
    let mut processes = vec![];
    for i in 1..5 {
        let process = run_proxy(i, tmp_dir);
        processes.push(process);
        for j in 0..4 {
            let process = run_server(i, j, tmp_dir);
            processes.push(process);
        }
    }
    processes
}

async fn build_application(tmp_dir: &TempDir) -> (PathBuf, PathBuf) {
    let examples_dir = env::current_dir().unwrap().join("../linera-examples/");
    tokio::process::Command::new("cargo")
        .current_dir(tmp_dir.path().canonicalize().unwrap())
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-unknown-unknown")
        .arg("--manifest-path")
        .arg(examples_dir.join("counter-graphql/Cargo.toml"))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap();

    let contract =
        examples_dir.join("target/wasm32-unknown-unknown/release/counter_graphql_contract.wasm");
    let service =
        examples_dir.join("target/wasm32-unknown-unknown/release/counter_graphql_service.wasm");

    (contract, service)
}

async fn publish_application(
    contract: PathBuf,
    service: PathBuf,
    tmp_dir: &TempDir,
    counter_value: u64,
) {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("client")
        .arg("--")
        .arg("--storage")
        .arg("rocksdb:client.db")
        .arg("--wallet")
        .arg("wallet.json")
        .arg("--genesis")
        .arg("genesis.json")
        .arg("--max-pending-messages")
        .arg("10000")
        .arg("publish")
        .arg(contract)
        .arg(service)
        .arg(counter_value.to_string())
        .spawn()
        .unwrap()
        .wait()
        .await
        .unwrap();
}

async fn run_node_service(tmp_dir: &TempDir) -> Child {
    let mut cargo_run = cargo_run(tmp_dir);
    cargo_run
        .arg("--bin")
        .arg("client")
        .arg("--")
        .arg("--storage")
        .arg("rocksdb:client.db")
        .arg("--wallet")
        .arg("wallet.json")
        .arg("--genesis")
        .arg("genesis.json")
        .arg("--max-pending-messages")
        .arg("10000")
        .arg("service")
        .spawn()
        .unwrap()
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

    let tmp_dir = tempdir().unwrap();
    let original_counter_value = 35;
    let increment = 5;

    generate_server_config(&tmp_dir).await;
    generate_client_config(&tmp_dir).await;
    let _local_net = run_local_net(&tmp_dir);
    let (contract, service) = build_application(&tmp_dir).await;

    // wait for net to start
    tokio::time::sleep(Duration::from_millis(10_000)).await;

    publish_application(contract, service, &tmp_dir, original_counter_value).await;
    let _node_service = run_node_service(&tmp_dir).await;

    // wait for node service to start
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    let application_uri = get_application_uri().await;

    let counter_value = get_counter_value(&application_uri).await;
    assert_eq!(counter_value, original_counter_value);

    increment_counter_value(&application_uri, increment).await;

    let counter_value = get_counter_value(&application_uri).await;
    assert_eq!(counter_value, original_counter_value + increment);
}
