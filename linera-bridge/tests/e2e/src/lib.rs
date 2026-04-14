// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for linera-bridge end-to-end tests that require
//! `testcontainers` with docker-compose support.

use std::process::Command;

use alloy::primitives::Address;
use testcontainers::{
    compose::DockerCompose,
    core::{CmdWaitFor, ExecCommand},
};

/// Path inside the container where `linera net up --path` stores wallet files.
/// Must match the `LINERA_NET_PATH` default in docker-compose.bridge-test.yml.
pub const WALLET_DIR: &str = "/tmp/wallet";

/// Extra wallet index (created by copying wallet_0).
/// Uses separate client storage to avoid RocksDB lock conflicts with the faucet.
pub const EXTRA_WALLET_ID: u32 = 1;

/// Anvil account 0 private key.
pub const ANVIL_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

/// Anvil account 0 address (derived from the private key above).
const ANVIL_DEPLOYER: &str = "f39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

/// Computes the deterministic CREATE address for a deployer at a given nonce.
/// Uses `keccak256(rlp([sender, nonce]))[12..]`.
pub fn create_address(deployer: Address, nonce: u64) -> Address {
    deployer.create(nonce)
}

/// Returns the LightClient contract address, assuming it is the first contract
/// deployed by Anvil account 0 (nonce 0).
pub fn light_client_address() -> Address {
    let deployer: Address = ANVIL_DEPLOYER
        .parse()
        .expect("valid anvil deployer address");
    create_address(deployer, 0)
}

/// Returns the path to the compose file relative to this crate's manifest dir.
pub fn compose_file_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("docker/docker-compose.bridge-test.yml")
}

/// Dumps docker compose logs for debugging failures.
pub fn dump_compose_logs(project_name: &str, compose_file: &std::path::Path) {
    let output = Command::new("docker")
        .args(["compose", "-p", project_name, "-f"])
        .arg(compose_file)
        .args(["logs", "--tail", "100"])
        .output();

    if let Ok(output) = output {
        tracing::info!(
            stdout=%String::from_utf8_lossy(&output.stdout),
            stderr=%String::from_utf8_lossy(&output.stderr),
            "Docker compose logs"
        );
    }
}

/// Executes a shell command inside a docker compose service, panicking on failure.
pub async fn exec_ok(
    compose: &DockerCompose,
    service_name: &str,
    cmd: &str,
    project_name: &str,
    compose_file: &std::path::Path,
) {
    exec_output(compose, service_name, cmd, project_name, compose_file).await;
}

/// Executes a shell command inside a docker compose service and returns stdout.
/// Panics on non-zero exit code.
pub async fn exec_output(
    compose: &DockerCompose,
    service_name: &str,
    cmd: &str,
    project_name: &str,
    compose_file: &std::path::Path,
) -> String {
    let service = compose
        .service(service_name)
        .unwrap_or_else(|| panic!("{service_name} service"));
    let result = service
        .exec(
            ExecCommand::new(["sh", "-c", &format!("{cmd} 2>&1")])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await;

    match result {
        Ok(mut r) => {
            let exit_code: Option<i64> = r.exit_code().await.unwrap_or(Some(-1));
            let stdout_bytes: Vec<u8> = r.stdout_to_vec().await.unwrap_or_default();
            let stdout = String::from_utf8_lossy(&stdout_bytes).to_string();
            tracing::info!(?exit_code, %stdout, "Exec output");
            if exit_code != Some(0) {
                dump_compose_logs(project_name, compose_file);
                panic!("exec failed (exit {exit_code:?}):\n{stdout}");
            }
            stdout
        }
        Err(e) => {
            dump_compose_logs(project_name, compose_file);
            panic!("Failed to exec command: {e}");
        }
    }
}

/// Environment variables prefix for using the extra wallet copy.
pub fn extra_wallet_env() -> String {
    format!(
        "LINERA_WALLET={WALLET_DIR}/wallet_{EXTRA_WALLET_ID}.json \
         LINERA_KEYSTORE={WALLET_DIR}/keystore_{EXTRA_WALLET_ID}.json \
         LINERA_STORAGE=rocksdb:{WALLET_DIR}/client_{EXTRA_WALLET_ID}.db"
    )
}

/// Creates a copy of wallet_0 with separate client storage so we can run
/// CLI commands without conflicting with the faucet's RocksDB lock.
pub async fn create_extra_wallet(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
) {
    tracing::info!("Creating extra wallet copy...");
    exec_ok(
        compose,
        "linera-network",
        &format!(
            "cp {WALLET_DIR}/wallet_0.json {WALLET_DIR}/wallet_{EXTRA_WALLET_ID}.json && \
             cp {WALLET_DIR}/keystore_0.json {WALLET_DIR}/keystore_{EXTRA_WALLET_ID}.json && \
             cp -r {WALLET_DIR}/client_0.db {WALLET_DIR}/client_{EXTRA_WALLET_ID}.db"
        ),
        project_name,
        compose_file,
    )
    .await;
}

/// Parse a "Deployed to: 0x..." address from `forge create` output.
pub fn parse_deployed_address(output: &str) -> anyhow::Result<Address> {
    for line in output.lines() {
        if let Some(addr) = line.strip_prefix("Deployed to: ") {
            return Ok(addr.trim().parse()?);
        }
    }
    anyhow::bail!("Could not find 'Deployed to:' in forge output:\n{output}");
}

/// Queries the evm-bridge app to check whether a deposit has been processed.
/// Mirrors `linera_bridge::monitor::query_deposit_processed` for use in tests
/// without enabling the `relay` feature.
pub async fn query_deposit_processed<E: linera_core::environment::Environment>(
    chain_client: &linera_core::client::ChainClient<E>,
    bridge_app_id: linera_base::identifiers::ApplicationId,
    deposit_key: &linera_bridge::proof::DepositKey,
) -> anyhow::Result<bool> {
    use linera_execution::{Query, QueryResponse};

    #[derive(serde::Serialize)]
    struct GqlRequest {
        query: String,
    }

    let hash_hex = format!("0x{}", alloy::primitives::hex::encode(deposit_key.hash()));
    let gql = format!(r#"{{ isDepositProcessed(hash: "{hash_hex}") }}"#);
    let query = Query::user_without_abi(bridge_app_id, &GqlRequest { query: gql })?;
    let (outcome, _) = chain_client.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    Ok(response["data"]["isDepositProcessed"].as_bool() == Some(true))
}

/// Waits for the `bridge-init` container to deploy the LightClient contract.
/// Must be called before deploying any test contracts to avoid a nonce race
/// (bridge-init and the test both use Anvil account 0).
pub async fn wait_for_light_client(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
) {
    tracing::info!("Waiting for LightClient deployment (bridge-init)...");
    for attempt in 0..60 {
        let result = exec_output(
            compose,
            "foundry-tools",
            &format!(
                "cast code {} --rpc-url http://anvil:8545",
                light_client_address()
            ),
            project_name,
            compose_file,
        )
        .await;
        if result.trim() != "0x" && !result.trim().is_empty() {
            tracing::info!(attempt, "LightClient deployed");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    dump_compose_logs(project_name, compose_file);
    panic!("LightClient not deployed within timeout");
}

/// Installs the rustls crypto provider if not already set.
/// Required because enabling the `relay` feature links rustls which
/// needs an explicit provider before any TLS usage.
pub fn ensure_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Starts docker compose stack with pre-cleanup of stale state.
pub async fn start_compose(compose_file: &std::path::Path, project_name: &str) -> DockerCompose {
    let compose_file_str = compose_file
        .to_str()
        .expect("compose file path should be valid UTF-8");

    // Pre-cleanup: remove stale state from a previous (possibly crashed) run.
    let _ = Command::new("docker")
        .args(["compose", "-p", project_name, "-f"])
        .arg(compose_file)
        .args(["down", "-v"])
        .status();

    tracing::info!("Starting docker compose stack...");
    let mut compose =
        DockerCompose::with_local_client(&[compose_file_str]).with_project_name(project_name);
    compose.with_remove_volumes(true);
    if let Err(e) = compose.up().await {
        dump_compose_logs(project_name, compose_file);
        panic!("docker compose up failed: {e}");
    }

    compose
}
