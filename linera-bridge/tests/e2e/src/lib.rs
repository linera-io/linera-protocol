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
            tracing::info!(exit_code=?exit_code, %stdout, "Exec output");
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
