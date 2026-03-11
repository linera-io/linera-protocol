// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: trigger a committee rotation on Linera and verify the exporter relays
//! it to the LightClient contract on Anvil (via docker-compose).

use std::{
    process::Command,
    time::{Duration, Instant},
};

use alloy::{primitives::Address, providers::ProviderBuilder, sol};
use testcontainers::{
    compose::DockerCompose,
    core::{CmdWaitFor, ExecCommand},
};

sol! {
    #[sol(rpc)]
    interface ILightClient {
        function currentEpoch() external view returns (uint32);
    }
}

/// Deterministic address: first contract deployed by Anvil account 0.
const LIGHT_CLIENT_ADDRESS: &str = "5FbDB2315678afecb367f032d93F642f64180aa3";

/// Well-known secp256k1 compressed public key (generator point, private key = 1).
/// Used as the validator's network-identity key for `--public-key`.
const TEST_VALIDATOR_PUBLIC_KEY: &str =
    "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

/// Hex-encoded BCS of `AccountPublicKey::Ed25519([1u8; 32])`.
/// Layout: `00` (Ed25519 variant tag) + 32 bytes of 0x01.
/// Used as the validator's `--account-key` (only stored in the committee, never verified).
const TEST_ACCOUNT_KEY: &str =
    "000101010101010101010101010101010101010101010101010101010101010101";

/// Path inside the container where `linera net up --path` stores wallet files.
/// Must match the `LINERA_NET_PATH` default in docker-compose.bridge-test.yml.
const WALLET_DIR: &str = "/tmp/wallet";

/// Extra wallet index (created by `--extra-wallets 1`).
/// Uses separate client storage to avoid RocksDB lock conflicts with the faucet.
const EXTRA_WALLET_ID: u32 = 1;

/// Queries the current epoch from the LightClient contract on Anvil.
async fn query_current_epoch() -> Result<u32, String> {
    let rpc_url = "http://localhost:8545"
        .parse()
        .map_err(|e| format!("Invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let address: Address = LIGHT_CLIENT_ADDRESS
        .parse()
        .map_err(|e| format!("Invalid address: {e}"))?;

    let contract = ILightClient::new(address, &provider);

    let epoch = contract
        .currentEpoch()
        .call()
        .await
        .map_err(|e| format!("currentEpoch() call failed: {e}"))?;

    Ok(epoch)
}

/// Dumps docker compose logs for debugging failures.
fn dump_compose_logs(project_name: &str, compose_file: &std::path::Path) {
    let output = Command::new("docker")
        .args(["compose", "-p", project_name, "-f"])
        .arg(compose_file)
        .args(["logs", "--tail", "100"])
        .output();

    if let Ok(output) = output {
        eprintln!(
            "=== docker compose logs ===\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Executes a shell command inside the linera-network service, panicking on failure.
async fn exec_ok(
    compose: &DockerCompose,
    cmd: &str,
    project_name: &str,
    compose_file: &std::path::Path,
) {
    let service = compose.service("linera-network").expect("linera-network service");
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
            let stdout = String::from_utf8_lossy(&stdout_bytes);
            eprintln!("exec output (exit {exit_code:?}):\n{stdout}");
            if exit_code != Some(0) {
                dump_compose_logs(project_name, compose_file);
                panic!("exec failed (exit {exit_code:?}):\n{stdout}");
            }
        }
        Err(e) => {
            dump_compose_logs(project_name, compose_file);
            panic!("Failed to exec command: {e}");
        }
    }
}

/// Returns the path to the compose file relative to this crate's manifest dir.
fn compose_file_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("docker/docker-compose.bridge-test.yml")
}

#[tokio::test]
#[ignore] // Requires pre-built docker images: `make -C linera-bridge build-all`
async fn test_committee_rotation_updates_evm_light_client() {
    let compose_file = compose_file_path();
    let compose_file_str = compose_file
        .to_str()
        .expect("compose file path should be valid UTF-8");

    let project_name = "linera-bridge-test";

    // Pre-cleanup: remove stale state from a previous (possibly crashed) run.
    // Must use the same project name as testcontainers to find its containers.
    let _ = Command::new("docker")
        .args(["compose", "-p", project_name, "-f"])
        .arg(&compose_file)
        .args(["down", "-v"])
        .status();

    eprintln!("Starting docker compose stack...");
    let mut compose =
        DockerCompose::with_local_client(&[compose_file_str]).with_project_name(project_name);
    compose.with_remove_volumes(true);
    if let Err(e) = compose.up().await {
        dump_compose_logs(project_name, &compose_file);
        panic!("docker compose up failed: {e}");
    }

    // Verify initial epoch is 0.
    let epoch = query_current_epoch()
        .await
        .expect("should query initial epoch");
    assert_eq!(epoch, 0, "initial epoch should be 0");
    eprintln!("Initial epoch verified: {epoch}");

    // Create a copy of wallet_0 with separate client storage so we can run
    // CLI commands without conflicting with the faucet's RocksDB lock.
    eprintln!("Creating extra wallet copy...");
    exec_ok(&compose, &format!(
        "cp {WALLET_DIR}/wallet_0.json {WALLET_DIR}/wallet_{EXTRA_WALLET_ID}.json && \
         cp {WALLET_DIR}/keystore_0.json {WALLET_DIR}/keystore_{EXTRA_WALLET_ID}.json && \
         cp -r {WALLET_DIR}/client_0.db {WALLET_DIR}/client_{EXTRA_WALLET_ID}.db"
    ), project_name, &compose_file).await;

    // Trigger committee rotation by adding a fake validator.
    eprintln!("Triggering committee rotation...");
    exec_ok(&compose, &format!(
        "LINERA_WALLET={WALLET_DIR}/wallet_{EXTRA_WALLET_ID}.json \
         LINERA_KEYSTORE={WALLET_DIR}/keystore_{EXTRA_WALLET_ID}.json \
         LINERA_STORAGE=rocksdb:{WALLET_DIR}/client_{EXTRA_WALLET_ID}.db \
         ./linera validator add \
         --public-key {TEST_VALIDATOR_PUBLIC_KEY} \
         --account-key {TEST_ACCOUNT_KEY} \
         --address grpc:fake-validator:19100 \
         --votes 1 \
         --skip-online-check"
    ), project_name, &compose_file).await;

    eprintln!("Committee rotation triggered, waiting for exporter to relay...");

    // Poll until epoch advances to 1 (timeout 120s — compose startup is already done,
    // but the exporter needs to pick up the new committee and relay it).
    let timeout = Duration::from_secs(120);
    let poll_interval = Duration::from_secs(3);
    let start = Instant::now();

    loop {
        match query_current_epoch().await {
            Ok(epoch) if epoch >= 1 => {
                eprintln!("Epoch advanced to {epoch}");
                assert_eq!(epoch, 1, "epoch should be exactly 1 after one rotation");
                return;
            }
            Ok(epoch) => {
                eprintln!("Current epoch: {epoch}, waiting...");
            }
            Err(e) => {
                eprintln!("Error querying epoch: {e}, retrying...");
            }
        }

        if start.elapsed() > timeout {
            dump_compose_logs(project_name, &compose_file);
            panic!(
                "Timed out waiting for epoch to advance to 1 (waited {:?})",
                start.elapsed()
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}
