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

/// Reads the deployed contract address from a `forge script` broadcast
/// artifact. The script is assumed to deploy a single contract via
/// `vm.broadcast()`, so `transactions[0].contractAddress` is what we want.
pub async fn parse_broadcast_address(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
    script_name: &str,
) -> anyhow::Result<Address> {
    let output = exec_output(
        compose,
        "foundry-tools",
        &format!(
            "CHAIN_ID=$(cast chain-id --rpc-url http://anvil:8545); \
             jq -r '.transactions[0].contractAddress' \
             /contracts/broadcast/{script_name}/$CHAIN_ID/run-latest.json"
        ),
        project_name,
        compose_file,
    )
    .await;
    Ok(output.trim().parse()?)
}

/// Deploys LineraToken via the `DeployLineraToken.s.sol` forge script and
/// returns its deployed address (parsed from the broadcast artifact).
pub async fn deploy_linera_token(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
) -> anyhow::Result<Address> {
    exec_ok(
        compose,
        "foundry-tools",
        &format!(
            "forge script /contracts/script/DeployLineraToken.s.sol \
             --root /contracts \
             --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             --broadcast"
        ),
        project_name,
        compose_file,
    )
    .await;
    parse_broadcast_address(compose, project_name, compose_file, "DeployLineraToken.s.sol").await
}

/// Deploys FungibleBridge via the `DeployFungibleBridge.s.sol` forge
/// script and returns the deployed contract address.
pub async fn deploy_fungible_bridge(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
    light_client: Address,
    chain_id_bytes32: &str,
    token: Address,
    fungible_app_id_bytes32: &str,
) -> anyhow::Result<Address> {
    exec_ok(
        compose,
        "foundry-tools",
        &format!(
            "env LIGHT_CLIENT={light_client} \
                 BRIDGE_CHAIN_ID={chain_id_bytes32} \
                 TOKEN_ADDRESS={token} \
                 FUNGIBLE_APP_ID={fungible_app_id_bytes32} \
             forge script /contracts/script/DeployFungibleBridge.s.sol \
             --root /contracts \
             --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             --broadcast"
        ),
        project_name,
        compose_file,
    )
    .await;
    parse_broadcast_address(compose, project_name, compose_file, "DeployFungibleBridge.s.sol")
        .await
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
    // `install_default` returns Err if a provider is already installed, which is the
    // expected case when multiple tests in the same process call this.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

/// Starts docker compose stack with pre-cleanup of stale state.
pub async fn start_compose(compose_file: &std::path::Path, project_name: &str) -> DockerCompose {
    let compose_file_str = compose_file
        .to_str()
        .expect("compose file path should be valid UTF-8");

    // Pre-cleanup: remove stale state from a previous (possibly crashed) run.
    if let Err(error) = Command::new("docker")
        .args(["compose", "-p", project_name, "-f"])
        .arg(compose_file)
        .args(["down", "-v"])
        .status()
    {
        tracing::debug!(?error, "Pre-cleanup `docker compose down` failed");
    }

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

/// Parses a Prometheus text-format response and returns the integer
/// value of the metric named `name`. Returns 0 if the metric is absent
/// (a counter never incremented).
pub fn parse_metric_value(body: &str, name: &str) -> i64 {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((metric, value)) = line.split_once(' ') else {
            continue;
        };
        if metric.trim() == name {
            return value.trim().parse::<f64>().unwrap_or(0.0) as i64;
        }
    }
    0
}

/// Publishes the wrapped-fungible Wasm module on `chain_client` and
/// creates an application instance with the given `mint_chain_id` and
/// an initial `initial_balance_tokens` balance for `initial_holder`.
/// Returns the resulting `application_id`. The Wasm artifacts are
/// loaded from `examples/target/wasm32-unknown-unknown/release/` —
/// bridge tests must build the examples crate before invoking this.
pub async fn publish_and_create_wrapped_fungible<E>(
    chain_client: &linera_core::client::ChainClient<E>,
    initial_holder: linera_base::identifiers::AccountOwner,
    mint_chain_id: linera_base::identifiers::ChainId,
    erc20_addr: Address,
    initial_balance_tokens: u128,
) -> anyhow::Result<linera_base::identifiers::ApplicationId>
where
    E: linera_core::environment::Environment,
{
    use anyhow::Context as _;
    use linera_base::{data_types::Bytecode, vm::VmRuntime};

    let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = chain_client
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");
    chain_client.synchronize_from_validators().await?;
    chain_client.process_inbox().await?;

    let initial_balance = linera_base::data_types::Amount::from_tokens(initial_balance_tokens);
    let (fungible_app_id, _) = chain_client
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&wrapped_fungible::WrappedParameters {
                ticker_symbol: "wTEST".to_string(),
                minter: None,
                mint_chain_id: Some(mint_chain_id),
                evm_token_address: erc20_addr.0 .0,
                evm_source_chain_id: 31337,
                bridge_app_id: None,
            })?,
            serde_json::to_vec(&wrapped_fungible::InitialState {
                accounts: std::collections::BTreeMap::from([(initial_holder, initial_balance)]),
            })?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");
    Ok(fungible_app_id)
}

/// Transfers `amount` ERC-20 attos from the deployer (Anvil account 0)
/// to the bridge contract via `cast send` inside the foundry-tools
/// service. Used to seed the bridge with enough liquidity to release
/// burned tokens.
pub async fn fund_bridge_erc20(
    compose: &DockerCompose,
    project_name: &str,
    compose_file: &std::path::Path,
    erc20: Address,
    bridge: Address,
    amount: u128,
) {
    exec_ok(
        compose,
        "foundry-tools",
        &format!(
            "cast send --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             {erc20} \
             'transfer(address,uint256)(bool)' \
             {bridge} \
             {amount}"
        ),
        project_name,
        compose_file,
    )
    .await;
}

/// Polls the relayer's `/metrics` endpoint until it responds (any HTTP
/// status), indicating the embedded server is up and the spawned relay
/// task is past its boot phase. Bails on `timeout`. Call after
/// `tokio::spawn`-ing `relay::run` to gate test traffic on readiness.
pub async fn wait_for_relay_http_ready(
    http: &reqwest::Client,
    relay_url: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if http
            .get(format!("{relay_url}/metrics"))
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
    anyhow::bail!("relay did not become ready within {timeout:?}")
}

/// Polls the relayer's `/metrics` endpoint every 2s and returns
/// `Ok(())` once `predicate(detected, completed, pending, failed)`
/// returns true. Bails on `timeout`. The poll interval matches the
/// relayer's own scan loop so callers don't redundantly hammer the
/// endpoint between iterations.
pub async fn wait_for_relay_metrics<F>(
    http: &reqwest::Client,
    relay_url: &str,
    mut predicate: F,
    timeout: std::time::Duration,
) -> anyhow::Result<()>
where
    F: FnMut(i64, i64, i64, i64) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let body = http
            .get(format!("{relay_url}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        let detected = parse_metric_value(&body, "linera_bridge_burns_detected");
        let completed = parse_metric_value(&body, "linera_bridge_burns_completed");
        let pending = parse_metric_value(&body, "linera_bridge_burns_pending");
        let failed = parse_metric_value(&body, "linera_bridge_burns_failed");
        if predicate(detected, completed, pending, failed) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    anyhow::bail!("wait_for_relay_metrics timed out after {timeout:?}")
}
