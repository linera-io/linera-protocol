// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: trigger a committee rotation on Linera and verify the exporter relays
//! it to the LightClient contract on Anvil (via docker-compose).

use std::time::{Duration, Instant};

use alloy::{providers::ProviderBuilder, sol};
use linera_base::crypto::{AccountPublicKey, ValidatorKeypair};
use linera_bridge_e2e::{
    compose_file_path, create_extra_wallet, dump_compose_logs, exec_ok, extra_wallet_env,
    light_client_address, start_compose,
};

sol! {
    #[sol(rpc)]
    interface ILightClient {
        function currentEpoch() external view returns (uint32);
    }
}

/// Queries the current epoch from the LightClient contract on Anvil.
async fn query_current_epoch() -> Result<u32, String> {
    let rpc_url = "http://localhost:8545"
        .parse()
        .map_err(|e| format!("Invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let contract = ILightClient::new(light_client_address(), &provider);

    let epoch = contract
        .currentEpoch()
        .call()
        .await
        .map_err(|e| format!("currentEpoch() call failed: {e}"))?;

    Ok(epoch)
}

#[tokio::test]
#[ignore] // Requires pre-built docker images: `make -C linera-bridge build-all`
async fn test_committee_rotation_updates_evm_light_client() {
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-test";

    let compose = start_compose(&compose_file, project_name).await;

    // Verify initial epoch is 0.
    let epoch = query_current_epoch()
        .await
        .expect("should query initial epoch");
    assert_eq!(epoch, 0, "initial epoch should be 0");
    eprintln!("Initial epoch verified: {epoch}");

    create_extra_wallet(&compose, project_name, &compose_file).await;

    // Trigger committee rotation by adding a fake validator.
    eprintln!("Triggering committee rotation...");
    let mut rng = rand::rngs::OsRng;
    let validator_keypair = ValidatorKeypair::generate_from(&mut rng);
    let validator_public_key = validator_keypair.public_key;
    let account_keypair = ValidatorKeypair::generate_from(&mut rng);
    let account_key = AccountPublicKey::Secp256k1(account_keypair.public_key);

    let wallet_env = extra_wallet_env();
    exec_ok(
        &compose,
        "linera-network",
        &format!(
            "{wallet_env} \
             ./linera validator add \
             --public-key {validator_public_key} \
             --account-key {account_key} \
             --address grpc:fake-validator:19100 \
             --votes 1 \
             --skip-online-check"
        ),
        project_name,
        &compose_file,
    )
    .await;

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
