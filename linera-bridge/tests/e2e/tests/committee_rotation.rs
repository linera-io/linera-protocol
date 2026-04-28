// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! End-to-end test: trigger a committee rotation on Linera and verify the relay relays
//! it to the LightClient contract on Anvil (via docker-compose).

use std::time::{Duration, Instant};

use alloy::{providers::ProviderBuilder, sol};
use linera_base::{
    crypto::{AccountPublicKey, InMemorySigner, ValidatorKeypair},
    identifiers::AccountOwner,
};
use linera_bridge_e2e::{
    compose_file_path, create_extra_wallet, dump_compose_logs, exec_ok, extra_wallet_env,
    light_client_address, start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_faucet_client::Faucet;

sol! {
    #[sol(rpc)]
    interface ILightClient {
        function currentEpoch() external view returns (uint32);
    }
}

/// Queries the current epoch from the LightClient contract on Anvil.
async fn query_current_epoch() -> anyhow::Result<u32> {
    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let contract = ILightClient::new(light_client_address(), &provider);

    let epoch = contract.currentEpoch().call().await?;

    Ok(epoch)
}

#[tokio::test]
#[ignore] // Requires pre-built docker images: `make -C linera-bridge build-all`
async fn test_committee_rotation_updates_evm_light_client() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // Verify initial epoch is 0.
    let epoch = query_current_epoch().await?;
    assert_eq!(epoch, 0, "initial epoch should be 0");
    tracing::info!(epoch, "Initial epoch verified");

    // Claim a chain for the relay so it can listen for admin chain notifications.
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let mut signer = InMemorySigner::new(None);
    let relay_owner = AccountOwner::from(signer.generate_new());
    let relay_chain_desc = faucet.claim(&relay_owner).await?;
    let relay_chain_id = relay_chain_desc.id();
    tracing::info!(%relay_chain_id, %relay_owner, "Relay chain claimed");

    let relay_dir = tempfile::tempdir()?;
    let keystore_path = relay_dir.path().join("keystore.json");
    {
        use linera_persistent::Persist;
        let mut ks = linera_persistent::File::new(&keystore_path, signer)?;
        ks.persist().await?;
    }

    // Start the relay as an in-process background task.
    let light_client = light_client_address();
    let wallet_path = relay_dir.path().join("wallet.json");
    let storage_config = format!("rocksdb:{}", relay_dir.path().join("client.db").display());
    let relay_port = 3003u16;

    let relay_handle = tokio::spawn(async move {
        Box::pin(linera_bridge::relay::run(
            "http://localhost:8545",
            Some("http://localhost:8080"),
            Some(wallet_path.as_path()),
            Some(keystore_path.as_path()),
            Some(&storage_config),
            Some(relay_chain_id),
            Some(relay_owner),
            // Dummy values — committee relay only needs the LightClient, not the bridge apps.
            "0x0000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            ANVIL_PRIVATE_KEY,
            Some(&light_client.to_string()),
            relay_port,
            linera_storage_runtime::CommonStorageOptions::with_defaults().storage_cache_config(),
            5,  // monitor_scan_interval
            0,  // monitor_start_block
            5,  // max_retries
            None,
        ))
        .await
    });

    // Wait for relay's HTTP server to be ready.
    let relay_url = format!("http://localhost:{relay_port}");
    let client = reqwest::Client::new();
    for attempt in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if client
            .get(format!("{relay_url}/metrics"))
            .send()
            .await
            .is_ok()
        {
            tracing::info!(attempt, "Relay is ready");
            break;
        }
        if attempt == 29 {
            anyhow::bail!("Relay did not become ready");
        }
    }

    create_extra_wallet(&compose, project_name, &compose_file).await;

    // Trigger committee rotation by adding a fake validator.
    tracing::info!("Triggering committee rotation...");
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

    tracing::info!("Committee rotation triggered, waiting for relay to forward...");

    // Poll until epoch advances to 1 (timeout 120s — compose startup is already done,
    // but the relay needs to pick up the new committee and relay it).
    let timeout = Duration::from_secs(120);
    let poll_interval = Duration::from_secs(3);
    let start = Instant::now();

    loop {
        match query_current_epoch().await {
            Ok(epoch) if epoch >= 1 => {
                tracing::info!(epoch, "Epoch advanced");
                assert_eq!(epoch, 1, "epoch should be exactly 1 after one rotation");
                relay_handle.abort();
                return Ok(());
            }
            Ok(epoch) => {
                tracing::info!(epoch, "Waiting for epoch to advance");
            }
            Err(e) => {
                tracing::info!(error=%e, "Error querying epoch, retrying");
            }
        }

        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }

        if start.elapsed() > timeout {
            relay_handle.abort();
            dump_compose_logs(project_name, &compose_file);
            anyhow::bail!(
                "Timed out waiting for epoch to advance to 1 (waited {:?})",
                start.elapsed()
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}
