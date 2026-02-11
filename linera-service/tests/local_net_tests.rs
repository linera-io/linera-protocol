// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
))]

mod common;
mod guard;

use std::{env, path::PathBuf, time::Duration};

use anyhow::Result;
use guard::INTEGRATION_TEST_GUARD;
#[cfg(any(feature = "opentelemetry", feature = "ethereum"))]
use linera_base::vm::VmRuntime;
use linera_base::{
    crypto::Secp256k1SecretKey,
    data_types::{Amount, BlockHeight, Epoch},
    identifiers::{Account, AccountOwner},
};
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use linera_sdk::linera_base_types::AccountSecretKey;
use linera_service::{
    cli_wrappers::{
        local_net::{get_node_port, Database, LocalNetConfig, ProcessInbox},
        ClientWrapper, LineraNet, LineraNetConfig, Network, NotificationsExt,
    },
    test_name,
    util::eventually,
};
use test_case::test_case;
#[cfg(feature = "ethereum")]
use {alloy_primitives::U256, linera_service::cli_wrappers::ApplicationWrapper};
#[cfg(feature = "storage-service")]
use {
    assert_matches::assert_matches, linera_base::port::get_free_port,
    linera_service::cli_wrappers::Faucet, std::process::Command,
};

#[cfg(feature = "opentelemetry")]
fn get_fungible_account_owner(client: &ClientWrapper) -> AccountOwner {
    client.get_owner().unwrap()
}

#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Udp) ; "scylladb_udp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration(config: LocalNetConfig) -> Result<()> {
    let _guard: tokio::sync::MutexGuard<'_, ()> = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let network = config.network.external;
    let (mut net, client) = config.instantiate().await?;

    let faucet_client = net.make_client().await;
    faucet_client.wallet_init(None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    assert_eq!(faucet.current_validators().await?.len(), 4);

    let client_2 = net.make_client().await;
    client_2.wallet_init(None).await?;
    let chain_1 = client
        .load_wallet()?
        .default_chain()
        .expect("should have a default chain");

    let chain_2 = client
        .open_and_assign(&client_2, Amount::from_tokens(3))
        .await?;
    let port = get_node_port().await;
    let mut node_service_2 = match network {
        Network::Grpc | Network::Grpcs => {
            let service = client_2.run_node_service(port, ProcessInbox::Skip).await?;
            let notifications = service.notifications(chain_1).await?;
            Some((service, notifications))
        }
        Network::Tcp | Network::Udp => None,
    };

    client.query_validators(None).await?;

    let address = format!(
        "{}:127.0.0.1:{}",
        network.short(),
        net.proxy_public_port(0, 0)
    );
    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Restart the first shard for the 4th validator.
    // TODO(#2286): The proxy currently only re-establishes the connection with gRPC.
    if matches!(network, Network::Grpc) {
        net.terminate_server(3, 0).await?;
        net.start_server(3, 0).await?;
    }

    // Create configurations for two more validators
    net.generate_validator_config(4).await?;
    net.generate_validator_config(5).await?;

    // Start the validators
    net.start_validator(4).await?;
    net.start_validator(5).await?;

    let address = format!(
        "{}:127.0.0.1:{}",
        network.short(),
        net.proxy_public_port(4, 0)
    );

    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th and 6th validators in a single epoch using change-validators
    let key_4 = net.validator_keys(4).unwrap();
    let key_5 = net.validator_keys(5).unwrap();
    client
        .change_validators(
            &[
                (
                    key_4.0.clone(),
                    key_4.1.clone(),
                    net.proxy_public_port(4, 0),
                    100,
                ),
                (
                    key_5.0.clone(),
                    key_5.1.clone(),
                    net.proxy_public_port(5, 0),
                    100,
                ),
            ],
            &[],
            &[],
        )
        .await?;

    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;

    if matches!(network, Network::Grpc) {
        assert!(
            eventually(|| async { faucet.current_validators().await.unwrap().len() == 6 }).await
        );
    }

    // Remove 5th validator
    client
        .remove_validator(&net.validator_keys(4).unwrap().0)
        .await?;
    net.remove_validator(4)?;
    if matches!(network, Network::Grpc) {
        assert!(
            eventually(|| async { faucet.current_validators().await.unwrap().len() == 5 }).await
        )
    }
    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;
    if let Some((service, notifications)) = &mut node_service_2 {
        let admin_height = client
            .load_wallet()?
            .get(chain_1)
            .unwrap()
            .next_block_height;
        let event_height = admin_height.try_sub_one()?;
        notifications.wait_for_events(event_height).await?;
        assert!(!service.process_inbox(&chain_2).await?.is_empty());
        client.revoke_epochs(Epoch(1)).await?;
        notifications.wait_for_events(None).await?;
        assert!(!service.process_inbox(&chain_2).await.unwrap().is_empty());
        let committees = service.query_committees(&chain_2).await?;
        let epochs = committees.into_keys().collect::<Vec<_>>();
        assert_eq!(&epochs, &[Epoch(2)]);
    } else {
        client_2.process_inbox(chain_2).await?;
        client.revoke_epochs(Epoch(1)).await?;
        client_2.process_inbox(chain_2).await?;
    }

    // Remove the first 4 validators in a single epoch using change-validators.
    let validators_to_remove: Vec<String> = (0..4)
        .map(|i| net.validator_keys(i).unwrap().0.clone())
        .collect();
    client
        .change_validators(&[], &[], &validators_to_remove)
        .await?;

    if let Some((service, notifications)) = &mut node_service_2 {
        notifications.wait_for_events(None).await?;
        assert!(!service.process_inbox(&chain_2).await.unwrap().is_empty());
        client.revoke_epochs(Epoch(2)).await?;
        notifications.wait_for_events(None).await?;
        assert!(!service.process_inbox(&chain_2).await?.is_empty());
        let committees = service.query_committees(&chain_2).await?;
        let epochs = committees.into_keys().collect::<Vec<_>>();
        assert_eq!(&epochs, &[Epoch(3)]);
    } else {
        client_2.process_inbox(chain_2).await?;
        client.revoke_epochs(Epoch(2)).await?;
        client_2.process_inbox(chain_2).await?;
    }
    for i in 0..4 {
        net.remove_validator(i)?;
    }

    let recipient =
        AccountOwner::from(AccountSecretKey::Secp256k1(Secp256k1SecretKey::generate()).public());
    let account_recipient = Account::new(chain_2, recipient);
    client
        .transfer_with_accounts(
            Amount::from_tokens(5),
            Account::chain(chain_1),
            account_recipient,
        )
        .await?;

    if let Some((service, notifications)) = &mut node_service_2 {
        let height = client
            .load_wallet()?
            .get(chain_1)
            .unwrap()
            .next_block_height
            .try_sub_one()?;
        notifications.wait_for_block(height).await?;
        assert!(!service.process_inbox(&chain_2).await?.is_empty());
        let balance = service.balance(&account_recipient).await?;
        assert_eq!(balance, Amount::from_tokens(5));
        let committees = service.query_committees(&chain_2).await?;
        let epochs = committees.into_keys().collect::<Vec<_>>();
        assert_eq!(&epochs, &[Epoch(3)]);

        service.ensure_is_running()?;
    } else {
        client_2.sync(chain_2).await?;
        client_2.process_inbox(chain_2).await?;
        assert_eq!(
            client_2.local_balance(account_recipient).await?,
            Amount::from_tokens(5),
        );
    }

    if matches!(network, Network::Grpc) {
        let client = net.make_client().await;
        client.wallet_init(Some(&faucet)).await?;
        let (chain_id, _owner) = client.request_chain(&faucet, true).await?;
        let port = get_node_port().await;
        let service = client
            .run_node_service(port, ProcessInbox::Automatic)
            .await?;
        service
            .publish_data_blob(&chain_id, b"blob bytes".to_vec())
            .await?;
    }

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Test if it's possible to receive epoch change messages for past epochs.
///
/// The epoch change messages are protected, and can't be rejected.
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_receipt_of_old_create_committee_messages(
    config: LocalNetConfig,
) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let network = config.network.external;
    let (mut net, client) = config.instantiate().await?;

    let faucet_client = net.make_client().await;
    faucet_client.wallet_init(None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
            .await?;

        faucet_service.ensure_is_running()?;

        let faucet = faucet_service.instance();
        assert_eq!(faucet.current_validators().await?.len(), 4);

        faucet_service.terminate().await?;
    }

    client.query_validators(None).await?;

    // Start a new validator
    net.generate_validator_config(4).await?;
    net.start_validator(4).await?;

    let address = format!(
        "{}:127.0.0.1:{}",
        network.short(),
        net.proxy_public_port(4, 0)
    );

    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th validator to the network
    client
        .set_validator(
            net.validator_keys(4).unwrap(),
            net.proxy_public_port(4, 0),
            100,
        )
        .await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch
    faucet_client.process_inbox(faucet_chain).await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 5);
    }

    // Create a new chain starting on the new epoch
    let new_owner = client.keygen().await?;
    let chain_id = faucet.claim(&new_owner).await?.id();
    client.assign(new_owner, chain_id).await?;

    // Attempt to receive the existing epoch change message
    client.process_inbox(chain_id).await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Test if it's possible to receive epoch change messages for past epochs, even if they have been
/// deprecated.
///
/// The epoch change messages are protected, and can't be rejected.
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_receipt_of_old_remove_committee_messages(
    config: LocalNetConfig,
) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let network = config.network.external;
    let (mut net, client) = config.instantiate().await?;

    let faucet_client = net.make_client().await;
    faucet_client.wallet_init(None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
            .await?;

        faucet_service.ensure_is_running()?;

        let faucet = faucet_service.instance();
        assert_eq!(faucet.current_validators().await?.len(), 4);

        faucet_service.terminate().await?;
    }

    client.query_validators(None).await?;

    // Start a new validator
    net.generate_validator_config(4).await?;
    net.start_validator(4).await?;

    let address = format!(
        "{}:127.0.0.1:{}",
        network.short(),
        net.proxy_public_port(4, 0)
    );

    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th validator to the network
    client
        .set_validator(
            net.validator_keys(4).unwrap(),
            net.proxy_public_port(4, 0),
            100,
        )
        .await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch before removing the old ones.
    faucet_client.process_inbox(faucet_chain).await?;
    client.revoke_epochs(Epoch::ZERO).await?;
    faucet_client.process_inbox(faucet_chain).await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
            .await?;

        faucet_service.ensure_is_running()?;

        let faucet = faucet_service.instance();
        assert_eq!(faucet.current_validators().await?.len(), 5);

        faucet_service.terminate().await?;
    }

    // We need the epoch before the latest to still be active, so that it can send all the epoch
    // change messages in a batch where the latest message is signed by a committee that the
    // receiving chain trusts.

    // Start another new validator
    net.generate_validator_config(5).await?;
    net.start_validator(5).await?;

    let address = format!(
        "{}:127.0.0.1:{}",
        network.short(),
        net.proxy_public_port(5, 0)
    );

    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 6th validator to the network
    client
        .set_validator(
            net.validator_keys(5).unwrap(),
            net.proxy_public_port(5, 0),
            100,
        )
        .await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch
    faucet_client.process_inbox(faucet_chain).await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, Some(faucet_chain), Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 6);
    }

    // Create a new chain starting on the new epoch
    let new_owner = client.keygen().await?;
    let chain_id = faucet.claim(&new_owner).await?.id();
    client.assign(new_owner, chain_id).await?;

    // Attempt to receive the existing epoch change messages
    client.process_inbox(chain_id).await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_notification_stream(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client1) = config.instantiate().await?;

    let (chain, chain1) = {
        let wallet = client1.load_wallet()?;
        let chains = wallet.owned_chain_ids();
        (chains[0], chains[1])
    };

    let client2 = net.make_client().await;
    let mut height = 0;
    client2.wallet_init(None).await?;
    client2.follow_chain(chain, false).await?;
    client2.set_default_chain(chain).await?;

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let port = get_node_port().await;
    let mut node_service2 = client2.run_node_service(port, ProcessInbox::Skip).await?;
    let response = node_service2
        .query_node(format!(
            "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
        ))
        .await?;
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The first validator has an outage and gets restarted!
    net.remove_validator(0)?;
    net.restart_validator(0).await?;

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1
                .transfer(Amount::from_tokens(1), chain, chain1)
                .await?;
            linera_base::time::timer::sleep(Duration::from_secs(i)).await;
            height += 1;
            let response = node_service2
                .query_node(format!(
                    "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
                ))
                .await?;
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_project_publish(database: Database, network: Network) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let _rustflags_override = common::override_disable_warnings_as_errors();
    let config = LocalNetConfig {
        num_initial_validators: 1,
        num_shards: 1,
        ..LocalNetConfig::new_test(database, network)
    };

    let (mut net, client) = config.instantiate().await?;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await?;
    let project_dir = tmp_dir.path().join("init-test");

    client
        .project_publish(project_dir, vec![], None, &0)
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(Database::Service, Network::Grpc ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_example_publish(database: Database, network: Network) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig {
        num_initial_validators: 1,
        num_shards: 1,
        ..LocalNetConfig::new_test(database, network)
    };
    let (mut net, client) = config.instantiate().await?;

    let example_dir = ClientWrapper::example_path("counter")?;
    client
        .project_publish(example_dir, vec![], None, &0)
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Test if the wallet file is correctly locked when used.
#[cfg(feature = "storage-service")]
// TODO(#2053): this test passes only if the wallet hasn't been saved
#[ignore]
#[test_log::test(tokio::test)]
async fn test_storage_service_wallet_lock() -> Result<()> {
    use std::mem::drop;

    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let wallet = linera_service::Wallet::read(&client.wallet_path())?;

    let chain_id = wallet.default_chain().unwrap();

    let lock = wallet;
    assert!(client.process_inbox(chain_id).await.is_err());

    drop(lock);
    assert!(client.process_inbox(chain_id).await.is_ok());

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_log::test(tokio::test)]
#[cfg(feature = "storage-service")]
async fn test_storage_service_linera_net_up_simple() -> Result<()> {
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
    };

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let port = get_free_port().await?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_linera"));
    command.args([
        "net",
        "up",
        "--with-faucet",
        "--faucet-chain",
        "1",
        "--faucet-port",
        &port.to_string(),
    ]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = BufReader::new(child.stderr.take().unwrap());
    let mut lines = stderr.lines();

    let mut is_ready = false;
    eprintln!("waiting for network to be ready");
    for line in &mut lines {
        let line = line?;
        eprintln!("[net up]: {line}");
        if line.starts_with("READY!") {
            is_ready = true;
            break;
        }
    }

    if !is_ready {
        assert!(is_ready, "unexpected EOF for stderr");
    } else {
        eprintln!("network is ready");
    }

    // Echo faucet stderr for debugging and to empty the buffer.
    std::thread::spawn(move || {
        for line in lines {
            eprintln!("[net up] {}", line.unwrap());
        }
    });

    insta::assert_snapshot!(stdout
        .lines()
        .map_while(|line| {
            println!("{line:?}");
            line.unwrap()
                .split_once("=")
                .map(|x| x.0.to_owned())
                .filter(|line| line.starts_with("export"))
        })
        .collect::<Vec<_>>()
        .join("\n"));

    // Test faucet.
    let faucet = Faucet::new(format!("http://localhost:{}/", port));
    faucet.version_info().await.unwrap();

    // Send SIGINT to the child process.
    Command::new("kill")
        .args(["-s", "INT", &child.id().to_string()])
        .output()?;

    assert!(child.wait()?.success());
    return Ok(());
}

#[cfg_attr(
    all(feature = "storage-service", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc")
)]
#[cfg_attr(
    all(feature = "storage-service", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp")
)]
#[cfg_attr(
    all(feature = "scylladb", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc")
)]
#[cfg_attr(
    all(feature = "dynamodb", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc")
)]
#[cfg_attr(
    all(feature = "scylladb", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp")
)]
#[cfg_attr(
    all(feature = "dynamodb", feature = "opentelemetry"),
    test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp")
)]
#[cfg(feature = "opentelemetry")]
#[test_log::test(tokio::test)]
async fn test_end_to_end_benchmark(mut config: LocalNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use fungible::{FungibleTokenAbi, InitialState, Parameters};
    use linera_service::cli::command::{BenchmarkCommand, BenchmarkOptions};

    config.num_other_initial_chains = 2;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    assert_eq!(client.load_wallet()?.num_chains(), 3);
    // Launch local benchmark using some additional chains.
    client
        .benchmark(BenchmarkCommand::Single {
            options: BenchmarkOptions {
                num_chains: 2,
                transactions_per_block: 10,
                bps: 2,
                runtime_in_seconds: Some(5),
                close_chains: true,
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(client.load_wallet()?.num_chains(), 3);

    // Now we run the benchmark again, with the fungible token application instead of the
    // native token.
    let account_owner = get_fungible_account_owner(&client);
    let accounts = BTreeMap::from([(account_owner, Amount::from_tokens(1_000_000))]);
    let state = InitialState { accounts };
    let (contract, service) = client.build_example("fungible").await?;
    let params = Parameters::new("FUN");
    let application_id = client
        .publish_and_create::<FungibleTokenAbi, Parameters, InitialState>(
            contract,
            service,
            VmRuntime::Wasm,
            &params,
            &state,
            &[],
            None,
        )
        .await?;
    client
        .benchmark(BenchmarkCommand::Single {
            options: BenchmarkOptions {
                num_chains: 2,
                transactions_per_block: 10,
                bps: 2,
                runtime_in_seconds: Some(5),
                fungible_application_id: Some(application_id.forget_abi()),
                close_chains: true,
                ..Default::default()
            },
        })
        .await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Tests if the `sync-validator` command uploads missing certificates to a validator.
// TODO(#3258): Fix test for simple-net
// #[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Udp) ; "scylladb_udp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
// #[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
// #[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
// #[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
// #[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_sync_validator(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    const BLOCKS_TO_CREATE: usize = 5;
    const LAGGING_VALIDATOR_INDEX: usize = 0;

    let (mut net, client) = config.instantiate().await?;

    // Stop a validator to force it to lag behind the others
    net.stop_validator(LAGGING_VALIDATOR_INDEX).await?;

    // Create some blocks
    let sender_chain = client.default_chain().expect("Client has no default chain");
    let (receiver_chain, _) = client
        .open_chain(sender_chain, None, Amount::from_tokens(1_000))
        .await?;

    for amount in 1..=BLOCKS_TO_CREATE {
        client
            .transfer(
                Amount::from_tokens(amount as u128),
                sender_chain,
                receiver_chain,
            )
            .await?;
    }

    // Restart the stopped validator
    net.restart_validator(LAGGING_VALIDATOR_INDEX).await?;

    let lagging_validator = net.validator_client(LAGGING_VALIDATOR_INDEX)?;

    let state_before_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(state_before_sync.info.next_block_height, BlockHeight::ZERO);

    // Synchronize the validator
    let validator_address = net.validator_address(LAGGING_VALIDATOR_INDEX);
    client
        .sync_validator([&sender_chain], validator_address)
        .await
        .expect("Missing lagging validator name");

    let state_after_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(
        state_after_sync.info.next_block_height,
        BlockHeight(BLOCKS_TO_CREATE as u64 + 1)
    );

    Ok(())
}

/// Tests if a validator can process blocks on a child chain without syncing the parent
/// chain.
// #[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Udp) ; "scylladb_udp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
// #[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
// #[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
// #[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
// #[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_sync_child_chain(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    const BLOCKS_TO_CREATE: usize = 5;
    const LAGGING_VALIDATOR_INDEX: usize = 0;

    let (mut net, client) = config.instantiate().await?;

    // Stop a validator to force it to lag behind the others
    net.stop_validator(LAGGING_VALIDATOR_INDEX).await?;

    // Create some blocks
    let sender_chain = client.default_chain().expect("Client has no default chain");
    let (receiver_chain, _) = client
        .open_chain(sender_chain, None, Amount::from_tokens(1_000))
        .await?;

    for amount in 1..=BLOCKS_TO_CREATE {
        client
            .transfer(
                Amount::from_tokens(amount as u128),
                sender_chain,
                receiver_chain,
            )
            .await?;
    }

    // Create a second child chain at a point in the sender chain the stopped validator
    // won't be aware of.
    let (second_child_chain, _) = client
        .open_chain(sender_chain, None, Amount::from_tokens(1000))
        .await?;

    for amount in 1..=BLOCKS_TO_CREATE {
        client
            .transfer(
                Amount::from_tokens(amount as u128),
                second_child_chain,
                receiver_chain,
            )
            .await?;
    }

    // Restart the stopped validator
    net.restart_validator(LAGGING_VALIDATOR_INDEX).await?;

    let lagging_validator = net.validator_client(LAGGING_VALIDATOR_INDEX)?;

    let state_before_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(state_before_sync.info.next_block_height, BlockHeight::ZERO);

    // Synchronize the second chain without synchronizing the parent chain.
    let validator_address = net.validator_address(LAGGING_VALIDATOR_INDEX);
    client
        .sync_validator([&second_child_chain], validator_address)
        .await
        .expect("Missing lagging validator name");

    // The parent chain should remain out of sync.
    let state_after_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(state_after_sync.info.next_block_height, BlockHeight::ZERO);

    // But the second child chain should be synchronized properly.
    let second_chain_state_after_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(second_child_chain))
        .await?;
    assert_eq!(
        second_chain_state_after_sync.info.next_block_height,
        BlockHeight(BLOCKS_TO_CREATE as u64)
    );

    Ok(())
}

#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_update_validator_sender_gaps(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    const UNAWARE_VALIDATOR_INDEX: usize = 0;
    const STOPPED_VALIDATOR_INDEX: usize = 1;

    let (mut net, client) = config.instantiate().await?;

    let sender_client = net.make_client().await;
    sender_client.wallet_init(None).await?;

    let sender_chain = client
        .open_and_assign(&sender_client, Amount::from_tokens(1000))
        .await?;

    let receiver_client = net.make_client().await;
    receiver_client.wallet_init(None).await?;

    let receiver_chain = client
        .open_and_assign(&receiver_client, Amount::from_tokens(1000))
        .await?;

    // Stop a validator so that it is not aware of the blocks on the sender chain
    net.stop_validator(UNAWARE_VALIDATOR_INDEX).await?;

    // Create some blocks
    sender_client
        .transfer(Amount::from_tokens(1), sender_chain, receiver_chain)
        .await?;
    // send to itself so that this doesn't generate messages to receiver_chain
    sender_client
        .transfer(Amount::from_tokens(2), sender_chain, sender_chain)
        .await?;
    receiver_client.process_inbox(receiver_chain).await?;
    // transfer some more to create a gap in the chain from the recipient's perspective
    sender_client
        .transfer(Amount::from_tokens(3), sender_chain, receiver_chain)
        .await?;

    // Restart the stopped validator and stop another one.
    net.restart_validator(UNAWARE_VALIDATOR_INDEX).await?;
    net.stop_validator(STOPPED_VALIDATOR_INDEX).await?;

    let unaware_validator = net.validator_client(UNAWARE_VALIDATOR_INDEX)?;

    let sender_state_before_sync = unaware_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(
        sender_state_before_sync.info.next_block_height,
        BlockHeight::ZERO
    );

    let receiver_state_before_sync = unaware_validator
        .handle_chain_info_query(ChainInfoQuery::new(receiver_chain))
        .await?;
    assert_eq!(
        receiver_state_before_sync.info.next_block_height,
        BlockHeight::ZERO
    );

    // Process the last sender block. The client has a gap in the sender chain and does
    // not update the unaware validator about block 1.
    receiver_client.process_inbox(receiver_chain).await?;

    // Synchronize the validator
    let validator_address = net.validator_address(UNAWARE_VALIDATOR_INDEX);
    receiver_client
        .sync_validator([&receiver_chain], validator_address)
        .await
        .expect("Missing lagging validator name");

    let sender_state_after_sync = unaware_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    // The next block height should be 1 - only block 0 has been processed fully, block 2
    // has only been preprocessed.
    assert_eq!(
        sender_state_after_sync.info.next_block_height,
        BlockHeight(1)
    );

    let receiver_state_after_sync = unaware_validator
        .handle_chain_info_query(ChainInfoQuery::new(receiver_chain))
        .await?;
    // On the receiver side, block 0 received the transfers from sender and block 1 made a
    // transfer.
    assert_eq!(
        receiver_state_after_sync.info.next_block_height,
        BlockHeight(2)
    );

    Ok(())
}

#[cfg(feature = "ethereum")]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_ethereum_tracker(config: impl LineraNetConfig) -> Result<()> {
    use ethereum_tracker::{EthereumTrackerAbi, InstantiationArgument};
    use linera_ethereum::{
        client::EthereumQueries,
        provider::EthereumClientSimplified,
        test_utils::{get_anvil, SimpleTokenContractFunction},
    };
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Setting up the Ethereum smart contract
    let anvil_test = get_anvil().await?;
    let address0 = anvil_test.get_address(0);
    let address1 = anvil_test.get_address(1);
    let ethereum_endpoint = anvil_test.endpoint.clone();
    let ethereum_client_simp = EthereumClientSimplified::new(ethereum_endpoint.clone());

    let simple_token = SimpleTokenContractFunction::new(anvil_test).await?;
    let contract_address = simple_token.contract_address.clone();
    let event_name_expanded = "Initial(address,uint256)";
    let events = ethereum_client_simp
        .read_events(&contract_address, event_name_expanded, 0, 2)
        .await?;
    let start_block = events.first().unwrap().block_number;
    let argument = InstantiationArgument {
        ethereum_endpoint,
        contract_address,
        start_block,
    };

    // Setting up the validators
    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    // Change the ownership so that the blocks inserted are not
    // fast blocks. Fast blocks are not allowed for the oracles.
    let owner1 = {
        let wallet = client.load_wallet()?;
        let user_chain = wallet.get(chain).unwrap();
        *user_chain.owner.as_ref().unwrap()
    };

    client.change_ownership(chain, vec![], vec![owner1]).await?;
    let (contract, service) = client.build_example("ethereum-tracker").await?;

    tracing::info!("Publishing Ethereum tracker contract");
    let application_id = client
        .publish_and_create::<EthereumTrackerAbi, (), InstantiationArgument>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &argument,
            &[],
            None,
        )
        .await?;

    tracing::info!("Application ID: {:?}", application_id);
    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let app = EthereumTrackerApp(node_service.make_application(&chain, &application_id)?);

    // Check after the initialization

    app.assert_balances([
        (address0.clone(), U256::from(1000)),
        (address1.clone(), U256::from(0)),
    ])
    .await;

    // Doing a transfer and updating the smart contract
    // First await gets you the pending transaction, second gets it mined.

    let value = U256::from(10);
    simple_token.transfer(&address0, &address1, value).await?;
    let last_block = ethereum_client_simp.get_block_number().await?;
    // increment by 1 since the read_events is exclusive in the last block.
    app.update(last_block + 1).await;

    // Now checking the balances after the operations.

    app.assert_balances([
        (address0.clone(), U256::from(990)),
        (address1.clone(), U256::from(10)),
    ])
    .await;

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg(feature = "ethereum")]
struct EthereumTrackerApp(ApplicationWrapper<ethereum_tracker::EthereumTrackerAbi>);

#[cfg(feature = "ethereum")]
impl EthereumTrackerApp {
    async fn get_amount(&self, account_owner: &str) -> U256 {
        use ethereum_tracker::U256Cont;
        let query = format!(
            "accounts {{ entry(key: \"{}\") {{ value }} }}",
            account_owner
        );
        let response_body = self.0.query(&query).await.unwrap();
        let amount_option = serde_json::from_value::<Option<U256Cont>>(
            response_body["accounts"]["entry"]["value"].clone(),
        )
        .unwrap();
        match amount_option {
            None => U256::from(0),
            Some(value) => {
                let U256Cont { value } = value;
                value
            }
        }
    }

    async fn assert_balances(&self, accounts: impl IntoIterator<Item = (String, U256)>) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn update(&self, to_block: u64) {
        let mutation = format!("update(toBlock: {})", to_block);
        self.0.mutate(mutation).await.unwrap();
    }
}

/// Test that the node service can start with task processor options.
/// This is a basic smoke test that verifies the CLI arguments are accepted
/// and the service starts correctly with task processor configuration.
#[cfg(feature = "storage-service")]
#[test_log::test(tokio::test)]
async fn test_node_service_with_task_processor() -> Result<()> {
    use std::{io::Write, os::unix::fs::PermissionsExt};

    use linera_base::{abi::ContractAbi, identifiers::ApplicationId};

    // Dummy ABI type for the task-processor application.
    // The actual ABI doesn't matter for the GraphQL interface.
    struct TaskProcessorAbi;

    impl ContractAbi for TaskProcessorAbi {
        type Operation = ();
        type Response = ();
    }

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    // Publish and create the task-processor example application.
    let example_dir = ClientWrapper::example_path("task-processor")?;
    let app_id_str = client
        .project_publish(example_dir, vec![], None, &())
        .await?;
    let app_id: ApplicationId = app_id_str.trim().parse()?;

    // Create an echo operator script that reads stdin and writes it to stdout.
    let tmp_dir = tempfile::tempdir()?;
    let operator_path = tmp_dir.path().join("echo-operator");
    {
        let mut file = std::fs::File::create(&operator_path)?;
        writeln!(file, "#!/bin/sh")?;
        writeln!(file, "cat")?;
    }
    std::fs::set_permissions(&operator_path, std::fs::Permissions::from_mode(0o755))?;

    // Start the node service with task processor options.
    let port = get_node_port().await;
    let operators = vec![("echo".to_string(), operator_path)];
    let mut node_service = client
        .run_node_service_with_options(port, ProcessInbox::Skip, &[app_id], &operators, false)
        .await?;

    node_service.ensure_is_running()?;

    // Subscribe to notifications for the chain.
    let mut notifications = Box::pin(node_service.notifications(chain).await?);

    // Query the initial task count (should be 0).
    let app = node_service.make_application(&chain, &app_id.with_abi::<TaskProcessorAbi>())?;
    let task_count: u64 = app.query_json("taskCount").await?;
    assert_eq!(task_count, 0);

    // Submit a mutation to request a task.
    // This creates a block with the RequestTask operation.
    app.mutate(r#"requestTask(operator: "echo", input: "hello world")"#)
        .await?;

    // Wait for the block containing the RequestTask operation.
    notifications.wait_for_block(None).await?;

    // The task should now be pending. Query nextActions to verify.
    let response = app.query("nextActions(now: 0)").await?;
    let actions = &response["nextActions"];
    assert!(
        actions["execute_tasks"]
            .as_array()
            .is_some_and(|arr| !arr.is_empty()),
        "Expected at least one task in execute_tasks, got: {actions}"
    );

    // The task processor will execute the task and submit a StoreResult operation.
    // Wait for the block containing the StoreResult operation.
    notifications.wait_for_block(None).await?;

    // Check that the task was processed (task count should be 1).
    let task_count: u64 = app.query_json("taskCount").await?;
    assert_eq!(task_count, 1);

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Test that task processor outcomes are submitted in the order they were requested,
/// even when a later task finishes before an earlier one.
#[cfg(feature = "storage-service")]
#[test_log::test(tokio::test)]
async fn test_task_processor_outcome_ordering() -> Result<()> {
    use std::{io::Write, os::unix::fs::PermissionsExt, time::Duration};

    use linera_base::{abi::ContractAbi, identifiers::ApplicationId};

    struct TaskProcessorAbi;

    impl ContractAbi for TaskProcessorAbi {
        type Operation = ();
        type Response = ();
    }

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();

    // Publish and create the task-processor example application.
    let example_dir = ClientWrapper::example_path("task-processor")?;
    let app_id_str = client
        .project_publish(example_dir, vec![], None, &())
        .await?;
    let app_id: ApplicationId = app_id_str.trim().parse()?;

    // Create a slow operator that sleeps before echoing its input.
    let tmp_dir = tempfile::tempdir()?;
    let slow_path = tmp_dir.path().join("slow-operator");
    {
        let mut file = std::fs::File::create(&slow_path)?;
        writeln!(file, "#!/bin/sh")?;
        writeln!(file, "sleep 1")?;
        writeln!(file, "cat")?;
    }
    std::fs::set_permissions(&slow_path, std::fs::Permissions::from_mode(0o755))?;

    // Create a fast operator that echoes immediately.
    let fast_path = tmp_dir.path().join("fast-operator");
    {
        let mut file = std::fs::File::create(&fast_path)?;
        writeln!(file, "#!/bin/sh")?;
        writeln!(file, "cat")?;
    }
    std::fs::set_permissions(&fast_path, std::fs::Permissions::from_mode(0o755))?;

    // Start the node service with both operators.
    let port = get_node_port().await;
    let operators = vec![
        ("slow".to_string(), slow_path),
        ("fast".to_string(), fast_path),
    ];
    let mut node_service = client
        .run_node_service_with_options(port, ProcessInbox::Skip, &[app_id], &operators, false)
        .await?;

    node_service.ensure_is_running()?;

    let app = node_service.make_application(&chain, &app_id.with_abi::<TaskProcessorAbi>())?;

    // Submit both tasks in a single block: slow first, then fast.
    // The slow task takes longer but should have its result stored first.
    app.multiple_mutate(&[
        r#"requestTask(operator: "slow", input: "slow_result")"#.to_string(),
        r#"requestTask(operator: "fast", input: "fast_result")"#.to_string(),
    ])
    .await?;

    // Wait until both tasks have been processed.
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let count: u64 = app.query_json("taskCount").await?;
        if count >= 2 {
            break;
        }
    }

    let task_count: u64 = app.query_json("taskCount").await?;
    assert!(task_count >= 2, "Expected at least 2 tasks, got {task_count}");

    // Verify the results are in request order (slow first, fast second),
    // not completion order (which would be fast first).
    let results: Vec<String> = app.query_json("results").await?;
    assert!(
        results.len() >= 2,
        "Expected at least 2 results, got {results:?}"
    );
    assert_eq!(results[0], "slow_result", "First result should be from the slow task");
    assert_eq!(results[1], "fast_result", "Second result should be from the fast task");

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Test that the node service read-only mode disables mutations and prevents query-triggered operations.
#[cfg(feature = "storage-service")]
#[test_log::test(tokio::test)]
async fn test_node_service_read_only_mode() -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let (mut net, client) = config.instantiate().await?;
    let chain = client.load_wallet()?.default_chain().unwrap();
    let owner = client.get_owner().unwrap();

    // Start the node service in read-only mode.
    let port = get_node_port().await;
    let mut node_service = client
        .run_node_service_with_options(port, ProcessInbox::Skip, &[], &[], true)
        .await?;

    node_service.ensure_is_running()?;

    // Verify that queries work by checking the chain balance.
    let chain_account = Account::chain(chain);
    let balance = node_service.balance(&chain_account).await?;
    assert!(balance > Amount::ZERO, "Expected chain to have balance");

    // Verify that mutations are disabled by trying to transfer.
    // In read-only mode, the mutation type doesn't exist in the schema, so this should fail.
    let recipient = Account::new(chain, owner);
    let result = node_service
        .transfer(
            chain,
            AccountOwner::CHAIN,
            recipient,
            Amount::from_tokens(1),
        )
        .await;
    assert_matches!(result, Err(_));

    // Terminate the read-only mode service.
    node_service.terminate().await?;

    // Restart the node service without read-only mode.
    let mut node_service = client
        .run_node_service_with_options(port, ProcessInbox::Skip, &[], &[], false)
        .await?;
    node_service.ensure_is_running()?;

    // Verify that mutations now succeed by transferring from chain to owner.
    node_service
        .transfer(
            chain,
            AccountOwner::CHAIN,
            recipient,
            Amount::from_tokens(1),
        )
        .await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}
