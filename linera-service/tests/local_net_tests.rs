// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "dynamodb",
    feature = "scylladb",
    feature = "storage-service",
))]

mod common;

use std::{env, path::PathBuf, process::Command, time::Duration};

use anyhow::Result;
use common::INTEGRATION_TEST_GUARD;
use linera_base::{
    data_types::{Amount, BlockHeight},
    identifiers::{Account, AccountOwner, ChainId},
};
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use linera_service::{
    cli_wrappers::{
        local_net::{
            get_node_port, Database, LocalNet, LocalNetConfig, PathProvider, ProcessInbox,
        },
        ClientWrapper, FaucetOption, LineraNet, LineraNetConfig, Network, OnClientDrop,
    },
    faucet::ClaimOutcome,
    test_name,
};
use test_case::test_case;
#[cfg(feature = "storage-service")]
use {linera_base::port::get_free_port, linera_service::cli_wrappers::Faucet};

#[cfg(feature = "benchmark")]
fn get_fungible_account_owner(client: &ClientWrapper) -> AccountOwner {
    let owner = client.get_owner().unwrap();
    AccountOwner::User(owner)
}

/// Clears the `RUSTFLAGS` environment variable, if it was configured to make warnings fail as
/// errors.
///
/// The returned [`RestoreVarOnDrop`] restores the environment variable to its original value when
/// it is dropped.
fn override_disable_warnings_as_errors() -> Option<RestoreVarOnDrop> {
    if matches!(env::var("RUSTFLAGS"), Ok(value) if value == "-D warnings") {
        env::set_var("RUSTFLAGS", "");
        Some(RestoreVarOnDrop)
    } else {
        None
    }
}

/// Restores the `RUSTFLAGS` environment variable to make warnings fail as errors.
struct RestoreVarOnDrop;

impl Drop for RestoreVarOnDrop {
    fn drop(&mut self) {
        env::set_var("RUSTFLAGS", "-D warnings");
    }
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
    use linera_base::{crypto::KeyPair, identifiers::Owner};
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let network = config.network.external;
    let (mut net, client) = config.instantiate().await?;

    let faucet_client = net.make_client().await;
    faucet_client.wallet_init(&[], FaucetOption::None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, faucet_chain, Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    assert_eq!(faucet.current_validators().await?.len(), 4);

    let client_2 = net.make_client().await;
    client_2.wallet_init(&[], FaucetOption::None).await?;
    let chain_1 = ChainId::root(0);

    let chain_2 = client
        .open_and_assign(&client_2, Amount::from_tokens(3))
        .await?;
    let port = get_node_port().await;
    let node_service_2 = match network {
        Network::Grpc | Network::Grpcs => {
            Some(client_2.run_node_service(port, ProcessInbox::Skip).await?)
        }
        Network::Tcp | Network::Udp => None,
    };

    client.query_validators(None).await?;

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

    let address = format!("{}:localhost:{}", network.short(), LocalNet::proxy_port(4));
    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th validator
    client
        .set_validator(net.validator_name(4).unwrap(), LocalNet::proxy_port(4), 100)
        .await?;
    client.finalize_committee().await?;

    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;

    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 5);
    }

    // Add 6th validator
    client
        .set_validator(net.validator_name(5).unwrap(), LocalNet::proxy_port(5), 100)
        .await?;
    client.finalize_committee().await?;
    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 6);
    }

    // Remove 5th validator
    client
        .remove_validator(net.validator_name(4).unwrap())
        .await?;
    client.finalize_committee().await?;
    net.remove_validator(4)?;
    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 5);
    }
    client.query_validators(None).await?;
    client.query_validators(Some(chain_1)).await?;
    if let Some(service) = &node_service_2 {
        service.process_inbox(&chain_2).await?;
    } else {
        client_2.process_inbox(chain_2).await?;
    }

    // Remove the first 4 validators, so only the last one remains.
    for i in 0..4 {
        let name = net.validator_name(i).unwrap();
        client.remove_validator(name).await?;
        client.finalize_committee().await?;
        if let Some(service) = &node_service_2 {
            service.process_inbox(&chain_2).await?;
        } else {
            client_2.process_inbox(chain_2).await?;
        }
        net.remove_validator(i)?;
    }

    let recipient = AccountOwner::User(Owner::from(KeyPair::generate().public()));
    client
        .transfer_with_accounts(
            Amount::from_tokens(5),
            Account::chain(chain_1),
            Account::owner(chain_2, recipient),
        )
        .await?;

    if let Some(mut node_service_2) = node_service_2 {
        node_service_2.process_inbox(&chain_2).await?;
        let query = format!(
            "query {{ chain(chainId:\"{chain_2}\") {{
                executionState {{ system {{ balances {{
                    entry(key:\"{recipient}\") {{ value }}
                }} }} }}
            }} }}"
        );
        let response = node_service_2.query_node(query.clone()).await?;
        let balances = &response["chain"]["executionState"]["system"]["balances"];
        assert_eq!(balances["entry"]["value"].as_str(), Some("5."));

        let query = format!(
            "query {{ chain(chainId:\"{chain_2}\") {{
                executionState {{ system {{ committees }} }}
            }} }}"
        );
        let response = node_service_2.query_node(query.clone()).await?;
        let committees = &response["chain"]["executionState"]["system"]["committees"];
        let epochs = committees.as_object().unwrap().keys().collect::<Vec<_>>();
        assert_eq!(&epochs, &["7"]);

        node_service_2.ensure_is_running()?;
    } else {
        client_2.sync(chain_2).await?;
        client_2.process_inbox(chain_2).await?;
        assert_eq!(
            client_2
                .local_balance(Account::owner(chain_2, recipient))
                .await?,
            Amount::from_tokens(5),
        );
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
    faucet_client.wallet_init(&[], FaucetOption::None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, faucet_chain, Amount::from_tokens(2))
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

    let address = format!("{}:localhost:{}", network.short(), LocalNet::proxy_port(4));
    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th validator to the network
    client
        .set_validator(net.validator_name(4).unwrap(), LocalNet::proxy_port(4), 100)
        .await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch
    faucet_client.process_inbox(faucet_chain).await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, faucet_chain, Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 5);
    }

    // Create a new chain starting on the new epoch
    let new_owner = client.keygen().await?;
    let ClaimOutcome {
        chain_id,
        message_id,
        ..
    } = faucet.claim(&new_owner).await?;
    client.assign(new_owner, message_id).await?;

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
    faucet_client.wallet_init(&[], FaucetOption::None).await?;

    let faucet_chain = client
        .open_and_assign(&faucet_client, Amount::from_tokens(1_000u128))
        .await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, faucet_chain, Amount::from_tokens(2))
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

    let address = format!("{}:localhost:{}", network.short(), LocalNet::proxy_port(4));
    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 5th validator to the network
    client
        .set_validator(net.validator_name(4).unwrap(), LocalNet::proxy_port(4), 100)
        .await?;
    client.finalize_committee().await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch
    faucet_client.process_inbox(faucet_chain).await?;

    if matches!(network, Network::Grpc) {
        let mut faucet_service = faucet_client
            .run_faucet(None, faucet_chain, Amount::from_tokens(2))
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

    let address = format!("{}:localhost:{}", network.short(), LocalNet::proxy_port(5));
    assert_eq!(
        client.query_validator(&address).await?,
        net.genesis_config()?.hash()
    );

    // Add 6th validator to the network
    client
        .set_validator(net.validator_name(5).unwrap(), LocalNet::proxy_port(5), 100)
        .await?;

    client.query_validators(None).await?;

    // Ensure the faucet is on the new epoch
    faucet_client.process_inbox(faucet_chain).await?;

    let mut faucet_service = faucet_client
        .run_faucet(None, faucet_chain, Amount::from_tokens(2))
        .await?;

    faucet_service.ensure_is_running()?;

    let faucet = faucet_service.instance();

    if matches!(network, Network::Grpc) {
        assert_eq!(faucet.current_validators().await?.len(), 6);
    }

    // Create a new chain starting on the new epoch
    let new_owner = client.keygen().await?;
    let ClaimOutcome {
        chain_id,
        message_id,
        ..
    } = faucet.claim(&new_owner).await?;
    client.assign(new_owner, message_id).await?;

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

    let client2 = net.make_client().await;
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain], FaucetOption::None).await?;

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
    net.start_validator(0).await?;

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1
                .transfer(Amount::from_tokens(1), chain, ChainId::root(1))
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

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_pending_block(config: LocalNetConfig) -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    // Create runner and client.
    let (mut net, client) = config.instantiate().await?;
    let chain_id = client.load_wallet()?.default_chain().unwrap();
    let account = Account::chain(chain_id);
    let balance = client.local_balance(account).await?;
    // Stop validators.
    for i in 0..4 {
        net.remove_validator(i)?;
    }
    let result = client
        .transfer_with_silent_logs(Amount::from_tokens(2), chain_id, ChainId::root(1))
        .await;
    assert!(result.is_err());
    // The transfer didn't get confirmed.
    assert_eq!(client.local_balance(account).await?, balance);
    // Restart validators.
    for i in 0..4 {
        net.start_validator(i).await?;
    }
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result?.is_some());
    client.sync(chain_id).await?;
    // After retrying, the transfer got confirmed.
    assert!(client.local_balance(account).await? <= balance - Amount::from_tokens(2));
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result?.is_none());

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

    let _rustflags_override = override_disable_warnings_as_errors();
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
    let chain = client.load_wallet()?.default_chain().unwrap();

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    assert_eq!(
        node_service.try_get_applications_uri(&chain).await?.len(),
        1
    );

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
    let chain = client.load_wallet()?.default_chain().unwrap();

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    assert_eq!(
        node_service.try_get_applications_uri(&chain).await?.len(),
        1
    );

    node_service.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_project_new() -> Result<()> {
    let _rustflags_override = override_disable_warnings_as_errors();
    let path_provider = PathProvider::create_temporary_directory()?;
    let id = 0;
    let client = ClientWrapper::new(
        path_provider,
        Network::Grpc,
        None,
        id,
        OnClientDrop::LeakChains,
    );
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await?;
    let project_dir = tmp_dir.path().join("init-test");
    client
        .build_application(project_dir.as_path(), "init-test", false)
        .await?;

    let mut child = Command::new("cargo")
        .args(["fmt", "--check"])
        .current_dir(project_dir.as_path())
        .spawn()?;
    assert!(child.wait()?.success());

    let mut child = Command::new("cargo")
        .arg("test")
        .current_dir(project_dir.as_path())
        .spawn()?;
    assert!(child.wait()?.success());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_project_test() -> Result<()> {
    let path_provider = PathProvider::create_temporary_directory()?;
    let id = 0;
    let client = ClientWrapper::new(
        path_provider,
        Network::Grpc,
        None,
        id,
        OnClientDrop::LeakChains,
    );
    client
        .project_test(&ClientWrapper::example_path("counter")?)
        .await?;

    Ok(())
}

/// Test if the wallet file is correctly locked when used.
#[cfg(feature = "storage-service")]
#[test_log::test(tokio::test)]
async fn test_storage_service_wallet_lock() -> Result<()> {
    use std::mem::drop;

    use linera_client::config::WalletState;
    let config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let wallet_state = WalletState::read_from_file(client.wallet_path().as_path())?;
    let chain_id = wallet_state.default_chain().unwrap();

    let lock = wallet_state;
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
        "--with-faucet-chain",
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
    for line in &mut lines {
        let line = line?;
        if line.starts_with("READY!") {
            is_ready = true;
            break;
        }
    }
    assert!(is_ready, "Unexpected EOF for stderr");

    // Echo faucet stderr for debugging and to empty the buffer.
    std::thread::spawn(move || {
        for line in lines {
            let line = line.unwrap();
            eprintln!("{}", line);
        }
    });

    let mut exports = stdout.lines();
    assert!(exports
        .next()
        .unwrap()?
        .starts_with("export LINERA_WALLET="));
    assert!(exports
        .next()
        .unwrap()?
        .starts_with("export LINERA_STORAGE="));
    assert_eq!(exports.next().unwrap()?, "");

    // Test faucet.
    let faucet = Faucet::new(format!("http://localhost:{}/", port));
    faucet.version_info().await.unwrap();

    // Send SIGINT to the child process.
    Command::new("kill")
        .args(["-s", "INT", &child.id().to_string()])
        .output()?;

    assert!(exports.next().is_none());
    assert!(child.wait()?.success());
    return Ok(());
}

#[cfg(feature = "benchmark")]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc) ; "storage_service_grpc"))]
#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Tcp) ; "storage_service_tcp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_benchmark(mut config: LocalNetConfig) -> Result<()> {
    use std::collections::BTreeMap;

    use fungible::{FungibleTokenAbi, InitialState, Parameters};

    config.num_other_initial_chains = 2;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    assert_eq!(client.load_wallet()?.num_chains(), 3);
    // Launch local benchmark using some additional chains.
    client.benchmark(2, 4, 10, None).await?;
    assert_eq!(client.load_wallet()?.num_chains(), 7);

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
            &params,
            &state,
            &[],
            None,
        )
        .await?;
    client.benchmark(2, 5, 10, Some(application_id)).await?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

/// Tests if the `sync-validator` command uploads missing certificates to a validator.
// TODO: Fix test for simple-net
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

    let (mut net, client) = config.instantiate().await?;

    // Stop a validator to force it to lag behind the others
    net.stop_validator(0).await?;

    // Create some blocks
    let sender_chain = client.default_chain().expect("Client has no default chain");
    let (_, receiver_chain) = client
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
    net.start_validator(0).await?;

    let lagging_validator = net.validator_client(0).await?;

    let state_before_sync = lagging_validator
        .handle_chain_info_query(ChainInfoQuery::new(sender_chain))
        .await?;
    assert_eq!(state_before_sync.info.next_block_height, BlockHeight::ZERO);

    // Synchronize the validator
    let validator_name = net
        .validator_name(0)
        .expect("Missing name for the first validator")
        .parse()?;
    client
        .sync_validator([&sender_chain], validator_name)
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
