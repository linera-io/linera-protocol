// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "rocksdb", feature = "aws", feature = "scylladb"))]

mod common;

use common::INTEGRATION_TEST_GUARD;
use linera_base::{data_types::Amount, identifiers::ChainId};
#[cfg(feature = "kubernetes")]
use linera_service::cli_wrappers::local_kubernetes_net::LocalKubernetesNetTestingConfig;
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNet, LocalNetConfig, LocalNetTestingConfig},
        ClientWrapper, LineraNet, LineraNetConfig, Network,
    },
    util,
};
use linera_views::test_utils::get_table_name;
use std::{path::PathBuf, sync::Arc, time::Duration};
use test_case::test_case;

#[tokio::test]
async fn test_resolve_binary() {
    util::resolve_binary("linera", env!("CARGO_PKG_NAME"))
        .await
        .unwrap();
    util::resolve_binary("linera-proxy", env!("CARGO_PKG_NAME"))
        .await
        .unwrap();
    assert!(
        util::resolve_binary("linera-spaceship", env!("CARGO_PKG_NAME"))
            .await
            .is_err()
    );
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Tcp) ; "rocksdb_tcp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Tcp) ; "scylladb_tcp"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Tcp) ; "aws_tcp"))]
#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Udp) ; "rocksdb_udp"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Udp) ; "scylladb_udp"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Udp) ; "aws_udp"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration(config: LocalNetTestingConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let network = config.network;
    let (mut net, client) = config.instantiate().await.unwrap();

    let client_2 = net.make_client().await;
    client_2.wallet_init(&[], None).await.unwrap();
    let chain_1 = client.get_wallet().unwrap().default_chain().unwrap();

    let (node_service_2, chain_2) = match network {
        Network::Grpc => {
            let chain_2 = client.open_and_assign(&client_2).await.unwrap();
            let node_service_2 = client_2.run_node_service(8081).await.unwrap();
            (Some(node_service_2), chain_2)
        }
        _ => {
            client
                .transfer(Amount::from_tokens(10), ChainId::root(9), ChainId::root(8))
                .await
                .unwrap();
            (None, ChainId::root(9))
        }
    };

    client.query_validators(None).await.unwrap();

    // Query balance for first and last user chain
    assert_eq!(
        client.query_balance(chain_1).await.unwrap(),
        Amount::from_tokens(10)
    );
    assert_eq!(
        client.query_balance(chain_2).await.unwrap(),
        Amount::from_tokens(0)
    );

    // Transfer 3 units
    client
        .transfer(Amount::from_tokens(3), chain_1, chain_2)
        .await
        .unwrap();

    // Restart the first shard for the 4th validator.
    net.terminate_server(3, 0).await.unwrap();
    net.start_server(3, 0).await.unwrap();

    // Query balances again
    assert_eq!(
        client.query_balance(chain_1).await.unwrap(),
        Amount::from_tokens(7)
    );
    assert_eq!(
        client.query_balance(chain_2).await.unwrap(),
        Amount::from_tokens(3)
    );

    #[cfg(benchmark)]
    {
        // Launch local benchmark using all user chains
        client.benchmark(500).await;
    }

    // Create derived chain
    let (_, chain_3) = client.open_chain(chain_1, None).await.unwrap();

    // Inspect state of derived chain
    assert!(client.is_chain_present_in_wallet(chain_3).await);

    // Create configurations for two more validators
    net.generate_validator_config(4).await.unwrap();
    net.generate_validator_config(5).await.unwrap();

    // Start the validators
    net.start_validator(4).await.unwrap();
    net.start_validator(5).await.unwrap();

    // Add 5th validator
    client
        .set_validator(net.validator_name(4).unwrap(), LocalNet::proxy_port(4), 100)
        .await
        .unwrap();

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Add 6th validator
    client
        .set_validator(net.validator_name(5).unwrap(), LocalNet::proxy_port(5), 100)
        .await
        .unwrap();

    // Remove 5th validator
    client
        .remove_validator(net.validator_name(4).unwrap())
        .await
        .unwrap();
    net.remove_validator(4).unwrap();

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Remove the first 4 validators, so only the last one remains.
    for i in 0..4 {
        let name = net.validator_name(i).unwrap();
        client.remove_validator(name).await.unwrap();
        net.remove_validator(i).unwrap();
    }

    client
        .transfer(Amount::from_tokens(5), chain_1, chain_2)
        .await
        .unwrap();
    client.synchronize_balance(chain_2).await.unwrap();
    assert_eq!(
        client.query_balance(chain_2).await.unwrap(),
        Amount::from_tokens(8)
    );

    if let Some(node_service_2) = node_service_2 {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = node_service_2
                .query_node(format!(
                    "query {{ chain(chainId:\"{chain_2}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
                ))
                .await
                .unwrap();
            if response["chain"]["executionState"]["system"]["balance"].as_str() == Some("8.") {
                return;
            }
        }
        panic!("Failed to receive new block");
    }

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_open_chain_node_service(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let (mut net, client) = config.instantiate().await.unwrap();

    let default_chain = client.get_wallet().unwrap().default_chain().unwrap();
    let public_key = client
        .get_wallet()
        .unwrap()
        .get(default_chain)
        .unwrap()
        .key_pair
        .as_ref()
        .unwrap()
        .public();

    let node_service = client.run_node_service(8080).await.unwrap();

    // Open a new chain with the same public key.
    // The node service should automatically create a client for it internally.
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{default_chain}\", \
            publicKey:\"{public_key}\"\
        ) }}"
    );
    node_service.query_node(query).await.unwrap();

    // Open another new chain.
    // This is a regression test; a PR had to be reverted because this was hanging:
    // https://github.com/linera-io/linera-protocol/pull/899
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{default_chain}\", \
            publicKey:\"{public_key}\"\
        ) }}"
    );
    let data = node_service.query_node(query).await.unwrap();
    let new_chain: ChainId = serde_json::from_value(data["openChain"].clone()).unwrap();

    // Send 8 tokens to the new chain.
    let query = format!(
        "mutation {{ transfer(\
            chainId:\"{default_chain}\", \
            recipient: {{ Account: {{ chain_id:\"{new_chain}\" }} }}, \
            amount:\"8\"\
        ) }}"
    );
    node_service.query_node(query).await.unwrap();

    // Send 4 tokens back.
    let query = format!(
        "mutation {{ transfer(\
            chainId:\"{new_chain}\", \
            recipient: {{ Account: {{ chain_id:\"{default_chain}\" }} }}, \
            amount:\"4\"\
        ) }}"
    );
    node_service.query_node(query).await.unwrap();

    // Verify that the default chain now has 6 and the new one has 4 tokens.
    for i in 0..10 {
        tokio::time::sleep(Duration::from_secs(i)).await;
        let response1 = node_service
            .query_node(format!(
                "query {{ chain(chainId:\"{default_chain}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
            ))
            .await
            .unwrap();
        let response2 = node_service
            .query_node(format!(
                "query {{ chain(chainId:\"{new_chain}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
            ))
            .await
            .unwrap();
        if response1["chain"]["executionState"]["system"]["balance"].as_str() == Some("6.")
            && response2["chain"]["executionState"]["system"]["balance"].as_str() == Some("4.")
        {
            net.ensure_is_running().await.unwrap();
            net.terminate().await.unwrap();
            return;
        }
    }
    panic!("Failed to receive new block");
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_notification_stream(config: LocalNetTestingConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain], None).await.unwrap();

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();
    let response = node_service2
        .query_node(format!(
            "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
        ))
        .await
        .unwrap();
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The first validator has an outage and gets restarted!
    net.remove_validator(0).unwrap();
    net.start_validator(0).await.unwrap();

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1
                .transfer(Amount::from_tokens(1), chain, ChainId::root(9))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(i)).await;
            height += 1;
            let response = node_service2
                .query_node(format!(
                    "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
                ))
                .await
                .unwrap();
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(all(feature = "rocksdb", not(feature = "kubernetes")), test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(all(feature = "scylladb", not(feature = "kubernetes")), test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(all(feature = "aws", not(feature = "kubernetes")), test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(LocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_scylladb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create net and two clients.
    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    // Get some chain owned by Client 1.
    let chain1 = *client1.get_wallet().unwrap().chain_ids().first().unwrap();

    // Generate a key for Client 2.
    let client2_key = client2.keygen().await.unwrap();

    // Open chain on behalf of Client 2.
    let (message_id, chain2) = client1.open_chain(chain1, Some(client2_key)).await.unwrap();

    // Assign chain2 to client2_key.
    assert_eq!(
        chain2,
        client2.assign(client2_key, message_id).await.unwrap()
    );

    // Check initial balance of Chain 1.
    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(10)
    );

    // Transfer 5 units from Chain 1 to Chain 2.
    client1
        .transfer(Amount::from_tokens(5), chain1, chain2)
        .await
        .unwrap();
    client2.synchronize_balance(chain2).await.unwrap();

    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(5)
    );
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(5)
    );

    // Transfer 2 units from Chain 2 to Chain 1.
    client2
        .transfer(Amount::from_tokens(2), chain2, chain1)
        .await
        .unwrap();
    client1.synchronize_balance(chain1).await.unwrap();

    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(7)
    );
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(3)
    );

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_project_new() {
    let tmp_dir = Arc::new(tempfile::tempdir().unwrap());
    let client = ClientWrapper::new(tmp_dir, Network::Grpc, None, 0);
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");
    client
        .build_application(project_dir.as_path(), "init-test", false)
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_project_test() {
    let tmp_dir = Arc::new(tempfile::tempdir().unwrap());
    let client = ClientWrapper::new(tmp_dir, Network::Grpc, None, 0);
    client
        .project_test(&ClientWrapper::example_path("counter").unwrap())
        .await
        .unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(Database::RocksDb, Network::Grpc ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_project_publish(database: Database, network: Network) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let table_name = get_table_name();
    let config = LocalNetConfig {
        database,
        network,
        testing_prng_seed: Some(37),
        table_name,
        num_initial_validators: 1,
        num_shards: 1,
    };

    let (mut net, client) = config.instantiate().await.unwrap();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");

    client
        .project_publish(project_dir, vec![], None, &())
        .await
        .unwrap();
    let chain = client.get_wallet().unwrap().default_chain().unwrap();

    let node_service = client.run_node_service(None).await.unwrap();

    assert_eq!(
        node_service
            .try_get_applications_uri(&chain)
            .await
            .unwrap()
            .len(),
        1
    );

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_linera_net_up_simple() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let mut command = Command::new(env!("CARGO_BIN_EXE_linera"));
    command.args(["net", "up"]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = BufReader::new(child.stderr.take().unwrap());

    for line in stderr.lines() {
        let line = line.unwrap();
        if line.starts_with("READY!") {
            let mut exports = stdout.lines();
            assert!(exports
                .next()
                .unwrap()
                .unwrap()
                .starts_with("export LINERA_WALLET="));
            assert!(exports
                .next()
                .unwrap()
                .unwrap()
                .starts_with("export LINERA_STORAGE="));
            assert_eq!(exports.next().unwrap().unwrap(), "");

            // Send SIGINT to the child process.
            Command::new("kill")
                .args(["-s", "INT", &child.id().to_string()])
                .output()
                .unwrap();

            assert!(exports.next().is_none());
            assert!(child.wait().unwrap().success());
            return;
        }
    }
    panic!("Unexpected EOF for stderr");
}

#[cfg_attr(feature = "rocksdb", test_case(Database::RocksDb, Network::Grpc ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(Database::ScyllaDb, Network::Grpc ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(Database::DynamoDb, Network::Grpc ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_example_publish(database: Database, network: Network) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let table_name = get_table_name();
    let config = LocalNetConfig {
        database,
        network,
        testing_prng_seed: Some(37),
        table_name,
        num_initial_validators: 1,
        num_shards: 1,
    };
    let (mut net, client) = config.instantiate().await.unwrap();

    let example_dir = ClientWrapper::example_path("counter").unwrap();
    client
        .project_publish(example_dir, vec![], None, &0)
        .await
        .unwrap();
    let chain = client.get_wallet().unwrap().default_chain().unwrap();

    let node_service = client.run_node_service(None).await.unwrap();

    assert_eq!(
        node_service
            .try_get_applications_uri(&chain)
            .await
            .unwrap()
            .len(),
        1
    );

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_open_multi_owner_chain(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = *client1.get_wallet().unwrap().chain_ids().first().unwrap();

    // Generate keys for both clients.
    let client1_key = client1.keygen().await.unwrap();
    let client2_key = client2.keygen().await.unwrap();

    // Open chain on behalf of Client 2.
    let (message_id, chain2) = client1
        .open_multi_owner_chain(
            chain1,
            vec![client1_key, client2_key],
            vec![100, 100],
            u32::MAX,
        )
        .await
        .unwrap();

    // Assign chain2 to client1_key.
    assert_eq!(
        chain2,
        client1.assign(client1_key, message_id).await.unwrap()
    );

    // Assign chain2 to client2_key.
    assert_eq!(
        chain2,
        client2.assign(client2_key, message_id).await.unwrap()
    );

    // Transfer 6 units from Chain 1 to Chain 2.
    client1
        .transfer(Amount::from_tokens(6), chain1, chain2)
        .await
        .unwrap();
    client2.synchronize_balance(chain2).await.unwrap();

    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(4)
    );
    assert_eq!(
        client1.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(6)
    );
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(6)
    );

    // Transfer 2 + 1 units from Chain 2 to Chain 1 using both clients.
    client2
        .transfer(Amount::from_tokens(2), chain2, chain1)
        .await
        .unwrap();
    client1
        .transfer(Amount::from_tokens(1), chain2, chain1)
        .await
        .unwrap();
    client1.synchronize_balance(chain1).await.unwrap();
    client2.synchronize_balance(chain2).await.unwrap();

    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(7)
    );
    assert_eq!(
        client1.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(3)
    );
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(3)
    );

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_assign_greatgrandchild_chain(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = *client1.get_wallet().unwrap().chain_ids().first().unwrap();

    // Generate keys for client 2.
    let client2_key = client2.keygen().await.unwrap();

    // Open a great-grandchild chain on behalf of client 2.
    let (_, grandparent) = client1.open_chain(chain1, None).await.unwrap();
    let (_, parent) = client1.open_chain(grandparent, None).await.unwrap();
    let (message_id, chain2) = client1.open_chain(parent, Some(client2_key)).await.unwrap();
    client2.assign(client2_key, message_id).await.unwrap();

    // Transfer 6 units from Chain 1 to Chain 2.
    client1
        .transfer(Amount::from_tokens(6), chain1, chain2)
        .await
        .unwrap();
    client2.synchronize_balance(chain2).await.unwrap();
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(6)
    );

    // Transfer 2 units from Chain 2 to Chain 1.
    client2
        .transfer(Amount::from_tokens(2), chain2, chain1)
        .await
        .unwrap();
    client1.synchronize_balance(chain1).await.unwrap();
    client2.synchronize_balance(chain2).await.unwrap();
    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(6)
    );
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(4)
    );

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_faucet(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();

    // Generate keys for client 2.
    let client2_key = client2.keygen().await.unwrap();

    let mut faucet = client1
        .run_faucet(None, chain1, Amount::from_tokens(2))
        .await
        .unwrap();
    let outcome = faucet.claim(&client2_key).await.unwrap();
    let chain2 = outcome.chain_id;
    let message_id = outcome.message_id;

    // Use the faucet directly to initialize client 3.
    let client3 = net.make_client().await;
    let outcome = client3.wallet_init(&[], Some(&faucet.url())).await.unwrap();
    let chain3 = outcome.unwrap().chain_id;
    assert_eq!(
        chain3,
        client3.get_wallet().unwrap().default_chain().unwrap()
    );

    faucet.ensure_is_running().unwrap();
    faucet.terminate().await.unwrap();

    // Chain 1 should have transferred four tokens, two to each child. So it should have six left.
    client1.synchronize_balance(chain1).await.unwrap();
    assert_eq!(
        client1.query_balance(chain1).await.unwrap(),
        Amount::from_tokens(6)
    );

    // Assign chain2 to client2_key.
    assert_eq!(
        chain2,
        client2.assign(client2_key, message_id).await.unwrap()
    );

    // Clients 2 and 3 should have the tokens, and own the chain.
    client2.synchronize_balance(chain2).await.unwrap();
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(2)
    );
    client2
        .transfer(Amount::from_tokens(1), chain2, chain1)
        .await
        .unwrap();
    assert_eq!(
        client2.query_balance(chain2).await.unwrap(),
        Amount::from_tokens(1)
    );

    client3.synchronize_balance(chain3).await.unwrap();
    assert_eq!(
        client3.query_balance(chain3).await.unwrap(),
        Amount::from_tokens(2)
    );
    client3
        .transfer(Amount::from_tokens(2), chain3, chain1)
        .await
        .unwrap();
    assert_eq!(client3.query_balance(chain3).await.unwrap(), Amount::ZERO);
    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_pending_block(config: LocalNetTestingConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    // Create runner and client.
    let (mut net, client) = config.instantiate().await.unwrap();
    let chain_id = client.get_wallet().unwrap().default_chain().unwrap();
    assert_eq!(
        client.query_balance(chain_id).await.unwrap(),
        Amount::from_tokens(10)
    );
    // Stop validators.
    for i in 0..4 {
        net.remove_validator(i).unwrap();
    }
    let result = client
        .transfer(Amount::from_tokens(2), chain_id, ChainId::root(5))
        .await;
    assert!(result.is_err());
    assert_eq!(
        client.query_balance(chain_id).await.unwrap(),
        Amount::from_tokens(10)
    );
    // Restart validators.
    for i in 0..4 {
        net.start_validator(i).await.unwrap();
    }
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result.unwrap().is_some());
    assert_eq!(
        client.synchronize_balance(chain_id).await.unwrap(),
        Amount::from_tokens(8)
    );
    let result = client.retry_pending_block(Some(chain_id)).await;
    assert!(result.unwrap().is_none());

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}
