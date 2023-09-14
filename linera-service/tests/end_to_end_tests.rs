// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::INTEGRATION_TEST_GUARD;
use linera_base::identifiers::ChainId;
use linera_service::client::{Database, LocalNetwork, Network};
use std::time::Duration;

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_reconfiguration_grpc() {
    run_end_to_end_reconfiguration(Database::RocksDb, Network::Grpc).await;
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_reconfiguration_grpc() {
    run_end_to_end_reconfiguration(Database::DynamoDb, Network::Grpc).await;
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_reconfiguration_grpc() {
    run_end_to_end_reconfiguration(Database::ScyllaDb, Network::Grpc).await;
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_reconfiguration_simple() {
    run_end_to_end_reconfiguration(Database::RocksDb, Network::Simple).await;
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_reconfiguration_simple() {
    run_end_to_end_reconfiguration(Database::DynamoDb, Network::Simple).await;
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_reconfiguration_simple() {
    run_end_to_end_reconfiguration(Database::ScyllaDb, Network::Simple).await;
}

#[cfg(any(feature = "aws", feature = "rocksdb", feature = "scylladb"))]
async fn run_end_to_end_reconfiguration(database: Database, network: Network) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let mut local_net = LocalNetwork::new(database, network, 4).unwrap();
    let client = local_net.make_client(network);
    let client_2 = local_net.make_client(network);

    let servers = local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    client_2.wallet_init(&[]).await.unwrap();
    local_net.run().await.unwrap();

    let chain_1 = client.get_wallet().default_chain().unwrap();

    let (node_service_2, chain_2) = match network {
        Network::Grpc => {
            let chain_2 = client.open_and_assign(&client_2).await.unwrap();
            let node_service_2 = client_2.run_node_service(8081).await.unwrap();
            (Some(node_service_2), chain_2)
        }
        Network::Simple => {
            client
                .transfer("10", ChainId::root(9), ChainId::root(8))
                .await
                .unwrap();
            (None, ChainId::root(9))
        }
    };

    client.query_validators(None).await.unwrap();

    // Query balance for first and last user chain
    assert_eq!(client.query_balance(chain_1).await.unwrap(), "10.");
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "0.");

    // Transfer 3 units
    client.transfer("3", chain_1, chain_2).await.unwrap();

    // Restart last server (dropping it kills the process)
    local_net.kill_server(4, 3).unwrap();
    local_net.start_server(4, 3).await.unwrap();

    // Query balances again
    assert_eq!(client.query_balance(chain_1).await.unwrap(), "7.");
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "3.");

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
    let server_5 = local_net.generate_validator_config(5).await.unwrap();
    let server_6 = local_net.generate_validator_config(6).await.unwrap();

    // Start the validators
    local_net.start_validators(5..=6).await.unwrap();

    // Add validator 5
    client.set_validator(&server_5, 9500, 100).await.unwrap();

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Add validator 6
    client.set_validator(&server_6, 9600, 100).await.unwrap();

    // Remove validator 5
    client.remove_validator(&server_5).await.unwrap();
    local_net.remove_validator(5).unwrap();

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Remove validators 1, 2, 3 and 4, so only 6 remains.
    for (i, server) in servers.into_iter().enumerate() {
        client.remove_validator(&server).await.unwrap();
        local_net.remove_validator(i + 1).unwrap();
    }

    client.transfer("5", chain_1, chain_2).await.unwrap();
    client.synchronize_balance(chain_2).await.unwrap();
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "8.");

    if let Some(node_service_2) = node_service_2 {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = node_service_2
                .query_node(&format!(
                    "query {{ chain(chainId:\"{chain_2}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
                ))
                .await;
            if response["chain"]["executionState"]["system"]["balance"].as_str() == Some("8.") {
                return;
            }
        }
        panic!("Failed to receive new block");
    }
}

#[test_log::test(tokio::test)]
async fn test_memory_open_chain_node_service() {
    run_open_chain_node_service(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_open_chain_node_service() {
    run_open_chain_node_service(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_open_chain_node_service() {
    run_open_chain_node_service(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_open_chain_node_service() {
    run_open_chain_node_service(Database::ScyllaDb).await
}

async fn run_open_chain_node_service(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 4).unwrap();
    let client = local_net.make_client(network);
    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    local_net.run().await.unwrap();

    let default_chain = client.get_wallet().default_chain().unwrap();
    let public_key = client
        .get_wallet()
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
    node_service.query_node(&query).await;

    // Open another new chain.
    // This is a regression test; a PR had to be reverted because this was hanging:
    // https://github.com/linera-io/linera-protocol/pull/899
    let query = format!(
        "mutation {{ openChain(\
            chainId:\"{default_chain}\", \
            publicKey:\"{public_key}\"\
        ) }}"
    );
    let data = node_service.query_node(&query).await;
    let new_chain: ChainId = serde_json::from_value(data["openChain"].clone()).unwrap();

    // Send 8 tokens to the new chain.
    let query = format!(
        "mutation {{ transfer(\
            chainId:\"{default_chain}\", \
            recipient: {{ Account: {{ chain_id:\"{new_chain}\" }} }}, \
            amount:\"8\"\
        ) }}"
    );
    node_service.query_node(&query).await;

    // Send 4 tokens back.
    let query = format!(
        "mutation {{ transfer(\
            chainId:\"{new_chain}\", \
            recipient: {{ Account: {{ chain_id:\"{default_chain}\" }} }}, \
            amount:\"4\"\
        ) }}"
    );
    node_service.query_node(&query).await;

    // Verify that the default chain now has 6 and the new one has 4 tokens.
    for i in 0..10 {
        tokio::time::sleep(Duration::from_secs(i)).await;
        let response1 = node_service
            .query_node(&format!(
                "query {{ chain(chainId:\"{default_chain}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
            ))
            .await;
        let response2 = node_service
            .query_node(&format!(
                "query {{ chain(chainId:\"{new_chain}\") \
                    {{ executionState {{ system {{ balance }} }} }} }}"
            ))
            .await;
        if response1["chain"]["executionState"]["system"]["balance"].as_str() == Some("6.")
            && response2["chain"]["executionState"]["system"]["balance"].as_str() == Some("4.")
        {
            return;
        }
    }
    panic!("Failed to receive new block");
}

#[test_log::test(tokio::test)]
async fn test_memory_end_to_end_retry_notification_stream() {
    run_end_to_end_retry_notification_stream(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_retry_notification_stream() {
    run_end_to_end_retry_notification_stream(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_retry_notification_stream() {
    run_end_to_end_retry_notification_stream(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_retry_notification_stream() {
    run_end_to_end_retry_notification_stream(Database::ScyllaDb).await
}

async fn run_end_to_end_retry_notification_stream(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 1).unwrap();
    let client1 = local_net.make_client(network);
    let client2 = local_net.make_client(network);

    // Create initial server and client config.
    local_net.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain]).await.unwrap();

    // Start local network.
    local_net.run().await.unwrap();

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();
    let response = node_service2
        .query_node(&format!(
            "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
        ))
        .await;
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The validator has an outage and gets restarted!
    local_net.remove_validator(1).unwrap();
    local_net.start_validators(1..=1).await.unwrap();

    // The node service should try to reconnect.
    'success: {
        for i in 0..10 {
            // Add a new block on the chain, triggering a notification.
            client1
                .transfer("1", chain, ChainId::root(9))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(i)).await;
            height += 1;
            let response = node_service2
                .query_node(&format!(
                    "query {{ chain(chainId:\"{chain}\") {{ tipState {{ nextBlockHeight }} }} }}"
                ))
                .await;
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.assert_is_running();
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_multiple_wallets() {
    run_end_to_end_multiple_wallets(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_multiple_wallets() {
    run_end_to_end_multiple_wallets(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_multiple_wallets() {
    run_end_to_end_multiple_wallets(Database::ScyllaDb).await
}

#[cfg(any(feature = "aws", feature = "rocksdb", feature = "scylladb"))]
async fn run_end_to_end_multiple_wallets(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create local_net and two clients.
    let mut local_net = LocalNetwork::new(database, Network::Grpc, 4).unwrap();
    let client_1 = local_net.make_client(Network::Grpc);
    let client_2 = local_net.make_client(Network::Grpc);

    // Create initial server and client config.
    local_net.generate_initial_validator_config().await.unwrap();
    client_1.create_genesis_config().await.unwrap();
    client_2.wallet_init(&[]).await.unwrap();

    // Start local network.
    local_net.run().await.unwrap();

    // Get some chain owned by Client 1.
    let chain_1 = *client_1.get_wallet().chain_ids().first().unwrap();

    // Generate a key for Client 2.
    let client_2_key = client_2.keygen().await.unwrap();

    // Open chain on behalf of Client 2.
    let (message_id, chain_2) = client_1
        .open_chain(chain_1, Some(client_2_key))
        .await
        .unwrap();

    // Assign chain_2 to client_2_key.
    assert_eq!(
        chain_2,
        client_2.assign(client_2_key, message_id).await.unwrap()
    );

    // Check initial balance of Chain 1.
    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "10.");

    // Transfer 5 units from Chain 1 to Chain 2.
    client_1.transfer("5", chain_1, chain_2).await.unwrap();
    client_2.synchronize_balance(chain_2).await.unwrap();

    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "5.");
    assert_eq!(client_2.query_balance(chain_2).await.unwrap(), "5.");

    // Transfer 2 units from Chain 2 to Chain 1.
    client_2.transfer("2", chain_2, chain_1).await.unwrap();
    client_1.synchronize_balance(chain_1).await.unwrap();

    assert_eq!(client_1.query_balance(chain_1).await.unwrap(), "7.");
    assert_eq!(client_2.query_balance(chain_2).await.unwrap(), "3.");
}

#[test_log::test(tokio::test)]
async fn test_memory_project_new() {
    run_project_new(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_project_new() {
    run_project_new(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_project_new() {
    run_project_new(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_project_new() {
    run_project_new(Database::ScyllaDb).await
}

async fn run_project_new(database: Database) {
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 0).unwrap();
    let client = local_net.make_client(network);

    let tmp_dir = client.project_new("init-test").await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");
    local_net
        .build_application(project_dir.as_path(), "init-test", false)
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_memory_project_test() {
    run_project_test(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_project_test() {
    run_project_test(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_project_test() {
    run_project_test(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_project_test() {
    run_project_test(Database::ScyllaDb).await
}

async fn run_project_test(database: Database) {
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 0).unwrap();
    let client = local_net.make_client(network);
    client
        .project_test(&LocalNetwork::example_path("counter").unwrap())
        .await;
}

#[test_log::test(tokio::test)]
async fn test_memory_project_publish() {
    run_project_publish(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_project_publish() {
    run_project_publish(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_project_publish() {
    run_project_publish(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_project_publish() {
    run_project_publish(Database::ScyllaDb).await
}

async fn run_project_publish(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 1).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    local_net.run().await.unwrap();

    let tmp_dir = client.project_new("init-test").await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");

    client
        .project_publish(project_dir, vec![], None, &())
        .await
        .unwrap();
    let chain = client.get_wallet().default_chain().unwrap();

    let node_service = client.run_node_service(None).await.unwrap();

    assert_eq!(node_service.try_get_applications_uri(&chain).await.len(), 1)
}

#[test_log::test(tokio::test)]
async fn test_memory_example_publish() {
    run_example_publish(Database::Memory).await
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_example_publish() {
    run_example_publish(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_example_publish() {
    run_example_publish(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_example_publish() {
    run_example_publish(Database::ScyllaDb).await
}

async fn run_example_publish(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(database, network, 1).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    local_net.run().await.unwrap();

    let example_dir = LocalNetwork::example_path("counter").unwrap();
    client
        .project_publish(example_dir, vec![], None, &0)
        .await
        .unwrap();
    let chain = client.get_wallet().default_chain().unwrap();

    let node_service = client.run_node_service(None).await.unwrap();

    assert_eq!(node_service.try_get_applications_uri(&chain).await.len(), 1)
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_open_multi_owner_chain() {
    run_end_to_end_open_multi_owner_chain(Database::RocksDb).await
}

#[ignore]
#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_open_multi_owner_chain() {
    run_end_to_end_open_multi_owner_chain(Database::DynamoDb).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_open_multi_owner_chain() {
    run_end_to_end_open_multi_owner_chain(Database::ScyllaDb).await
}

#[cfg(any(feature = "aws", feature = "rocksdb", feature = "scylladb"))]
async fn run_end_to_end_open_multi_owner_chain(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let mut runner = LocalNetwork::new(database, Network::Grpc, 4).unwrap();
    let client1 = runner.make_client(Network::Grpc);
    let client2 = runner.make_client(Network::Grpc);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Start local network.
    runner.run().await.unwrap();

    let chain1 = *client1.get_wallet().chain_ids().first().unwrap();

    // Generate keys for both clients.
    let client1_key = client1.keygen().await.unwrap();
    let client2_key = client2.keygen().await.unwrap();

    // Open chain on behalf of Client 2.
    let (message_id, chain2) = client1
        .open_multi_owner_chain(chain1, vec![client1_key, client2_key])
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
    client1.transfer("6", chain1, chain2).await.unwrap();
    client2.synchronize_balance(chain2).await.unwrap();

    assert_eq!(client1.query_balance(chain1).await.unwrap(), "4.");
    assert_eq!(client1.query_balance(chain2).await.unwrap(), "6.");
    assert_eq!(client2.query_balance(chain2).await.unwrap(), "6.");

    // Transfer 2 + 1 units from Chain 2 to Chain 1 using both clients.
    client2.transfer("2", chain2, chain1).await.unwrap();
    client1.transfer("1", chain2, chain1).await.unwrap();
    client1.synchronize_balance(chain1).await.unwrap();
    client2.synchronize_balance(chain2).await.unwrap();

    assert_eq!(client1.query_balance(chain1).await.unwrap(), "7.");
    assert_eq!(client1.query_balance(chain2).await.unwrap(), "3.");
    assert_eq!(client2.query_balance(chain2).await.unwrap(), "3.");
}
