// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(not(any(feature = "wasmer", feature = "wasmtime")), allow(dead_code))]

use async_graphql::InputType;
use linera_base::identifiers::ChainId;

use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

use tokio::sync::Mutex;

use tracing::{info, warn};

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::ApplicationId,
};
use linera_service::client::{LocalNet, Network};

/// A static lock to prevent integration tests from running in parallel.
static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub struct Application {
    uri: String,
}

#[cfg(test)]
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl Application {
    fn new(uri: String) -> Self {
        Self { uri }
    }
    pub async fn get_fungible_account_owner_amount(
        &self,
        account_owner: &fungible::AccountOwner,
    ) -> Amount {
        let query = format!(
            "query {{ accounts(accountOwner: {} ) }}",
            account_owner.to_value()
        );
        let response_body = self.query_application(&query).await;
        serde_json::from_value(response_body["accounts"].clone()).unwrap_or_default()
    }

    pub async fn assert_fungible_account_balances(
        &self,
        accounts: impl IntoIterator<Item = (fungible::AccountOwner, Amount)>,
    ) {
        for (account_owner, amount) in accounts {
            let value = self.get_fungible_account_owner_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    pub async fn get_counter_value(&self) -> u64 {
        let data = self.query_application("query { value }").await;
        serde_json::from_value(data["value"].clone()).unwrap()
    }

    pub async fn query_application(&self, query: &str) -> Value {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.uri)
            .json(&json!({ "query": query }))
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            panic!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                response.text().await.unwrap()
            );
        }
        let value: Value = response.json().await.unwrap();
        if let Some(errors) = value.get("errors") {
            panic!(
                "Query \"{}\" failed: {}",
                query.get(..200).unwrap_or(query),
                errors
            );
        }
        value["data"].clone()
    }

    pub async fn increment_counter_value(&self, increment: u64) {
        let query = format!("mutation {{ increment(value: {})}}", increment);
        self.query_application(&query).await;
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client = runner.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    runner.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    runner.run_local_net().await.unwrap();
    let (contract, service) = runner.build_example("counter").await.unwrap();

    let application_id = client
        .publish_and_create::<CounterAbi>(
            contract,
            service,
            &(),
            &original_counter_value,
            vec![],
            None,
        )
        .await
        .unwrap();
    let mut node_service = client.run_node_service(None, None).await.unwrap();

    let application = Application::new(
        node_service
            .make_application(&application_id)
            .await
            .unwrap(),
    );

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_counter_value(increment).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter_publish_create() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client = runner.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    runner.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    runner.run_local_net().await.unwrap();
    let (contract, service) = runner.build_example("counter").await.unwrap();

    let bytecode_id = client
        .publish_bytecode(contract, service, None)
        .await
        .unwrap();
    let application_id = client
        .create_application::<CounterAbi>(bytecode_id, &original_counter_value, None)
        .await
        .unwrap();
    let mut node_service = client.run_node_service(None, None).await.unwrap();

    let application = Application::new(
        node_service
            .make_application(&application_id)
            .await
            .unwrap(),
    );

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_counter_value(increment).await;

    let counter_value = application.get_counter_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let mut runner = LocalNet::new(Network::Grpc, 4).unwrap();
    let client_1 = runner.make_client(Network::Grpc);
    let client_2 = runner.make_client(Network::Grpc);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await.unwrap();
    client_1.create_genesis_config().await.unwrap();
    client_2.wallet_init(&[]).await.unwrap();

    // Start local network.
    runner.run_local_net().await.unwrap();

    // Get some chain owned by Client 1.
    let chain_1 = *client_1.get_wallet().unwrap().chain_ids().first().unwrap();

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
async fn test_end_to_end_reconfiguration_grpc() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    test_reconfiguration(Network::Grpc).await;
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_reconfiguration_simple() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    test_reconfiguration(Network::Simple).await;
}

async fn test_reconfiguration(network: Network) {
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client = runner.make_client(network);
    let client_2 = runner.make_client(network);

    let servers = runner.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    client_2.wallet_init(&[]).await.unwrap();
    runner.run_local_net().await.unwrap();

    let chain_1 = client.get_wallet().unwrap().default_chain().unwrap();

    let (node_service_2, chain_2) = match network {
        Network::Grpc => {
            let chain_2 = client.open_and_assign(&client_2).await.unwrap();
            let node_service_2 = client_2.run_node_service(chain_2, 8081).await.unwrap();
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
    runner.kill_server(4, 3);
    runner.start_server(4, 3).await.unwrap();

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
    assert!(client.is_chain_present_in_wallet(chain_3).await.unwrap());

    // Create configurations for two more validators
    let server_5 = runner.generate_validator_config(5).await.unwrap();
    let server_6 = runner.generate_validator_config(6).await.unwrap();

    // Start the validators
    runner.start_validators(5..=6).await.unwrap();

    // Add validator 5
    client.set_validator(&server_5, 9500, 100).await.unwrap();

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Add validator 6
    client.set_validator(&server_6, 9600, 100).await.unwrap();

    // Remove validator 5
    client.remove_validator(&server_5).await.unwrap();
    runner.remove_validator(5);

    client.query_validators(None).await.unwrap();
    client.query_validators(Some(chain_1)).await.unwrap();

    // Remove validators 1, 2, 3 and 4, so only 6 remains.
    for (i, server) in servers.into_iter().enumerate() {
        client.remove_validator(&server).await.unwrap();
        runner.remove_validator(i + 1);
    }

    client.transfer("5", chain_1, chain_2).await.unwrap();
    client.synchronize_balance(chain_2).await.unwrap();
    assert_eq!(client.query_balance(chain_2).await.unwrap(), "8.");

    if let Some(node_service_2) = node_service_2 {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = node_service_2
                .query_node("query { chain { executionState { system { balance } } } }")
                .await
                .unwrap();
            if response["chain"]["executionState"]["system"]["balance"].as_str() == Some("8.") {
                return;
            }
        }
        panic!("Failed to receive new block");
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_social_user_pub_sub() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Start local network.
    runner.run_local_net().await.unwrap();
    let (contract, service) = runner.build_example("social").await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();

    let mut node_service1 = client1.run_node_service(chain1, 8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(chain2, 8081).await.unwrap();

    let bytecode_id = node_service1
        .publish_bytecode(contract, service)
        .await
        .unwrap();
    node_service1.process_inbox().await;
    let application_id = node_service1
        .create_application(&bytecode_id)
        .await
        .unwrap();

    // Request the application so chain 2 has it, too.
    node_service2
        .request_application(&application_id)
        .await
        .unwrap();

    let app2 = Application::new(
        node_service2
            .make_application(&application_id)
            .await
            .unwrap(),
    );
    let subscribe = format!("mutation {{ subscribe(chainId: \"{chain1}\") }}");
    let hash = app2.query_application(&subscribe).await;

    // The returned hash should now be the latest one.
    let query = format!("query {{ chain(chainId: \"{chain2}\") {{ tipState {{ blockHash }} }} }}");
    let response = node_service2.query_node(&query).await.unwrap();
    assert_eq!(hash, response["chain"]["tipState"]["blockHash"]);

    let app1 = Application::new(
        node_service1
            .make_application(&application_id)
            .await
            .unwrap(),
    );
    let post = "mutation { post(text: \"Linera Social is the new Mastodon!\") }";
    app1.query_application(post).await;

    // Instead of retrying, we could call `node_service1.process_inbox().await` here.
    // However, we prefer to test the notification system for a change.
    let query = "query { receivedPostsKeys(count: 5) { author, index } }";
    let expected_response = json!({ "receivedPostsKeys": [
        { "author": chain1, "index": 0 }
    ]});
    'success: {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = app2.query_application(query).await;
            if response == expected_response {
                info!("Confirmed post");
                break 'success;
            }
            warn!("Waiting to confirm post: {}", response);
        }
        panic!("Failed to confirm post");
    }

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_retry_notification_stream() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 1).unwrap();
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    // Create initial server and client config.
    runner.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    let chain = ChainId::root(0);
    let mut height = 0;
    client2.wallet_init(&[chain]).await.unwrap();

    // Start local network.
    runner.run_local_net().await.unwrap();

    // Listen for updates on root chain 0. There are no blocks on that chain yet.
    let mut node_service2 = client2.run_node_service(chain, 8081).await.unwrap();
    let response = node_service2
        .query_node("query { chain { tipState { nextBlockHeight } } }")
        .await
        .unwrap();
    assert_eq!(
        response["chain"]["tipState"]["nextBlockHeight"].as_u64(),
        Some(height)
    );

    // Oh no! The validator has an outage and gets restarted!
    runner.remove_validator(1);
    runner.start_validators(1..=1).await.unwrap();

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
                .query_node("query { chain { tipState { nextBlockHeight } } }")
                .await
                .unwrap();
            if response["chain"]["tipState"]["nextBlockHeight"].as_u64() == Some(height) {
                break 'success;
            }
        }
        panic!("Failed to re-establish notification stream");
    }

    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_fungible() {
    use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    runner.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Create initial server and client config.
    runner.run_local_net().await.unwrap();
    let (contract, service) = runner.build_example("fungible").await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();

    // The players
    let owner1 = client1.get_owner().unwrap();
    let account_owner1 = AccountOwner::User(owner1);
    let owner2 = client2.get_owner().unwrap();
    let account_owner2 = AccountOwner::User(owner2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi>(contract, service, &(), &state, vec![], None)
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(chain1, 8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(chain2, 8081).await.unwrap();

    let app1 = Application::new(
        node_service1
            .make_application(&application_id)
            .await
            .unwrap(),
    );
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Transferring
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ transfer(owner: {}, amount: \"{}\", targetAccount: {}) }}",
        account_owner1.to_value(),
        amount_transfer,
        destination.to_value(),
    );
    app1.query_application(&query).await;

    // Checking the final values on chain1 and chain2.
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2 = Application::new(
        node_service2
            .make_application(&application_id)
            .await
            .unwrap(),
    );

    app2.assert_fungible_account_balances(BTreeMap::from([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::ONE),
    ]))
    .await;

    // Claiming more money from chain1 to chain2.
    let source = Account {
        chain_id: chain1,
        owner: account_owner2,
    };
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer: Amount = Amount::from_tokens(2);
    let query = format!(
        "mutation {{ claim(sourceAccount: {}, amount: \"{}\", targetAccount: {}) }}",
        source.to_value(),
        amount_transfer,
        destination.to_value()
    );
    app2.query_application(&query).await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox().await;
    node_service2.process_inbox().await;

    // Checking the final value
    app1.assert_fungible_account_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::ZERO),
    ])
    .await;
    app2.assert_fungible_account_balances([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::from_tokens(3)),
    ])
    .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_crowd_funding() {
    use crowd_funding::{CrowdFundingAbi, InitializationArgument};
    use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 4).unwrap();
    let client1 = runner.make_client(network);
    let client2 = runner.make_client(network);

    runner.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Create initial server and client config.
    runner.run_local_net().await.unwrap();
    let (contract_fungible, service_fungible) = runner.build_example("fungible").await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();

    // The players
    let owner1 = client1.get_owner().unwrap();
    let account_owner1 = AccountOwner::User(owner1);
    let owner2 = client2.get_owner().unwrap();
    let account_owner2 = AccountOwner::User(owner2);
    // The initial accounts on chain1
    let accounts = BTreeMap::from([(account_owner1, Amount::from_tokens(6))]);
    let state_fungible = InitialState { accounts };

    // Setting up the application fungible
    let application_id_fungible = client1
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible,
            service_fungible,
            &(),
            &state_fungible,
            vec![],
            None,
        )
        .await
        .unwrap();

    // Setting up the application crowd funding
    let deadline = Timestamp::from(std::u64::MAX);
    let target = Amount::ONE;
    let state_crowd = InitializationArgument {
        owner: account_owner1,
        deadline,
        target,
    };
    let (contract_crowd, service_crowd) = runner.build_example("crowd-funding").await.unwrap();
    let application_id_crowd = client1
        .publish_and_create::<CrowdFundingAbi>(
            contract_crowd,
            service_crowd,
            // TODO(#723): This hack will disappear soon.
            &application_id_fungible
                .parse::<ApplicationId>()
                .unwrap()
                .with_abi(),
            &state_crowd,
            vec![application_id_fungible.clone()],
            None,
        )
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(chain1, 8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(chain2, 8081).await.unwrap();

    let app_fungible1 = Application::new(
        node_service1
            .make_application(&application_id_fungible)
            .await
            .unwrap(),
    );

    let app_crowd1 = Application::new(
        node_service1
            .make_application(&application_id_crowd)
            .await
            .unwrap(),
    );

    // Transferring tokens to user2 on chain2
    let destination = Account {
        chain_id: chain2,
        owner: account_owner2,
    };
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ transfer(owner: {}, amount: \"{}\", targetAccount: {}) }}",
        account_owner1.to_value(),
        amount_transfer,
        destination.to_value(),
    );
    app_fungible1.query_application(&query).await;

    // Register the campaign on chain2.
    node_service2
        .request_application(&application_id_crowd)
        .await
        .unwrap();

    let app_crowd2 = Application::new(
        node_service2
            .make_application(&application_id_crowd)
            .await
            .unwrap(),
    );

    // Transferring
    let amount_transfer = Amount::ONE;
    let query = format!(
        "mutation {{ pledgeWithTransfer(owner: {}, amount: \"{}\") }}",
        account_owner2.to_value(),
        amount_transfer,
    );
    app_crowd2.query_application(&query).await;

    // Make sure that the pledge is processed fast enough by client1.
    node_service1.process_inbox().await;

    // Ending the campaign.
    app_crowd1.query_application("mutation { collect }").await;

    // The rich gets their money back.
    app_fungible1
        .assert_fungible_account_balances([(account_owner1, Amount::from_tokens(6))])
        .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_project_new() {
    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 0).unwrap();
    let client = runner.make_client(network);

    let tmp_dir = client.project_new("init-test").await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");
    runner
        .build_application(project_dir.as_path(), "init-test", false)
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_project_test() {
    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 0).unwrap();
    let client = runner.make_client(network);
    client
        .project_test(&LocalNet::example_path("counter"))
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_project_publish() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut runner = LocalNet::new(network, 1).unwrap();
    let client = runner.make_client(network);

    runner.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    runner.run_local_net().await.unwrap();

    let tmp_dir = client.project_new("init-test").await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");

    client
        .project_publish(project_dir, vec![], None)
        .await
        .unwrap();

    let node_service = client.run_node_service(None, None).await.unwrap();

    assert_eq!(
        node_service.try_get_applications_uri().await.unwrap().len(),
        1
    )
}
