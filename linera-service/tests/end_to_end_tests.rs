// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(not(any(feature = "wasmer", feature = "wasmtime")), allow(dead_code))]
use linera_base::identifiers::ChainId;
use linera_service::{
    client::{cargo_build_binary, ClientWrapper, LocalNetwork, Network},
    node_service::Chains,
};
use once_cell::sync::Lazy;
use std::{io::prelude::*, time::Duration};
use tokio::{process::Command, sync::Mutex};

/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use {
    async_graphql::InputType,
    linera_base::{
        data_types::{Amount, Timestamp},
        identifiers::ApplicationId,
    },
    serde_json::{json, Value},
    std::collections::BTreeMap,
    tracing::{info, warn},
};

fn get_fungible_account_owner(client: &ClientWrapper) -> AccountOwner {
    let owner = client.get_owner().unwrap();
    AccountOwner::User(owner)
}

struct Application {
    uri: String,
}

impl From<String> for Application {
    fn from(uri: String) -> Self {
        Application { uri }
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl Application {
    async fn query(&self, query: &str) -> Value {
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
}

struct CounterApp(Application);

impl From<String> for CounterApp {
    fn from(uri: String) -> Self {
        CounterApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl CounterApp {
    async fn get_value(&self) -> u64 {
        let data = self.0.query("query { value }").await;
        serde_json::from_value(data["value"].clone()).unwrap()
    }

    async fn increment_value(&self, increment: u64) {
        let query = format!("mutation {{ increment(value: {})}}", increment);
        self.0.query(&query).await;
    }
}

struct FungibleApp(Application);

impl From<String> for FungibleApp {
    fn from(uri: String) -> Self {
        FungibleApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl FungibleApp {
    async fn get_amount(&self, account_owner: &fungible::AccountOwner) -> Amount {
        let query = format!(
            "query {{ accounts(accountOwner: {} ) }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await;
        serde_json::from_value(response_body["accounts"].clone()).unwrap_or_default()
    }

    async fn assert_balances(
        &self,
        accounts: impl IntoIterator<Item = (fungible::AccountOwner, Amount)>,
    ) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn transfer(
        &self,
        account_owner: &fungible::AccountOwner,
        amount_transfer: Amount,
        destination: fungible::Account,
    ) -> Value {
        let query = format!(
            "mutation {{ transfer(owner: {}, amount: \"{}\", targetAccount: {}) }}",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.query(&query).await
    }

    async fn claim(&self, source: fungible::Account, target: fungible::Account, amount: Amount) {
        // Claiming tokens from chain1 to chain2.
        let query = format!(
            "mutation {{ claim(sourceAccount: {}, amount: \"{}\", targetAccount: {}) }}",
            source.to_value(),
            amount,
            target.to_value()
        );

        self.0.query(&query).await;
    }
}

struct SocialApp(Application);

impl From<String> for SocialApp {
    fn from(uri: String) -> Self {
        SocialApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl SocialApp {
    async fn subscribe(&self, chain_id: ChainId) -> Value {
        let query = format!("mutation {{ requestSubscribe(field0: \"{chain_id}\") }}");
        self.0.query(&query).await
    }

    async fn post(&self, text: &str) -> Value {
        let query = format!("mutation {{ post(field0: \"{text}\") }}");
        self.0.query(&query).await
    }

    async fn received_posts_keys(&self, count: u32) -> Value {
        let query = format!("query {{ receivedPostsKeys(count: {count}) {{ author, index }} }}");
        self.0.query(&query).await
    }
}

struct CrowdFundingApp(Application);

impl From<String> for CrowdFundingApp {
    fn from(uri: String) -> Self {
        CrowdFundingApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl CrowdFundingApp {
    async fn pledge_with_transfer(
        &self,
        account_owner: fungible::AccountOwner,
        amount: Amount,
    ) -> Value {
        let query = format!(
            "mutation {{ pledgeWithTransfer(owner: {}, amount: \"{}\") }}",
            account_owner.to_value(),
            amount,
        );
        self.0.query(&query).await
    }

    async fn collect(&self) -> Value {
        self.0.query("mutation { collect }").await
    }
}

struct MatchingEngineApp(Application);

impl From<String> for MatchingEngineApp {
    fn from(uri: String) -> Self {
        MatchingEngineApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl MatchingEngineApp {
    async fn get_account_info(
        &self,
        account_owner: &fungible::AccountOwner,
    ) -> Vec<matching_engine::OrderId> {
        let query = format!(
            "query {{ accountInfo(accountOwner: {}) {{ orders }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await;
        serde_json::from_value(response_body["accountInfo"]["orders"].clone()).unwrap()
    }

    async fn order(&self, order: matching_engine::Order) -> Value {
        let query_string = format!("mutation {{ executeOrder(order: {}) }}", order.to_value());
        self.0.query(&query_string).await
    }
}

struct AmmApp(Application);

impl From<String> for AmmApp {
    fn from(uri: String) -> Self {
        AmmApp(Application { uri })
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
impl AmmApp {
    async fn add_liquidity(&self, owner: AccountOwner, token0_amount: u64, token1_amount: u64) {
        let operation = amm::OperationType::AddLiquidity {
            owner,
            token0_amount,
            token1_amount,
        };

        let query = format!(
            "mutation {{ operation(operation: {}) }}",
            operation.to_value(),
        );
        self.0.query_application(&query).await;
    }

    async fn remove_liquidity(
        &self,
        owner: AccountOwner,
        input_token_idx: u32,
        other_token_idx: u32,
        input_amount: u64,
    ) {
        let operation = amm::OperationType::RemoveLiquidity {
            owner,
            input_token_idx,
            other_token_idx,
            input_amount,
        };

        let query = format!(
            "mutation {{ operation(operation: {}) }}",
            operation.to_value(),
        );
        self.0.query_application(&query).await;
    }

    async fn swap(
        &self,
        owner: AccountOwner,
        input_token_idx: u32,
        output_token_idx: u32,
        input_amount: u64,
    ) {
        let operation = amm::OperationType::Swap {
            owner,
            input_token_idx,
            output_token_idx,
            input_amount,
        };

        let query = format!(
            "mutation {{ operation(operation: {}) }}",
            operation.to_value(),
        );
        self.0.query_application(&query).await;
    }
}

fn make_graphql_query(file: &str, operation_name: &str, variables: &[(String, String)]) -> String {
    let mut file = std::fs::File::open(file).expect("failed to open query file");
    let mut query = String::new();
    file.read_to_string(&mut query)
        .expect("failed to read query file");
    let query = query.replace('\n', " ");
    let variables = if variables.is_empty() {
        "".to_string()
    } else {
        let list: Vec<String> = variables
            .iter()
            .map(|(key, value)| format!("\"{}\": \"{}\"", key, value))
            .collect();
        format!(", \"variables\": {{ {} }}", list.join(", "))
    };
    format!(
        "{{\"query\": \"{}\", \"operationName\": \"{}\"{}}}",
        query, operation_name, variables
    )
}

async fn check_request(query: String, good_result: &str, port: u16) {
    let client = reqwest::Client::new();
    for i in 0..10 {
        tokio::time::sleep(Duration::from_secs(i)).await;
        let request = client
            .post(format!("http://localhost:{}/", port))
            .body(query.clone())
            .send()
            .await;
        if request.is_ok() {
            let result = request.unwrap().text().await.unwrap();
            assert_eq!(
                result, good_result,
                "wrong response to chain queries:\nexpected:\n{:?}\ngot:\n{:?}\n",
                good_result, result
            );
            return;
        } else {
            continue;
        }
    }
    panic!("query {:?} failed after 10 retries", query)
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_chains_query() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();

    let good_result = {
        let wallet = client.get_wallet();
        let list = wallet.chain_ids();
        let default = wallet.default_chain();
        let chains = serde_json::to_string(&Chains { list, default }).unwrap();
        format!("{{\"data\":{{\"chains\":{}}}}}", chains)
    };

    local_net.run().await.unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    let query = make_graphql_query("../linera-explorer/graphql/chains.graphql", "Chains", &[]);
    check_request(query, &good_result, node_service.port()).await;
    node_service.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_applications_query() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();

    local_net.run().await.unwrap();
    let chain = client.get_wallet().default_chain().unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    // only checks if application input type is good
    let good_result = "{\"data\":{\"applications\":[]}}";
    let query = make_graphql_query(
        "../linera-explorer/graphql/applications.graphql",
        "Applications",
        &[("chainId".to_string(), chain.to_string())],
    );
    check_request(query, good_result, node_service.port()).await;
    node_service.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_blocks_query() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();

    local_net.run().await.unwrap();
    let chain = client.get_wallet().default_chain().unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    // only checks if block input type is good
    let good_result = "{\"data\":{\"blocks\":[]}}";
    let query = make_graphql_query(
        "../linera-explorer/graphql/blocks.graphql",
        "Blocks",
        &[("chainId".to_string(), chain.to_string())],
    );
    check_request(query, good_result, node_service.port()).await;
    node_service.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_block_query() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();

    local_net.run().await.unwrap();
    let chain = client.get_wallet().default_chain().unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    // only checks if block input type is good
    let good_result = "{\"data\":{\"block\":null}}";
    let query = make_graphql_query(
        "../linera-explorer/graphql/block.graphql",
        "Block",
        &[("chainId".to_string(), chain.to_string())],
    );
    check_request(query, good_result, node_service.port()).await;
    node_service.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_end_to_end_check_schema() {
    let path = cargo_build_binary("linera-schema-export").await;
    let mut command = Command::new(path);
    let output = command
        .kill_on_drop(true)
        .output()
        .await
        .expect("failed to run linera-schema-export");
    let service_schema =
        String::from_utf8(output.stdout).expect("failed to read the service GraphQL schema");
    let mut file_base = std::fs::File::open("../linera-explorer/graphql/schema.graphql")
        .expect("failed to open schema.graphql");
    let mut graphql_schema = String::new();
    file_base
        .read_to_string(&mut graphql_schema)
        .expect("failed to read schema.graphql");
    assert_eq!(graphql_schema, service_schema, "GraphQL schema has changed -> regenerate schema following steps in linera-explorer/README.md")
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    let chain = client.get_wallet().default_chain().unwrap();
    local_net.run().await.unwrap();
    let (contract, service) = local_net.build_example("counter").await.unwrap();

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
    let mut node_service = client.run_node_service(None).await.unwrap();

    let application: CounterApp = node_service
        .make_application(&chain, &application_id)
        .await
        .into();

    let counter_value = application.get_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_value(increment).await;

    let counter_value = application.get_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_counter_publish_create() {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client = local_net.make_client(network);

    let original_counter_value = 35;
    let increment = 5;

    local_net.generate_initial_validator_config().await.unwrap();
    client.create_genesis_config().await.unwrap();
    let chain = client.get_wallet().default_chain().unwrap();
    local_net.run().await.unwrap();
    let (contract, service) = local_net.build_example("counter").await.unwrap();

    let bytecode_id = client
        .publish_bytecode(contract, service, None)
        .await
        .unwrap();
    let application_id = client
        .create_application::<CounterAbi>(bytecode_id, &original_counter_value, None)
        .await
        .unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    let application: CounterApp = node_service
        .make_application(&chain, &application_id)
        .await
        .into();

    let counter_value = application.get_value().await;
    assert_eq!(counter_value, original_counter_value);

    application.increment_value(increment).await;

    let counter_value = application.get_value().await;
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_multiple_wallets() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create local_net and two clients.
    let mut local_net = LocalNetwork::new(Network::Grpc, 4).unwrap();
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
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
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
async fn test_open_chain_node_service() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
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

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_social_user_pub_sub() {
    use social::SocialAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client1 = local_net.make_client(network);
    let client2 = local_net.make_client(network);

    // Create initial server and client config.
    local_net.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Start local network.
    local_net.run().await.unwrap();
    let (contract, service) = local_net.build_example("social").await.unwrap();

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();
    let bytecode_id = client1
        .publish_bytecode(contract, service, None)
        .await
        .unwrap();
    let application_id = client1
        .create_application::<SocialAbi>(bytecode_id, &(), None)
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    node_service1.process_inbox(&chain1).await;

    // Request the application so chain 2 has it, too.
    node_service2
        .request_application(&chain2, &application_id)
        .await;

    let app2: SocialApp = node_service2
        .make_application(&chain2, &application_id)
        .await
        .into();
    let hash = app2.subscribe(chain1).await;

    // The returned hash should now be the latest one.
    let query = format!("query {{ chain(chainId: \"{chain2}\") {{ tipState {{ blockHash }} }} }}");
    let response = node_service2.query_node(&query).await;
    assert_eq!(hash, response["chain"]["tipState"]["blockHash"]);

    let app1: SocialApp = node_service1
        .make_application(&chain1, &application_id)
        .await
        .into();
    app1.post("Linera Social is the new Mastodon!").await;

    // Instead of retrying, we could call `node_service1.process_inbox(chain1).await` here.
    // However, we prefer to test the notification system for a change.
    let expected_response = json!({ "receivedPostsKeys": [
        { "author": chain1, "index": 0 }
    ]});
    'success: {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = app2.received_posts_keys(5).await;
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
    let mut local_net = LocalNetwork::new(network, 1).unwrap();
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

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_fungible() {
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client1 = local_net.make_client(network);
    let client2 = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Create initial server and client config.
    local_net.run().await.unwrap();
    let (contract, service) = local_net.build_example("fungible").await.unwrap();

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();

    // The players
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = get_fungible_account_owner(&client2);
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

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    let app1: FungibleApp = node_service1
        .make_application(&chain1, &application_id)
        .await
        .into();
    app1.assert_balances([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Transferring
    app1.transfer(
        &account_owner1,
        Amount::ONE,
        fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Checking the final values on chain1 and chain2.
    app1.assert_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Fungible didn't exist on chain2 initially but now it does and we can talk to it.
    let app2: FungibleApp = node_service2
        .make_application(&chain2, &application_id)
        .await
        .into();

    app2.assert_balances(BTreeMap::from([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::ONE),
    ]))
    .await;

    // Claiming more money from chain1 to chain2.
    app2.claim(
        fungible::Account {
            chain_id: chain1,
            owner: account_owner2,
        },
        fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
        Amount::from_tokens(2),
    )
    .await;

    // Make sure that the cross-chain communication happens fast enough.
    node_service1.process_inbox(&chain1).await;
    node_service2.process_inbox(&chain2).await;

    // Checking the final value
    app1.assert_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::ZERO),
    ])
    .await;
    app2.assert_balances([
        (account_owner1, Amount::ZERO),
        (account_owner2, Amount::from_tokens(3)),
    ])
    .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_same_wallet_fungible() {
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client1 = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();

    // Create initial server and client config.
    local_net.run().await.unwrap();
    let (contract, service) = local_net.build_example("fungible").await.unwrap();

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = ChainId::root(2);

    // The players
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = {
        let wallet = client1.get_wallet();
        let user_chain = wallet.get(chain2).unwrap();
        let public_key = user_chain.key_pair.as_ref().unwrap().public();
        AccountOwner::User(public_key.into())
    };
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

    let mut node_service = client1.run_node_service(8080).await.unwrap();

    let app1: FungibleApp = node_service
        .make_application(&chain1, &application_id)
        .await
        .into();
    app1.assert_balances([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    // Transferring
    app1.transfer(
        &account_owner1,
        Amount::ONE,
        fungible::Account {
            chain_id: chain2,
            owner: account_owner2,
        },
    )
    .await;

    // Checking the final values on chain1 and chain2.
    app1.assert_balances([
        (account_owner1, Amount::from_tokens(4)),
        (account_owner2, Amount::from_tokens(2)),
    ])
    .await;

    let app2: FungibleApp = node_service
        .make_application(&chain2, &application_id)
        .await
        .into();
    app2.assert_balances([(account_owner2, Amount::ONE)]).await;

    node_service.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_crowd_funding() {
    use crowd_funding::{CrowdFundingAbi, InitializationArgument};
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client1 = local_net.make_client(network);
    let client2 = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client1.create_genesis_config().await.unwrap();
    client2.wallet_init(&[]).await.unwrap();

    // Create initial server and client config.
    local_net.run().await.unwrap();
    let (contract_fungible, service_fungible) = local_net.build_example("fungible").await.unwrap();

    let chain1 = client1.get_wallet().default_chain().unwrap();
    let chain2 = client1.open_and_assign(&client2).await.unwrap();

    // The players
    let account_owner1 = get_fungible_account_owner(&client1); // operator
    let account_owner2 = get_fungible_account_owner(&client2); // contributor

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
    let (contract_crowd, service_crowd) = local_net.build_example("crowd-funding").await.unwrap();
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

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    let app_fungible1: FungibleApp = node_service1
        .make_application(&chain1, &application_id_fungible)
        .await
        .into();

    let app_crowd1: CrowdFundingApp = node_service1
        .make_application(&chain1, &application_id_crowd)
        .await
        .into();

    // Transferring tokens to user2 on chain2
    app_fungible1
        .transfer(
            &account_owner1,
            Amount::ONE,
            fungible::Account {
                chain_id: chain2,
                owner: account_owner2,
            },
        )
        .await;

    // Register the campaign on chain2.
    node_service2
        .request_application(&chain2, &application_id_crowd)
        .await;

    let app_crowd2: CrowdFundingApp = node_service2
        .make_application(&chain2, &application_id_crowd)
        .await
        .into();

    // Transferring
    app_crowd2
        .pledge_with_transfer(account_owner2, Amount::ONE)
        .await;

    // Make sure that the pledge is processed fast enough by client1.
    node_service1.process_inbox(&chain1).await;

    // Ending the campaign.
    app_crowd1.collect().await;

    // The rich gets their money back.
    app_fungible1
        .assert_balances([(account_owner1, Amount::from_tokens(6))])
        .await;

    node_service1.assert_is_running();
    node_service2.assert_is_running();
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_matching_engine() {
    use fungible::{FungibleTokenAbi, InitialState};
    use matching_engine::{OrderNature, Parameters, Price};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client_admin = local_net.make_client(network);
    let client_a = local_net.make_client(network);
    let client_b = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client_admin.create_genesis_config().await.unwrap();
    client_a.wallet_init(&[]).await.unwrap();
    client_b.wallet_init(&[]).await.unwrap();

    // Create initial server and client config.
    local_net.run().await.unwrap();
    let (contract_fungible, service_fungible) = local_net.build_example("fungible").await.unwrap();
    let (contract_matching, service_matching) =
        local_net.build_example("matching-engine").await.unwrap();

    let chain_admin = client_admin.get_wallet().default_chain().unwrap();
    let chain_a = client_admin.open_and_assign(&client_a).await.unwrap();
    let chain_b = client_admin.open_and_assign(&client_b).await.unwrap();

    // The players
    let owner_admin = get_fungible_account_owner(&client_admin);
    let owner_a = get_fungible_account_owner(&client_a);
    let owner_b = get_fungible_account_owner(&client_b);
    // The initial accounts on chain_a and chain_b
    let accounts0 = BTreeMap::from([(owner_a, Amount::from_tokens(10))]);
    let state_fungible0 = InitialState {
        accounts: accounts0,
    };
    let accounts1 = BTreeMap::from([(owner_b, Amount::from_tokens(9))]);
    let state_fungible1 = InitialState {
        accounts: accounts1,
    };

    // Setting up the application fungible on chain_a and chain_b
    let application_id_fungible0 = client_a
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible.clone(),
            service_fungible.clone(),
            &(),
            &state_fungible0,
            vec![],
            None,
        )
        .await
        .unwrap();
    let application_id_fungible1 = client_b
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible,
            service_fungible,
            &(),
            &state_fungible1,
            vec![],
            None,
        )
        .await
        .unwrap();

    // Now creating the service and exporting the applications
    let mut node_service_admin = client_admin.run_node_service(8080).await.unwrap();
    let mut node_service_a = client_a.run_node_service(8081).await.unwrap();
    let mut node_service_b = client_b.run_node_service(8082).await.unwrap();

    let app_fungible0_a: FungibleApp = node_service_a
        .make_application(&chain_a, &application_id_fungible0)
        .await
        .into();
    let app_fungible1_b: FungibleApp = node_service_b
        .make_application(&chain_b, &application_id_fungible1)
        .await
        .into();
    app_fungible0_a
        .assert_balances([
            (owner_a, Amount::from_tokens(10)),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    app_fungible1_b
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::from_tokens(9)),
            (owner_admin, Amount::ZERO),
        ])
        .await;

    node_service_admin
        .request_application(&chain_admin, &application_id_fungible0)
        .await;
    let app_fungible0_admin: FungibleApp = node_service_admin
        .make_application(&chain_admin, &application_id_fungible0)
        .await
        .into();
    node_service_admin
        .request_application(&chain_admin, &application_id_fungible1)
        .await;
    let app_fungible1_admin: FungibleApp = node_service_admin
        .make_application(&chain_admin, &application_id_fungible1)
        .await
        .into();
    app_fungible0_admin
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner_a, Amount::ZERO),
            (owner_b, Amount::ZERO),
            (owner_admin, Amount::ZERO),
        ])
        .await;

    // Setting up the application matching engine.
    let token0 = application_id_fungible0
        .parse::<ApplicationId>()
        .unwrap()
        .with_abi();
    let token1 = application_id_fungible1
        .parse::<ApplicationId>()
        .unwrap()
        .with_abi();
    let parameter = Parameters {
        tokens: [token0, token1],
    };
    let bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_matching, service_matching)
        .await;
    let parameter_str = serde_json::to_string(&parameter).unwrap();
    let argument_str = serde_json::to_string(&()).unwrap();
    let application_id_matching = node_service_admin
        .create_application(
            &chain_admin,
            bytecode_id,
            parameter_str,
            argument_str,
            vec![
                application_id_fungible0.clone(),
                application_id_fungible1.clone(),
            ],
        )
        .await;
    let app_matching_admin: MatchingEngineApp = node_service_admin
        .make_application(&chain_admin, &application_id_matching)
        .await
        .into();
    node_service_a
        .request_application(&chain_a, &application_id_matching)
        .await;
    let app_matching_a: MatchingEngineApp = node_service_a
        .make_application(&chain_a, &application_id_matching)
        .await
        .into();
    node_service_b
        .request_application(&chain_b, &application_id_matching)
        .await;
    let app_matching_b: MatchingEngineApp = node_service_b
        .make_application(&chain_b, &application_id_matching)
        .await
        .into();

    // Now creating orders
    for price in [1, 2] {
        // 1 is expected not to match, but 2 is expected to match
        app_matching_a
            .order(matching_engine::Order::Insert {
                owner: owner_a,
                amount: Amount::from_tokens(3),
                nature: OrderNature::Bid,
                price: Price { price },
            })
            .await;
    }
    for price in [4, 2] {
        // price 2 is expected to match, but not 4.
        app_matching_b
            .order(matching_engine::Order::Insert {
                owner: owner_b,
                amount: Amount::from_tokens(4),
                nature: OrderNature::Ask,
                price: Price { price },
            })
            .await;
    }
    node_service_admin.process_inbox(&chain_admin).await;
    node_service_a.process_inbox(&chain_a).await;
    node_service_b.process_inbox(&chain_b).await;

    // Now reading the order_ids
    let order_ids_a = app_matching_admin.get_account_info(&owner_a).await;
    let order_ids_b = app_matching_admin.get_account_info(&owner_b).await;
    // The deal that occurred is that 6 token0 were exchanged for 3 token1.
    assert_eq!(order_ids_a.len(), 1); // The order of price 2 is completely filled.
    assert_eq!(order_ids_b.len(), 2); // The order of price 2 is partially filled.

    // Now cancelling all the orders
    for (owner, order_ids) in [(owner_a, order_ids_a), (owner_b, order_ids_b)] {
        for order_id in order_ids {
            app_matching_admin
                .order(matching_engine::Order::Cancel { owner, order_id })
                .await;
        }
    }
    node_service_admin.process_inbox(&chain_admin).await;

    // Check balances
    app_fungible0_a
        .assert_balances([
            (owner_a, Amount::from_tokens(1)),
            (owner_b, Amount::from_tokens(0)),
        ])
        .await;
    app_fungible0_admin
        .assert_balances([
            (owner_a, Amount::from_tokens(3)),
            (owner_b, Amount::from_tokens(6)),
        ])
        .await;
    app_fungible1_b
        .assert_balances([
            (owner_a, Amount::from_tokens(0)),
            (owner_b, Amount::from_tokens(1)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner_a, Amount::from_tokens(3)),
            (owner_b, Amount::from_tokens(5)),
        ])
        .await;

    node_service_admin.assert_is_running();
    node_service_a.assert_is_running();
    node_service_b.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_project_new() {
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 0).unwrap();
    let client = local_net.make_client(network);

    let tmp_dir = client.project_new("init-test").await.unwrap();
    let project_dir = tmp_dir.path().join("init-test");
    local_net
        .build_application(project_dir.as_path(), "init-test", false)
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_project_test() {
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 0).unwrap();
    let client = local_net.make_client(network);
    client
        .project_test(&LocalNetwork::example_path("counter").unwrap())
        .await;
}

#[test_log::test(tokio::test)]
async fn test_project_publish() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 1).unwrap();
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
async fn test_example_publish() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 1).unwrap();
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

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_open_multi_owner_chain() {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    // Create runner and two clients.
    let mut runner = LocalNetwork::new(Network::Grpc, 4).unwrap();
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

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_amm() {
    use fungible::InitialState;
    use matching_engine::Parameters;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new(network, 4).unwrap();
    let client_admin = local_net.make_client(network);
    let client0 = local_net.make_client(network);
    let client1 = local_net.make_client(network);

    local_net.generate_initial_validator_config().await.unwrap();
    client_admin.create_genesis_config().await.unwrap();
    client0.wallet_init(&[]).await.unwrap();
    client1.wallet_init(&[]).await.unwrap();

    local_net.run().await.unwrap();
    let (contract_fungible, service_fungible) = local_net.build_example("fungible").await.unwrap();
    let (contract_amm, service_amm) = local_net.build_example("amm").await.unwrap();

    // Admin chain
    let chain_admin = client_admin.get_wallet().default_chain().unwrap();

    // User chains
    let chain0 = client_admin.open_and_assign(&client0).await.unwrap();
    let chain1 = client_admin.open_and_assign(&client1).await.unwrap();

    // Admin user
    let owner_admin = get_fungible_account_owner(&client_admin);

    // Users
    let owner0 = get_fungible_account_owner(&client0);
    let owner1 = get_fungible_account_owner(&client1);

    let mut node_service_admin = client_admin.run_node_service(8080).await.unwrap();
    let mut node_service0 = client0.run_node_service(8081).await.unwrap();
    let mut node_service1 = client1.run_node_service(8082).await.unwrap();

    // Amounts of token0 that will be owned by each user
    let state_fungible0 = InitialState {
        accounts: BTreeMap::from([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::from_tokens(150)),
        ]),
    };

    // Amounts of token1 that will be owned by each user
    let state_fungible1 = InitialState {
        accounts: BTreeMap::from([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::from_tokens(100)),
        ]),
    };

    // Create fungible applications on the Admin chain, which will hold
    // the token0 and token1 amounts
    let fungible_bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_fungible, service_fungible)
        .await;

    let application_id_fungible0 = node_service_admin
        .create_application(
            &chain_admin,
            fungible_bytecode_id.clone(),
            serde_json::to_string(&()).unwrap(),
            serde_json::to_string(&state_fungible0).unwrap(),
            vec![],
        )
        .await;
    let application_id_fungible1 = node_service_admin
        .create_application(
            &chain_admin,
            fungible_bytecode_id,
            serde_json::to_string(&()).unwrap(),
            serde_json::to_string(&state_fungible1).unwrap(),
            vec![],
        )
        .await;

    // Create wrappers
    let app_fungible0_admin: FungibleApp = node_service_admin
        .make_application(&chain_admin, &application_id_fungible0)
        .await
        .into();
    let app_fungible1_admin: FungibleApp = node_service_admin
        .make_application(&chain_admin, &application_id_fungible1)
        .await
        .into();

    // Check initial balances
    app_fungible0_admin
        .assert_fungible_account_balances([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::from_tokens(150)),
            (owner_admin, Amount::ZERO),
        ])
        .await;
    app_fungible1_admin
        .assert_fungible_account_balances([
            (owner0, Amount::from_tokens(100)),
            (owner1, Amount::from_tokens(100)),
            (owner_admin, Amount::ZERO),
        ])
        .await;

    let token0 = application_id_fungible0
        .parse::<ApplicationId>()
        .unwrap()
        .with_abi();
    let token1 = application_id_fungible1
        .parse::<ApplicationId>()
        .unwrap()
        .with_abi();
    let parameters = Parameters {
        tokens: [token0, token1],
    };

    // Create AMM application on Admin chain
    let bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_amm, service_amm)
        .await;
    let parameter_str = serde_json::to_string(&parameters).unwrap();
    let argument_str = serde_json::to_string(&()).unwrap();
    let (application_id_amm, application_id_amm_struct) = node_service_admin
        .create_application_tuple(
            &chain_admin,
            bytecode_id,
            parameter_str,
            argument_str,
            vec![
                application_id_fungible0.clone(),
                application_id_fungible1.clone(),
            ],
        )
        .await;

    let owner_amm = AccountOwner::Application(application_id_amm_struct);

    // Create AMM wrappers
    node_service0
        .request_application(&chain0, &application_id_amm)
        .await;
    let app_amm0: AmmApp = node_service0
        .make_application(&chain0, &application_id_amm)
        .await
        .into();
    node_service1
        .request_application(&chain1, &application_id_amm)
        .await;
    let app_amm1: AmmApp = node_service1
        .make_application(&chain1, &application_id_amm)
        .await
        .into();

    // Initial balances for both tokens are 0

    // Adding liquidity for token0 and token1 by owner0
    // We have to call it from the AAM instance in the owner0's
    // user chain, chain0, so it's properly authenticated
    app_amm0.add_liquidity(owner0, 100, 100).await;
    // Processing inbox for messages from chain0 to chain_admin, where the AMM
    // was created
    node_service_admin.process_inbox(&chain_admin).await;

    // Ownership of owner0 tokens should be with the AMM now
    app_fungible0_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(150)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(100)),
        ])
        .await;
    app_fungible1_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(100)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(100)),
        ])
        .await;

    // Adding liquidity for token0 and token1 by owner1 now
    app_amm1.add_liquidity(owner1, 100, 100).await;
    node_service_admin.process_inbox(&chain_admin).await;

    app_fungible0_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(50)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(200)),
        ])
        .await;
    app_fungible1_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(200)),
        ])
        .await;

    app_amm1.swap(owner1, 0, 1, 50).await;
    node_service_admin.process_inbox(&chain_admin).await;

    app_fungible0_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(250)),
        ])
        .await;
    app_fungible1_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(40)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(160)),
        ])
        .await;

    app_amm1.remove_liquidity(owner1, 0, 1, 62).await;
    node_service_admin.process_inbox(&chain_admin).await;

    app_fungible0_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(62)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(188)),
        ])
        .await;
    // Rounding down happens here, which is why it's 79 instead of 80
    app_fungible1_admin
        .assert_fungible_account_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(79)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(121)),
        ])
        .await;

    node_service_admin.assert_is_running();
    node_service0.assert_is_running();
    node_service1.assert_is_running();
}
