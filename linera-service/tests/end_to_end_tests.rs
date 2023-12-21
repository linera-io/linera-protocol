// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "rocksdb", feature = "aws", feature = "scylladb"))]

mod common;

use async_graphql::InputType;
use common::INTEGRATION_TEST_GUARD;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::ChainId,
};
#[cfg(feature = "kubernetes")]
use linera_service::cli_wrappers::local_kubernetes_net::SharedLocalKubernetesNetTestingConfig;
use linera_service::{
    cli_wrappers::{
        local_net::{Database, LocalNet, LocalNetConfig, LocalNetTestingConfig},
        ApplicationWrapper, ClientWrapper, LineraNet, LineraNetConfig, Network,
    },
    util,
};
use linera_views::test_utils::get_table_name;
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use test_case::test_case;
use tracing::{info, warn};

fn get_fungible_account_owner(client: &ClientWrapper) -> fungible::AccountOwner {
    let owner = client.get_owner().unwrap();
    fungible::AccountOwner::User(owner)
}

struct FungibleApp(ApplicationWrapper<fungible::FungibleTokenAbi>);

impl FungibleApp {
    async fn get_amount(&self, account_owner: &fungible::AccountOwner) -> Amount {
        let query = format!(
            "accounts {{ entry(key: {}) {{ value }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        serde_json::from_value(response_body["accounts"]["entry"]["value"].clone())
            .unwrap_or_default()
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
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn claim(&self, source: fungible::Account, target: fungible::Account, amount: Amount) {
        // Claiming tokens from chain1 to chain2.
        let mutation = format!(
            "claim(sourceAccount: {}, amount: \"{}\", targetAccount: {})",
            source.to_value(),
            amount,
            target.to_value()
        );

        self.0.mutate(mutation).await.unwrap();
    }
}

struct MatchingEngineApp(ApplicationWrapper<matching_engine::MatchingEngineAbi>);

impl MatchingEngineApp {
    async fn get_account_info(
        &self,
        account_owner: &fungible::AccountOwner,
    ) -> Vec<matching_engine::OrderId> {
        let query = format!(
            "accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(query).await.unwrap();
        serde_json::from_value(response_body["accountInfo"]["entry"]["value"]["orders"].clone())
            .unwrap()
    }

    async fn order(&self, order: matching_engine::Order) -> Value {
        let mutation = format!("executeOrder(order: {})", order.to_value());
        self.0.mutate(mutation).await.unwrap()
    }
}

struct AmmApp(ApplicationWrapper<amm::AmmAbi>);

impl AmmApp {
    async fn add_liquidity(
        &self,
        owner: fungible::AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    ) {
        let operation = amm::Operation::AddLiquidity {
            owner,
            max_token0_amount,
            max_token1_amount,
        };

        let mutation = format!("operation(operation: {})", operation.to_value(),);
        self.0.mutate(mutation).await.unwrap();
    }

    async fn remove_liquidity(
        &self,
        owner: fungible::AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    ) {
        let operation = amm::Operation::RemoveLiquidity {
            owner,
            token_to_remove_idx,
            token_to_remove_amount,
        };

        let mutation = format!("operation(operation: {})", operation.to_value(),);
        self.0.mutate(mutation).await.unwrap();
    }

    async fn swap(
        &self,
        owner: fungible::AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    ) {
        let operation = amm::Operation::Swap {
            owner,
            input_token_idx,
            input_amount,
        };

        let mutation = format!("operation(operation: {})", operation.to_value(),);
        self.0.mutate(mutation).await.unwrap();
    }
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter(config: impl LineraNetConfig) {
    use counter::CounterAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await.unwrap();

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.get_wallet().unwrap().default_chain().unwrap();
    let (contract, service) = client.build_example("counter").await.unwrap();

    let application_id = client
        .publish_and_create::<CounterAbi>(
            contract,
            service,
            &(),
            &original_counter_value,
            &[],
            None,
        )
        .await
        .unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    let application = node_service
        .make_application(&chain, &application_id)
        .await
        .unwrap();

    let counter_value: u64 = application.query_json("value").await.unwrap();
    assert_eq!(counter_value, original_counter_value);

    let mutation = format!("increment(value: {increment})");
    application.mutate(mutation).await.unwrap();

    let counter_value: u64 = application.query_json("value").await.unwrap();
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_counter_publish_create(config: impl LineraNetConfig) {
    use counter::CounterAbi;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await.unwrap();

    let original_counter_value = 35;
    let increment = 5;

    let chain = client.get_wallet().unwrap().default_chain().unwrap();
    let (contract, service) = client.build_example("counter").await.unwrap();

    let bytecode_id = client
        .publish_bytecode(contract, service, None)
        .await
        .unwrap();
    let application_id = client
        .create_application::<CounterAbi>(&bytecode_id, &original_counter_value, None)
        .await
        .unwrap();
    let mut node_service = client.run_node_service(None).await.unwrap();

    let application = node_service
        .make_application(&chain, &application_id)
        .await
        .unwrap();

    let counter_value: u64 = application.query_json("value").await.unwrap();
    assert_eq!(counter_value, original_counter_value);

    let mutation = format!("increment(value: {increment})");
    application.mutate(mutation).await.unwrap();

    let counter_value: u64 = application.query_json("value").await.unwrap();
    assert_eq!(counter_value, original_counter_value + increment);

    node_service.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_social_user_pub_sub(config: impl LineraNetConfig) {
    use social::SocialAbi;
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1
        .open_and_assign(&client2, Amount::ZERO)
        .await
        .unwrap();
    let (contract, service) = client1.build_example("social").await.unwrap();
    let bytecode_id = client1
        .publish_bytecode(contract, service, None)
        .await
        .unwrap();
    let application_id = client1
        .create_application::<SocialAbi>(&bytecode_id, &(), None)
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    node_service1.process_inbox(&chain1).await.unwrap();

    // Request the application so chain 2 has it, too.
    node_service2
        .request_application(&chain2, &application_id)
        .await
        .unwrap();

    let app2 = node_service2
        .make_application(&chain2, &application_id)
        .await
        .unwrap();
    let hash = app2
        .mutate(format!("requestSubscribe(field0: \"{chain1}\")"))
        .await
        .unwrap();

    // The returned hash should now be the latest one.
    let query = format!("query {{ chain(chainId: \"{chain2}\") {{ tipState {{ blockHash }} }} }}");
    let response = node_service2.query_node(&query).await.unwrap();
    assert_eq!(hash, response["chain"]["tipState"]["blockHash"]);

    let app1 = node_service1
        .make_application(&chain1, &application_id)
        .await
        .unwrap();
    app1.mutate("post(field0: \"Linera Social is the new Mastodon!\")")
        .await
        .unwrap();

    // Instead of retrying, we could call `node_service1.process_inbox(chain1).await` here.
    // However, we prefer to test the notification system for a change.
    let expected_response = json!({
        "receivedPosts": {
            "keys": [
                { "author": chain1, "index": 0 }
            ]
        }
    });
    'success: {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let response = app2
                .query("receivedPosts { keys { author, index } }")
                .await
                .unwrap();
            if response == expected_response {
                info!("Confirmed post");
                break 'success;
            }
            warn!("Waiting to confirm post: {}", response);
        }
        panic!("Failed to confirm post");
    }

    node_service1.ensure_is_running().unwrap();
    node_service2.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_fungible(config: impl LineraNetConfig) {
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1
        .open_and_assign(&client2, Amount::ZERO)
        .await
        .unwrap();

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
    let (contract, service) = client1.build_example("fungible").await.unwrap();
    let params = fungible::Parameters::new("FUN");
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi>(contract, service, &params, &state, &[], None)
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    let app1 = FungibleApp(
        node_service1
            .make_application(&chain1, &application_id)
            .await
            .unwrap(),
    );
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
    let app2 = FungibleApp(
        node_service2
            .make_application(&chain2, &application_id)
            .await
            .unwrap(),
    );

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
    node_service1.process_inbox(&chain1).await.unwrap();
    node_service2.process_inbox(&chain2).await.unwrap();

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

    node_service1.ensure_is_running().unwrap();
    node_service2.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_same_wallet_fungible(config: impl LineraNetConfig) {
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let (mut net, client1) = config.instantiate().await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    // Get a chain different than the default
    let chain2 = client1
        .get_wallet()
        .unwrap()
        .chain_ids()
        .into_iter()
        .find(|chain_id| chain_id != &chain1)
        .expect("Failed to obtain a chain ID from the wallet");

    // The players
    let account_owner1 = get_fungible_account_owner(&client1);
    let account_owner2 = {
        let wallet = client1.get_wallet().unwrap();
        let user_chain = wallet.get(chain2).unwrap();
        let public_key = user_chain.key_pair.as_ref().unwrap().public();
        fungible::AccountOwner::User(public_key.into())
    };
    // The initial accounts on chain1
    let accounts = BTreeMap::from([
        (account_owner1, Amount::from_tokens(5)),
        (account_owner2, Amount::from_tokens(2)),
    ]);
    let state = InitialState { accounts };
    // Setting up the application and verifying
    let (contract, service) = client1.build_example("fungible").await.unwrap();
    let params = fungible::Parameters::new("FUN");
    let application_id = client1
        .publish_and_create::<FungibleTokenAbi>(contract, service, &params, &state, &[], None)
        .await
        .unwrap();

    let mut node_service = client1.run_node_service(8080).await.unwrap();

    let app1 = FungibleApp(
        node_service
            .make_application(&chain1, &application_id)
            .await
            .unwrap(),
    );
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

    let app2 = FungibleApp(
        node_service
            .make_application(&chain2, &application_id)
            .await
            .unwrap(),
    );
    app2.assert_balances([(account_owner2, Amount::ONE)]).await;

    node_service.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_crowd_funding(config: impl LineraNetConfig) {
    use crowd_funding::{CrowdFundingAbi, InitializationArgument};
    use fungible::{FungibleTokenAbi, InitialState};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client1) = config.instantiate().await.unwrap();

    let client2 = net.make_client().await;
    client2.wallet_init(&[], None).await.unwrap();

    let chain1 = client1.get_wallet().unwrap().default_chain().unwrap();
    let chain2 = client1
        .open_and_assign(&client2, Amount::ZERO)
        .await
        .unwrap();

    // The players
    let account_owner1 = get_fungible_account_owner(&client1); // operator
    let account_owner2 = get_fungible_account_owner(&client2); // contributor

    // The initial accounts on chain1
    let accounts = BTreeMap::from([(account_owner1, Amount::from_tokens(6))]);
    let state_fungible = InitialState { accounts };

    // Setting up the application fungible
    let (contract_fungible, service_fungible) = client1.build_example("fungible").await.unwrap();
    let params = fungible::Parameters::new("FUN");
    let application_id_fungible = client1
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible,
            service_fungible,
            &params,
            &state_fungible,
            &[],
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
    let (contract_crowd, service_crowd) = client1.build_example("crowd-funding").await.unwrap();
    let application_id_crowd = client1
        .publish_and_create::<CrowdFundingAbi>(
            contract_crowd,
            service_crowd,
            // TODO(#723): This hack will disappear soon.
            &application_id_fungible,
            &state_crowd,
            &[application_id_fungible.forget_abi()],
            None,
        )
        .await
        .unwrap();

    let mut node_service1 = client1.run_node_service(8080).await.unwrap();
    let mut node_service2 = client2.run_node_service(8081).await.unwrap();

    let app_fungible1 = FungibleApp(
        node_service1
            .make_application(&chain1, &application_id_fungible)
            .await
            .unwrap(),
    );

    let app_crowd1 = node_service1
        .make_application(&chain1, &application_id_crowd)
        .await
        .unwrap();

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
        .await
        .unwrap();

    let app_crowd2 = node_service2
        .make_application(&chain2, &application_id_crowd)
        .await
        .unwrap();

    // Transferring
    let mutation = format!(
        "pledgeWithTransfer(owner: {}, amount: \"{}\")",
        account_owner2.to_value(),
        Amount::ONE,
    );
    app_crowd2.mutate(mutation).await.unwrap();

    // Make sure that the pledge is processed fast enough by client1.
    node_service1.process_inbox(&chain1).await.unwrap();

    // Ending the campaign.
    app_crowd1.mutate("collect").await.unwrap();

    // The rich gets their money back.
    app_fungible1
        .assert_balances([(account_owner1, Amount::from_tokens(6))])
        .await;

    node_service1.ensure_is_running().unwrap();
    node_service2.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[ignore] // TODO(#1159)
#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_matching_engine(config: impl LineraNetConfig) {
    use fungible::{FungibleTokenAbi, InitialState};
    use matching_engine::{MatchingEngineAbi, OrderNature, Parameters, Price};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client_admin) = config.instantiate().await.unwrap();

    let client_a = net.make_client().await;
    let client_b = net.make_client().await;

    client_a.wallet_init(&[], None).await.unwrap();
    client_b.wallet_init(&[], None).await.unwrap();

    // Create initial server and client config.
    let (contract_fungible_a, service_fungible_a) =
        client_a.build_example("fungible").await.unwrap();
    let (contract_fungible_b, service_fungible_b) =
        client_b.build_example("fungible").await.unwrap();
    let (contract_matching, service_matching) =
        client_admin.build_example("matching-engine").await.unwrap();

    let chain_admin = client_admin.get_wallet().unwrap().default_chain().unwrap();
    let chain_a = client_admin
        .open_and_assign(&client_a, Amount::ZERO)
        .await
        .unwrap();
    let chain_b = client_admin
        .open_and_assign(&client_b, Amount::ZERO)
        .await
        .unwrap();

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
    let params0 = fungible::Parameters::new("ZERO");
    let token0 = client_a
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible_a,
            service_fungible_a,
            &params0,
            &state_fungible0,
            &[],
            None,
        )
        .await
        .unwrap();
    let params1 = fungible::Parameters::new("ONE");
    let token1 = client_b
        .publish_and_create::<FungibleTokenAbi>(
            contract_fungible_b,
            service_fungible_b,
            &params1,
            &state_fungible1,
            &[],
            None,
        )
        .await
        .unwrap();

    // Now creating the service and exporting the applications
    let mut node_service_admin = client_admin.run_node_service(8080).await.unwrap();
    let mut node_service_a = client_a.run_node_service(8081).await.unwrap();
    let mut node_service_b = client_b.run_node_service(8082).await.unwrap();

    let app_fungible0_a = FungibleApp(
        node_service_a
            .make_application(&chain_a, &token0)
            .await
            .unwrap(),
    );
    let app_fungible1_b = FungibleApp(
        node_service_b
            .make_application(&chain_b, &token1)
            .await
            .unwrap(),
    );
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
        .request_application(&chain_admin, &token0)
        .await
        .unwrap();
    let app_fungible0_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token0)
            .await
            .unwrap(),
    );
    node_service_admin
        .request_application(&chain_admin, &token1)
        .await
        .unwrap();
    let app_fungible1_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token1)
            .await
            .unwrap(),
    );
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
    let parameter = Parameters {
        tokens: [token0, token1],
    };
    let bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_matching, service_matching)
        .await
        .unwrap();
    let application_id_matching = node_service_admin
        .create_application::<MatchingEngineAbi>(
            &chain_admin,
            &bytecode_id,
            &parameter,
            &(),
            &[token0.forget_abi(), token1.forget_abi()],
        )
        .await
        .unwrap();
    let app_matching_admin = MatchingEngineApp(
        node_service_admin
            .make_application(&chain_admin, &application_id_matching)
            .await
            .unwrap(),
    );
    node_service_a
        .request_application(&chain_a, &application_id_matching)
        .await
        .unwrap();
    let app_matching_a = MatchingEngineApp(
        node_service_a
            .make_application(&chain_a, &application_id_matching)
            .await
            .unwrap(),
    );
    node_service_b
        .request_application(&chain_b, &application_id_matching)
        .await
        .unwrap();
    let app_matching_b = MatchingEngineApp(
        node_service_b
            .make_application(&chain_b, &application_id_matching)
            .await
            .unwrap(),
    );

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
    node_service_admin
        .process_inbox(&chain_admin)
        .await
        .unwrap();
    node_service_a.process_inbox(&chain_a).await.unwrap();
    node_service_b.process_inbox(&chain_b).await.unwrap();

    // Now reading the order_ids
    let order_ids_a = app_matching_admin.get_account_info(&owner_a).await;
    let order_ids_b = app_matching_admin.get_account_info(&owner_b).await;
    // The deal that occurred is that 6 token0 were exchanged for 3 token1.
    assert_eq!(order_ids_a.len(), 1); // The order of price 2 is completely filled.
    assert_eq!(order_ids_b.len(), 2); // The order of price 2 is partially filled.

    // Now cancelling all the orders
    for order_id in order_ids_a {
        app_matching_a
            .order(matching_engine::Order::Cancel {
                owner: owner_a,
                order_id,
            })
            .await;
    }
    for order_id in order_ids_b {
        app_matching_b
            .order(matching_engine::Order::Cancel {
                owner: owner_b,
                order_id,
            })
            .await;
    }
    node_service_admin
        .process_inbox(&chain_admin)
        .await
        .unwrap();

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

    node_service_admin.ensure_is_running().unwrap();
    node_service_a.ensure_is_running().unwrap();
    node_service_b.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_amm(config: impl LineraNetConfig) {
    use amm::{AmmAbi, Parameters};
    use fungible::{FungibleTokenAbi, InitialState};
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client_admin) = config.instantiate().await.unwrap();

    let client0 = net.make_client().await;
    let client1 = net.make_client().await;
    client0.wallet_init(&[], None).await.unwrap();
    client1.wallet_init(&[], None).await.unwrap();

    let (contract_fungible, service_fungible) =
        client_admin.build_example("fungible").await.unwrap();
    let (contract_amm, service_amm) = client_admin.build_example("amm").await.unwrap();

    // Admin chain
    let chain_admin = client_admin.get_wallet().unwrap().default_chain().unwrap();

    // User chains
    let chain0 = client_admin
        .open_and_assign(&client0, Amount::ZERO)
        .await
        .unwrap();
    let chain1 = client_admin
        .open_and_assign(&client1, Amount::ZERO)
        .await
        .unwrap();

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
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(50)),
            (owner_admin, Amount::from_tokens(200)),
        ]),
    };

    // Amounts of token1 that will be owned by each user
    let state_fungible1 = InitialState {
        accounts: BTreeMap::from([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::from_tokens(200)),
        ]),
    };

    // Create fungible applications on the Admin chain, which will hold
    // the token0 and token1 amounts
    let fungible_bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_fungible, service_fungible)
        .await
        .unwrap();

    let params0 = fungible::Parameters::new("ZERO");
    let token0 = node_service_admin
        .create_application::<FungibleTokenAbi>(
            &chain_admin,
            &fungible_bytecode_id,
            &params0,
            &state_fungible0,
            &[],
        )
        .await
        .unwrap();
    let params1 = fungible::Parameters::new("ONE");
    let token1 = node_service_admin
        .create_application::<FungibleTokenAbi>(
            &chain_admin,
            &fungible_bytecode_id,
            &params1,
            &state_fungible1,
            &[],
        )
        .await
        .unwrap();

    // Create wrappers
    let app_fungible0_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token0)
            .await
            .unwrap(),
    );
    let app_fungible1_admin = FungibleApp(
        node_service_admin
            .make_application(&chain_admin, &token1)
            .await
            .unwrap(),
    );

    // Check initial balances
    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(50)),
            (owner_admin, Amount::from_tokens(200)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::from_tokens(200)),
        ])
        .await;

    let parameters = Parameters {
        tokens: [token0, token1],
    };

    // Create AMM application on Admin chain
    let bytecode_id = node_service_admin
        .publish_bytecode(&chain_admin, contract_amm, service_amm)
        .await
        .unwrap();
    let application_id_amm = node_service_admin
        .create_application::<AmmAbi>(
            &chain_admin,
            &bytecode_id,
            &parameters,
            &(),
            &[token0.forget_abi(), token1.forget_abi()],
        )
        .await
        .unwrap();

    let owner_amm = fungible::AccountOwner::Application(application_id_amm.forget_abi());

    // Create AMM wrappers
    let app_amm_admin = AmmApp(
        node_service_admin
            .make_application(&chain_admin, &application_id_amm)
            .await
            .unwrap(),
    );
    node_service0
        .request_application(&chain0, &application_id_amm)
        .await
        .unwrap();
    let app_amm0 = AmmApp(
        node_service0
            .make_application(&chain0, &application_id_amm)
            .await
            .unwrap(),
    );
    node_service1
        .request_application(&chain1, &application_id_amm)
        .await
        .unwrap();
    let app_amm1 = AmmApp(
        node_service1
            .make_application(&chain1, &application_id_amm)
            .await
            .unwrap(),
    );

    // Initial balances for both tokens are 0

    // Adding liquidity for token0 and token1 by owner0
    // We have to call it from the AMM instance in the owner0's
    // user chain, chain0, so it's properly authenticated
    app_amm_admin
        .add_liquidity(
            owner_admin,
            Amount::from_tokens(100),
            Amount::from_tokens(100),
        )
        .await;

    // Ownership of owner0 tokens should be with the AMM now
    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(50)),
            (owner_admin, Amount::from_tokens(100)),
            (owner_amm, Amount::from_tokens(100)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::from_tokens(100)),
            (owner_amm, Amount::from_tokens(100)),
        ])
        .await;

    // Adding liquidity for token0 and token1 by owner1 now
    app_amm_admin
        .add_liquidity(
            owner_admin,
            Amount::from_tokens(100),
            Amount::from_tokens(100),
        )
        .await;

    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(50)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(200)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(200)),
        ])
        .await;

    app_amm1.swap(owner1, 0, Amount::from_tokens(50)).await;
    node_service_admin
        .process_inbox(&chain_admin)
        .await
        .unwrap();

    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::ZERO),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(250)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::from_tokens(40)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(160)),
        ])
        .await;

    // Can only remove liquidity from main chain
    app_amm_admin
        .remove_liquidity(owner1, 0, Amount::from_tokens(62))
        .await;

    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(62)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(188)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::from_milli(79_680)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_milli(120_320)),
        ])
        .await;

    app_amm1.swap(owner1, 0, Amount::from_tokens(50)).await;
    node_service_admin
        .process_inbox(&chain_admin)
        .await
        .unwrap();

    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_tokens(12)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_tokens(238)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::from_tokens(50)),
            (owner1, Amount::from_atto(104_957310924369747899)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_atto(95_042689075630252101)),
        ])
        .await;

    app_amm0.swap(owner0, 1, Amount::from_tokens(50)).await;
    node_service_admin
        .process_inbox(&chain_admin)
        .await
        .unwrap();

    app_fungible0_admin
        .assert_balances([
            (owner0, Amount::from_atto(82_044810916287757646)),
            (owner1, Amount::from_tokens(12)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_atto(155_955189083712242354)),
        ])
        .await;
    app_fungible1_admin
        .assert_balances([
            (owner0, Amount::ZERO),
            (owner1, Amount::from_atto(104_957310924369747899)),
            (owner_admin, Amount::ZERO),
            (owner_amm, Amount::from_atto(145_042689075630252101)),
        ])
        .await;

    node_service_admin.ensure_is_running().unwrap();
    node_service0.ensure_is_running().unwrap();
    node_service1.ensure_is_running().unwrap();

    net.ensure_is_running().await.unwrap();
    net.terminate().await.unwrap();
}

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
            let chain_2 = client
                .open_and_assign(&client_2, Amount::ZERO)
                .await
                .unwrap();
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
    let (_, chain_3) = client
        .open_chain(chain_1, None, Amount::ZERO)
        .await
        .unwrap();

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
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
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

#[cfg_attr(feature = "rocksdb", test_case(LocalNetTestingConfig::new(Database::RocksDb, Network::Grpc) ; "rocksdb_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetTestingConfig::new(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "aws", test_case(LocalNetTestingConfig::new(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
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
    let (message_id, chain2) = client1
        .open_chain(chain1, Some(client2_key), Amount::ZERO)
        .await
        .unwrap();

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
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
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
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
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
    let (_, grandparent) = client1
        .open_chain(chain1, None, Amount::ZERO)
        .await
        .unwrap();
    let (_, parent) = client1
        .open_chain(grandparent, None, Amount::ZERO)
        .await
        .unwrap();
    let (message_id, chain2) = client1
        .open_chain(parent, Some(client2_key), Amount::ZERO)
        .await
        .unwrap();
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
#[cfg_attr(feature = "kubernetes", test_case(SharedLocalKubernetesNetTestingConfig::new(Network::Grpc, None) ; "kubernetes_grpc"))]
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
