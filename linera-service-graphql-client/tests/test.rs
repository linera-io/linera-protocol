// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "rocksdb", feature = "aws", feature = "scylladb"))]

use fungible::{FungibleTokenAbi, InitialState};
use linera_base::{data_types::Amount, identifiers::ChainId};
use linera_service::cli_wrappers::{resolve_binary, Database, LocalNetwork, Network};
use linera_service_graphql_client::{
    applications, block, blocks, chains, request, transfer, Applications, Block, Blocks, Chains,
    Transfer,
};
use once_cell::sync::Lazy;
use std::{collections::BTreeMap, io::Read, rc::Rc, str::FromStr};
use tempfile::tempdir;
use tokio::{process::Command, sync::Mutex};

/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

async fn transfer(client: &reqwest::Client, url: &str, from: ChainId, to: ChainId, amount: &str) {
    let variables = transfer::Variables {
        chain_id: from,
        recipient: to,
        amount: Amount::from_str(amount).unwrap(),
    };
    request::<Transfer, _>(client, url, variables)
        .await
        .unwrap();
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_end_to_end_queries() {
    run_end_to_end_queries(Database::RocksDb).await
}

#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_end_to_end_queries() {
    run_end_to_end_queries(Database::DynamoDb).await
}

#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_end_to_end_queries() {
    run_end_to_end_queries(Database::ScyllaDb).await
}

async fn run_end_to_end_queries(database: Database) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let network = Network::Grpc;
    let mut local_net = LocalNetwork::new_for_testing(database, network).unwrap();
    let client = local_net.make_client(network);
    local_net.generate_initial_validator_config().await.unwrap();

    client.create_genesis_config().await.unwrap();
    local_net.run().await.unwrap();

    let node_chains = {
        let wallet = client.get_wallet();
        (wallet.default_chain(), wallet.chain_ids())
    };
    let chain_id = node_chains.0.unwrap();

    // publishing an application
    let (contract, service) = local_net.build_example("fungible").await.unwrap();
    let state = InitialState {
        accounts: BTreeMap::new(),
    };
    let application_id = client
        .publish_and_create::<FungibleTokenAbi>(contract, service, &(), &state, vec![], None)
        .await
        .unwrap();

    let mut node_service = client.run_node_service(None).await.unwrap();
    let req_client = &reqwest::Client::new();
    let url = &format!("http://localhost:{}/", node_service.port());

    // sending a few transfers
    let chain0 = ChainId::root(0);
    let chain1 = ChainId::root(1);
    for _ in 0..10 {
        transfer(req_client, url, chain0, chain1, "0.1").await;
    }

    // check chains query
    let chains = request::<Chains, _>(req_client, url, chains::Variables)
        .await
        .unwrap()
        .chains;
    assert_eq!((chains.default, chains.list), node_chains);

    // check applications query
    let applications = request::<Applications, _>(
        req_client,
        url,
        applications::Variables {
            chain_id: node_chains.0.unwrap(),
        },
    )
    .await
    .unwrap()
    .applications;
    assert_eq!(applications[0].id, application_id);

    // check blocks query
    let blocks = request::<Blocks, _>(
        req_client,
        url,
        blocks::Variables {
            chain_id,
            from: None,
            limit: None,
        },
    )
    .await
    .unwrap()
    .blocks;
    assert_eq!(blocks.len(), 10);

    // check block query
    let _block = request::<Block, _>(
        &reqwest::Client::new(),
        &format!("http://localhost:{}/", node_service.port()),
        block::Variables {
            chain_id,
            hash: None,
        },
    )
    .await
    .unwrap()
    .block;

    node_service.assert_is_running();
}

#[test_log::test(tokio::test)]
async fn test_check_service_schema() {
    let tmp_dir = Rc::new(tempdir().unwrap());
    let path = resolve_binary("linera-schema-export", Some("linera-service"))
        .await
        .unwrap();
    let mut command = Command::new(path);
    let output = command
        .current_dir(tmp_dir.path().canonicalize().unwrap())
        .output()
        .await
        .unwrap();
    let service_schema = String::from_utf8(output.stdout).unwrap();
    let mut file_base = std::fs::File::open("gql/service_schema.graphql").unwrap();
    let mut graphql_schema = String::new();
    file_base.read_to_string(&mut graphql_schema).unwrap();
    assert_eq!(
        graphql_schema, service_schema,
        "\nGraphQL service schema has changed -> \
         regenerate schema following steps in linera-service-graphql-client/README.md\n"
    )
}
