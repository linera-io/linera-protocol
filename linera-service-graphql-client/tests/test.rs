// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "test-storage-service",
    feature = "test-dynamodb",
    feature = "test-scylladb"
))]

use std::{collections::BTreeMap, str::FromStr, sync::LazyLock, time::Duration};

use fungible::{FungibleTokenAbi, InitialState};
use linera_base::{data_types::Amount, identifiers::ChainId};
use linera_service::cli_wrappers::{
    local_net::{Database, LocalNetConfig, ProcessInbox},
    LineraNet, LineraNetConfig, Network,
};
use linera_service_graphql_client::{
    applications, block, blocks, chains, request, transfer, Applications, Block, Blocks, Chains,
    Transfer,
};
use test_case::test_case;
use tokio::sync::Mutex;

/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn reqwest_client() -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

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

#[cfg_attr(feature = "test-storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc); "storage_service_grpc"))]
#[cfg_attr(feature = "test-scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "test-dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "dynamodb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_queries(config: impl LineraNetConfig) {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await.unwrap();

    let node_chains = {
        let wallet = client.load_wallet().unwrap();
        (wallet.default_chain(), wallet.chain_ids())
    };
    let chain_id = node_chains.0.unwrap();

    // publishing an application
    let (contract, service) = client.build_example("fungible").await.unwrap();
    let state = InitialState {
        accounts: BTreeMap::new(),
    };
    let params = fungible::Parameters::new("FUN");
    let application_id = client
        .publish_and_create::<FungibleTokenAbi, fungible::Parameters, InitialState>(
            contract,
            service,
            &params,
            &state,
            &[],
            None,
        )
        .await
        .unwrap();

    let mut node_service = client
        .run_node_service(None, ProcessInbox::Automatic)
        .await
        .unwrap();
    let req_client = &reqwest_client();
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
    assert_eq!(applications[0].id, application_id.forget_abi().to_string());

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
        &reqwest_client(),
        &format!("http://localhost:{}/", node_service.port()),
        block::Variables {
            chain_id,
            hash: None,
        },
    )
    .await
    .unwrap()
    .block;

    node_service.ensure_is_running().unwrap();
    net.terminate().await.unwrap();
}
