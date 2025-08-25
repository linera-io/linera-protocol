// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "storage-service",
    feature = "dynamodb",
    feature = "scylladb"
))]

use std::{collections::BTreeMap, str::FromStr, sync::LazyLock, time::Duration};

use fungible::{FungibleTokenAbi, InitialState};
use linera_base::{
    data_types::Amount,
    identifiers::{Account, AccountOwner, ChainId},
    vm::VmRuntime,
};
use linera_service::cli_wrappers::{
    local_net::{Database, LocalNetConfig, ProcessInbox},
    LineraNet, LineraNetConfig, Network,
};
use linera_service_graphql_client::{
    block, blocks, chains, request, transfer, Block, Blocks, Chains, Transfer,
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

async fn transfer(client: &reqwest::Client, url: &str, from: ChainId, to: Account, amount: &str) {
    let variables = transfer::Variables {
        chain_id: from,
        owner: AccountOwner::CHAIN,
        recipient: transfer::Account {
            chain_id: to.chain_id,
            owner: to.owner,
        },
        amount: Amount::from_str(amount).unwrap(),
    };
    request::<Transfer, _>(client, url, variables)
        .await
        .unwrap();
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc); "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "dynamodb_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_queries(config: impl LineraNetConfig) -> anyhow::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await?;
    let owner = client.get_owner().unwrap();

    let node_chains = {
        let wallet = client.load_wallet()?;
        (wallet.default_chain(), wallet.chain_ids())
    };
    let chain0 = node_chains.0.unwrap();

    // publishing an application
    let (contract, service) = client.build_example("fungible").await?;
    let vm_runtime = VmRuntime::Wasm;
    let accounts = BTreeMap::from([(owner, Amount::from_tokens(9))]);
    let state = InitialState { accounts };
    let params = fungible::Parameters::new("FUN");
    let _application_id = client
        .publish_and_create::<FungibleTokenAbi, fungible::Parameters, InitialState>(
            contract,
            service,
            vm_runtime,
            &params,
            &state,
            &[],
            None,
        )
        .await?;

    let mut node_service = client
        .run_node_service(None, ProcessInbox::Automatic)
        .await?;
    let req_client = &reqwest_client();
    let url = &format!("http://localhost:{}/", node_service.port());

    // sending a few transfers
    let chain1 = Account::chain(node_chains.1[1]);
    for _ in 0..10 {
        transfer(req_client, url, chain0, chain1, "0.1").await;
    }

    // check chains query
    let chains = request::<Chains, _>(req_client, url, chains::Variables)
        .await?
        .chains;
    assert_eq!((chains.default, chains.list), node_chains);

    // check blocks query
    let blocks = request::<Blocks, _>(
        req_client,
        url,
        blocks::Variables {
            chain_id: chain0,
            from: None,
            limit: None,
        },
    )
    .await?
    .blocks;
    assert_eq!(blocks.len(), 10);

    // check block query
    let _block = request::<Block, _>(
        &reqwest_client(),
        &format!("http://localhost:{}/", node_service.port()),
        block::Variables {
            chain_id: chain0,
            hash: None,
        },
    )
    .await?
    .block;

    node_service.ensure_is_running()?;
    net.terminate().await?;
    Ok(())
}
