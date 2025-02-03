// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "storage-service",
    feature = "dynamodb",
    feature = "scylladb"
))]

use std::{str::FromStr, sync::LazyLock, time::Duration};

use linera_base::{command::resolve_binary, data_types::Amount, identifiers::ChainId};
use linera_indexer_graphql_client::{
    indexer::{plugins, state, Plugins, State},
    operations::{get_operation, GetOperation, OperationKey},
};
use linera_service::cli_wrappers::{
    local_net::{Database, LocalNetConfig, PathProvider, ProcessInbox},
    LineraNet, LineraNetConfig, Network,
};
use linera_service_graphql_client::{block, request, transfer, Block, Transfer};
use test_case::test_case;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use tracing::{info, warn};

/// A static lock to prevent integration tests from running in parallel.
static INTEGRATION_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

async fn run_indexer(path_provider: &PathProvider) -> Child {
    let port = 8081;
    let path = resolve_binary("linera-indexer", "linera-indexer-example")
        .await
        .unwrap();
    let mut command = Command::new(path);
    command
        .current_dir(path_provider.path())
        .kill_on_drop(true)
        .args(["run"]);
    let child = command.spawn().unwrap();
    let client = reqwest_client();
    for i in 0..10 {
        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        let request = client
            .get(format!("http://localhost:{}/", port))
            .send()
            .await;
        if request.is_ok() {
            info!("Indexer has started");
            return child;
        } else {
            warn!("Waiting for indexer to start");
        }
    }
    panic!("Failed to start indexer");
}

fn indexer_running(child: &mut Child) {
    if let Some(status) = child.try_wait().unwrap() {
        assert!(status.success());
    }
}

async fn transfer(client: &reqwest::Client, from: ChainId, to: ChainId, amount: &str) {
    let variables = transfer::Variables {
        chain_id: from,
        recipient: to,
        amount: Amount::from_str(amount).unwrap(),
    };
    request::<Transfer, _>(client, "http://localhost:8080", variables)
        .await
        .unwrap();
}

#[cfg(debug_assertions)]
const TRANSFER_DELAY_MILLIS: u64 = 1000;

#[cfg(not(debug_assertions))]
const TRANSFER_DELAY_MILLIS: u64 = 100;

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc); "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_operations_indexer(config: impl LineraNetConfig) {
    // launching network, service and indexer
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await.unwrap();
    let mut node_service = client
        .run_node_service(None, ProcessInbox::Automatic)
        .await
        .unwrap();
    let mut indexer = run_indexer(&client.path_provider).await;

    // check operations plugin
    let req_client = reqwest_client();
    let plugins = request::<Plugins, _>(&req_client, "http://localhost:8081", plugins::Variables)
        .await
        .unwrap()
        .plugins;
    assert_eq!(
        plugins,
        vec!["operations"],
        "Indexer plugin 'operations' not loaded",
    );

    // making a few transfers
    let chain0 = ChainId::root(0);
    let chain1 = ChainId::root(1);
    for _ in 0..10 {
        transfer(&req_client, chain0, chain1, "0.1").await;
        linera_base::time::timer::sleep(Duration::from_millis(TRANSFER_DELAY_MILLIS)).await;
    }
    linera_base::time::timer::sleep(Duration::from_secs(2)).await;

    // checking indexer state
    let variables = block::Variables {
        hash: None,
        chain_id: chain0,
    };
    let last_block = request::<Block, _>(&req_client, "http://localhost:8080", variables)
        .await
        .unwrap()
        .block
        .unwrap_or_else(|| panic!("no block found"));
    let last_hash = last_block.clone().hash;

    let indexer_state = request::<State, _>(&req_client, "http://localhost:8081", state::Variables)
        .await
        .unwrap()
        .state;
    let indexer_hash =
        indexer_state
            .iter()
            .find_map(|arg| if arg.chain == chain0 { arg.block } else { None });
    assert_eq!(
        Some(last_hash),
        indexer_hash,
        "Different states between service and indexer"
    );

    // checking indexer operation
    let last_operation = last_block.value.block.body.operations[0].clone();
    let variables = get_operation::Variables {
        key: get_operation::OperationKeyKind::Last(chain0),
    };

    let indexer_operation =
        request::<GetOperation, _>(&req_client, "http://localhost:8081/operations", variables)
            .await
            .unwrap()
            .operation;
    match indexer_operation {
        Some(get_operation::GetOperationOperation {
            key,
            block,
            content,
            ..
        }) => {
            assert_eq!(
                (key, block, content),
                (
                    OperationKey {
                        chain_id: chain0,
                        height: last_block.value.block.header.height,
                        index: 0
                    },
                    last_hash,
                    last_operation
                ),
                "service and indexer operations are different"
            )
        }
        None => panic!("no operation found"),
    }

    indexer_running(&mut indexer);
    node_service.ensure_is_running().unwrap();
    net.terminate().await.unwrap();
}
