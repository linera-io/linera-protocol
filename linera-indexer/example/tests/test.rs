// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(
    feature = "storage-service",
    feature = "dynamodb",
    feature = "scylladb"
))]

use std::{str::FromStr, sync::LazyLock, time::Duration};

use linera_base::{
    command::resolve_binary,
    data_types::Amount,
    identifiers::{Account, AccountOwner, ChainId},
};
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

async fn run_indexer(path_provider: &PathProvider) -> anyhow::Result<Child> {
    let port = 8081;
    let path = resolve_binary("linera-indexer", "linera-indexer-example").await?;
    let mut command = Command::new(path);
    command
        .current_dir(path_provider.path())
        .kill_on_drop(true)
        .args(["run-graph-ql"]);
    let child = command.spawn()?;
    let client = reqwest_client();
    for i in 0..10 {
        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
        let request = client
            .get(format!("http://localhost:{}/", port))
            .send()
            .await;
        if request.is_ok() {
            info!("Indexer has started");
            return Ok(child);
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

async fn transfer(
    client: &reqwest::Client,
    from: ChainId,
    to: Account,
    amount: &str,
) -> anyhow::Result<()> {
    let variables = transfer::Variables {
        chain_id: from,
        owner: AccountOwner::CHAIN,
        recipient: transfer::Account {
            chain_id: to.chain_id,
            owner: to.owner,
        },
        amount: Amount::from_str(amount)?,
    };
    request::<Transfer, _>(client, "http://localhost:8080", variables).await?;
    Ok(())
}

#[cfg(debug_assertions)]
const TRANSFER_DELAY_MILLIS: u64 = 1000;

#[cfg(not(debug_assertions))]
const TRANSFER_DELAY_MILLIS: u64 = 100;

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::new_test(Database::Service, Network::Grpc); "storage_service_grpc"))]
#[cfg_attr(feature = "scylladb", test_case(LocalNetConfig::new_test(Database::ScyllaDb, Network::Grpc) ; "scylladb_grpc"))]
#[cfg_attr(feature = "dynamodb", test_case(LocalNetConfig::new_test(Database::DynamoDb, Network::Grpc) ; "aws_grpc"))]
#[test_log::test(tokio::test)]
async fn test_end_to_end_operations_indexer(config: impl LineraNetConfig) -> anyhow::Result<()> {
    // launching network, service and indexer
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let (mut net, client) = config.instantiate().await?;
    let mut node_service = client
        .run_node_service(None, ProcessInbox::Automatic)
        .await?;
    let mut indexer = run_indexer(&client.path_provider).await?;

    // check operations plugin
    let req_client = reqwest_client();
    let plugins = request::<Plugins, _>(&req_client, "http://localhost:8081", plugins::Variables)
        .await?
        .plugins;
    assert_eq!(
        plugins,
        vec!["operations"],
        "Indexer plugin 'operations' not loaded",
    );

    // making a few transfers
    let node_chains = {
        let wallet = client.load_wallet()?;
        wallet.chain_ids()
    };
    let chain0 = node_chains[0];
    let chain1 = Account::chain(node_chains[1]);
    for _ in 0..10 {
        transfer(&req_client, chain0, chain1, "0.1").await?;
        linera_base::time::timer::sleep(Duration::from_millis(TRANSFER_DELAY_MILLIS)).await;
    }
    linera_base::time::timer::sleep(Duration::from_secs(2)).await;

    // checking indexer state
    let variables = block::Variables {
        hash: None,
        chain_id: chain0,
    };
    let last_block = request::<Block, _>(&req_client, "http://localhost:8080", variables)
        .await?
        .block
        .unwrap_or_else(|| panic!("no block found"));
    let last_hash = last_block.clone().hash;

    let indexer_state = request::<State, _>(&req_client, "http://localhost:8081", state::Variables)
        .await?
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

    // checking indexer operation (updated for new transaction structure)
    // Note: The transactions field is not exposed via GraphQL due to technical limitations,
    // but we can still verify that the indexer correctly tracks operations

    let variables = get_operation::Variables {
        key: get_operation::OperationKeyKind::Last(chain0),
    };

    let indexer_operation =
        request::<GetOperation, _>(&req_client, "http://localhost:8081/operations", variables)
            .await?
            .operation;
    match indexer_operation {
        Some(get_operation::GetOperationOperation { key, block, .. }) => {
            assert_eq!(
                (key, block),
                (
                    OperationKey {
                        chain_id: chain0,
                        height: last_block.block.header.height,
                        index: 0
                    },
                    last_hash,
                ),
                "service and indexer operations are different"
            )
        }
        None => panic!("no operation found"),
    }

    indexer_running(&mut indexer);
    node_service.ensure_is_running()?;
    net.terminate().await?;
    Ok(())
}
