// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! WASM specific worker tests.

use super::init_worker_with_chains;
use linera_base::{crypto::KeyPair, data_types::ChainDescription};
use linera_execution::system::Balance;
use linera_storage::{DynamoDbStoreClient, MemoryStoreClient, RocksdbStoreClient, Store};
use linera_views::{test_utils::LocalStackTestContext, views::ViewError};
use test_log::test;

#[test(tokio::test)]
async fn test_memory_handle_certificates_to_create_application() -> Result<(), anyhow::Error> {
    let client = MemoryStoreClient::default();
    run_test_handle_certificates_to_create_application(client).await
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificates_to_create_application() -> Result<(), anyhow::Error> {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf());
    run_test_handle_certificates_to_create_application(client).await
}

#[test(tokio::test)]
#[ignore]
async fn test_dynamo_db_handle_certificates_to_create_application() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table).await?;
    run_test_handle_certificates_to_create_application(client).await
}

async fn run_test_handle_certificates_to_create_application<S>(
    client: S,
) -> Result<(), anyhow::Error>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let admin_id = ChainDescription::Root(0);
    let publisher_key_pair = KeyPair::generate();
    let publisher_chain = ChainDescription::Root(1);
    let creator_key_pair = KeyPair::generate();
    let creator_chain = ChainDescription::Root(2);
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                publisher_chain,
                publisher_key_pair.public(),
                Balance::from(0),
            ),
            (creator_chain, creator_key_pair.public(), Balance::from(0)),
        ],
    )
    .await;

    todo!();
}
