// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

mod guard;

use anyhow::Result;
use guard::INTEGRATION_TEST_GUARD;
use linera_base::{crypto::CryptoHash, vm::VmRuntime};
use linera_sdk::linera_base_types::{BlobContent, DataBlobHash};
use linera_service::{
    cli_wrappers::{
        local_net::{get_node_port, Database, LocalNetConfig, ProcessInbox},
        LineraNet, LineraNetConfig, Network,
    },
    test_name,
};
use test_case::test_case;

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::minimal_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_create_and_call_end_to_end_(config: impl LineraNetConfig) -> Result<()> {
    use create_and_call::{CreateAndCallAbi, CreateAndCallRequest};
    use linera_base::data_types::Bytecode;

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    // Step 1: Download the contract and service of "counter-no-graphql" as Vec<u8>
    let (counter_contract_path, counter_service_path) =
        client.build_example("counter-no-graphql").await?;
    let counter_contract_bytecode = Bytecode::load_from_file(&counter_contract_path).await?;
    let counter_service_bytecode = Bytecode::load_from_file(&counter_service_path).await?;
    let contract_bytes = counter_contract_bytecode.bytes;
    let service_bytes = counter_service_bytecode.bytes;

    // Step 2: Instantiate the contract "create-and-call"
    let chain = client.load_wallet()?.default_chain().unwrap();
    let (create_call_contract, create_call_service) =
        client.build_example("create-and-call").await?;

    let application_id = client
        .publish_and_create::<CreateAndCallAbi, (), ()>(
            create_call_contract,
            create_call_service,
            VmRuntime::Wasm,
            &(),
            &(), // Initial value
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    // Step 3: Call a mutation that takes the Vec<u8> of "contract", "service",
    // the initialization value of 43 and the increment of 5
    let initialization_value = 43;
    let increment_value = 5;

    let mutation_request = CreateAndCallRequest::CreateAndCall(
        contract_bytes,
        service_bytes,
        initialization_value,
        increment_value,
    );

    application.run_json_query(&mutation_request).await?;

    // Step 4: Query the contract and see if we obtain 48 (43 + 5)
    let query_request = CreateAndCallRequest::Query;
    let result_value = application.run_json_query(&query_request).await?;
    assert_eq!(result_value, 48);

    node_service.ensure_is_running()?;
    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}

#[cfg_attr(feature = "storage-service", test_case(LocalNetConfig::minimal_test(Database::Service, Network::Grpc) ; "storage_test_service_grpc"))]
#[test_log::test(tokio::test)]
async fn test_wasm_end_to_end_publish_read_data_blob(config: impl LineraNetConfig) -> Result<()> {
    use publish_read_data_blob::{PublishReadDataBlobAbi, ServiceQuery};

    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {}", test_name!());

    let (mut net, client) = config.instantiate().await?;

    let chain = client.load_wallet()?.default_chain().unwrap();
    let (contract, service) = client.build_example("publish-read-data-blob").await?;

    let application_id = client
        .publish_and_create::<PublishReadDataBlobAbi, (), ()>(
            contract,
            service,
            VmRuntime::Wasm,
            &(),
            &(),
            &[],
            None,
        )
        .await?;

    let port = get_node_port().await;
    let mut node_service = client.run_node_service(port, ProcessInbox::Skip).await?;

    let application = node_service
        .make_application(&chain, &application_id)
        .await?;

    // Method 1: Publishing and reading in different blocks.

    let test_data = b"This is test data for method 1.".to_vec();

    // publishing the data.
    let mutation = ServiceQuery::PublishDataBlob(test_data.clone());
    application.run_json_query(&mutation).await?;

    // getting the hash
    let content = BlobContent::new_data(test_data.clone());
    let hash = DataBlobHash(CryptoHash::new(&content));

    // reading and checking
    let mutation = ServiceQuery::ReadDataBlob(hash, test_data);
    application.run_json_query(&mutation).await?;

    // Method 2: Publishing and reading in the same transaction

    let test_data = b"This is test data for method 2.".to_vec();

    let mutation = ServiceQuery::PublishAndCreateOneOperation(test_data);
    application.run_json_query(&mutation).await?;

    // Method 3: Publishing and reading in the same block but different transactions

    let test_data = b"This is test data for method 3.".to_vec();

    let mutation = ServiceQuery::PublishAndCreateTwoOperations(test_data);
    application.run_json_query(&mutation).await?;

    // Winding down

    node_service.ensure_is_running()?;
    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}
