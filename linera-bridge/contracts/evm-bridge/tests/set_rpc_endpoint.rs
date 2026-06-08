// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the EVM bridge app's SetRpcEndpoint operation.

#![cfg(not(target_arch = "wasm32"))]

use evm_bridge::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters, EvmBridgeAbi};
use linera_sdk::{
    linera_base_types::{ApplicationId, CryptoHash, TestString},
    test::{ActiveChain, QueryOutcome, TestValidator},
};

/// Boots a TestValidator with the bridge module and instantiates a fresh
/// bridge application with finality verification disabled (empty RPC endpoint).
async fn setup_bridge() -> (ActiveChain, ApplicationId<EvmBridgeAbi>) {
    let (validator, bridge_module_id) = TestValidator::with_current_module::<
        EvmBridgeAbi,
        BridgeParameters,
        BridgeInstantiationArgument,
    >()
    .await;
    let mut chain = validator.new_chain().await;

    let bridge_params = BridgeParameters {
        source_chain_id: 8453u64,
        token_address: [0xA0; 20],
        bridge_chain_id: chain.id(),
        // This suite never mints/burns; a placeholder fungible app id suffices.
        fungible_app_id: ApplicationId::new(CryptoHash::new(&TestString::new("dummy_fungible"))),
    };
    let bridge_app_id = chain
        .create_application(
            bridge_module_id,
            bridge_params,
            BridgeInstantiationArgument::default(),
            vec![],
        )
        .await;

    (chain, bridge_app_id)
}

/// Reads the bridge service's `rpcEndpoint` GraphQL field.
async fn query_rpc_endpoint(chain: &ActiveChain, app_id: ApplicationId<EvmBridgeAbi>) -> String {
    let QueryOutcome { response, .. } = chain.graphql_query(app_id, "query { rpcEndpoint }").await;
    response["rpcEndpoint"]
        .as_str()
        .expect("rpcEndpoint should be a string")
        .to_string()
}

#[tokio::test]
async fn test_set_rpc_endpoint_updates_state() {
    let (chain, bridge_app_id) = setup_bridge().await;

    // Initially the endpoint is empty (finality verification disabled).
    assert_eq!(query_rpc_endpoint(&chain, bridge_app_id).await, "");

    let new_endpoint = "https://rpc.example.com".to_string();
    chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::SetRpcEndpoint {
                    rpc_endpoint: new_endpoint.clone(),
                },
            );
        })
        .await;

    // The new endpoint must be observable on the service, which reads from state.
    assert_eq!(
        query_rpc_endpoint(&chain, bridge_app_id).await,
        new_endpoint,
    );

    // Empty string is a valid value (disables finality verification).
    chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::SetRpcEndpoint {
                    rpc_endpoint: String::new(),
                },
            );
        })
        .await;

    assert_eq!(query_rpc_endpoint(&chain, bridge_app_id).await, "");
}
