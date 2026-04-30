// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use alloy_primitives::B256;
use async_graphql::{EmptyMutation, EmptySubscription, Object, Request, Response, Schema};
use evm_bridge::{BridgeParameters, EvmBridgeAbi};
use linera_sdk::{
    ethereum::{EthereumQueries, ServiceEthereumClient},
    linera_base_types::WithServiceAbi,
    views::{linera_views, RegisterView, RootView, SetView, View, ViewStorageContext},
    Service, ServiceRuntime,
};

/// On-chain state (mirrors contract state).
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<[u8; 32]>,
    pub bridge_contract_address: RegisterView<Option<[u8; 20]>>,
}

#[derive(Clone)]
pub struct EvmBridgeService {
    #[allow(dead_code)]
    state: Arc<BridgeState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(EvmBridgeService);

impl WithServiceAbi for EvmBridgeService {
    type Abi = EvmBridgeAbi;
}

impl Service for EvmBridgeService {
    type Parameters = BridgeParameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = BridgeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EvmBridgeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(self.clone(), EmptyMutation, EmptySubscription).finish();
        schema.execute(request).await
    }
}

#[Object]
impl EvmBridgeService {
    /// The EVM source chain ID this bridge is configured for.
    async fn source_chain_id(&self) -> u64 {
        let params: BridgeParameters = self.runtime.application_parameters();
        params.source_chain_id
    }

    /// The bridge contract address on EVM (hex-encoded), or `None` if it has
    /// not yet been registered via `RegisterFungibleBridge`.
    async fn bridge_contract_address(&self) -> Option<String> {
        self.state
            .bridge_contract_address
            .get()
            .map(|addr| format!("0x{}", hex::encode(addr)))
    }

    /// The ERC-20 token address on the source EVM chain (hex-encoded).
    async fn token_address(&self) -> String {
        let params: BridgeParameters = self.runtime.application_parameters();
        format!("0x{}", hex::encode(params.token_address))
    }

    /// Whether a deposit with the given hash has been processed.
    ///
    /// The hash is the hex-encoded keccak-256 of the deposit key
    /// (see [`evm_bridge::DepositKey::hash`]).
    async fn is_deposit_processed(&self, hash: String) -> bool {
        let bytes: [u8; 32] = hex::decode(hash.strip_prefix("0x").unwrap_or(&hash))
            .expect("invalid hex")
            .try_into()
            .expect("hash must be 32 bytes");
        self.state
            .processed_deposits
            .contains(&bytes)
            .await
            .expect("failed to check processed deposits")
    }

    /// Verifies that the given EVM block hash is finalized on the source chain.
    ///
    /// Makes the EVM JSON-RPC calls in the service runtime so that the contract
    /// sees a single deterministic oracle response (the boolean result) instead
    /// of multiple raw HTTP responses with non-deterministic headers.
    async fn is_block_hash_finalized(&self, block_hash: String) -> bool {
        let bytes: [u8; 32] = hex::decode(block_hash.strip_prefix("0x").unwrap_or(&block_hash))
            .expect("invalid hex")
            .try_into()
            .expect("hash must be 32 bytes");
        let params: BridgeParameters = self.runtime.application_parameters();
        assert!(
            !params.rpc_endpoint.is_empty(),
            "rpc_endpoint must be configured to verify block hashes"
        );
        let client = ServiceEthereumClient::new(params.rpc_endpoint);
        client
            .is_block_hash_finalized(B256::from(bytes))
            .await
            .expect("failed to check block finality — block may not exist")
    }
}
