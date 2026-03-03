// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use evm_bridge::{BridgeOperation, BridgeParameters, DepositKey, EvmBridgeAbi};
use linera_sdk::{
    linera_base_types::WithServiceAbi,
    views::{linera_views, RootView, SetView, View, ViewStorageContext},
    Service, ServiceRuntime,
};

/// On-chain state (mirrors contract state).
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<DepositKey>,
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
        let schema = Schema::build(
            self.clone(),
            MutationRoot {
                runtime: self.runtime.clone(),
            },
            EmptySubscription,
        )
        .finish();
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

    /// The bridge contract address on EVM (hex-encoded).
    async fn bridge_contract_address(&self) -> String {
        let params: BridgeParameters = self.runtime.application_parameters();
        format!("0x{}", hex::encode(params.bridge_contract_address))
    }

    /// The ERC-20 token address on the source EVM chain (hex-encoded).
    async fn token_address(&self) -> String {
        let params: BridgeParameters = self.runtime.application_parameters();
        format!("0x{}", hex::encode(params.token_address))
    }
}

struct MutationRoot {
    runtime: Arc<ServiceRuntime<EvmBridgeService>>,
}

#[Object]
impl MutationRoot {
    /// Submit a deposit proof for verification.
    ///
    /// All binary parameters are hex-encoded (no `0x` prefix).
    async fn process_deposit(
        &self,
        block_header_rlp: String,
        receipt_rlp: String,
        proof_nodes: Vec<String>,
        tx_index: u64,
        log_index: u64,
    ) -> [u8; 0] {
        let operation = BridgeOperation::ProcessDeposit {
            block_header_rlp: hex::decode(&block_header_rlp)
                .expect("invalid hex in block_header_rlp"),
            receipt_rlp: hex::decode(&receipt_rlp).expect("invalid hex in receipt_rlp"),
            proof_nodes: proof_nodes
                .iter()
                .map(|n| hex::decode(n).expect("invalid hex in proof_nodes"))
                .collect(),
            tx_index,
            log_index,
        };
        self.runtime.schedule_operation(&operation);
        []
    }
}
