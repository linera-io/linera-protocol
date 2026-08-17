// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use alloy_primitives::B256;
use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use evm_bridge::{BridgeOperation, BridgeParameters, EvmBridgeAbi};
use linera_sdk::{
    ethereum::{EthereumQueries, ServiceEthereumClient},
    linera_base_types::{WithServiceAbi, U128},
    views::{linera_views, RegisterView, RootView, SetView, View, ViewStorageContext},
    Service, ServiceRuntime,
};

/// On-chain state (mirrors contract state). Field order MUST match
/// `BridgeState` in `contract.rs` because `RootView` keys are derived
/// from field position.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<[u8; 32]>,
    pub verified_block_hashes: SetView<[u8; 32]>,
    pub bridge_contract_address: RegisterView<Option<[u8; 20]>>,
    pub rpc_endpoint: RegisterView<String>,
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

/// Decodes a hex string (optional `0x` prefix) into bytes.
fn decode_hex(label: &str, s: &str) -> async_graphql::Result<Vec<u8>> {
    hex::decode(s.strip_prefix("0x").unwrap_or(s))
        .map_err(|e| async_graphql::Error::new(format!("invalid {label} hex: {e}")))
}

/// Decodes a hex string into a fixed-size byte array.
fn decode_hex_array<const N: usize>(label: &str, s: &str) -> async_graphql::Result<[u8; N]> {
    decode_hex(label, s)?.as_slice().try_into().map_err(|_| {
        async_graphql::Error::new(format!(
            "{label} must be exactly {N} bytes ({} hex chars)",
            N * 2
        ))
    })
}

/// GraphQL mutation root: schedules bridge operations submitted by a client (the
/// demo UI, an admin tool, or a custom relayer). The application bytecode — and
/// thus this interface — cannot change after deployment, so every
/// [`BridgeOperation`] is exposed; on-chain authorization still gates each.
/// Byte-array arguments are hex-encoded strings (with or without `0x`).
pub struct MutationRoot {
    runtime: Arc<ServiceRuntime<EvmBridgeService>>,
}

#[Object]
impl MutationRoot {
    /// Burns `amount` wrapped tokens from the operation's authenticated signer
    /// and releases the corresponding ERC-20 to `evm_target` (20-byte EVM
    /// address) on the source chain.
    async fn burn(&self, amount: U128, evm_target: String) -> async_graphql::Result<[u8; 0]> {
        let evm_target = decode_hex_array::<20>("evm_target", &evm_target)?;
        self.runtime
            .schedule_operation(&BridgeOperation::Burn { amount, evm_target });
        Ok([])
    }

    /// Verifies a deposit proof from the source chain and mints wrapped tokens.
    /// `block_header_rlp`, `receipt_rlp`, and each `proof_nodes` entry are
    /// hex-encoded; `tx_index`/`log_index` select the log within the block.
    async fn process_deposit(
        &self,
        block_header_rlp: String,
        receipt_rlp: String,
        proof_nodes: Vec<String>,
        tx_index: u64,
        log_index: u64,
    ) -> async_graphql::Result<[u8; 0]> {
        let block_header_rlp = decode_hex("block_header_rlp", &block_header_rlp)?;
        let receipt_rlp = decode_hex("receipt_rlp", &receipt_rlp)?;
        let proof_nodes = proof_nodes
            .iter()
            .map(|n| decode_hex("proof_nodes", n))
            .collect::<async_graphql::Result<Vec<_>>>()?;
        self.runtime
            .schedule_operation(&BridgeOperation::ProcessDeposit {
                block_header_rlp,
                receipt_rlp,
                proof_nodes,
                tx_index,
                log_index,
            });
        Ok([])
    }

    /// Verifies that an EVM block hash (32 bytes) is authentic and finalized.
    async fn verify_block_hash(&self, block_hash: String) -> async_graphql::Result<[u8; 0]> {
        let block_hash = decode_hex_array::<32>("block_hash", &block_hash)?;
        self.runtime
            .schedule_operation(&BridgeOperation::VerifyBlockHash { block_hash });
        Ok([])
    }

    /// Registers the EVM `FungibleBridge` contract address (20 bytes). One-shot,
    /// chain-owner-only.
    async fn register_fungible_bridge(&self, address: String) -> async_graphql::Result<[u8; 0]> {
        let address = decode_hex_array::<20>("address", &address)?;
        self.runtime
            .schedule_operation(&BridgeOperation::RegisterFungibleBridge { address });
        Ok([])
    }

    /// Sets the EVM JSON-RPC endpoint used for finality verification (empty
    /// string disables it). Chain-owner-only.
    async fn set_rpc_endpoint(&self, rpc_endpoint: String) -> async_graphql::Result<[u8; 0]> {
        self.runtime
            .schedule_operation(&BridgeOperation::SetRpcEndpoint { rpc_endpoint });
        Ok([])
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

    /// The configured EVM JSON-RPC endpoint, or empty if finality verification
    /// is disabled.
    async fn rpc_endpoint(&self) -> String {
        self.state.rpc_endpoint.get().clone()
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
        let rpc_endpoint = self.state.rpc_endpoint.get().clone();
        assert!(
            !rpc_endpoint.is_empty(),
            "rpc_endpoint must be configured to verify block hashes"
        );
        let client = ServiceEthereumClient::new(rpc_endpoint);
        client
            .is_block_hash_finalized(B256::from(bytes))
            .await
            .expect("failed to check block finality — block may not exist")
    }
}
