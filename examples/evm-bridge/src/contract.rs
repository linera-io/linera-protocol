// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use alloy_primitives::{Bytes, B256};
use evm_bridge::{BridgeOperation, BridgeParameters, DepositKey, EvmBridgeAbi};
use linera_bridge::proof;
use linera_sdk::{
    ethereum::{ContractEthereumClient, EthereumQueries},
<<<<<<< HEAD
    linera_base_types::{Account, AccountOwner, Amount, ChainId, WithContractAbi},
    views::{linera_views, RootView, SetView, View, ViewStorageContext},
=======
    linera_base_types::{AccountOwner, Amount, ApplicationId, ChainId, WithContractAbi},
    views::{linera_views, RegisterView, RootView, SetView, View, ViewStorageContext},
>>>>>>> 070ac118ea (Rearchitect linera bridge minting and burning mechanisms (#5929))
    Contract, ContractRuntime,
};
use wrapped_fungible::{WrappedFungibleOperation, WrappedFungibleTokenAbi};

/// On-chain state: tracks processed deposits, verified block hashes,
/// and the registered wrapped-fungible application.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<[u8; 32]>,
    pub verified_block_hashes: SetView<[u8; 32]>,
    pub fungible_app_id: RegisterView<Option<ApplicationId>>,
}

pub struct EvmBridgeContract {
    state: BridgeState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(EvmBridgeContract);

impl WithContractAbi for EvmBridgeContract {
    type Abi = EvmBridgeAbi;
}

impl Contract for EvmBridgeContract {
    type Message = ();
    type Parameters = BridgeParameters;
    type InstantiationArgument = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = BridgeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EvmBridgeContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        let params = self.runtime.application_parameters();
        if !params.rpc_endpoint.is_empty() {
            let client = ContractEthereumClient::new(params.rpc_endpoint.clone());
            let chain_id = client
                .get_chain_id()
                .await
                .expect("failed to query chain ID from RPC endpoint");
            assert_eq!(
                chain_id, params.source_chain_id,
                "RPC endpoint chain ID {chain_id} does not match configured source_chain_id {}",
                params.source_chain_id
            );
        }
    }

    async fn execute_operation(&mut self, operation: BridgeOperation) {
        match operation {
            BridgeOperation::RegisterFungibleApp { app_id } => {
                self.runtime
                    .authenticated_signer()
                    .expect("RegisterFungibleApp requires an authenticated signer");
                assert!(
                    self.state.fungible_app_id.get().is_none(),
                    "fungible app is already registered and cannot be changed"
                );
                self.state.fungible_app_id.set(Some(app_id));
            }
            BridgeOperation::ProcessDeposit {
                block_header_rlp,
                receipt_rlp,
                proof_nodes,
                tx_index,
                log_index,
            } => {
                self.process_deposit(
                    &block_header_rlp,
                    &receipt_rlp,
                    &proof_nodes,
                    tx_index,
                    log_index,
                )
                .await;
            }
            BridgeOperation::VerifyBlockHash { block_hash } => {
                self.verify_block_hash(block_hash).await;

                // Only cache when called by an authenticated signer (chain owner),
                // preventing unauthenticated callers from bloating state.
                if self.runtime.authenticated_owner().is_some() {
                    self.state
                        .verified_block_hashes
                        .insert(&block_hash)
                        .expect("failed to insert verified block hash");
                }
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {}

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl EvmBridgeContract {
    async fn verify_block_hash(&mut self, block_hash: [u8; 32]) {
        let params = self.runtime.application_parameters();
        assert!(
            !params.rpc_endpoint.is_empty(),
            "rpc_endpoint must be configured to verify block hashes"
        );

        let client = ContractEthereumClient::new(params.rpc_endpoint.clone());
        assert!(
            client
                .is_block_hash_finalized(B256::from(block_hash))
                .await
                .expect("failed to check block finality — block may not exist"),
            "block is not finalized"
        );

        log::info!(
            "verified block hash {} is finalized",
            hex::encode(block_hash)
        );
    }

    async fn process_deposit(
        &mut self,
        block_header_rlp: &[u8],
        receipt_rlp: &[u8],
        proof_nodes: &[Vec<u8>],
        tx_index: u64,
        log_index: u64,
    ) {
        let params = self.runtime.application_parameters();

        // 1. Decode block header → (block_hash, receipts_root)
        let (block_hash, receipts_root) =
            proof::decode_block_header(block_header_rlp).expect("invalid block header RLP");

        // 1b. Finality check: when an endpoint is configured, verify the block hash
        //     is finalized. Uses cached result if a previous deposit from this block
        //     was already processed.
        if params.rpc_endpoint.is_empty() {
            log::warn!("rpc_endpoint is empty — skipping block finality verification.");
        } else if !self
            .state
            .verified_block_hashes
            .contains(&block_hash.0)
            .await
            .expect("failed to check verified block hashes")
        {
            self.verify_block_hash(block_hash.0).await;
        }

        // 2. Verify receipt inclusion via MPT proof
        let proof_bytes: Vec<Bytes> = proof_nodes
            .iter()
            .map(|n| Bytes::copy_from_slice(n))
            .collect();
        proof::verify_receipt_inclusion(receipts_root, tx_index, receipt_rlp, &proof_bytes)
            .expect("receipt inclusion proof failed");

        // 3. Decode receipt logs and parse the deposit event
        let logs = proof::decode_receipt_logs(receipt_rlp).expect("failed to decode receipt logs");
        assert!(
            (log_index as usize) < logs.len(),
            "log_index {} out of range (receipt has {} logs)",
            log_index,
            logs.len()
        );
        let bridge_contract = alloy_primitives::Address::from(params.bridge_contract_address);
        let deposit = proof::parse_deposit_event(&logs[log_index as usize], bridge_contract)
            .expect("failed to parse DepositInitiated event");

        // 4. Validate deposit fields against bridge parameters
        assert_eq!(
            deposit.source_chain_id.as_limbs()[0],
            params.source_chain_id,
            "source chain ID mismatch"
        );
        assert_eq!(
            deposit.token.as_slice(),
            &params.token_address,
            "token address mismatch"
        );

        // 5. Replay protection
        let deposit_key = DepositKey {
            source_chain_id: params.source_chain_id,
            block_hash: block_hash.0,
            tx_index,
            log_index,
        };
        let deposit_hash = deposit_key.hash();
        assert!(
            !self
                .state
                .processed_deposits
                .contains(&deposit_hash)
                .await
                .expect("failed to check processed deposits"),
            "deposit already processed"
        );
        self.state
            .processed_deposits
            .insert(&deposit_hash)
            .expect("failed to insert deposit hash");

        // 5b. Cache the verified block hash so subsequent deposits from the same
        //     block skip the RPC finality check.
        if !params.rpc_endpoint.is_empty() {
            self.state
                .verified_block_hashes
                .insert(&block_hash.0)
                .expect("failed to cache verified block hash");
        }

        // 6. Convert deposit fields to Linera types and call Mint
        let target_chain_id =
            ChainId::try_from(deposit.target_chain_id.as_slice()).expect("invalid target chain ID");
        let target_owner = AccountOwner::from(deposit.target_account_owner.0);
        let amount = Amount::try_from(deposit.amount).expect("deposit amount exceeds u128");

        let mint_op = WrappedFungibleOperation::Mint {
            target_account: Account {
                chain_id: target_chain_id,
                owner: target_owner,
            },
            amount,
        };

        // Forward authenticated signer (chain owner = minter) to the fungible app.
        let fungible_app_id = self
            .state
            .fungible_app_id
            .get()
            .expect("fungible app not registered — call RegisterFungibleApp first")
            .with_abi::<WrappedFungibleTokenAbi>();
        self.runtime
            .call_application(true, fungible_app_id, &mint_op);
    }
}
