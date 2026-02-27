// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use alloy_primitives::Bytes;
use evm_bridge::{BridgeOperation, BridgeParameters, BridgeResponse, DepositKey, EvmBridgeAbi};
use fungible::{Account, FungibleOperation};
use linera_bridge::proof;
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, ChainId, WithContractAbi},
    views::{linera_views, RootView, SetView, View, ViewStorageContext},
    Contract, ContractRuntime,
};
use wrapped_fungible::WrappedFungibleTokenAbi;

/// On-chain state: tracks processed deposits for replay protection.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<DepositKey>,
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
        // Validate parameters are present.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: BridgeOperation) -> BridgeResponse {
        match operation {
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
                BridgeResponse::Ok
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {}

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl EvmBridgeContract {
    async fn process_deposit(
        &mut self,
        block_header_rlp: &[u8],
        receipt_rlp: &[u8],
        proof_nodes: &[Vec<u8>],
        tx_index: u64,
        log_index: u64,
    ) {
        let params: BridgeParameters = self.runtime.application_parameters();

        // 1. Decode block header → (block_hash, receipts_root)
        let (block_hash, receipts_root) =
            proof::decode_block_header(block_header_rlp).expect("invalid block header RLP");

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
        let deposit = proof::parse_deposit_event(&logs[log_index as usize])
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
        assert!(
            !self
                .state
                .processed_deposits
                .contains(&deposit_key)
                .await
                .expect("failed to check processed deposits"),
            "deposit already processed"
        );
        self.state
            .processed_deposits
            .insert(&deposit_key)
            .expect("failed to insert deposit key");

        // 6. Convert deposit fields to Linera types and call Mint
        let target_chain_id =
            ChainId::try_from(deposit.target_chain_id.as_slice()).expect("invalid target chain ID");
        let target_owner = AccountOwner::Address32(deposit.target_account_owner.0.into());
        let amount_u128: u128 = deposit
            .amount
            .try_into()
            .expect("deposit amount exceeds u128");
        let amount = Amount::from_attos(amount_u128);

        let mint_op = FungibleOperation::Mint {
            target_account: Account {
                chain_id: target_chain_id,
                owner: target_owner,
            },
            amount,
        };

        // Forward authenticated signer (chain owner = minter) to the fungible app.
        let fungible_app_id = params.fungible_app_id.with_abi::<WrappedFungibleTokenAbi>();
        self.runtime
            .call_application(true, fungible_app_id, &mint_op);
    }
}
