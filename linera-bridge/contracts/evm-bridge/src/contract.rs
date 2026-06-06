// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use alloy_primitives::Bytes;
use evm_bridge::{
    BridgeInstantiationArgument, BridgeMessage, BridgeOperation, BridgeParameters, DepositKey,
    EvmBridgeAbi,
};
use fungible::Account;
use linera_bridge::proof;
use linera_sdk::{
    ethereum::{ContractEthereumClient, EthereumQueries},
    linera_base_types::{StreamName, WithContractAbi, U128},
    views::{linera_views, RegisterView, RootView, SetView, View, ViewStorageContext},
    Contract, ContractRuntime,
};
use wrapped_fungible::{BurnEvent, WrappedFungibleOperation, WrappedFungibleTokenAbi};

/// On-chain state: tracks processed deposits, verified block hashes, and the
/// registered EVM FungibleBridge contract address. The wrapped-fungible
/// application is a creation parameter (`BridgeParameters::fungible_app_id`),
/// not state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct BridgeState {
    pub processed_deposits: SetView<[u8; 32]>,
    pub verified_block_hashes: SetView<[u8; 32]>,
    pub bridge_contract_address: RegisterView<Option<[u8; 20]>>,
    pub rpc_endpoint: RegisterView<String>,
}

pub struct EvmBridge {
    state: BridgeState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(EvmBridge);

impl WithContractAbi for EvmBridge {
    type Abi = EvmBridgeAbi;
}

impl Contract for EvmBridge {
    type Message = BridgeMessage;
    type Parameters = BridgeParameters;
    type InstantiationArgument = BridgeInstantiationArgument;
    type EventValue = BurnEvent;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = BridgeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EvmBridge { state, runtime }
    }

    async fn instantiate(&mut self, argument: BridgeInstantiationArgument) {
        let params = self.runtime.application_parameters();
        if !argument.rpc_endpoint.is_empty() {
            let client = ContractEthereumClient::new(argument.rpc_endpoint.clone());
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
        self.state.rpc_endpoint.set(argument.rpc_endpoint);
    }

    async fn execute_operation(&mut self, operation: BridgeOperation) {
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
            }
            BridgeOperation::VerifyBlockHash { block_hash } => {
                self.verify_block_hash(block_hash).await;

                // Only cache when called by an authenticated signer (chain owner),
                // preventing unauthenticated callers from bloating state.
                if self.runtime.authenticated_signer().is_some() {
                    self.state
                        .verified_block_hashes
                        .insert(&block_hash)
                        .expect("failed to insert verified block hash");
                }
            }
            BridgeOperation::RegisterFungibleBridge { address } => {
                self.runtime
                    .authenticated_signer()
                    .expect("RegisterFungibleBridge requires an authenticated signer");
                assert!(
                    self.state.bridge_contract_address.get().is_none(),
                    "bridge contract address is already registered and cannot be changed"
                );
                self.state.bridge_contract_address.set(Some(address));
            }
            BridgeOperation::SetRpcEndpoint { rpc_endpoint } => {
                self.runtime
                    .authenticated_signer()
                    .expect("SetRpcEndpoint requires an authenticated signer");
                self.state.rpc_endpoint.set(rpc_endpoint);
            }
            BridgeOperation::Burn { amount, evm_target } => {
                self.initiate_burn(amount, evm_target);
            }
        }
    }

    async fn execute_message(&mut self, message: BridgeMessage) {
        match message {
            BridgeMessage::Burn { amount, evm_target } => {
                // A bouncing delivery is a no-op: the funding transfer is part
                // of the same outgoing bundle and bounces on its own, refunding
                // the user on their chain via wrapped-fungible's Credit handler.
                let is_bouncing = self
                    .runtime
                    .message_is_bouncing()
                    .expect("Delivery status is available when executing a message");
                if !is_bouncing {
                    self.execute_burn(amount, evm_target);
                }
            }
        }
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}

impl EvmBridge {
    async fn verify_block_hash(&mut self, block_hash: [u8; 32]) {
        let rpc_endpoint = self.state.rpc_endpoint.get();
        assert!(
            !rpc_endpoint.is_empty(),
            "rpc_endpoint must be configured to verify block hashes"
        );

        // Use our own service as an oracle so that the underlying EVM JSON-RPC
        // calls (two of them) collapse into one deterministic boolean in the
        // block's oracle responses.
        let application_id = self.runtime.application_id();
        let hash_hex = hex::encode(block_hash);
        let query = async_graphql::Request::new(format!(
            "query {{ isBlockHashFinalized(blockHash: \"0x{hash_hex}\") }}"
        ));
        let response = self.runtime.query_service(application_id, query);

        let async_graphql::Value::Object(data) = response.data else {
            panic!("Unexpected response from service: {response:#?}");
        };
        let finalized = match &data["isBlockHashFinalized"] {
            async_graphql::Value::Boolean(b) => *b,
            other => panic!("Unexpected value for isBlockHashFinalized: {other:#?}"),
        };
        assert!(finalized, "block is not finalized");

        log::info!("verified block hash 0x{hash_hex} is finalized");
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
        if self.state.rpc_endpoint.get().is_empty() {
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
        // `log_index` is a u64 but indexes a Vec (usize). On wasm32 `usize` is
        // 32-bit, so an unchecked `as usize` cast would truncate — letting
        // `log_index` and `log_index + 2^32` select the same log while hashing
        // to different `DepositKey`s (replay-guard bypass → double mint). A
        // checked cast rejects any value that does not fit `usize`; the full
        // u64 is preserved for the `DepositKey` below.
        let log_index_usize = usize::try_from(log_index).expect("log_index out of range");
        assert!(
            log_index_usize < logs.len(),
            "log_index {} out of range (receipt has {} logs)",
            log_index,
            logs.len()
        );
        let bridge_contract_bytes =
            self.state.bridge_contract_address.get().expect(
                "bridge contract address not registered — call RegisterFungibleBridge first",
            );
        let bridge_contract = alloy_primitives::Address::from(bridge_contract_bytes);
        let deposit = proof::parse_deposit_event(&logs[log_index_usize], bridge_contract)
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
            block_hash,
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
        if !self.state.rpc_endpoint.get().is_empty() {
            self.state
                .verified_block_hashes
                .insert(&block_hash.0)
                .expect("failed to cache verified block hash");
        }

        // 6. Convert deposit fields to Linera types and call Mint
        let amount = U128(
            deposit
                .amount
                .try_into()
                .expect("deposit amount exceeds u128"),
        );

        let mint_op = WrappedFungibleOperation::Mint {
            target_account: Account {
                chain_id: deposit.target_chain_id,
                owner: deposit.target_account_owner,
            },
            amount,
        };

        // Forward authenticated signer (chain owner = minter) to the fungible app.
        let fungible_app_id = params.fungible_app_id.with_abi::<WrappedFungibleTokenAbi>();
        self.runtime
            .call_application(true, fungible_app_id, &mint_op);
    }

    /// Drives a user-initiated burn. Runs on the *user's* chain: moves `amount`
    /// of the authenticated signer's wrapped tokens into the signer's own escrow
    /// account on the bridge chain (a tracked transfer via the bridge's
    /// `fungible_app_id` parameter) and, in the same outgoing bundle, sends a
    /// tracked [`BridgeMessage::Burn`] to the bridge chain. Both messages share
    /// one bundle, so if the burn is rejected the funding transfer bounces back
    /// and the signer is refunded. `.with_authentication()` propagates the
    /// signer to the bridge chain, so `execute_burn` there burns the very escrow
    /// account this signer funded.
    fn initiate_burn(&mut self, amount: U128, evm_target: [u8; 20]) {
        assert!(amount.0 > 0, "Burn amount must be non-zero");
        let signer = self
            .runtime
            .authenticated_signer()
            .expect("Burn requires an authenticated signer");
        let params = self.runtime.application_parameters();
        let bridge_chain_id = params.bridge_chain_id;

        // A burn must originate from a *user* chain distinct from the bridge
        // chain. The atomic funding-then-burn bundle relies on a tracked
        // cross-chain `Credit` that bounces — refunding the signer — if the
        // burn is rejected. On the bridge chain itself the funding `Transfer`
        // is a local credit with no Credit message to bounce, so a rejected
        // self-delivered burn would strand the escrow in the signer's account.
        // Refuse to run that unsafe path: a same-chain burn is a no-op.
        if self.runtime.chain_id() == bridge_chain_id {
            log::warn!(
                "ignoring Burn submitted on the bridge chain; \
                 burns must originate from a separate user chain"
            );
            return;
        }
        let fungible_app_id = params.fungible_app_id.with_abi::<WrappedFungibleTokenAbi>();

        // Move the signer's tokens into the signer's own escrow account on the
        // bridge chain. Forwarding the signer's authentication lets the
        // wrapped-fungible app authorize the debit.
        let transfer_op = WrappedFungibleOperation::Transfer {
            owner: signer,
            amount,
            target_account: Account {
                chain_id: bridge_chain_id,
                owner: signer,
            },
        };
        self.runtime
            .call_application(true, fungible_app_id, &transfer_op);

        // Ask the bridge chain to burn the escrowed tokens and emit the event.
        self.runtime
            .prepare_message(BridgeMessage::Burn { amount, evm_target })
            .with_authentication()
            .with_tracking()
            .send_to(bridge_chain_id);
    }

    /// Burns the escrowed tokens on the bridge chain and emits the [`BurnEvent`]
    /// the relayer forwards to EVM. Runs when a non-bouncing
    /// [`BridgeMessage::Burn`] is delivered to the bridge chain. The escrow is
    /// held under the originating signer (propagated via the authenticated
    /// message), so a message can only ever burn the escrow its own signer
    /// funded.
    fn execute_burn(&mut self, amount: U128, evm_target: [u8; 20]) {
        let signer = self
            .runtime
            .authenticated_signer()
            .expect("burn message carries the originating authenticated signer");
        let fungible_app_id = self
            .runtime
            .application_parameters()
            .fungible_app_id
            .with_abi::<WrappedFungibleTokenAbi>();
        let burn_op = WrappedFungibleOperation::Burn {
            owner: signer,
            amount,
        };
        self.runtime
            .call_application(true, fungible_app_id, &burn_op);
        self.runtime.emit(
            StreamName::from("burns"),
            &BurnEvent {
                target: evm_target,
                amount,
            },
        );
    }
}
