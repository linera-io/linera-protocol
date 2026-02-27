// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for FungibleBridge contract.

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolEvent;
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::{Amount, BlockHeight, Epoch, Round},
        identifiers::{AccountOwner, ChainId},
    };
    use linera_chain::{
        block::ConfirmedBlock,
        data_types::{Transaction, Vote},
        types::ConfirmedBlockCertificate,
    };
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::Address,
    };

    use crate::{evm::microchain::addBlockCall, test_helpers::*};

    mod erc20 {
        use alloy_sol_types::sol;

        sol! {
            function balanceOf(address account) external view returns (uint256);
            function transfer(address to, uint256 amount) external returns (bool);
            function approve(address spender, uint256 amount) external returns (bool);
        }
    }

    mod bridge {
        use alloy_sol_types::sol;

        sol! {
            function deposit(
                bytes32 target_chain_id,
                bytes32 target_application_id,
                bytes32 target_account_owner,
                uint256 amount
            ) external;

            event DepositInitiated(
                uint256 source_chain_id,
                bytes32 target_chain_id,
                bytes32 target_application_id,
                bytes32 target_account_owner,
                address token,
                uint256 amount
            );
        }
    }

    /// Common test state for FungibleBridge tests.
    struct TestBridge {
        db: CacheDB<EmptyDB>,
        deployer: Address,
        secret: ValidatorSecretKey,
        public: linera_base::crypto::ValidatorPublicKey,
        chain_id: CryptoHash,
        app_id: CryptoHash,
        bridge: Address,
        token: Address,
        origin: ChainId,
        next_height: u64,
    }

    /// Initial token supply for tests (1 million tokens with 18 decimals).
    const INITIAL_SUPPLY: u128 = 1_000_000_000_000_000_000_000_000;

    impl TestBridge {
        fn new() -> Self {
            let mut db = CacheDB::new(EmptyDB::default());
            let deployer = Address::ZERO;

            let secret = ValidatorSecretKey::generate();
            let public = secret.public();
            let validator_addr = validator_evm_address(&public);
            let admin_chain_id = test_admin_chain_id();
            let light_client = deploy_light_client(
                &mut db,
                deployer,
                &[validator_addr],
                &[1],
                admin_chain_id,
                0,
            );

            let token = deploy_mock_erc20(
                &mut db,
                deployer,
                alloy_primitives::U256::from(INITIAL_SUPPLY),
            );

            let chain_id = CryptoHash::new(&TestString::new("test_chain"));
            let app_id = CryptoHash::new(&TestString::new("fungible_app"));
            let bridge =
                deploy_fungible_bridge(&mut db, deployer, light_client, chain_id, 1, app_id, token);
            let origin = ChainId(CryptoHash::new(&TestString::new("origin_chain")));

            // Fund the bridge with the full token supply
            call_contract(
                &mut db,
                deployer,
                token,
                erc20::transferCall {
                    to: bridge,
                    amount: alloy_primitives::U256::from(INITIAL_SUPPLY),
                },
            );

            Self {
                db,
                deployer,
                secret,
                public,
                chain_id,
                app_id,
                bridge,
                token,
                origin,
                next_height: 1,
            }
        }

        /// Submits a block containing a single credit message, returns logs and gas used.
        fn submit_credit(
            &mut self,
            target: AccountOwner,
            amount: Amount,
            source: AccountOwner,
        ) -> (Vec<revm::primitives::Log>, u64) {
            let msg = fungible::Message::Credit {
                target,
                amount,
                source,
            };
            let txn = fungible_message_transaction(self.origin, self.app_id, &msg);
            self.submit_block(vec![txn])
        }

        /// Submits a block with the given transactions at the next sequential height.
        fn submit_block(
            &mut self,
            transactions: Vec<Transaction>,
        ) -> (Vec<revm::primitives::Log>, u64) {
            let height = BlockHeight(self.next_height);
            self.next_height += 1;
            let cert = create_certificate_with_transactions(
                &self.secret,
                &self.public,
                self.chain_id,
                height,
                transactions,
            );
            let (_, logs, gas) = call_contract(
                &mut self.db,
                self.deployer,
                self.bridge,
                addBlockCall {
                    data: bcs::to_bytes(&cert).unwrap().into(),
                },
            );
            (logs, gas)
        }

        /// Queries the token balance of an address on the mock ERC20.
        fn query_token_balance(&mut self, account: Address) -> alloy_primitives::U256 {
            let (balance, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.token,
                erc20::balanceOfCall { account },
            );
            balance
        }
    }

    /// Creates a certificate containing a specific set of transactions.
    fn create_certificate_with_transactions(
        secret: &ValidatorSecretKey,
        public: &linera_base::crypto::ValidatorPublicKey,
        chain_id: CryptoHash,
        height: BlockHeight,
        transactions: Vec<Transaction>,
    ) -> ConfirmedBlockCertificate {
        let block = create_test_block(chain_id, Epoch::ZERO, height, transactions);
        let confirmed = ConfirmedBlock::new(block);
        let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
        ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
    }

    const TEST_TARGET: [u8; 20] = [0xAB; 20];
    const TEST_SOURCE_NAME: &str = "source_owner";

    fn test_target() -> AccountOwner {
        AccountOwner::Address20(TEST_TARGET)
    }

    fn test_source() -> AccountOwner {
        AccountOwner::Address32(CryptoHash::new(&TestString::new(TEST_SOURCE_NAME)))
    }

    fn test_target_evm_address() -> Address {
        Address::from_slice(&TEST_TARGET)
    }

    #[test]
    fn test_fungible_bridge_transfers_on_credit() {
        let mut t = TestBridge::new();

        assert_eq!(
            t.query_token_balance(test_target_evm_address()),
            alloy_primitives::U256::ZERO,
            "target should start with zero balance"
        );

        let transfer_amount = 100_000_000_000_000_000_000u128;

        t.submit_credit(
            test_target(),
            Amount::from_attos(transfer_amount),
            test_source(),
        );

        assert_eq!(
            t.query_token_balance(test_target_evm_address()),
            alloy_primitives::U256::from(transfer_amount),
            "target should have 100 tokens"
        );
    }

    #[test]
    fn test_fungible_bridge_accumulates_transfers() {
        let mut t = TestBridge::new();
        let target = test_target();
        let source = test_source();

        let transfer_amount = 100_000_000_000_000_000_000u128;

        t.submit_credit(target, Amount::from_attos(transfer_amount), source);
        assert_eq!(
            t.query_token_balance(test_target_evm_address()),
            alloy_primitives::U256::from(transfer_amount),
            "balance should be 100 tokens after first credit"
        );

        let second_transfer = 50_000_000_000_000_000_000u128;

        t.submit_credit(target, Amount::from_attos(second_transfer), source);
        assert_eq!(
            t.query_token_balance(test_target_evm_address()),
            alloy_primitives::U256::from(transfer_amount + second_transfer),
            "balance should be 150 tokens after two credits"
        );

        // Bridge balance should have decreased accordingly
        assert_eq!(
            t.query_token_balance(t.bridge),
            alloy_primitives::U256::from(INITIAL_SUPPLY - (transfer_amount + second_transfer)),
            "bridge balance should decrease"
        );
    }

    #[test]
    fn test_fungible_bridge_skips_non_evm_targets() {
        let mut t = TestBridge::new();
        let target = AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner")));

        t.submit_credit(target, Amount::from_tokens(100), test_source());

        // Bridge balance should be unchanged
        assert_eq!(
            t.query_token_balance(t.bridge),
            alloy_primitives::U256::from(INITIAL_SUPPLY),
            "bridge balance should be unchanged for non-EVM targets"
        );
    }

    #[test]
    fn test_fungible_bridge_ignores_other_applications() {
        let mut t = TestBridge::new();
        let other_app_id = CryptoHash::new(&TestString::new("other_app"));

        let msg = fungible::Message::Credit {
            target: test_target(),
            amount: Amount::from_tokens(50),
            source: test_source(),
        };
        let txn = fungible_message_transaction(t.origin, other_app_id, &msg);
        t.submit_block(vec![txn]);

        assert_eq!(
            t.query_token_balance(test_target_evm_address()),
            alloy_primitives::U256::ZERO,
            "should not transfer for other applications"
        );
    }

    #[test]
    fn test_fungible_bridge_gas_measurement() {
        let mut t = TestBridge::new();

        let (_, gas_used) = t.submit_credit(test_target(), Amount::from_tokens(100), test_source());

        println!("Gas used for fungible bridge addBlock with Credit message: {gas_used}");
    }

    // --- EVM→Linera deposit tests ---

    /// A depositor account (distinct from deployer who owns the bridge).
    const DEPOSITOR: Address = Address::new([0xDD; 20]);
    const DEPOSIT_AMOUNT: u128 = 500_000_000_000_000_000_000; // 500 tokens

    fn target_owner_bytes() -> [u8; 32] {
        [0x03; 32]
    }

    /// Sets up a bridge for deposit tests where the depositor has tokens
    /// (bridge is NOT pre-funded — deposits flow from user to bridge).
    fn setup_deposit_test() -> TestBridge {
        let mut db = CacheDB::new(EmptyDB::default());
        let deployer = Address::ZERO;

        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let validator_addr = validator_evm_address(&public);
        let admin_chain_id = test_admin_chain_id();
        let light_client = deploy_light_client(
            &mut db,
            deployer,
            &[validator_addr],
            &[1],
            admin_chain_id,
            0,
        );

        let token = deploy_mock_erc20(
            &mut db,
            deployer,
            alloy_primitives::U256::from(INITIAL_SUPPLY),
        );

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let app_id = CryptoHash::new(&TestString::new("fungible_app"));
        let bridge =
            deploy_fungible_bridge(&mut db, deployer, light_client, chain_id, 1, app_id, token);
        let origin = ChainId(CryptoHash::new(&TestString::new("origin_chain")));

        // Give depositor tokens (instead of funding the bridge)
        call_contract(
            &mut db,
            deployer,
            token,
            erc20::transferCall {
                to: DEPOSITOR,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        TestBridge {
            db,
            deployer,
            secret,
            public,
            chain_id,
            app_id,
            bridge,
            token,
            origin,
            next_height: 1,
        }
    }

    #[test]
    fn test_deposit_emits_event() {
        let mut t = setup_deposit_test();

        // Approve bridge to spend depositor's tokens
        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        // Call deposit
        let (_, logs, _) = call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: <[u8; 32]>::from(*t.chain_id.as_bytes()).into(),
                target_application_id: <[u8; 32]>::from(*t.app_id.as_bytes()).into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        // Verify event was emitted
        assert!(!logs.is_empty(), "should emit at least one log");

        // Find the DepositInitiated event
        let deposit_log = logs
            .iter()
            .find(|log| {
                !log.data.topics().is_empty()
                    && log.data.topics()[0] == bridge::DepositInitiated::SIGNATURE_HASH
            })
            .expect("DepositInitiated event not found");

        // Decode and verify event data
        let alloy_log = alloy_primitives::Log::new(
            deposit_log.address,
            deposit_log.data.topics().to_vec(),
            deposit_log.data.data.clone(),
        )
        .expect("failed to construct alloy Log");
        let event = bridge::DepositInitiated::decode_log(&alloy_log)
            .expect("failed to decode DepositInitiated event");

        // chain id = 1 in revm mainnet context
        assert_eq!(event.data.source_chain_id, alloy_primitives::U256::from(1));
        assert_eq!(
            event.data.target_chain_id,
            alloy_primitives::B256::from(<[u8; 32]>::from(*t.chain_id.as_bytes()))
        );
        assert_eq!(
            event.data.target_application_id,
            alloy_primitives::B256::from(<[u8; 32]>::from(*t.app_id.as_bytes()))
        );
        assert_eq!(
            event.data.target_account_owner,
            alloy_primitives::B256::from(target_owner_bytes())
        );
        assert_eq!(
            event.data.token,
            alloy_primitives::Address::from_slice(t.token.as_slice())
        );
        assert_eq!(
            event.data.amount,
            alloy_primitives::U256::from(DEPOSIT_AMOUNT)
        );
    }

    #[test]
    fn test_deposit_transfers_tokens() {
        let mut t = setup_deposit_test();

        let bridge_balance_before = t.query_token_balance(t.bridge);
        let depositor_balance_before = t.query_token_balance(DEPOSITOR);

        // Approve and deposit
        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: <[u8; 32]>::from(*t.chain_id.as_bytes()).into(),
                target_application_id: <[u8; 32]>::from(*t.app_id.as_bytes()).into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        let bridge_balance_after = t.query_token_balance(t.bridge);
        let depositor_balance_after = t.query_token_balance(DEPOSITOR);

        assert_eq!(
            bridge_balance_after - bridge_balance_before,
            alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            "bridge should receive deposited tokens"
        );
        assert_eq!(
            depositor_balance_before - depositor_balance_after,
            alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            "depositor should lose deposited tokens"
        );
    }

    #[test]
    fn test_deposit_reverts_without_approval() {
        let mut t = setup_deposit_test();

        // Deposit without approval should revert
        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: <[u8; 32]>::from(*t.chain_id.as_bytes()).into(),
                target_application_id: <[u8; 32]>::from(*t.app_id.as_bytes()).into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        assert!(result.is_err(), "deposit without approval should revert");
    }

    #[test]
    fn test_deposit_reverts_wrong_target_chain() {
        let mut t = setup_deposit_test();

        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: [0xFF; 32].into(),
                target_application_id: <[u8; 32]>::from(*t.app_id.as_bytes()).into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        assert!(
            result.is_err(),
            "deposit with wrong target chain should revert"
        );
    }

    #[test]
    fn test_deposit_reverts_wrong_target_application() {
        let mut t = setup_deposit_test();

        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: <[u8; 32]>::from(*t.chain_id.as_bytes()).into(),
                target_application_id: [0xFF; 32].into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        assert!(
            result.is_err(),
            "deposit with wrong target application should revert"
        );
    }

    #[test]
    fn test_deposit_reverts_insufficient_balance() {
        let mut t = setup_deposit_test();

        let too_much = DEPOSIT_AMOUNT + 1;

        // Approve more than balance
        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(too_much),
            },
        );

        // Deposit more than balance should revert
        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            bridge::depositCall {
                target_chain_id: <[u8; 32]>::from(*t.chain_id.as_bytes()).into(),
                target_application_id: <[u8; 32]>::from(*t.app_id.as_bytes()).into(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(too_much),
            },
        );

        assert!(
            result.is_err(),
            "deposit with insufficient balance should revert"
        );
    }
}
