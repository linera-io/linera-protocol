// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for FungibleBridge contract.

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolEvent;
    use linera_base::crypto::{CryptoHash, TestString, ValidatorSecretKey};
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::Address,
    };

    use crate::test_helpers::*;

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
                address indexed depositor,
                uint256 source_chain_id,
                bytes32 target_chain_id,
                bytes32 target_application_id,
                bytes32 target_account_owner,
                address token,
                uint256 amount,
                uint256 nonce
            );
        }
    }

    /// Common test state for FungibleBridge tests.
    struct TestBridge {
        db: CacheDB<EmptyDB>,
        deployer: Address,
        chain_id: CryptoHash,
        app_id: CryptoHash,
        bridge: Address,
        token: Address,
    }

    /// Initial token supply for tests (1 million tokens with 18 decimals).
    const INITIAL_SUPPLY: u128 = 1_000_000_000_000_000_000_000_000;

    impl TestBridge {
        /// Queries the token balance of an address on the mock ERC20.
        fn query_token_balance(&mut self, account: Address) -> alloy_primitives::U256 {
            let (balance, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.token,
                &erc20::balanceOfCall { account },
            );
            balance
        }
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
        let deployer = Address::from([0x01; 20]);

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

        let token = deploy_linera_token(
            &mut db,
            deployer,
            alloy_primitives::U256::from(INITIAL_SUPPLY),
        );

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let app_id = CryptoHash::new(&TestString::new("fungible_app"));
        let bridge = deploy_fungible_bridge(
            &mut db,
            deployer,
            light_client,
            chain_id,
            token,
            app_id,
            app_id,
        );

        // Give depositor tokens (instead of funding the bridge)
        call_contract(
            &mut db,
            deployer,
            token,
            &erc20::transferCall {
                to: DEPOSITOR,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        TestBridge {
            db,
            deployer,
            chain_id,
            app_id,
            bridge,
            token,
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
            &erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        // Call deposit
        let (_, logs, _) = call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            &bridge::depositCall {
                target_chain_id: *t.chain_id.as_bytes(),
                target_application_id: *t.app_id.as_bytes(),
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
        assert_eq!(event.data.target_chain_id, *t.chain_id.as_bytes());
        assert_eq!(event.data.target_application_id, *t.app_id.as_bytes());
        assert_eq!(
            event.data.target_account_owner,
            alloy_primitives::B256::from(target_owner_bytes())
        );
        assert_eq!(
            event.depositor,
            alloy_primitives::Address::from_slice(DEPOSITOR.as_slice()),
            "depositor should be the caller"
        );
        assert_eq!(
            event.data.token,
            alloy_primitives::Address::from_slice(t.token.as_slice())
        );
        assert_eq!(
            event.data.amount,
            alloy_primitives::U256::from(DEPOSIT_AMOUNT)
        );
        assert_eq!(
            event.data.nonce,
            alloy_primitives::U256::ZERO,
            "first deposit should have nonce 0"
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
            &erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            &bridge::depositCall {
                target_chain_id: *t.chain_id.as_bytes(),
                target_application_id: *t.app_id.as_bytes(),
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
            &bridge::depositCall {
                target_chain_id: *t.chain_id.as_bytes(),
                target_application_id: *t.app_id.as_bytes(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        assert!(result.is_err(), "deposit without approval should revert");
    }

    #[test]
    fn test_deposit_reverts_wrong_target_application() {
        let mut t = setup_deposit_test();

        call_contract(
            &mut t.db,
            DEPOSITOR,
            t.token,
            &erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(DEPOSIT_AMOUNT),
            },
        );

        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            &bridge::depositCall {
                target_chain_id: *t.chain_id.as_bytes(),
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
            &erc20::approveCall {
                spender: t.bridge,
                amount: alloy_primitives::U256::from(too_much),
            },
        );

        // Deposit more than balance should revert
        let result = try_call_contract(
            &mut t.db,
            DEPOSITOR,
            t.bridge,
            &bridge::depositCall {
                target_chain_id: *t.chain_id.as_bytes(),
                target_application_id: *t.app_id.as_bytes(),
                target_account_owner: target_owner_bytes().into(),
                amount: alloy_primitives::U256::from(too_much),
            },
        );

        assert!(
            result.is_err(),
            "deposit with insufficient balance should revert"
        );
    }

    #[test]
    fn constructor_rejects_zero_app_id() {
        use alloy_sol_types::SolValue;

        let mut db = CacheDB::new(EmptyDB::default());
        let deployer = Address::from([0x01; 20]);
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
        let token = deploy_linera_token(
            &mut db,
            deployer,
            alloy_primitives::U256::from(INITIAL_SUPPLY),
        );
        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let chain_id_bytes: [u8; 32] = (*chain_id.as_bytes()).into();

        let bytecode = compile_contract(
            crate::evm::FUNGIBLE_BRIDGE_SOURCE,
            "FungibleBridge.sol",
            "FungibleBridge",
        );
        let zero_app_id: [u8; 32] = [0u8; 32];
        let valid_app_id: [u8; 32] =
            (*CryptoHash::new(&TestString::new("valid_app")).as_bytes()).into();

        // Zero fungible application id must revert.
        {
            let mut deploy_data = bytecode.clone();
            let constructor_args = (
                light_client,
                chain_id_bytes,
                token,
                zero_app_id,
                valid_app_id,
                test_pause_guardian(),
                test_proposer(),
                test_canceller(),
                test_timelock_delay(),
            )
                .abi_encode_params();
            deploy_data.extend_from_slice(&constructor_args);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                deploy_contract(&mut db, deployer, deploy_data)
            }));
            assert!(
                result.is_err(),
                "deployment with zero fungible app id must revert"
            );
        }

        // Zero bridge application id must revert.
        {
            let mut deploy_data = bytecode;
            let constructor_args = (
                light_client,
                chain_id_bytes,
                token,
                valid_app_id,
                zero_app_id,
                test_pause_guardian(),
                test_proposer(),
                test_canceller(),
                test_timelock_delay(),
            )
                .abi_encode_params();
            deploy_data.extend_from_slice(&constructor_args);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                deploy_contract(&mut db, deployer, deploy_data)
            }));
            assert!(
                result.is_err(),
                "deployment with zero bridge app id must revert"
            );
        }
    }
}
