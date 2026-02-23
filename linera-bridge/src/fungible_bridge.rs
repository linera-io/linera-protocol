// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for FungibleBridge contract.

#[cfg(test)]
mod tests {
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

    use crate::{microchain::addBlockCall, test_helpers::*};

    mod erc20 {
        use alloy_sol_types::sol;

        sol! {
            function balanceOf(address account) external view returns (uint256);
            function transfer(address to, uint256 amount) external returns (bool);
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
}
