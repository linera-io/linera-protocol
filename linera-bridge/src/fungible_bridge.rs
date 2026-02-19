// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for FungibleBridge contract.

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolEvent;
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::{Amount, BlockHeight, Epoch, Round, Timestamp},
        identifiers::{AccountOwner, ApplicationId, ChainId},
    };
    use linera_chain::{
        block::ConfirmedBlock,
        data_types::{
            IncomingBundle, MessageAction, MessageBundle, PostedMessage, Transaction, Vote,
        },
        types::ConfirmedBlockCertificate,
    };
    use linera_execution::{Message, MessageKind};
    use revm::{
        database::{CacheDB, EmptyDB},
        primitives::Address,
    };

    use crate::{microchain::addBlockCall, test_helpers::*};

    mod fungible_events {
        use alloy_sol_types::sol;

        sol! {
            struct CryptoHash {
                bytes32 value;
            }

            // Mirrors the AccountOwner struct from BridgeTypes.sol.
            struct AccountOwner {
                uint8 choice;
                uint8 reserved;
                CryptoHash address32;
                bytes20 address20;
            }

            // Mirrors the Credit event from FungibleBridge.sol.
            event Credit(
                AccountOwner target,
                uint128 amount,
                AccountOwner source
            );
        }
    }

    mod fungible_queries {
        use alloy_sol_types::sol;

        sol! {
            function balances(address account) external view returns (uint256);
        }
    }

    #[test]
    fn test_fungible_bridge_processes_credit() {
        let mut t = TestBridge::new();

        let (logs, _) = t.submit_credit(test_target(), Amount::from_tokens(100), test_source());

        assert_eq!(logs.len(), 1, "expected exactly one Credit event");
        let credit =
            fungible_events::Credit::decode_log(&logs[0]).expect("failed to decode Credit event");
        let source_hash = *CryptoHash::new(&TestString::new(TEST_SOURCE_NAME)).as_bytes();
        assert_eq!(credit.target.choice, 2);
        assert_eq!(&credit.target.address20[..], &TEST_TARGET[..]);
        assert_eq!(credit.amount, 100_000_000_000_000_000_000u128);
        assert_eq!(credit.source.choice, 1);
        assert_eq!(credit.source.address32.value, source_hash);
    }

    #[test]
    fn test_fungible_bridge_accumulates_balances() {
        let mut t = TestBridge::new();
        let target = test_target();
        let source = test_source();

        t.submit_credit(target, Amount::from_tokens(100), source);
        assert_eq!(
            t.query_balance(test_target_evm_address()),
            alloy_primitives::U256::from(100_000_000_000_000_000_000u128),
            "balance should be 100 tokens after first credit"
        );

        t.submit_credit(target, Amount::from_tokens(50), source);
        assert_eq!(
            t.query_balance(test_target_evm_address()),
            alloy_primitives::U256::from(150_000_000_000_000_000_000u128),
            "balance should be 150 tokens after two credits"
        );
    }

    #[test]
    fn test_fungible_bridge_skips_non_evm_targets() {
        let mut t = TestBridge::new();
        let target = AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner")));

        let (logs, _) = t.submit_credit(target, Amount::from_tokens(100), test_source());

        assert!(
            logs.is_empty(),
            "should not emit events for non-EVM address targets"
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
        let (logs, _) = t.submit_block(vec![txn]);

        assert!(
            logs.is_empty(),
            "should not emit events for other applications"
        );
    }

    #[test]
    fn test_fungible_bridge_gas_measurement() {
        let mut t = TestBridge::new();

        let (_, gas_used) = t.submit_credit(test_target(), Amount::from_tokens(100), test_source());

        println!("Gas used for fungible bridge addBlock with Credit message: {gas_used}");
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
        origin: ChainId,
        next_height: u64,
    }

    impl TestBridge {
        fn new() -> Self {
            let mut db = CacheDB::new(EmptyDB::default());
            let deployer = Address::ZERO;

            let secret = ValidatorSecretKey::generate();
            let public = secret.public();
            let validator_addr = validator_evm_address(&public);
            let admin_chain_id = test_admin_chain_id();
            let light_client =
                deploy_light_client(&mut db, deployer, &[validator_addr], &[1], admin_chain_id);

            let chain_id = CryptoHash::new(&TestString::new("test_chain"));
            let app_id = CryptoHash::new(&TestString::new("fungible_app"));
            let bridge = deploy_fungible_bridge(&mut db, deployer, light_client, chain_id, app_id);
            let origin = ChainId(CryptoHash::new(&TestString::new("origin_chain")));

            Self {
                db,
                deployer,
                secret,
                public,
                chain_id,
                app_id,
                bridge,
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

        /// Queries the balance of an EVM address.
        fn query_balance(&mut self, account: Address) -> alloy_primitives::U256 {
            let (balance, _, _) = call_contract(
                &mut self.db,
                self.deployer,
                self.bridge,
                fungible_queries::balancesCall { account },
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

    /// Creates a Transaction::ReceiveMessages containing a fungible Message as a user message.
    fn fungible_message_transaction(
        origin: ChainId,
        application_id: CryptoHash,
        message: &fungible::Message,
    ) -> Transaction {
        Transaction::ReceiveMessages(IncomingBundle {
            origin,
            bundle: MessageBundle {
                height: BlockHeight(0),
                timestamp: Timestamp::from(0),
                certificate_hash: CryptoHash::new(&TestString::new("cert")),
                transaction_index: 0,
                messages: vec![PostedMessage {
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Simple,
                    index: 0,
                    message: Message::User {
                        application_id: ApplicationId::new(application_id),
                        bytes: bcs::to_bytes(message).unwrap(),
                    },
                }],
            },
            action: MessageAction::Accept,
        })
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
}
