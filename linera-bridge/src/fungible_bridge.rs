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
    use revm::database::{CacheDB, EmptyDB};

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

    /// Creates a certificate containing a specific set of transactions.
    fn create_certificate_with_transactions(
        secret: &linera_base::crypto::ValidatorSecretKey,
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

    #[test]
    fn test_fungible_bridge_processes_credit() {
        let mut db = CacheDB::new(EmptyDB::default());
        let deployer = revm::primitives::Address::ZERO;

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
        let target = AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner")));
        let source = AccountOwner::Address32(CryptoHash::new(&TestString::new("source_owner")));
        let credit_msg = fungible::Message::Credit {
            target,
            amount: Amount::from_tokens(100),
            source,
        };

        let transactions = vec![fungible_message_transaction(origin, app_id, &credit_msg)];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        let (_, logs, _) = call_contract(
            &mut db,
            deployer,
            bridge,
            addBlockCall {
                data: cert_bytes.into(),
            },
        );

        assert_eq!(logs.len(), 1, "expected exactly one Credit event");
        let credit =
            fungible_events::Credit::decode_log(&logs[0]).expect("failed to decode Credit event");
        let target_hash = *CryptoHash::new(&TestString::new("target_owner")).as_bytes();
        let source_hash = *CryptoHash::new(&TestString::new("source_owner")).as_bytes();
        // AccountOwner::Address32 is choice==1
        assert_eq!(credit.target.choice, 1);
        assert_eq!(credit.target.address32.value, target_hash);
        assert_eq!(credit.amount, 100_000_000_000_000_000_000u128);
        assert_eq!(credit.source.choice, 1);
        assert_eq!(credit.source.address32.value, source_hash);
    }

    #[test]
    fn test_fungible_bridge_ignores_other_applications() {
        let mut db = CacheDB::new(EmptyDB::default());
        let deployer = revm::primitives::Address::ZERO;

        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let validator_addr = validator_evm_address(&public);
        let admin_chain_id = test_admin_chain_id();
        let light_client =
            deploy_light_client(&mut db, deployer, &[validator_addr], &[1], admin_chain_id);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let app_id = CryptoHash::new(&TestString::new("fungible_app"));
        let other_app_id = CryptoHash::new(&TestString::new("other_app"));
        let bridge = deploy_fungible_bridge(&mut db, deployer, light_client, chain_id, app_id);

        let origin = ChainId(CryptoHash::new(&TestString::new("origin_chain")));
        let credit_msg = fungible::Message::Credit {
            target: AccountOwner::Address32(CryptoHash::new(&TestString::new("target"))),
            amount: Amount::from_tokens(50),
            source: AccountOwner::Address32(CryptoHash::new(&TestString::new("source"))),
        };

        // Send message from a different application
        let transactions = vec![fungible_message_transaction(
            origin,
            other_app_id,
            &credit_msg,
        )];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        let (_, logs, _) = call_contract(
            &mut db,
            deployer,
            bridge,
            addBlockCall {
                data: cert_bytes.into(),
            },
        );

        assert!(
            logs.is_empty(),
            "should not emit events for other applications"
        );
    }

    #[test]
    fn test_fungible_bridge_gas_measurement() {
        let mut db = CacheDB::new(EmptyDB::default());
        let deployer = revm::primitives::Address::ZERO;

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
        let credit_msg = fungible::Message::Credit {
            target: AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner"))),
            amount: Amount::from_tokens(100),
            source: AccountOwner::Address32(CryptoHash::new(&TestString::new("source_owner"))),
        };

        let transactions = vec![fungible_message_transaction(origin, app_id, &credit_msg)];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        let (_, _, gas_used) = call_contract(
            &mut db,
            deployer,
            bridge,
            addBlockCall {
                data: cert_bytes.into(),
            },
        );

        println!("Gas used for fungible bridge addBlock with Credit message: {gas_used}");
    }
}
