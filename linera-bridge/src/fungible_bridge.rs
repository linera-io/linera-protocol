// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for FungibleBridge contract.

#[cfg(test)]
mod tests {
    use alloy_sol_types::{sol, SolCall};
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::{Amount, BlockHeight, Epoch, Round},
        identifiers::{AccountOwner, ApplicationId, ChainId},
    };
    use linera_chain::{
        block::ConfirmedBlock,
        data_types::{Transaction, Vote},
        types::ConfirmedBlockCertificate,
    };
    use linera_execution::Operation;
    use linera_sdk::abis::fungible;
    use revm::database::{CacheDB, EmptyDB};

    use crate::{microchain::addBlockCall, test_helpers::*};

    sol! {
        function operationCount() external view returns (uint64);
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

    /// Creates a Transaction::ExecuteOperation containing a FungibleOperation as a user operation.
    fn fungible_operation_transaction(
        application_id: CryptoHash,
        operation: &fungible::FungibleOperation,
    ) -> Transaction {
        Transaction::ExecuteOperation(Operation::User {
            application_id: ApplicationId::new(application_id),
            bytes: bcs::to_bytes(operation).unwrap(),
        })
    }

    #[test]
    fn test_fungible_bridge_processes_transfer() {
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

        // Create a FungibleOperation::Transfer
        let transfer_op = fungible::FungibleOperation::Transfer {
            owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("owner"))),
            amount: Amount::from_tokens(100),
            target_account: fungible::Account {
                chain_id: ChainId(CryptoHash::new(&TestString::new("target_chain"))),
                owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner"))),
            },
        };

        let transactions = vec![fungible_operation_transaction(app_id, &transfer_op)];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        call_contract(
            &mut db,
            deployer,
            bridge,
            addBlockCall {
                data: cert_bytes.into(),
            },
        );

        // Verify the operation was counted
        let count: u64 = call_contract(&mut db, deployer, bridge, operationCountCall {});
        assert_eq!(count, 1, "should have processed one fungible operation");
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

        // Create an operation from a different application
        let transfer_op = fungible::FungibleOperation::Transfer {
            owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("owner"))),
            amount: Amount::from_tokens(50),
            target_account: fungible::Account {
                chain_id: ChainId(CryptoHash::new(&TestString::new("target_chain"))),
                owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner"))),
            },
        };

        let transactions = vec![fungible_operation_transaction(other_app_id, &transfer_op)];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        call_contract(
            &mut db,
            deployer,
            bridge,
            addBlockCall {
                data: cert_bytes.into(),
            },
        );

        // Verify no operations were counted (wrong application_id)
        let count: u64 = call_contract(&mut db, deployer, bridge, operationCountCall {});
        assert_eq!(
            count, 0,
            "should not have processed operations from other app"
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

        let transfer_op = fungible::FungibleOperation::Transfer {
            owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("owner"))),
            amount: Amount::from_tokens(100),
            target_account: fungible::Account {
                chain_id: ChainId(CryptoHash::new(&TestString::new("target_chain"))),
                owner: AccountOwner::Address32(CryptoHash::new(&TestString::new("target_owner"))),
            },
        };

        let transactions = vec![fungible_operation_transaction(app_id, &transfer_op)];
        let cert = create_certificate_with_transactions(
            &secret,
            &public,
            chain_id,
            BlockHeight(1),
            transactions,
        );

        let cert_bytes = bcs::to_bytes(&cert).unwrap();
        let calldata = addBlockCall {
            data: cert_bytes.into(),
        }
        .abi_encode();
        let (_, gas_used) = call_contract_with_gas(&mut db, deployer, bridge, calldata);

        println!("Gas used for fungible bridge addBlock with Transfer: {gas_used}");
    }
}
