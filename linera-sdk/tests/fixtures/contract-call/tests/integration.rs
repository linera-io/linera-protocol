// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Contract Call application

#![cfg(not(target_arch = "wasm32"))]

use contract_call::{ContractTransferAbi, Parameters};
use linera_sdk::{
    linera_base_types::{Account, AccountOwner, Amount},
    test::TestValidator,
};

/// Test that mimics the end-to-end contract call test from linera_net_tests.rs
/// This test creates two accounts, publishes the contract-call module,
/// creates an application, and tests indirect transfer functionality.
#[tokio::test]
async fn test_contract_call_integration() {
    use contract_call::Operation;
    let (validator, module_id) =
        TestValidator::with_current_module::<ContractTransferAbi, Parameters, ()>().await;

    let mut chain = validator.new_chain().await;
    let owner1 = AccountOwner::from(chain.public_key());
    let account1 = Account {
        chain_id: chain.id(),
        owner: owner1,
    };

    let transfer_amount = Amount::from_tokens(100);
    let funding_chain = validator.get_chain(&validator.admin_chain_id());
    let (transfer_certificate, _) = funding_chain
        .add_block(|block| {
            block.with_native_token_transfer(AccountOwner::CHAIN, account1, transfer_amount);
        })
        .await;
    chain
        .add_block(|block| {
            block.with_messages_from(&transfer_certificate);
        })
        .await;

    // Generate a second owner
    let second_chain = validator.new_chain().await;
    let owner2 = AccountOwner::from(second_chain.public_key());

    // Create accounts for both owners
    let account2 = Account {
        chain_id: chain.id(),
        owner: owner2,
    };

    // Give initial balance to the first account (simulating the transfer_with_accounts)
    // In the test environment, chains start with default balance

    // Create parameters for the application
    let parameters = Parameters {
        module_id: module_id.forget_abi(),
    };

    // Create the application
    let application_id = chain
        .create_application(module_id, parameters, (), vec![])
        .await;

    // Test indirect transfer: transfer from owner1 to owner2 via application
    let transfer_amount = Amount::from_tokens(1);

    // Use Operation to trigger indirect transfer operation
    let operation = Operation::IndirectTransfer {
        source: owner1,
        destination: account2,
        amount: transfer_amount,
    };

    // Add a block with the operation
    chain
        .add_block(|block| {
            block.with_operation(application_id, operation);
        })
        .await;
}
