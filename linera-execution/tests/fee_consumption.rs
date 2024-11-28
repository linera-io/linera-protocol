// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for how the runtime computes fees based on consumed resources.

#![allow(clippy::items_after_test_module)]

use std::{sync::Arc, vec};

use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{Account, AccountOwner, ChainDescription, ChainId, MessageId, Owner},
};
use linera_execution::{
    test_utils::{ExpectedCall, RegisterMockApplication, SystemExecutionState},
    ContractRuntime, ExecutionError, ExecutionOutcome, Message, MessageContext,
    RawExecutionOutcome, ResourceControlPolicy, ResourceController, TransactionTracker,
};
use test_case::test_case;

/// Tests if the chain balance is updated based on the fees spent for consuming resources.
// Chain account only.
#[test_case(vec![], Amount::ZERO, None, None; "without any costs")]
#[test_case(vec![FeeSpend::Fuel(100)], Amount::from_tokens(1_000), None, None; "with only execution costs")]
#[test_case(vec![FeeSpend::Read(vec![0, 1], None)], Amount::from_tokens(1_000), None, None; "with only an empty read")]
#[test_case(
    vec![
        FeeSpend::Read(vec![0, 1], None),
        FeeSpend::Fuel(207),
    ],
    Amount::from_tokens(1_000),
    None,
    None;
    "with execution and an empty read"
)]
// Chain account and small owner account.
#[test_case(
    vec![FeeSpend::Fuel(100)],
    Amount::from_tokens(1_000),
    Some(Amount::from_tokens(1)),
    None;
    "with only execution costs and with owner account"
)]
#[test_case(
    vec![FeeSpend::Read(vec![0, 1], None)],
    Amount::from_tokens(1_000),
    Some(Amount::from_tokens(1)),
    None;
    "with only an empty read and with owner account"
)]
#[test_case(
    vec![
        FeeSpend::Read(vec![0, 1], None),
        FeeSpend::Fuel(207),
    ],
    Amount::from_tokens(1_000),
    Some(Amount::from_tokens(1)),
    None;
    "with execution and an empty read and with owner account"
)]
// Small chain account and larger owner account.
#[test_case(
    vec![FeeSpend::Fuel(100)],
    Amount::from_tokens(1),
    Some(Amount::from_tokens(1_000)),
    None;
    "with only execution costs and with larger owner account"
)]
#[test_case(
    vec![FeeSpend::Read(vec![0, 1], None)],
    Amount::from_tokens(1),
    Some(Amount::from_tokens(1_000)),
    None;
    "with only an empty read and with larger owner account"
)]
#[test_case(
    vec![
        FeeSpend::Read(vec![0, 1], None),
        FeeSpend::Fuel(207),
    ],
    Amount::from_tokens(1),
    Some(Amount::from_tokens(1_000)),
    None;
    "with execution and an empty read and with larger owner account"
)]
// Small chain account, small owner account, large grant.
#[test_case(
    vec![FeeSpend::Fuel(100)],
    Amount::from_tokens(2),
    Some(Amount::from_tokens(1)),
    Some(Amount::from_tokens(1_000));
    "with only execution costs and with owner account and grant"
)]
#[test_case(
    vec![FeeSpend::Read(vec![0, 1], None)],
    Amount::from_tokens(2),
    Some(Amount::from_tokens(1)),
    Some(Amount::from_tokens(1_000));
    "with only an empty read and with owner account and grant"
)]
#[test_case(
    vec![
        FeeSpend::Read(vec![0, 1], None),
        FeeSpend::Fuel(207),
    ],
    Amount::from_tokens(2),
    Some(Amount::from_tokens(1)),
    Some(Amount::from_tokens(1_000));
    "with execution and an empty read and with owner account and grant"
)]
// TODO(#1601): Add more test cases
#[tokio::test]
async fn test_fee_consumption(
    spends: Vec<FeeSpend>,
    chain_balance: Amount,
    owner_balance: Option<Amount>,
    initial_grant: Option<Amount>,
) -> anyhow::Result<()> {
    let mut state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..SystemExecutionState::default()
    };
    let (application_id, application) = state.register_mock_application().await?;
    let mut view = state.into_view().await;

    let signer = Owner::from(PublicKey::test_key(0));
    let owner = AccountOwner::User(signer);
    view.system.balance.set(chain_balance);
    if let Some(owner_balance) = owner_balance {
        view.system.balances.insert(&owner, owner_balance)?;
    }

    let prices = ResourceControlPolicy {
        block: Amount::from_tokens(2),
        fuel_unit: Amount::from_tokens(3),
        read_operation: Amount::from_tokens(5),
        write_operation: Amount::from_tokens(7),
        byte_read: Amount::from_tokens(11),
        byte_written: Amount::from_tokens(13),
        byte_stored: Amount::from_tokens(17),
        operation: Amount::from_tokens(19),
        operation_byte: Amount::from_tokens(23),
        message: Amount::from_tokens(29),
        message_byte: Amount::from_tokens(31),
        maximum_fuel_per_block: 4_868_145_137,
        maximum_executed_block_size: 37,
        maximum_blob_size: 41,
        maximum_bytecode_size: 43,
        maximum_block_proposal_size: 47,
        maximum_bytes_read_per_block: 53,
        maximum_bytes_written_per_block: 59,
    };

    let consumed_fees = spends
        .iter()
        .map(|spend| spend.amount(&prices))
        .fold(Amount::ZERO, |sum, spent_fees| {
            sum.saturating_add(spent_fees)
        });

    let authenticated_signer = if owner_balance.is_some() {
        Some(signer)
    } else {
        None
    };
    let mut controller = ResourceController {
        policy: Arc::new(prices),
        account: authenticated_signer,
        ..ResourceController::default()
    };

    application.expect_call(ExpectedCall::execute_message(
        move |runtime, _context, _operation| {
            for spend in spends {
                spend.execute(runtime)?;
            }
            Ok(())
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let refund_grant_to = Some(Account {
        chain_id: ChainId::root(0),
        owner: authenticated_signer.map(AccountOwner::User),
    });
    let context = MessageContext {
        chain_id: ChainId::root(0),
        is_bouncing: false,
        authenticated_signer,
        refund_grant_to,
        height: BlockHeight(0),
        certificate_hash: CryptoHash::default(),
        message_id: MessageId::default(),
    };
    let mut grant = initial_grant.unwrap_or_default();
    let mut txn_tracker = TransactionTracker::new(0, Some(Vec::new()));
    view.execute_message(
        context,
        Timestamp::from(0),
        Message::User {
            application_id,
            bytes: vec![],
        },
        if initial_grant.is_some() {
            Some(&mut grant)
        } else {
            None
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    let (outcomes, _, _) = txn_tracker.destructure()?;
    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome {
                    refund_grant_to,
                    authenticated_signer,
                    ..Default::default()
                }
            ),
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome {
                    refund_grant_to,
                    authenticated_signer,
                    ..Default::default()
                }
            )
        ]
    );

    match initial_grant {
        None => {
            let (expected_chain_balance, expected_owner_balance) = if chain_balance >= consumed_fees
            {
                (chain_balance.saturating_sub(consumed_fees), owner_balance)
            } else {
                let Some(owner_balance) = owner_balance else {
                    panic!("execution should have failed earlier");
                };
                (
                    Amount::ZERO,
                    Some(
                        owner_balance
                            .saturating_add(chain_balance)
                            .saturating_sub(consumed_fees),
                    ),
                )
            };
            assert_eq!(*view.system.balance.get(), expected_chain_balance);
            assert_eq!(
                view.system.balances.get(&owner).await?,
                expected_owner_balance
            );
            assert_eq!(grant, Amount::ZERO);
        }
        Some(initial_grant) => {
            let (expected_grant, expected_owner_balance) = if initial_grant >= consumed_fees {
                (initial_grant.saturating_sub(consumed_fees), owner_balance)
            } else {
                let Some(owner_balance) = owner_balance else {
                    panic!("execution should have failed earlier");
                };
                (
                    Amount::ZERO,
                    Some(
                        owner_balance
                            .saturating_add(initial_grant)
                            .saturating_sub(consumed_fees),
                    ),
                )
            };
            assert_eq!(*view.system.balance.get(), chain_balance);
            assert_eq!(
                view.system.balances.get(&owner).await?,
                expected_owner_balance
            );
            assert_eq!(grant, expected_grant);
        }
    }

    Ok(())
}

/// A runtime operation that costs some amount of fees.
pub enum FeeSpend {
    /// Consume some execution fuel.
    Fuel(u64),
    /// Reads from storage.
    Read(Vec<u8>, Option<Vec<u8>>),
}

impl FeeSpend {
    /// The fee amount required for this runtime operation.
    pub fn amount(&self, policy: &ResourceControlPolicy) -> Amount {
        match self {
            FeeSpend::Fuel(units) => policy.fuel_unit.saturating_mul(*units as u128),
            FeeSpend::Read(_key, value) => {
                let value_read_fee = value
                    .as_ref()
                    .map(|value| Amount::from(value.len() as u128))
                    .unwrap_or(Amount::ZERO);

                policy.read_operation.saturating_add(value_read_fee)
            }
        }
    }

    /// Executes the operation with the `runtime`
    pub fn execute(self, runtime: &mut impl ContractRuntime) -> Result<(), ExecutionError> {
        match self {
            FeeSpend::Fuel(units) => runtime.consume_fuel(units),
            FeeSpend::Read(key, value) => {
                let promise = runtime.read_value_bytes_new(key)?;
                let response = runtime.read_value_bytes_wait(&promise)?;
                assert_eq!(response, value);
                Ok(())
            }
        }
    }
}
