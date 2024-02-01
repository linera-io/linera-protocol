// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for how the runtime computes fees based on consumed resources.

#![allow(clippy::items_after_test_module)]

mod utils;

use self::utils::{register_mock_applications, ExpectedCall};
use linera_base::{
    data_types::{Amount, BlockHeight},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    policy::ResourceControlPolicy, ContractRuntime, ExecutionError, ExecutionOutcome,
    ExecutionRuntimeConfig, ExecutionStateView, Operation, OperationContext, RawExecutionOutcome,
    ResourceController, SystemExecutionState, TestExecutionRuntimeContext,
};
use linera_views::memory::MemoryContext;
use std::{sync::Arc, vec};
use test_case::test_case;

/// Tests if the chain balance is updated based on the fees spent for consuming resources.
#[test_case(vec![]; "without any costs")]
#[test_case(vec![FeeSpend::Fuel(100)]; "with only execution costs")]
#[test_case(vec![FeeSpend::Read(vec![0, 1], None)]; "with only an empty read")]
#[test_case(vec![
    FeeSpend::Read(vec![0, 1], None),
    FeeSpend::Fuel(207),
]; "with execution and an empty read")]
#[tokio::test]
async fn test_fee_consumption(spends: Vec<FeeSpend>) -> anyhow::Result<()> {
    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..SystemExecutionState::default()
    };
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            ExecutionRuntimeConfig::Synchronous,
        )
        .await;

    let initial_balance: Amount = Amount::from_tokens(1_000);
    view.system.balance.set(initial_balance);

    let mut applications = register_mock_applications(&mut view, 1).await?;
    let (application_id, application) = applications
        .next()
        .expect("Caller mock application should be registered");

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
        maximum_bytes_read_per_block: 37,
        maximum_bytes_written_per_block: 41,
    };

    let consumed_fees = spends
        .iter()
        .map(|spend| spend.amount(&prices))
        .fold(Amount::ZERO, |sum, spent_fees| {
            sum.saturating_add(spent_fees)
        });

    let mut controller = ResourceController {
        policy: Arc::new(prices),
        ..ResourceController::default()
    };

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            for spend in spends {
                spend.execute(runtime)?;
            }
            Ok(RawExecutionOutcome::default())
        },
    ));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_message_index: 0,
    };
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await?;

    let expected_final_balance = initial_balance.saturating_sub(consumed_fees);

    assert_eq!(*view.system.balance.get(), expected_final_balance);
    assert_eq!(
        outcomes,
        vec![ExecutionOutcome::User(
            application_id,
            RawExecutionOutcome::default()
        ),]
    );
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
