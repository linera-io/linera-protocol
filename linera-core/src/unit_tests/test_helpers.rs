// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_chain::{ChainError, ChainExecutionContext};
use linera_execution::{ExecutionError, SystemExecutionError};

use crate::{client::ChainClientError, local_node::LocalNodeError, worker::WorkerError};

/// Asserts that an error is due to insufficient funding during an operation.
pub fn assert_insufficient_funding_during_operation<T>(
    obtained_error: &Result<T, ChainClientError>,
    operation_index: u32,
) {
    let error = match obtained_error.as_ref().err().unwrap() {
        ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error),
        )) => error,
        _ => panic!("Expected a ChainClientError::LocalNodeError with a WorkerError::ChainError"),
    };

    let (execution_error, context) = match &**error {
        ChainError::ExecutionError(execution_error, context) => (execution_error, context),
        _ => panic!("Expected a ChainError::ExecutionError"),
    };

    if let ChainExecutionContext::Operation(index) = context {
        assert_eq!(index, &operation_index, "Operation index mismatch");
    } else {
        panic!("Expected ChainExecutionContext::Operation");
    }

    match &**execution_error {
        ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }) => {}
        _ => panic!("Expected ExecutionError::SystemError::InsufficientFunding"),
    }
}

/// Asserts that an error is due to insufficient funding for fees.
pub fn assert_insufficient_funding_fees<T>(obtained_error: &Result<T, ChainClientError>) {
    let error = match obtained_error.as_ref().err().unwrap() {
        ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error),
        )) => error,
        _ => panic!("Expected a ChainClientError::LocalNodeError with a WorkerError::ChainError"),
    };

    let (execution_error, context) = match &**error {
        ChainError::ExecutionError(execution_error, context) => (execution_error, context),
        _ => panic!("Expected a ChainError::ExecutionError"),
    };

    if !matches!(context, ChainExecutionContext::Block) {
        panic!("Expected ChainExecutionContext::Block, found {:?}", context);
    }

    match &**execution_error {
        ExecutionError::SystemError(SystemExecutionError::InsufficientFundingForFees { .. }) => {}
        _ => panic!("Expected ExecutionError::SystemError::InsufficientFundingForFees"),
    }
}

/// Asserts that an error is due to insufficient funding with a generic execution context.
pub fn assert_insufficient_funding<T>(
    obtained_error: &Result<T, ChainClientError>,
    expected_context: ChainExecutionContext,
) {
    let error = match obtained_error.as_ref().err().unwrap() {
        ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error),
        )) => error,
        _ => panic!("Expected a ChainClientError::LocalNodeError with a WorkerError::ChainError"),
    };

    let (execution_error, context) = match &**error {
        ChainError::ExecutionError(execution_error, context) => (execution_error, context),
        _ => panic!("Expected a ChainError::ExecutionError"),
    };

    if std::mem::discriminant(context) != std::mem::discriminant(&expected_context) {
        panic!(
            "Expected execution context {:?}, but found {:?}",
            expected_context, context
        );
    }

    match &**execution_error {
        ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }) => {}
        _ => panic!("Expected ExecutionError::SystemError::InsufficientFunding"),
    }
}