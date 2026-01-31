// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use assert_matches::assert_matches;
use linera_chain::{ChainError, ChainExecutionContext};
use linera_execution::ExecutionError;

use crate::{client::chain_client, local_node::LocalNodeError, worker::WorkerError};

/// Asserts that an error is due to insufficient balance during an operation.
pub fn assert_insufficient_balance_during_operation<T>(
    obtained_error: Result<T, chain_client::Error>,
    operation_index: u32,
) {
    let chain_client::Error::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(
        error,
    ))) = obtained_error.err().unwrap()
    else {
        panic!("Expected a chain_client::Error::LocalNodeError with a WorkerError::ChainError");
    };

    let ChainError::ExecutionError(execution_error, context, _) = *error else {
        panic!("Expected a ChainError::ExecutionError, found: {error:#?}");
    };

    let ChainExecutionContext::Operation(index) = context else {
        panic!("Expected ChainExecutionContext::Operation, found: {context:#?}");
    };

    assert_eq!(index, operation_index, "Operation index mismatch");

    assert_matches!(
        *execution_error,
        ExecutionError::InsufficientBalance { .. },
        "Expected ExecutionError::InsufficientBalance, found: {execution_error:#?}"
    );
}

/// Asserts that an error is due to insufficient funding for fees.
pub fn assert_fees_exceed_funding<T>(obtained_error: Result<T, chain_client::Error>) {
    let chain_client::Error::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(
        error,
    ))) = obtained_error.err().unwrap()
    else {
        panic!("Expected a chain_client::Error::LocalNodeError with a WorkerError::ChainError");
    };

    let ChainError::ExecutionError(execution_error, _context, _) = *error else {
        panic!("Expected a ChainError::ExecutionError, found: {error:#?}");
    };

    assert_matches!(
        *execution_error,
        ExecutionError::FeesExceedFunding { .. },
        "Expected ExecutionError::FeesExceedFunding, found: {execution_error:#?}"
    );
}

/// Asserts that an error is due to insufficient funding with a generic execution context.
pub fn assert_insufficient_funding<T>(
    obtained_error: Result<T, chain_client::Error>,
    expected_context: ChainExecutionContext,
) {
    let chain_client::Error::LocalNodeError(LocalNodeError::WorkerError(WorkerError::ChainError(
        error,
    ))) = obtained_error.err().unwrap()
    else {
        panic!("Expected a chain_client::Error::LocalNodeError with a WorkerError::ChainError");
    };

    let ChainError::ExecutionError(execution_error, context, _) = *error else {
        panic!("Expected a ChainError::ExecutionError, found: {error:#?}");
    };

    if std::mem::discriminant(&context) != std::mem::discriminant(&expected_context) {
        panic!("Expected execution context {expected_context:?}, but found {context:?}");
    }

    assert_matches!(
        *execution_error,
        ExecutionError::InsufficientBalance { .. },
        "Expected ExecutionError::InsufficientBalance, found: {execution_error:#?}"
    );
}
