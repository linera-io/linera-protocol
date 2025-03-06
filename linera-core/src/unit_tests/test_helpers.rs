// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use assert_matches::assert_matches;
use linera_chain::{ChainError, ChainExecutionContext};
use linera_execution::{ExecutionError, SystemExecutionError};

use crate::{client::ChainClientError, local_node::LocalNodeError, worker::WorkerError};

/// Helper function to assert that an error is due to insufficient funding during an operation.
#[cfg(test)]
pub fn assert_insufficient_funding_during_operation<T>(
    obtained_error: &Result<T, ChainClientError>,
    operation_index: u64,
) {
    let operation_index = operation_index as u32; // Converte u64 para u32

    assert_matches!(
        obtained_error.as_ref().err().unwrap(),
        ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error)
        )) if matches!(&**error, ChainError::ExecutionError(
            execution_error, ChainExecutionContext::Operation(index)
        ) if index == &operation_index && matches!(**execution_error, ExecutionError::SystemError(
            SystemExecutionError::InsufficientFunding { .. }
        )))
    );
}

/// Helper function to assert that an error is due to insufficient funding for fees.
#[cfg(test)]
pub fn assert_insufficient_funding_fees<T>(obtained_error: &Result<T, ChainClientError>) {
    assert_matches!(
        obtained_error.as_ref().err().unwrap(),
        ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(error))
        ) if matches!(&**error, ChainError::ExecutionError(
            execution_error, ChainExecutionContext::Block
        ) if matches!(**execution_error, ExecutionError::SystemError(
            SystemExecutionError::InsufficientFundingForFees { .. }
        )))
    );
}

/// Helper function to assert that an error is due to insufficient funding with a generic execution context.
#[cfg(test)]
pub fn assert_insufficient_funding<T>(
    obtained_error: &Result<T, ChainClientError>,
    expected_context: ChainExecutionContext,
) {
    assert_matches!(
        obtained_error.as_ref().err().unwrap(),
        ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
            WorkerError::ChainError(error)
        )) if matches!(&**error, ChainError::ExecutionError(
            execution_error, ctx
        ) if matches!(**execution_error, ExecutionError::SystemError(
            SystemExecutionError::InsufficientFunding { .. }
        )) && std::mem::discriminant(ctx) == std::mem::discriminant(&expected_context))
    );
}
