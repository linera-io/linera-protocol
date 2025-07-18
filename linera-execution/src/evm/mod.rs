// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for user applications compiled for the EVM
//!
//! We are using Revm for implementing it.

#![cfg(with_revm)]

mod data_types;
mod database;
pub mod revm;

use revm_context::result::{HaltReason, Output, SuccessReason};
use revm_primitives::Log;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvmExecutionError {
    #[error("Failed to load contract EVM module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service EVM module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[error("Commit error {0}")]
    CommitError(String),
    #[error("It is illegal to call {0} from an operation")]
    IllegalOperationCall(String),
    #[error("The function {0} is being called but is missing from the bytecode API")]
    MissingFunction(String),
    #[error("Incorrect contract creation: {0}")]
    IncorrectContractCreation(String),
    #[error("The operation should contain the evm selector and so have length 4 or more")]
    OperationIsTooShort,
    #[error("Transact error {0}")]
    TransactError(String),
    #[error("Transact commit error {0}")]
    TransactCommitError(String),
    #[error("Precompile error: {0}")]
    PrecompileError(String),
    #[error("The operation was reverted with {gas_used} gas used and output {output:?}")]
    Revert {
        gas_used: u64,
        output: revm_primitives::Bytes,
    },
    #[error("The operation was halted with {gas_used} gas used due to {reason:?}")]
    Halt { gas_used: u64, reason: HaltReason },
    #[error("The interpreter did not return, reason={:?}, gas_used={}, gas_refunded={}, logs={:?}, output={:?}",
            reason, gas_used, gas_refunded, logs, output)]
    NoReturnInterpreter {
        reason: SuccessReason,
        gas_used: u64,
        gas_refunded: u64,
        logs: Vec<Log>,
        output: Output,
    },
}
