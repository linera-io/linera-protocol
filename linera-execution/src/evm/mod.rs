// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for user applications compiled for the EVM
//!
//! We are using Revm for implementing it.

#![cfg(with_revm)]

mod database;
pub mod revm;

use revm_primitives::HaltReason;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvmExecutionError {
    #[error("Failed to load contract EVM module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service EVM module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[error("Commit error")]
    CommitError(String),
    #[error("It is illegal to call execute_message from an operation")]
    OperationCallExecuteMessage,
    #[error("The operation should contain the evm selector and so have length 4 or more")]
    OperationIsTooShort,
    #[error("Transact commit error")]
    TransactCommitError(String),
    #[error("The operation was reverted")]
    Revert {
        gas_used: u64,
        output: alloy::primitives::Bytes,
    },
    #[error("The operation was halted")]
    Halt { gas_used: u64, reason: HaltReason },
}
