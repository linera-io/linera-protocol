// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    execution::ApplicationResult,
    messages::{ChainId, ChannelId},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[async_trait]
pub trait SmartContract {
    type Operation;
    type Effect;

    async fn apply_operation(
        &self,
        context: &ExecutionContext,
        state: &mut ContractExecutionState,
        operation: Self::Operation,
        sender: Option<ExecutionContext>,
    ) -> Result<ApplicationResult, ExecutionError>;

    async fn apply_effect(
        &self,
        context: &ExecutionContext,
        state: &mut ContractExecutionState,
        effect: Self::Effect,
    ) -> Result<ApplicationResult, ExecutionError>;
}

/// Execution state of a smart contract.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ContractExecutionState {
    /// We avoid BTreeSet<String> because of a Serde/BCS limitation.
    pub subscriptions: BTreeMap<ChannelId, ()>,
    /// Specific state.
    pub data: Vec<u8>,
}

// #[async_trait]
// pub trait BlockValidatorSmartContract<C: Send> {
//     type Command;

//     async fn validate_block(
//         execution: &ExecutionContext,
//         storage: &mut C,
//         block: &Block,
//         command: Self::Command,
//     ) -> Result<(), ExecutionError>;
// }

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    chain_id: ChainId,
    application_id: crate::messages::ApplicationId,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Internal storage error: {message}")]
    Storage { message: String },

    #[error("Smart-contract exeuction failed: {message}")]
    Custom { message: String },
}

impl ExecutionError {
    pub fn storage(error: impl std::error::Error) -> Self {
        ExecutionError::Storage {
            message: error.to_string(),
        }
    }

    pub fn custom(message: impl Into<String>) -> Self {
        ExecutionError::Custom {
            message: message.into(),
        }
    }
}
