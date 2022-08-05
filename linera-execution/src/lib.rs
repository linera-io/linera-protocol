use async_trait::async_trait;
use linera_base::{
    execution::ApplicationResult,
    messages::{Block, ChainId, EffectId},
};
use thiserror::Error;

mod escrow;
mod fungible_token;
mod non_fungible_token;

#[async_trait]
pub trait SmartContract<C: Send> {
    type Parameters;
    type Operation;
    type Effect;
    type Query;
    type Response;

    async fn instantiate(
        execution: &ExecutionContext,
        parameters: Self::Parameters,
    ) -> Result<ApplicationResult, ExecutionError>;

    async fn apply_operation(
        execution: &ExecutionContext,
        storage: &mut C,
        operation: Self::Operation,
        sender: Option<ExecutionContext>,
    ) -> Result<ApplicationResult, ExecutionError>;

    async fn apply_effect(
        execution: &ExecutionContext,
        storage: &mut C,
        effect: Self::Effect,
    ) -> Result<ApplicationResult, ExecutionError>;

    async fn run_query(
        execution: &ExecutionContext,
        storage: &mut C,
        query: Self::Query,
    ) -> Result<Self::Response, ExecutionError>;
}

#[async_trait]
pub trait BlockValidatorSmartContract<C: Send> {
    type Command;

    async fn validate_block(
        execution: &ExecutionContext,
        storage: &mut C,
        block: &Block,
        command: Self::Command,
    ) -> Result<(), ExecutionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemplateId(EffectId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationId(EffectId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractShard(ChainId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    template_id: TemplateId,
    application_id: ApplicationId,
    shard: ContractShard,
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
