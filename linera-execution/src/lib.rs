use async_trait::async_trait;
use linera_base::{
    execution::ApplicationResult,
    messages::{ChainId, EffectId},
};
use thiserror::Error;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    contract_id: EffectId,
    application_id: EffectId,
    account: ChainId,
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
}
