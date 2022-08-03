use async_trait::async_trait;
use linera_base::{
    execution::ApplicationResult,
    messages::{ChainId, EffectId},
};

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
    ) -> ApplicationResult;

    async fn apply_operation(
        execution: &ExecutionContext,
        storage: &mut C,
        operation: Self::Operation,
    ) -> ApplicationResult;

    async fn apply_effect(
        execution: &ExecutionContext,
        storage: &mut C,
        effect: Self::Effect,
    ) -> ApplicationResult;

    async fn run_query(
        execution: &ExecutionContext,
        storage: &mut C,
        query: Self::Query,
    ) -> Self::Response;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionContext {
    contract_id: EffectId,
    application_id: EffectId,
    account: ChainId,
}
