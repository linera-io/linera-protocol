use crate::{ExecutionContext, SmartContract};
use async_trait::async_trait;
use linera_base::execution::ApplicationResult;

pub struct NonFungibleToken;

#[async_trait]
impl<C: Send> SmartContract<C> for NonFungibleToken {
    type Parameters = ();
    type Operation = ();
    type Effect = ();
    type Query = ();
    type Response = ();

    async fn instantiate(
        execution: &ExecutionContext,
        parameters: Self::Parameters,
    ) -> Result<ApplicationResult, ExecutionError> {
        todo!()
    }

    async fn apply_operation(
        execution: &ExecutionContext,
        storage: &mut C,
        operation: Self::Operation,
    ) -> Result<ApplicationResult, ExecutionError> {
        todo!()
    }

    async fn apply_effect(
        execution: &ExecutionContext,
        storage: &mut C,
        effect: Self::Effect,
    ) -> Result<ApplicationResult, ExecutionError> {
        todo!()
    }

    async fn run_query(
        execution: &ExecutionContext,
        storage: &mut C,
        query: Self::Query,
    ) -> Result<Self::Response, ExecutionError> {
        todo!()
    }
}
