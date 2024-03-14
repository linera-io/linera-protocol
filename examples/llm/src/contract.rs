#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Llm;
use async_trait::async_trait;
use linera_sdk::{
    base::{SessionId, WithContractAbi},
    ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, SessionCallOutcome,
    ViewStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(Llm);

impl WithContractAbi for Llm {
    type Abi = llm::LlmAbi;
}

#[async_trait]
impl Contract for Llm {
    type Error = ContractError;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _runtime: &mut ContractRuntime,
        _argument: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        _runtime: &mut ContractRuntime,
        _operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Ok(ExecutionOutcome::default())
    }

    async fn execute_message(
        &mut self,
        _runtime: &mut ContractRuntime,
        _message: Self::Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Ok(ExecutionOutcome::default())
    }

    async fn handle_application_call(
        &mut self,
        _runtime: &mut ContractRuntime,
        _call: Self::ApplicationCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<
        ApplicationCallOutcome<Self::Message, Self::Response, Self::SessionState>,
        Self::Error,
    > {
        Ok(ApplicationCallOutcome::default())
    }

    async fn handle_session_call(
        &mut self,
        _runtime: &mut ContractRuntime,
        _session: Self::SessionState,
        _call: Self::SessionCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Ok(SessionCallOutcome::default())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum ContractError {
    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
    // Add more error variants here.
}
