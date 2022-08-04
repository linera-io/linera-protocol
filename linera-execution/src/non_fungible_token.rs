use crate::{ExecutionContext, ExecutionError, SmartContract};
use async_trait::async_trait;
use linera_base::{ensure, execution::ApplicationResult, messages::ChainId};
use linera_views::views::{RegisterOperations, RegisterView, View};
use serde::Serialize;
use thiserror::Error;

pub struct NonFungibleToken;

#[async_trait]
impl<C> SmartContract<C> for NonFungibleToken
where
    C: RegisterOperations<bool> + Clone + Send + Sync,
{
    type Parameters = InitialOwner;
    type Operation = Operation;
    type Effect = Effect;
    type Query = Query;
    type Response = QueryResponse;

    async fn instantiate(
        execution: &ExecutionContext,
        initial_owner: Self::Parameters,
    ) -> Result<ApplicationResult, ExecutionError> {
        Ok(ApplicationResult {
            effects: vec![Effect::Receive.to_system_effect(execution)],
            operations: vec![],
            recipients: vec![initial_owner.0],
            subscribe: None,
            unsubscribe: None,
            need_channel_broadcast: Vec::new(),
        })
    }

    async fn apply_operation(
        execution: &ExecutionContext,
        storage: &mut C,
        operation: Self::Operation,
    ) -> Result<ApplicationResult, ExecutionError> {
        match operation {
            Operation::Transfer { recipient } => Self::transfer(recipient, execution, storage)
                .await
                .map_err(ExecutionError::storage),
        }
    }

    async fn apply_effect(
        _execution: &ExecutionContext,
        storage: &mut C,
        effect: Self::Effect,
    ) -> Result<ApplicationResult, ExecutionError> {
        match effect {
            Effect::Receive => Self::receive(storage)
                .await
                .map_err(ExecutionError::storage),
        }
    }

    async fn run_query(
        _execution: &ExecutionContext,
        storage: &mut C,
        query: Self::Query,
    ) -> Result<Self::Response, ExecutionError> {
        match query {
            Query::HasToken => Self::has_token(storage)
                .await
                .map(QueryResponse::HasToken)
                .map_err(ExecutionError::storage),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct InitialOwner(ChainId);

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    Transfer { recipient: Option<ChainId> },
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum Effect {
    Receive,
}

impl Effect {
    pub fn to_system_effect(&self, context: &ExecutionContext) -> linera_base::execution::Effect {
        linera_base::execution::Effect::application_specific(context.application_id, self)
            .expect("Receive effect is serializable")
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Query {
    HasToken,
}

#[derive(Clone, Copy, Debug)]
pub enum QueryResponse {
    HasToken(bool),
}

impl NonFungibleToken {
    async fn transfer<S>(
        recipient: Option<ChainId>,
        execution: &ExecutionContext,
        storage: &mut S,
    ) -> Result<ApplicationResult, TransferError>
    where
        S: RegisterOperations<bool> + Clone + Send + Sync,
    {
        let mut ownership = RegisterView::<_, bool>::load(storage.clone(), vec![].into())
            .await
            .map_err(TransferError::storage_error)?;
        let is_current_owner = *ownership.get();
        ensure!(is_current_owner, TransferError::NotCurrentOwner);
        ownership.set(false);
        ownership
            .commit()
            .await
            .map_err(TransferError::storage_error)?;
        let application = match recipient {
            None => ApplicationResult::default(),
            Some(recipient) => ApplicationResult {
                effects: vec![Effect::Receive.to_system_effect(execution)],
                operations: vec![],
                recipients: vec![recipient],
                subscribe: None,
                unsubscribe: None,
                need_channel_broadcast: Vec::new(),
            },
        };
        Ok(application)
    }

    async fn receive<S>(storage: &mut S) -> Result<ApplicationResult, S::Error>
    where
        S: RegisterOperations<bool> + Clone + Send + Sync,
    {
        let mut ownership = RegisterView::<_, bool>::load(storage.clone(), vec![].into()).await?;
        ownership.set(true);
        ownership.commit().await?;
        Ok(ApplicationResult::default())
    }

    async fn has_token<S>(storage: &mut S) -> Result<bool, S::Error>
    where
        S: RegisterOperations<bool> + Clone + Send + Sync,
    {
        let ownership = RegisterView::<_, bool>::load(storage.clone(), vec![].into()).await?;
        Ok(*ownership.get())
    }
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("Can't transfer if the chain is not the current owner of the token")]
    NotCurrentOwner,

    #[error("Failed to load or store balance")]
    StorageError { message: String },
}

impl TransferError {
    pub fn storage_error(storage_error: impl std::error::Error) -> Self {
        TransferError::StorageError {
            message: storage_error.to_string(),
        }
    }
}

impl From<TransferError> for ExecutionError {
    fn from(transfer_error: TransferError) -> ExecutionError {
        match transfer_error {
            TransferError::StorageError { message } => ExecutionError::Storage { message },
            other_error => ExecutionError::Custom {
                message: other_error.to_string(),
            },
        }
    }
}
