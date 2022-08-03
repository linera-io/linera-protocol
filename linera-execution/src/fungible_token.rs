use crate::{ExecutionContext, SmartContract};
use async_trait::async_trait;
use linera_base::{
    ensure,
    execution::{Amount, ApplicationResult, Balance},
    messages::ChainId,
};
use linera_views::views::{RegisterOperations, RegisterView, View};
use serde::Serialize;
use thiserror::Error;

pub struct FungibleToken;

#[async_trait]
impl<C> SmartContract<C> for FungibleToken
where
    C: RegisterOperations<u128> + Clone + Send + Sync,
{
    type Parameters = Genesis;
    type Operation = Operation;
    type Effect = Effect;
    type Query = Query;
    type Response = QueryResponse;

    async fn instantiate(
        execution: &ExecutionContext,
        parameters: Self::Parameters,
    ) -> ApplicationResult {
        ApplicationResult {
            effects: vec![Effect::Credit(parameters.initial_balance).to_system_effect(execution)],
            operations: vec![],
            recipients: vec![parameters.genesis_account],
            subscribe: None,
            unsubscribe: None,
            need_channel_broadcast: Vec::new(),
        }
    }

    async fn apply_operation(
        execution: &ExecutionContext,
        storage: &mut C,
        operation: Self::Operation,
    ) -> ApplicationResult {
        match operation {
            Operation::Transfer { recipient, amount } => {
                Self::transfer(recipient, amount, execution, storage)
                    .await
                    .unwrap_or_else(|error| {
                        log::error!("{error}");
                        ApplicationResult::default()
                    })
            }
        }
    }

    async fn apply_effect(
        _execution: &ExecutionContext,
        storage: &mut C,
        effect: Self::Effect,
    ) -> ApplicationResult {
        match effect {
            Effect::Credit(amount) => Self::credit(amount, storage).await.unwrap_or_else(|_| {
                log::error!("Failed to store updated balance");
                ApplicationResult::default()
            }),
        }
    }

    async fn run_query(
        _execution: &ExecutionContext,
        storage: &mut C,
        query: Self::Query,
    ) -> Self::Response {
        match query {
            Query::Balance => {
                QueryResponse::Balance(Self::get_balance(storage).await.unwrap_or_else(|_| {
                    log::error!("Failed to query balance");
                    Balance::zero()
                }))
            }
        }
    }
}

pub struct Genesis {
    genesis_account: ChainId,
    initial_balance: Amount,
}

pub enum Operation {
    Transfer {
        recipient: Option<ChainId>,
        amount: Amount,
    },
}

#[derive(Serialize)]
pub enum Effect {
    Credit(Amount),
}

impl Effect {
    pub fn to_system_effect(&self, context: &ExecutionContext) -> linera_base::execution::Effect {
        linera_base::execution::Effect::application_specific(context.application_id, self)
            .expect("Credit effect is serializable")
    }
}

pub enum Query {
    Balance,
}

pub enum QueryResponse {
    Balance(Balance),
}

impl FungibleToken {
    async fn transfer<S>(
        recipient: Option<ChainId>,
        amount: Amount,
        execution: &ExecutionContext,
        storage: &mut S,
    ) -> Result<ApplicationResult, TransferError>
    where
        S: RegisterOperations<u128> + Clone + Send + Sync,
    {
        ensure!(
            amount > Amount::zero(),
            TransferError::IncorrectTransferAmount
        );
        let mut balance = RegisterView::<_, u128>::load(storage.clone(), vec![].into())
            .await
            .map_err(|_| TransferError::StorageError)?;
        let current_balance = Balance::from(*balance.get());
        ensure!(
            current_balance >= amount.into(),
            TransferError::InsufficientFunding { current_balance }
        );
        balance.set(
            current_balance
                .try_sub(amount.into())
                .expect("Balance checked to contain at least amount")
                .into(),
        );
        balance
            .commit()
            .await
            .map_err(|_| TransferError::StorageError)?;
        let application = match recipient {
            None => ApplicationResult::default(),
            Some(recipient) => ApplicationResult {
                effects: vec![Effect::Credit(amount).to_system_effect(execution)],
                operations: vec![],
                recipients: vec![recipient],
                subscribe: None,
                unsubscribe: None,
                need_channel_broadcast: Vec::new(),
            },
        };
        Ok(application)
    }

    async fn credit<S>(amount: Amount, storage: &mut S) -> Result<ApplicationResult, S::Error>
    where
        S: RegisterOperations<u128> + Clone + Send + Sync,
    {
        let mut balance = RegisterView::<_, u128>::load(storage.clone(), vec![].into()).await?;
        let new_value = Balance::from(*balance.get())
            .try_add(amount.into())
            .unwrap_or_else(|_| Balance::max());
        balance.set(new_value.into());
        balance.commit().await?;
        Ok(ApplicationResult::default())
    }

    async fn get_balance<S>(storage: &mut S) -> Result<Balance, S::Error>
    where
        S: RegisterOperations<u128> + Clone + Send + Sync,
    {
        let balance = RegisterView::<_, u128>::load(storage.clone(), vec![].into()).await?;
        Ok(Balance::from(*balance.get()))
    }
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("Transfers must have positive amount")]
    IncorrectTransferAmount,

    #[error(
        "The transferred amount must be not exceed the current chain balance: {current_balance:?}"
    )]
    InsufficientFunding { current_balance: Balance },

    #[error("Failed to load or store balance")]
    StorageError,
}
