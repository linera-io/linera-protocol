// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::FungibleToken;
use async_trait::async_trait;
use fungible::{
    Account, AccountOwner, ApplicationCall, Destination, Effect, Operation, SessionCall,
};
use linera_sdk::{
    base::{Amount, ApplicationId, Owner, SessionId},
    contract::{system_api, system_api::WasmContext},
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult, FromBcsBytes,
    OperationContext, Session, SessionCallResult, ViewStateStorage,
};
use thiserror::Error;

pub type WritableFungibleToken = FungibleToken<WasmContext>;
linera_sdk::contract!(WritableFungibleToken);

#[async_trait]
impl Contract for FungibleToken<WasmContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let accounts = bcs::from_bytes(argument).map_err(Error::InvalidInitialState)?;
        self.initialize_accounts(accounts).await;
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let operation = Operation::from_bcs_bytes(operation).map_err(Error::InvalidOperation)?;
        let Operation::Transfer {
            owner,
            amount,
            target_account,
        } = operation;
        Self::check_account_authentication(None, context.authenticated_signer, owner)?;
        self.debit(owner, amount).await?;
        Ok(self
            .finish_transfer_to_account(amount, target_account)
            .await)
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let effect = Effect::from_bcs_bytes(effect).map_err(Error::InvalidEffect)?;
        let Effect::Credit { owner, amount } = effect;
        self.credit(owner, amount).await;
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let request =
            ApplicationCall::from_bcs_bytes(argument).map_err(Error::InvalidApplicationCall)?;
        match request {
            ApplicationCall::Balance { owner } => {
                let mut result = ApplicationCallResult::default();
                let balance = self.balance(&owner).await;
                result.value =
                    bcs::to_bytes(&balance).expect("Serializing amounts should not fail");
                Ok(result)
            }
            ApplicationCall::Transfer {
                owner,
                amount,
                destination,
            } => {
                Self::check_account_authentication(
                    context.authenticated_caller_id,
                    context.authenticated_signer,
                    owner,
                )?;
                self.debit(owner, amount).await?;
                Ok(self
                    .finish_transfer_to_destination(amount, destination)
                    .await)
            }
        }
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        session: Session,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        let request = SessionCall::from_bcs_bytes(argument).map_err(Error::InvalidSessionCall)?;
        match request {
            SessionCall::Balance => self.handle_session_balance(session.data),
            SessionCall::Transfer {
                amount,
                destination,
            } => {
                self.handle_session_transfer(session.data, amount, destination)
                    .await
            }
        }
    }
}

impl FungibleToken<WasmContext> {
    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(
        authenticated_application_id: Option<ApplicationId>,
        authenticated_signer: Option<Owner>,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) if authenticated_signer == Some(address) => Ok(()),
            AccountOwner::Application(id) if authenticated_application_id == Some(id) => Ok(()),
            _ => Err(Error::IncorrectAuthentication),
        }
    }

    /// Handles a session balance request sent by an application.
    fn handle_session_balance(&self, session_data: Vec<u8>) -> Result<SessionCallResult, Error> {
        let balance_bytes = session_data.clone();
        let application_call_result = ApplicationCallResult {
            value: balance_bytes,
            ..Default::default()
        };
        let session_call_result = SessionCallResult {
            inner: application_call_result,
            data: Some(session_data),
        };
        Ok(session_call_result)
    }

    /// Handles a transfer from a session.
    async fn handle_session_transfer(
        &mut self,
        session_data: Vec<u8>,
        amount: Amount,
        destination: Destination,
    ) -> Result<SessionCallResult, Error> {
        let mut balance =
            Amount::from_bcs_bytes(&session_data).expect("Session contains corrupt data");
        balance
            .try_sub_assign(amount)
            .map_err(|_| Error::InsufficientSessionBalance)?;

        let updated_session = (balance > Amount::zero())
            .then(|| bcs::to_bytes(&balance).expect("Serializing amounts should not fail"));

        Ok(SessionCallResult {
            inner: self
                .finish_transfer_to_destination(amount, destination)
                .await,
            data: updated_session,
        })
    }

    /// Execute the final step of a transfer where the tokens are sent to the destination.
    async fn finish_transfer_to_destination(
        &mut self,
        amount: Amount,
        destination: Destination,
    ) -> ApplicationCallResult {
        let mut result = ApplicationCallResult::default();
        match destination {
            Destination::Account(account) => {
                result.execution_result = self.finish_transfer_to_account(amount, account).await;
            }
            Destination::NewSession => {
                result.create_sessions.push(Self::new_session(amount));
            }
        }
        result
    }

    /// Execute the final step of a transfer where the tokens are sent to the destination.
    async fn finish_transfer_to_account(
        &mut self,
        amount: Amount,
        account: Account,
    ) -> ExecutionResult {
        if account.chain_id == system_api::current_chain_id() {
            self.credit(account.owner, amount).await;
            ExecutionResult::default()
        } else {
            let effect = Effect::Credit {
                owner: account.owner,
                amount,
            };
            ExecutionResult::default().with_effect(account.chain_id, &effect)
        }
    }

    /// Creates a new session with the specified `amount` of tokens.
    fn new_session(amount: Amount) -> Session {
        Session {
            kind: 0,
            data: bcs::to_bytes(&amount).expect("Serializing amounts should not fail"),
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid serialized initial state.
    #[error("Serialized initial state is invalid")]
    InvalidInitialState(#[source] bcs::Error),

    /// Invalid serialized [`SignedTransfer`].
    #[error("Operation is not a valid serialized signed transfer")]
    InvalidOperation(#[source] bcs::Error),

    /// Invalid serialized [`Credit`].
    #[error("Effect is not a valid serialized credit operation")]
    InvalidEffect(#[source] bcs::Error),

    /// Invalid serialized [`ApplicationCall`].
    #[error("Cross-application call argument is not a valid request")]
    InvalidApplicationCall(#[source] bcs::Error),

    /// Invalid serialized [`SessionCall`].
    #[error("Cross-application session call argument is not a valid request")]
    InvalidSessionCall(#[source] bcs::Error),

    /// Insufficient balance in source account.
    #[error("Source account does not have sufficient balance for transfer")]
    InsufficientBalance(#[from] state::InsufficientBalanceError),

    /// Insufficient balance in session.
    #[error("Session does not have sufficient balance for transfer")]
    InsufficientSessionBalance,

    /// Requested transfer does not have permission on this account.
    #[error("The requested transfer is not correctly authenticated.")]
    IncorrectAuthentication,
}
