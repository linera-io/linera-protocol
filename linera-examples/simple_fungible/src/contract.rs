// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleFungibleToken;
use async_trait::async_trait;
use linera_sdk::{
    contract::system_api, crypto::CryptoError, ensure, ApplicationCallResult, CalleeContext,
    Contract, EffectContext, ExecutionResult, FromBcsBytes, OperationContext, Session,
    SessionCallResult, SessionId, SimpleStateStorage,
};
use serde::{Deserialize, Serialize};
use simple_fungible::{
    types::{AccountOwner, Nonce},
    ApplicationCall, ApplicationTransfer, SessionCall, SignedTransfer, Transfer,
};
use thiserror::Error;

linera_sdk::contract!(SimpleFungibleToken);

#[async_trait]
impl Contract for SimpleFungibleToken {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        self.initialize_accounts(bcs::from_bytes(argument).map_err(Error::InvalidInitialState)?);
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let signed_transfer =
            SignedTransfer::from_bcs_bytes(operation).map_err(Error::InvalidOperation)?;

        self.handle_signed_transfer(signed_transfer)
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let credit = Credit::from_bcs_bytes(effect).map_err(Error::InvalidEffect)?;

        self.credit(credit.destination, credit.amount);

        Ok(ExecutionResult::default())
    }

    async fn call_application(
        &mut self,
        context: &CalleeContext,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let request =
            ApplicationCall::from_bcs_bytes(argument).map_err(Error::InvalidApplicationCall)?;
        let mut result = ApplicationCallResult::default();

        match request {
            ApplicationCall::Balance => {
                result.value = self.handle_application_balance(context)?;
            }
            ApplicationCall::Transfer(transfer) => {
                result = self.handle_application_transfer(context, transfer)?;
            }
            ApplicationCall::Delegated(transfer) => {
                result.execution_result = self.handle_signed_transfer(transfer)?;
            }
        }

        Ok(result)
    }

    async fn call_session(
        &mut self,
        _context: &CalleeContext,
        session: Session,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        let request = SessionCall::from_bcs_bytes(argument).map_err(Error::InvalidSessionCall)?;

        match request {
            SessionCall::Balance => self.handle_session_balance(session.data),
            SessionCall::Transfer(transfer) => self.handle_session_transfer(transfer, session.data),
        }
    }
}

impl SimpleFungibleToken {
    /// Handles a signed transfer.
    ///
    /// A signed transfer is either requested through an operation or a delegated application call.
    fn handle_signed_transfer(
        &mut self,
        signed_transfer: SignedTransfer,
    ) -> Result<ExecutionResult, Error> {
        let (source, transfer, nonce) = self.check_signed_transfer(signed_transfer)?;

        self.debit(source, transfer.amount)?;
        self.mark_nonce_as_used(source, nonce);

        Ok(self.finish_transfer(transfer))
    }

    /// Handles an account balance request sent by an application.
    fn handle_application_balance(&mut self, context: &CalleeContext) -> Result<Vec<u8>, Error> {
        let caller = context
            .authenticated_caller_id
            .ok_or(Error::MissingSourceApplication)?;
        let account = AccountOwner::Application(caller);

        let balance = self.balance(&account);
        let balance_bytes = bcs::to_bytes(&balance).expect("Couldn't serialize account balance");

        Ok(balance_bytes)
    }

    /// Handles a transfer requested by an application.
    fn handle_application_transfer(
        &mut self,
        context: &CalleeContext,
        transfer: ApplicationTransfer,
    ) -> Result<ApplicationCallResult, Error> {
        let caller = context
            .authenticated_caller_id
            .ok_or(Error::MissingSourceApplication)?;
        let source = AccountOwner::Application(caller);

        self.debit(source, transfer.amount())?;

        Ok(self.finish_application_transfer(transfer))
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

    /// Handles a session transfer request sent by an application.
    fn handle_session_transfer(
        &mut self,
        transfer: ApplicationTransfer,
        session_data: Vec<u8>,
    ) -> Result<SessionCallResult, Error> {
        let mut balance =
            u128::from_bcs_bytes(&session_data).expect("Session contains corrupt data");

        ensure!(
            balance >= transfer.amount(),
            Error::InsufficientSessionBalance
        );

        balance -= transfer.amount();

        let updated_session = (balance > 0)
            .then(|| bcs::to_bytes(&balance).expect("Serializing a `u128` should not fail"));

        Ok(SessionCallResult {
            inner: self.finish_application_transfer(transfer),
            data: updated_session,
        })
    }

    /// Checks if a signed transfer can be executed.
    ///
    /// If the transfer can be executed, return the source [`AccountOwner`], the [`Transfer`] to be
    /// executed and the [`Nonce`] used to prevent the transfer from being replayed.
    fn check_signed_transfer(
        &self,
        signed_transfer: SignedTransfer,
    ) -> Result<(AccountOwner, Transfer, Nonce), Error> {
        let (source, payload) = signed_transfer.check_signature()?;

        ensure!(
            payload.token_id == system_api::current_application_id(),
            Error::IncorrectTokenId
        );
        ensure!(
            payload.source_chain == system_api::current_chain_id(),
            Error::IncorrectSourceChain
        );
        ensure!(
            payload.nonce >= self.minimum_nonce(&source).ok_or(Error::ReusedNonce)?,
            Error::ReusedNonce
        );

        Ok((source, payload.transfer, payload.nonce))
    }

    /// Credits an account or forward it into a session or another micro-chain.
    fn finish_application_transfer(
        &mut self,
        application_transfer: ApplicationTransfer,
    ) -> ApplicationCallResult {
        let mut result = ApplicationCallResult::default();

        match application_transfer {
            ApplicationTransfer::Static(transfer) => {
                result.execution_result = self.finish_transfer(transfer);
            }
            ApplicationTransfer::Dynamic(amount) => {
                result.create_sessions.push(Self::new_session(amount));
            }
        }

        result
    }

    /// Credits an account or forward it to another micro-chain.
    fn finish_transfer(&mut self, transfer: Transfer) -> ExecutionResult {
        if transfer.destination_chain == system_api::current_chain_id() {
            self.credit(transfer.destination_account, transfer.amount);
            ExecutionResult::default()
        } else {
            ExecutionResult::default()
                .with_effect(transfer.destination_chain, &Credit::from(transfer))
        }
    }

    /// Creates a new session with the specified `amount` of tokens.
    fn new_session(amount: u128) -> Session {
        Session {
            kind: 0,
            data: bcs::to_bytes(&amount).expect("Serializing a `u128` should not fail"),
        }
    }
}

/// The credit effect.
#[derive(Deserialize, Serialize)]
pub struct Credit {
    destination: AccountOwner,
    amount: u128,
}

impl From<Transfer> for Credit {
    fn from(transfer: Transfer) -> Self {
        Credit {
            destination: transfer.destination_account,
            amount: transfer.amount,
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

    /// Incorrect signature for transfer.
    #[error("Operation does not have a valid signature")]
    IncorrectSignature(#[from] CryptoError),

    /// Invalid serialized [`Credit`].
    #[error("Effect is not a valid serialized credit operation")]
    InvalidEffect(#[source] bcs::Error),

    /// Cross-application call without a source application ID.
    #[error("Applications must identify themselves to perform transfers")]
    MissingSourceApplication,

    /// Invalid serialized [`ApplicationCall`].
    #[error("Cross-application call argument is not a valid request")]
    InvalidApplicationCall(#[source] bcs::Error),

    /// Invalid serialized [`SessionCall`].
    #[error("Cross-application session call argument is not a valid request")]
    InvalidSessionCall(#[source] bcs::Error),

    /// Incorrect token ID in operation.
    #[error("Operation attempts to transfer the incorrect token")]
    IncorrectTokenId,

    /// Incorrect source chain ID in operation.
    #[error("Operation is not valid on the current chain")]
    IncorrectSourceChain,

    /// Attempt to reuse a nonce.
    #[error("Operation uses a unique transaction number (nonce) that was previously used")]
    ReusedNonce,

    /// Insufficient balance in source account.
    #[error("Source account does not have sufficient balance for transfer")]
    InsufficientBalance(#[from] state::InsufficientBalanceError),

    /// Insufficient balance in session.
    #[error("Session does not have sufficient balance for transfer")]
    InsufficientSessionBalance,

    /// Application doesn't support any cross-application sessions.
    #[error("Application doesn't support any cross-application sessions")]
    SessionsNotSupported,
}
