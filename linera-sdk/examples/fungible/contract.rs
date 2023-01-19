// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;

use self::state::{AccountOwner, ApplicationState, FungibleToken, Nonce};
use async_trait::async_trait;
use linera_sdk::{
    crypto::{BcsSignable, CryptoError, PublicKey, Signature},
    ensure, ApplicationCallResult, ApplicationId, CalleeContext, ChainId, Contract, EffectContext,
    ExecutionResult, FromBcsBytes, OperationContext, Session, SessionCallResult, SessionId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[async_trait]
impl Contract for FungibleToken {
    type Error = Error;

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
        let request = ApplicationCall::from_bcs_bytes(argument).map_err(Error::InvalidArgument)?;
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
        let transfer =
            ApplicationTransfer::from_bcs_bytes(argument).map_err(Error::InvalidArgument)?;
        let mut balance =
            u128::from_bcs_bytes(&session.data).expect("Session contains corrupt data");

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
}

impl FungibleToken {
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
            payload.token_id == Self::current_application_id(),
            Error::IncorrectTokenId
        );
        ensure!(
            payload.source_chain == Self::current_chain_id(),
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
        if transfer.destination_chain == Self::current_chain_id() {
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

/// The transfer operation.
#[derive(Deserialize, Serialize)]
pub struct SignedTransfer {
    source: PublicKey,
    signature: Signature,
    payload: SignedTransferPayload,
}

/// The payload to be signed for a signed transfer.
///
/// Contains extra meta-data in to be included in signature to ensure that the transfer can't be
/// replayed:
///
/// - on the same chain
/// - on different chains
/// - on different tokens
#[derive(Debug, Deserialize, Serialize)]
pub struct SignedTransferPayload {
    token_id: ApplicationId,
    source_chain: ChainId,
    nonce: Nonce,
    transfer: Transfer,
}

/// A cross-application call.
#[derive(Deserialize, Serialize)]
pub enum ApplicationCall {
    /// A request for the application's account balance.
    Balance,
    /// A transfer from the application's account.
    Transfer(ApplicationTransfer),
    /// A signed transfer operation delegated to the application.
    Delegated(SignedTransfer),
}

/// A cross-application transfer request.
#[derive(Deserialize, Serialize)]
pub enum ApplicationTransfer {
    /// A static transfer to a specific destination.
    Static(Transfer),
    /// A dynamic transfer into a session, that can then be credited to destinations later.
    Dynamic(u128),
}

impl ApplicationTransfer {
    /// The amount of tokens to be transfered.
    pub fn amount(&self) -> u128 {
        match self {
            ApplicationTransfer::Static(transfer) => transfer.amount,
            ApplicationTransfer::Dynamic(amount) => *amount,
        }
    }
}

/// A transfer payload.
#[derive(Debug, Deserialize, Serialize)]
pub struct Transfer {
    destination_account: AccountOwner,
    destination_chain: ChainId,
    amount: u128,
}

impl SignedTransfer {
    /// Checks that the [`SignedTransfer`] is correctly signed.
    ///
    /// If correctly signed, returns the source of the transfer and the [`SignedTransferPayload`].
    pub fn check_signature(self) -> Result<(AccountOwner, SignedTransferPayload), Error> {
        self.signature.check(&self.payload, self.source)?;

        Ok((AccountOwner::Key(self.source), self.payload))
    }
}

impl BcsSignable for SignedTransferPayload {}

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

    /// Invalid serialized [`Transfer`].
    #[error("Cross-application call argument is not a valid serialized transfer")]
    InvalidArgument(#[source] bcs::Error),

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

#[path = "../boilerplate/contract/mod.rs"]
mod boilerplate;
