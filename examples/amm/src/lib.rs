use std::convert::Infallible;

use async_graphql::{scalar, Request, Response};
use fungible::AccountOwner;
use linera_sdk::base::{ArithmeticError, ContractAbi, ServiceAbi};
use linera_views::views::ViewError;
use matching_engine::Parameters;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub struct AmmAbi;

impl ContractAbi for AmmAbi {
    type InitializationArgument = ();
    type Parameters = Parameters;
    type Operation = Operation;
    type ApplicationCall = ApplicationCall;
    type Message = Message;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for AmmAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = Parameters;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationType {
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        output_token_idx: u32,
        input_amount: u64,
    },
    AddLiquidity {
        owner: AccountOwner,
        token0_amount: u64,
        token1_amount: u64,
    },
    RemoveLiquidity {
        owner: AccountOwner,
        input_token_idx: u32,
        other_token_idx: u32,
        input_amount: u64,
    },
}

scalar!(OperationType);

/// Operations that can be sent to the application.
#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    ExecuteOperation { operation: OperationType },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    ExecuteOperation { operation: OperationType },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ApplicationCall {
    ExecuteOperation { operation: OperationType },
}

pub fn calculate_output_amount(
    input_amount: u64,
    input_pool_balance: u64,
    output_pool_balance: u64,
) -> Result<u64, AmmError> {
    if input_pool_balance == 0 || output_pool_balance == 0 {
        return Err(AmmError::InvalidPoolBalanceError);
    }

    let numerator = input_amount * output_pool_balance;
    let denominator = input_pool_balance + input_amount;

    if denominator == 0 {
        return Err(AmmError::DivisionByZero);
    }

    let output_amount = numerator / denominator;

    Ok(output_amount)
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AmmError {
    #[error("Insufficient liquidity in the pool")]
    InsufficientLiquidityError,

    #[error("Swap tokens must be different")]
    EqualTokensError,

    #[error("Adding liquidity cannot alter balance ratio")]
    BalanceRatioAlteredError,

    #[error("Invalid pool balance")]
    InvalidPoolBalanceError,

    #[error("Token not found in the pool")]
    TokenNotFoundInPoolError,

    #[error("AMM application doesn't support any cross-chain messages")]
    MessagesNotSupported,

    #[error("AMM application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error("AMM application doesn't support any application calls")]
    ApplicationCallsNotSupported,

    #[error("Cannot divide by zero")]
    DivisionByZero,

    #[error("Action can only be executed on the chain that created the AMM")]
    AmmChainOnly,

    #[error("You can't add liquidity with zero tokens")]
    NoZeroAmounts,

    #[error("Invalid token index")]
    InvalidTokenIdx,

    /// Invalid query.
    #[error("Invalid query")]
    InvalidQuery(#[from] serde_json::Error),

    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    Infallible(#[from] Infallible),
}
