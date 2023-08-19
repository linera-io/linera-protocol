use std::convert::Infallible;

use async_graphql::{scalar, Request, Response};
use fungible::AccountOwner;
use linera_sdk::base::{Amount, ArithmeticError, ContractAbi, ServiceAbi};
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

/// Operations that can be sent to the application.
#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
    AddLiquidity {
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    },
    RemoveLiquidity {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
}

scalar!(Operation);

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ApplicationCall {
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AmmError {
    #[error("Insufficient liquidity in the pool")]
    InsufficientLiquidityError,

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

    #[error("Action can only be executed on the chain that created the AMM")]
    AmmChainOnly,

    #[error("You can't add liquidity with zero tokens")]
    NoZeroAmounts,

    #[error("Invalid token index")]
    InvalidTokenIdx,

    #[error("Can't remove liquidity from a remote chain")]
    RemovingLiquidityFromRemoteChain,

    #[error("Can't add liquidity from a remote chain")]
    AddingLiquidityFromRemoteChain,

    #[error("Can't swap locally")]
    SwappingLocally,

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
