use std::convert::Infallible;

use async_graphql::{scalar, Request, Response};
use fungible::AccountOwner;
use linera_sdk::base::{Amount, ArithmeticError, ContractAbi, ServiceAbi};
use linera_views::views::ViewError;
pub use matching_engine::Parameters;
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
    // TODO(#969): Need to also implement Swap Bids here
    /// Swap operation
    /// Given an input token idx (can be 0 or 1), and an input amount,
    /// Swap that token amount for an amount of the other token,
    /// calculated based on the current AMM ratio
    /// Owner here is the user executing the Swap
    Swap {
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    },
    /// Add liquidity operation
    /// Given a maximum token0 and token1 amount that you're willing to add,
    /// add liquidity to the AMM such that you'll be adding AT MOST
    /// `max_token0_amount` of token0, and `max_token1_amount` of token1,
    /// which will be calculated based on the current AMM ratio
    /// Owner here is the user adding liquidity, which currently can only
    /// be a chain owner
    AddLiquidity {
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    },
    /// Remove liquidity operation
    /// Given a token idx of the token you'd like to remove (can be 0 or 1),
    /// and an amount of that token that you'd like to remove, we'll calculate
    /// how much of the other token will also be removed, which will be calculated
    /// based on the current AMM ratio. Then remove the amounts from both tokens
    /// as a removal of liquidity
    /// Owner here is the user removing liquidity, which currently can only
    /// be a chain owner
    RemoveLiquidity {
        owner: AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
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
