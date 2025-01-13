// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! The ABI for the Automated Market Maker (AMM) Example Application */

use async_graphql::{scalar, Request, Response};
use linera_sdk::{
    base::{AccountOwner, Amount, ContractAbi, ServiceAbi},
    graphql::GraphQLMutationRoot,
};
pub use matching_engine::Parameters;
use serde::{Deserialize, Serialize};

pub struct AmmAbi;

impl ContractAbi for AmmAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for AmmAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Operations that can be sent to the application.
#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
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
    /// Remove all added liquidity operation
    /// Remove all the liquidity added by given user, that is remaining in the AMM.
    /// Owner here is the user removing liquidity, which currently can only
    /// be a chain owner
    RemoveAllAddedLiquidity { owner: AccountOwner },
    /// Close this chain, and remove all added liquidity
    /// Requires that this application is authorized to close the chain.
    CloseChain,
}

scalar!(Operation);

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
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
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    },
    RemoveAllAddedLiquidity {
        owner: AccountOwner,
    },
}
