// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Native Fungible Token Example Application */

use async_graphql::{Request, Response, SimpleObject};
use linera_sdk::{
    abi::{ContractAbi, ServiceAbi},
    abis::fungible::{Account, FungibleResponse},
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount},
};
use serde::{Deserialize, Serialize};

pub const TICKER_SYMBOL: &str = "NAT";

#[derive(Deserialize, SimpleObject)]
pub struct AccountEntry {
    pub key: AccountOwner,
    pub value: Amount,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Notify,
}

/// An operation
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Requests an account balance.
    Balance {
        /// Owner to query the balance for
        owner: AccountOwner,
    },
    /// Requests this fungible token's ticker symbol.
    TickerSymbol,
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        /// Owner to transfer from
        owner: AccountOwner,
        /// Amount to be transferred
        amount: Amount,
        /// Target account to transfer the amount to
        target_account: Account,
    },
    /// Same as `Transfer` but the source account may be remote. Depending on its
    /// configuration, the target chain may take time or refuse to process
    /// the message.
    Claim {
        /// Source account to claim amount from
        source_account: Account,
        /// Amount to be claimed
        amount: Amount,
        /// Target account to claim the amount into
        target_account: Account,
    },
}

/// An ABI for applications that implement a fungible token.
pub struct NativeFungibleTokenAbi;

impl ContractAbi for NativeFungibleTokenAbi {
    type Operation = Operation;
    type Response = FungibleResponse;
}

impl ServiceAbi for NativeFungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}
