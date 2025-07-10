// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/* ABI of the Delegated Fungible Token Example Application */

use async_graphql::{scalar, Request, Response};
pub use linera_sdk::{
    abi::{ContractAbi, ServiceAbi},
    abis::fungible::{Account, InitialState, Parameters},
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount},
};
use serde::{Deserialize, Serialize};

/// An ABI for applications that implement a fungible token.
pub struct DelegatedFungibleTokenAbi;

impl ContractAbi for DelegatedFungibleTokenAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for DelegatedFungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// A delegated fungible operation
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Approve the transfer of tokens
    Approve {
        /// Owner to transfer from
        owner: AccountOwner,
        /// The spender account
        spender: AccountOwner,
        /// Maximum amount to be transferred
        allowance: Amount,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        /// Owner to transfer from
        owner: AccountOwner,
        /// Amount to be transferred
        amount: Amount,
        /// Target account to transfer the amount to
        target_account: Account,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account by using the allowance.
    TransferFrom {
        /// Owner to transfer from
        owner: AccountOwner,
        /// The spender of the amount.
        spender: AccountOwner,
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

/// A message.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Credits the given `target` account, unless the message is bouncing, in which case
    /// `source` is credited instead.
    Credit {
        /// Target account to credit amount to
        target: AccountOwner,
        /// Amount to be credited
        amount: Amount,
        /// Source account to remove amount from
        source: AccountOwner,
    },

    /// Withdraws from the given account and starts a transfer to the target account.
    Withdraw {
        /// Account to withdraw from
        owner: AccountOwner,
        /// Amount to be withdrawn
        amount: Amount,
        /// Target account to transfer amount to
        target_account: Account,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OwnerSpender {
    /// Account to withdraw from
    pub owner: AccountOwner,
    /// Account to do the withdrawing
    pub spender: AccountOwner,
}

scalar!(OwnerSpender);

impl OwnerSpender {
    pub fn new(owner: AccountOwner, spender: AccountOwner) -> Self {
        if owner == spender {
            panic!("owner should be different from spender");
        }
        Self { owner, spender }
    }
}
