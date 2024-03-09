// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../README.md")]

use async_graphql::{scalar, InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    base::{AccountOwner, Amount, ChainId, ContractAbi, ServiceAbi},
    graphql::GraphQLMutationRoot,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
use {
    async_graphql::InputType,
    futures::{stream, StreamExt},
    linera_sdk::{
        base::{ApplicationId, BytecodeId},
        test::{ActiveChain, TestValidator},
    },
};

pub struct FungibleTokenAbi;

impl ContractAbi for FungibleTokenAbi {
    type InitializationArgument = InitialState;
    type Parameters = Parameters;
    type ApplicationCall = ApplicationCall;
    type Operation = Operation;
    type Message = Message;
    type SessionCall = SessionCall;
    type Response = FungibleResponse;
    type SessionState = Amount;
}

impl ServiceAbi for FungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = Parameters;
}

/// An operation.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
    /// Same as `Transfer` but the source account may be remote. Depending on its
    /// configuration, the target chain may take time or refuse to process
    /// the message.
    Claim {
        source_account: Account,
        amount: Amount,
        target_account: Account,
    },
}

/// A message.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Credits the given `target` account, unless the message is bouncing, in which case
    /// `source` is credited instead.
    Credit {
        target: AccountOwner,
        amount: Amount,
        source: AccountOwner,
    },

    /// Withdraws from the given account and starts a transfer to the target account.
    Withdraw {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
}

/// A cross-application call.
#[derive(Debug, Deserialize, Serialize)]
pub enum ApplicationCall {
    /// Requests an account balance.
    Balance { owner: AccountOwner },
    /// Transfers tokens from an account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        destination: Destination,
    },
    /// Same as transfer but the source account may be remote.
    Claim {
        source_account: Account,
        amount: Amount,
        target_account: Account,
    },
    /// Requests this fungible token's ticker symbol.
    TickerSymbol,
}

/// A cross-application call into a session.
#[derive(Debug, Deserialize, Serialize)]
pub enum SessionCall {
    /// Requests the session's balance.
    Balance,
    /// Transfers the given `amount` from the session to the given `destination`. If the
    /// destination is an account on a remote chain, any bouncing message will credit the
    /// local account for the same owner.
    Transfer {
        amount: Amount,
        destination: Destination,
    },
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub enum FungibleResponse {
    #[default]
    Ok,
    Balance(Amount),
    TickerSymbol(String),
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, Amount>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Parameters {
    pub ticker_symbol: String,
}

impl Parameters {
    pub fn new(ticker_symbol: &str) -> Self {
        let ticker_symbol = ticker_symbol.to_string();
        Self { ticker_symbol }
    }
}

/// An account.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    SimpleObject,
    InputObject,
)]
pub struct Account {
    pub chain_id: ChainId,
    pub owner: AccountOwner,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Destination {
    Account(Account),
    NewSession,
}

scalar!(Destination);

/// A builder type for constructing the initial state of the application.
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    account_balances: BTreeMap<AccountOwner, Amount>,
}

impl InitialStateBuilder {
    /// Adds an account to the initial state of the application.
    pub fn with_account(mut self, account: AccountOwner, balance: impl Into<Amount>) -> Self {
        self.account_balances.insert(account, balance.into());
        self
    }

    /// Returns the serialized initial state of the application, ready to used as the
    /// initialization argument.
    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}

#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
impl FungibleTokenAbi {
    /// Creates a fungible token application and distributes `initial_amounts` to new individual
    /// chains.
    pub async fn create_with_accounts(
        validator: &TestValidator,
        bytecode_id: BytecodeId<Self>,
        initial_amounts: impl IntoIterator<Item = Amount>,
    ) -> (
        ApplicationId<Self>,
        Vec<(ActiveChain, AccountOwner, Amount)>,
    ) {
        let mut token_chain = validator.new_chain().await;
        let mut initial_state = InitialStateBuilder::default();

        let accounts = stream::iter(initial_amounts)
            .then(|initial_amount| async move {
                let chain = validator.new_chain().await;
                let account = AccountOwner::from(chain.public_key());

                (chain, account, initial_amount)
            })
            .collect::<Vec<_>>()
            .await;

        for (_chain, account, initial_amount) in &accounts {
            initial_state = initial_state.with_account(*account, *initial_amount);
        }

        let params = Parameters::new("FUN");
        let application_id = token_chain
            .create_application(bytecode_id, params, initial_state.build(), vec![])
            .await;

        for (chain, account, initial_amount) in &accounts {
            chain.register_application(application_id).await;

            let claim_messages = chain
                .add_block(|block| {
                    block.with_operation(
                        application_id,
                        Operation::Claim {
                            source_account: Account {
                                chain_id: token_chain.id(),
                                owner: *account,
                            },
                            amount: *initial_amount,
                            target_account: Account {
                                chain_id: chain.id(),
                                owner: *account,
                            },
                        },
                    );
                })
                .await;

            assert_eq!(claim_messages.len(), 2);

            let transfer_messages = token_chain
                .add_block(|block| {
                    block.with_incoming_message(claim_messages[1]);
                })
                .await;

            assert_eq!(transfer_messages.len(), 2);

            chain
                .add_block(|block| {
                    block.with_incoming_message(transfer_messages[1]);
                })
                .await;
        }

        (application_id, accounts)
    }

    /// Queries the balance of an account owned by `account_owner` on a specific `chain`.
    pub async fn query_account(
        application_id: ApplicationId<FungibleTokenAbi>,
        chain: &ActiveChain,
        account_owner: AccountOwner,
    ) -> Option<Amount> {
        let query = format!(
            "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
            account_owner.to_value()
        );
        let response = chain.graphql_query(application_id, query).await;
        let balance = response.pointer("/accounts/entry/value")?.as_str()?;

        Some(
            balance
                .parse()
                .expect("Account balance cannot be parsed as a number"),
        )
    }
}
