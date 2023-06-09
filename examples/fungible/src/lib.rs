// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Fungible Token Example Application
//!
//! This example application implements fungible tokens. This demonstrates in particular
//! cross-chain messages and how applications are instantiated and auto-deployed.
//!
//! Once this application is built and its bytecode published on a Linera chain, the
//! published bytecode can be used to create multiple application instances, where each
//! instance represents a different fungible token.
//!
//! # How It Works
//!
//! Individual chains have a set of accounts, where each account has an owner and a balance. The
//! same owner can have accounts on multiple chains, with a different balance on each chain. This
//! means that an account's balance is sharded across one or more chains.
//!
//! There are two operations: `Transfer` and `Claim`. `Transfer` sends tokens from an account on the
//! chain where the operation is executed, while `Claim` sends a message from the current chain to
//! another chain in order to transfer tokens from that remote chain.
//!
//! Tokens can be transferred from an account to different destinations, such as:
//!
//! - other accounts on the same chain,
//! - the same account on another chain,
//! - other accounts on other chains,
//! - sessions so that other applications can use some tokens.
//!
//! # Usage
//!
//! ## Setting Up
//!
//! The WebAssembly binaries for the bytecode can be built and published using [steps from the
//! book](https://linera-io.github.io/linera-documentation/getting_started/first_app.html),
//! summarized below.
//!
//! First setup a local network with two wallets, and keep it running in a separate terminal:
//!
//! ```bash
//! ./scripts/run_local.sh
//! ```
//!
//! Compile the `fungible` application WebAssembly binaries, and publish them as an application
//! bytecode:
//!
//! ```bash
//! export LINERA_WALLET="$(realpath target/debug/wallet.json)"
//! export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"
//! export LINERA_WALLET_2="$(realpath target/debug/wallet_2.json)"
//! export LINERA_STORAGE_2="rocksdb:$(dirname "$LINERA_WALLET_2")/linera_2.db"
//!
//! cd examples/fungible && cargo build --release && cd ../..
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-bytecode \
//! examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm
//! ```
//!
//! This will output the new bytecode ID, e.g.:
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000
//! ```
//!
//! ## Creating a Token
//!
//! In order to use the published bytecode to create a token application, the initial state must be
//! specified. This initial state is where the tokens are minted. After the token is created, no
//! additional tokens can be minted and added to the application. The initial state is a JSON string
//! that specifies the accounts that start with tokens.
//!
//! In order to select the accounts to have initial tokens, the command below can be used to list
//! the chains created for the test:
//!
//! ```bash
//! linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" wallet show
//! ```
//!
//! A table will be shown with the chains registered in the wallet and their meta-data. The default
//! chain should be highlighted in green. Each chain has an `Owner` field, and that is what is used
//! for the account.
//!
//! The example below creates a token application where two accounts start with the minted tokens,
//! one with 100 of them and another with 200 of them:
//!
//! ```bash
//! linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" create-application $BYTECODE_ID \
//!     --json-argument '{ "accounts": {
//!         "User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1": "100.",
//!         "User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f": "200."
//!     } }'
//! ```
//!
//! This will output the application ID for the newly created token, e.g.:
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
//! ```
//!
//! ## Using the Token Application
//!
//! Before using the token, a source and target address should be selected. The source address
//! should ideally be on the default chain (used to create the token) and one of the accounts chosen
//! for the initial state, because it will already have some initial tokens to send. The target
//! address should be from a separate wallet due to current technical limitations (`linera service`
//! can only handle one chain per wallet at the same time). To see the available chains in the
//! secondary wallet, use:
//!
//! ```bash
//! linera --storage "$LINERA_STORAGE_2" --wallet "$LINERA_WALLET_2" wallet show
//! ```
//!
//! First, a node service has to be started for each wallet, using two different ports. The
//! `$SOURCE_CHAIN_ID` and `$TARGET_CHAIN_ID` can be left blank to use the default chains from each
//! wallet:
//!
//! ```bash
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" service --port 8080 $SOURCE_CHAIN_ID &
//! linera --wallet "$LINERA_WALLET_2" --storage "$LINERA_STORAGE_2" service --port 8081 $TARGET_CHAIN_ID &
//! ```
//!
//! Then the web frontend has to be started
//!
//! ```bash
//! cd examples/fungible/web-frontend
//! npm install
//! npm start
//! ```
//!
//! The web UI can then be opened by navigating to
//! `http://localhost:3000/$APPLICATION_ID?owner=$SOURCE_ACCOUNT?port=$PORT`, where:
//!
//! - `$APPLICATION_ID` is the token application ID obtained when creating the token
//! - `$SOURCE_ACCOUNT` is the owner of the chosen sender account
//! - `$PORT` is the port the sender wallet service is listening to (`8080` for the sender wallet
//!   and `8081` for the receiver wallet as per the previous commands)
//!
//! Two browser instances can be opened, one for the sender account and one for the receiver
//! account. In the sender account browser, the target chain ID and account can be specified, as
//! well as the amount to send. Once sent, the balance on the receiver account browser should
//! automatically update.

use async_graphql::{scalar, InputObject, Request, Response};
use linera_sdk::base::{Amount, ApplicationId, ChainId, ContractAbi, Owner, ServiceAbi};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::BTreeMap, str::FromStr};
#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
use {
    async_graphql::InputType,
    futures::{stream, StreamExt},
    linera_sdk::{
        base::BytecodeId,
        test::{ActiveChain, TestValidator},
    },
};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct FungibleTokenAbi;

impl ContractAbi for FungibleTokenAbi {
    type InitializationArgument = InitialState;
    type Parameters = ();
    type ApplicationCall = ApplicationCall;
    type Operation = Operation;
    type Message = Message;
    type SessionCall = SessionCall;
    type Response = Amount;
    type SessionState = Amount;
}

impl ServiceAbi for FungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = ();
}

/// An operation.
#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    /// A transfer from a (locally owned) account to a (possibly remote) account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
    /// Same as transfer but the source account may be remote. Depending on its
    /// configuration (see also #464), the target chain may take time or refuse to process
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
    /// Credit the given account.
    Credit { owner: AccountOwner, amount: Amount },

    /// Withdraw from the given account and starts a transfer to the target account.
    Withdraw {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
}

/// A cross-application call.
#[derive(Debug, Deserialize, Serialize)]
pub enum ApplicationCall {
    /// A request for an account balance.
    Balance { owner: AccountOwner },
    /// A transfer from an account.
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
}

/// A cross-application call into a session.
#[derive(Debug, Deserialize, Serialize)]
pub enum SessionCall {
    /// A request for the session's balance.
    Balance,
    /// A transfer from the session.
    Transfer {
        amount: Amount,
        destination: Destination,
    },
}

scalar!(AccountOwner);

/// An account owner.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AccountOwner {
    /// An account owned by a user.
    User(Owner),
    /// An account for an application.
    Application(ApplicationId),
}

impl Serialize for AccountOwner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AccountOwner::User(owner) => {
                let key = format!("User:{}", owner);
                serializer.serialize_str(&key)
            }
            AccountOwner::Application(app_id) => {
                let key = format!("Application:{}", app_id);
                serializer.serialize_str(&key)
            }
        }
    }
}

impl<'de> Deserialize<'de> for AccountOwner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AccountOwnerVisitor;

        impl<'de> serde::de::Visitor<'de> for AccountOwnerVisitor {
            type Value = AccountOwner;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing an AccountOwner")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let parts: Vec<&str> = value.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(Error::custom("string does not contain colon"));
                }

                match parts[0] {
                    "User" => {
                        let owner = Owner::from_str(parts[1]).map_err(|_| {
                            Error::custom(format!(
                                "failed to parse Owner from string: {}",
                                parts[1]
                            ))
                        })?;
                        Ok(AccountOwner::User(owner))
                    }
                    "Application" => {
                        let app_id = ApplicationId::from_str(parts[1]).map_err(|_| {
                            Error::custom(format!(
                                "failed to parse ApplicationId from string: {}",
                                parts[1]
                            ))
                        })?;
                        Ok(AccountOwner::Application(app_id))
                    }
                    _ => Err(Error::unknown_variant(parts[0], &["User", "Application"])),
                }
            }
        }
        deserializer.deserialize_str(AccountOwnerVisitor)
    }
}

impl<T> From<T> for AccountOwner
where
    T: Into<Owner>,
{
    fn from(owner: T) -> Self {
        AccountOwner::User(owner.into())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, Amount>,
}

/// An account.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, InputObject,
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

        let application_id = token_chain
            .create_application(bytecode_id, (), initial_state.build(), vec![])
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
            "query {{ accounts(accountOwner: {}) }}",
            account_owner.to_value()
        );
        let value = chain.graphql_query(application_id, query).await;
        let balance = value.as_object()?.get("accounts")?.as_str()?;

        Some(
            balance
                .parse()
                .expect("Account balance cannot be parsed as a number"),
        )
    }
}
