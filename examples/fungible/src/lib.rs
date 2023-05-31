// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
    type Effect = Effect;
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

/// An effect.
#[derive(Debug, Deserialize, Serialize)]
pub enum Effect {
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

            let claim_effects = chain
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

            assert_eq!(claim_effects.len(), 2);

            let transfer_effects = token_chain
                .add_block(|block| {
                    block.with_incoming_message(claim_effects[1]);
                })
                .await;

            assert_eq!(transfer_effects.len(), 2);

            chain
                .add_block(|block| {
                    block.with_incoming_message(transfer_effects[1]);
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

        let value = chain
            .query(application_id, Request::from(query))
            .await
            .data
            .into_json()
            .ok()?;

        let balance = value.as_object()?.get("accounts")?.as_str()?;

        Some(
            balance
                .parse()
                .expect("Account balance cannot be parsed as a number"),
        )
    }
}
