// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/* ABI of the Fungible Token Example Application */

use async_graphql::scalar;
pub use linera_sdk::abis::fungible::*;
use linera_sdk::linera_base_types::{Account, AccountOwner, Amount};
use serde::{Deserialize, Serialize};
#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
use {
    async_graphql::InputType,
    futures::{stream, StreamExt},
    linera_sdk::{
        linera_base_types::{ApplicationId, ModuleId},
        test::{ActiveChain, QueryOutcome, TestValidator},
    },
};

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

/// Creates a fungible token application and distributes `initial_amounts` to new individual
/// chains.
#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
pub async fn create_with_accounts(
    validator: &TestValidator,
    module_id: ModuleId<FungibleTokenAbi, Parameters, InitialState>,
    initial_amounts: impl IntoIterator<Item = Amount>,
) -> (
    ApplicationId<FungibleTokenAbi>,
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
        .create_application(module_id, params, initial_state.build(), vec![])
        .await;

    for (chain, account, initial_amount) in &accounts {
        let claim_certificate = chain
            .add_block(|block| {
                block.with_operation(
                    application_id,
                    FungibleOperation::Claim {
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

        assert_eq!(claim_certificate.outgoing_message_count(), 1);

        let transfer_certificate = token_chain
            .add_block(|block| {
                block.with_messages_from(&claim_certificate);
            })
            .await;

        assert_eq!(transfer_certificate.outgoing_message_count(), 1);

        chain
            .add_block(|block| {
                block.with_messages_from(&transfer_certificate);
            })
            .await;
    }

    (application_id, accounts)
}

/// Queries the balance of an account owned by `account_owner` on a specific `chain`.
#[cfg(all(any(test, feature = "test"), not(target_arch = "wasm32")))]
pub async fn query_account(
    application_id: ApplicationId<FungibleTokenAbi>,
    chain: &ActiveChain,
    account_owner: AccountOwner,
) -> Option<Amount> {
    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        account_owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, query).await;
    let balance = response.pointer("/accounts/entry/value")?.as_str()?;

    Some(
        balance
            .parse()
            .expect("Account balance cannot be parsed as a number"),
    )
}
