// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::OwnerSpender;
use linera_sdk::{
    linera_base_types::{AccountOwner, U128},
    views::{linera_views, MapView, RootView, ViewStorageContext},
};
use wrapped_fungible::InitialState;

/// Wrapped fungible token state, with balances stored as [`U128`]
/// (raw `u128` in the source ERC-20's decimal scale).
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct WrappedFungibleTokenState {
    pub accounts: MapView<AccountOwner, U128>,
    pub allowances: MapView<OwnerSpender, U128>,
}

#[allow(dead_code)]
impl WrappedFungibleTokenState {
    pub async fn initialize_accounts(&mut self, state: InitialState) {
        for (k, v) in state.accounts {
            if v.0 != 0 {
                self.accounts
                    .insert(&k, v)
                    .expect("Error in insert statement");
            }
        }
    }

    pub async fn balance(&self, account: &AccountOwner) -> Option<U128> {
        self.accounts
            .get(account)
            .await
            .expect("Failure in the retrieval")
    }

    pub async fn balance_or_default(&self, account: &AccountOwner) -> U128 {
        self.balance(account).await.unwrap_or_default()
    }

    pub async fn approve(
        &mut self,
        owner: AccountOwner,
        spender: AccountOwner,
        allowance: U128,
    ) {
        let owner_spender = OwnerSpender::new(owner, spender);
        if allowance.0 == 0 {
            self.allowances
                .remove(&owner_spender)
                .expect("Failed to remove allowance");
            return;
        }
        let total = self
            .allowances
            .get_mut_or_default(&owner_spender)
            .await
            .expect("Failed allowance access");
        *total = allowance;
    }

    pub async fn debit_for_transfer_from(
        &mut self,
        owner: AccountOwner,
        spender: AccountOwner,
        amount: U128,
    ) {
        if amount.0 == 0 {
            return;
        }
        let balance = self
            .accounts
            .get(&owner)
            .await
            .expect("Failed balance access")
            .unwrap_or_default();
        let new_balance = balance.0.checked_sub(amount.0).unwrap_or_else(|| {
            panic!("Source owner {owner} does not have sufficient balance for transfer_from")
        });
        if new_balance == 0 {
            self.accounts
                .remove(&owner)
                .expect("Failed to remove an empty account");
        } else {
            self.accounts
                .insert(&owner, U128(new_balance))
                .expect("Failed insertion operation");
        }
        let owner_spender = OwnerSpender::new(owner, spender);
        let allowance = self
            .allowances
            .get(&owner_spender)
            .await
            .expect("Failed allowance access")
            .unwrap_or_default();
        let new_allowance = allowance.0.checked_sub(amount.0).unwrap_or_else(|| {
            panic!(
                "Spender {spender} does not have sufficient allowance from owner {owner}; \
                 allowance={allowance} amount={amount}"
            )
        });
        if new_allowance == 0 {
            self.allowances
                .remove(&owner_spender)
                .expect("Failed to remove an empty allowance");
        } else {
            self.allowances
                .insert(&owner_spender, U128(new_allowance))
                .expect("Failed insertion operation");
        }
    }

    pub async fn credit(&mut self, account: AccountOwner, amount: U128) {
        if amount.0 == 0 {
            return;
        }
        let balance = self.balance_or_default(&account).await;
        let new_balance = U128(balance.0.saturating_add(amount.0));
        self.accounts
            .insert(&account, new_balance)
            .expect("Failed insert statement");
    }

    pub async fn debit(&mut self, account: AccountOwner, amount: U128) {
        if amount.0 == 0 {
            return;
        }
        let balance = self.balance_or_default(&account).await;
        let new_balance = balance.0.checked_sub(amount.0).unwrap_or_else(|| {
            panic!("Source account {account} does not have sufficient balance for transfer")
        });
        if new_balance == 0 {
            self.accounts
                .remove(&account)
                .expect("Failed to remove an empty account");
        } else {
            self.accounts
                .insert(&account, U128(new_balance))
                .expect("Failed insertion operation");
        }
    }
}
