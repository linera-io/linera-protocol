// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::Account;
use linera_sdk::{
    base::Amount,
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};

#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct Amm {
    pub shares: MapView<Account, Amount>,
    pub total_shares_supply: RegisterView<Amount>,
}

#[allow(dead_code)]
impl Amm {
    /// Obtains the current shares for an `account`.
    pub(crate) async fn current_shares_or_default(&self, account: &Account) -> Amount {
        self.shares
            .get(account)
            .await
            .expect("Failure in the retrieval")
            .unwrap_or_default()
    }
}
