// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::Account;
use linera_sdk::{
    base::Amount,
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};

#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct AmmState {
    pub shares: MapView<Account, Amount>,
    pub total_shares_supply: RegisterView<Amount>,
}
