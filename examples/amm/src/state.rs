// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::{Account, Amount},
    views::{linera_views, SyncMapView, SyncRegisterView, SyncView, ViewStorageContext},
};

#[derive(SyncView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct AmmState {
    pub shares: SyncMapView<Account, Amount>,
    pub total_shares_supply: SyncRegisterView<Amount>,
}
