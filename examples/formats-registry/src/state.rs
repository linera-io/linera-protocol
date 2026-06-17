// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use linera_sdk::{
    linera_base_types::{AccountOwner, ModuleId},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};

#[derive(RootView, async_graphql::SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct FormatsRegistryState {
    /// Maps a `ModuleId` to the bytes of its registered formats description.
    pub formats: MapView<ModuleId, Vec<u8>>,
    /// The admin accounts authorized to run admin commands and remote writes.
    /// `None` means no admin set has been configured yet, in which case only the
    /// creation chain can mutate the registry.
    pub admins: RegisterView<Option<HashSet<AccountOwner>>>,
}
