// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use linera_sdk::{
    linera_base_types::{AccountOwner, DataBlobHash, ModuleId},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};

#[derive(RootView, async_graphql::SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct FormatsRegistryState {
    /// Maps a `ModuleId` to the hash of the data blob holding its registered formats
    /// description. Exposed directly so callers can test membership or fetch the
    /// stored `DataBlobHash` (e.g. `formats { entry(key: "<MODULE_ID>") { value } }`)
    /// without downloading the blob; the service's `read` query additionally returns
    /// the decoded blob contents.
    pub formats: MapView<ModuleId, DataBlobHash>,
    /// The admin accounts authorized to run admin commands and remote writes.
    /// `None` means no admin set has been configured yet, in which case only the
    /// creation chain can mutate the registry.
    pub admins: RegisterView<Option<HashSet<AccountOwner>>>,
}
