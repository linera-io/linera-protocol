// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::ModuleId,
    views::{linera_views, MapView, RootView, ViewStorageContext},
};

#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct FormatsRegistryState {
    /// Maps a `ModuleId` to the bytes of its registered formats description.
    pub formats: MapView<ModuleId, Vec<u8>>,
}
