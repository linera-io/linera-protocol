// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::{DataBlobHash, ModuleId},
    views::{linera_views, MapView, RootView, ViewStorageContext},
};

#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct FormatsRegistryState {
    /// Maps a `ModuleId` to the hash of the data blob that holds its formats.
    pub formats: MapView<ModuleId, DataBlobHash>,
}
