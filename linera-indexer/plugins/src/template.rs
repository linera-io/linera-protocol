// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_indexer::{plugin, common::IndexerError};
use linera_views::{
    map_view::MapView,
    register_view::RegisterView,
    views::{RootView, View},
};

// A plugin is centered around a RootView
#[derive(RootView)]
pub struct Template<C> {
    view1: RegisterView<C, String>,
    view2: MapView<C, u32, String>,
    // more views
}

// The `plugin` macro attribute wraps the `Template` in an `Arc<Mutex<Template>>` and does the necessary transformations.
// The `self` attribute always refers to the `Template` struct.
#[plugin]
impl Template<C> {
    // The `register` function is the only required function.
    // It is used to register the wanted information from a HashedCertificateValue
    async fn register(&self, value: &HashedCertificateValue) -> Result<(), IndexerError> {
        // register information
    }

    // Private functions are directly applied to the `Template` struct.
    async fn helper1(&self, ...) -> Result<.., IndexerError> {
        // handle some things on the `Template` struct
    }

    // Public functions are the ones accessible through the GraphQL server.
    pub async fn entrypoint1(&self) -> String {
        self.view1.get()
    }

    pub async fn entrypoint2(&self, key: u32) -> Result<String, IndexerError> {
        Ok(self.view2.get(&key).await?)
    }

    // Other functions are derived to fill the `linera_indexer::plugin::Plugin` trait.
}
