// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for using [`linera_views`] to store application state.

mod aliases;
#[cfg(with_testing)]
mod mock_key_value_store;
mod system_api;

pub use linera_views::{
    self,
    common::CustomSerialize,
    views::{RootView, View, ViewError},
};

pub use self::{
    aliases::{
        ByteCollectionView, ByteMapView, ByteSetView, CollectionView, CustomCollectionView,
        CustomMapView, CustomSetView, LogView, MapView, QueueView, ReadGuardedView, RegisterView,
        SetView,
    },
    system_api::{KeyValueStore, ViewStorageContext},
};
