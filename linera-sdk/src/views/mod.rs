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
    sync_view::{SyncRootView, SyncRootView as RootView, SyncView, SyncView as View},
    ViewError,
};

pub use self::{
    aliases::{
        SyncByteCollectionView, SyncByteMapView, SyncByteSetView, SyncCollectionView,
        SyncCustomCollectionView, SyncCustomMapView, SyncCustomSetView, SyncLogView, SyncMapView,
        SyncQueueView, SyncReadGuardedView, SyncRegisterView, SyncSetView,
    },
    system_api::{KeyValueStore, ViewStorageContext},
};
