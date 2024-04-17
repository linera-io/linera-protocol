// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for using [`linera_views`] to store application state.

mod aliases;
mod conversions_to_wit;
#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(not(target_arch = "wasm32"), path = "system_api_stubs.rs")]
mod system_api;
#[cfg(target_arch = "wasm32")]
mod system_api;

pub use linera_views::{
    self,
    common::CustomSerialize,
    views::{RootView, View, ViewError},
};

pub(crate) use self::system_api::AppStateStore;
pub use self::{
    aliases::{
        ByteCollectionView, ByteMapView, ByteSetView, CollectionView, CustomCollectionView,
        CustomMapView, CustomSetView, LogView, MapView, QueueView, ReadGuardedView, RegisterView,
        SetView,
    },
    system_api::ViewStorageContext,
};
