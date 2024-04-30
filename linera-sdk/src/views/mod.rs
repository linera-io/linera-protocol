// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for using [`linera_views`] to store application state.

mod aliases;
mod conversions_to_wit;
#[cfg(not(with_testing))]
mod system_api;
#[cfg(with_testing)]
mod test_context;

pub use linera_views::{
    self,
    common::CustomSerialize,
    views::{RootView, View, ViewError},
};

pub use self::aliases::{
    ByteCollectionView, ByteMapView, ByteSetView, CollectionView, CustomCollectionView,
    CustomMapView, CustomSetView, LogView, MapView, QueueView, ReadGuardedView, RegisterView,
    SetView,
};
#[cfg(not(with_testing))]
pub use self::system_api::ViewStorageContext;
#[cfg(with_testing)]
pub use self::test_context::ViewStorageContext;
