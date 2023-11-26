// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for using [`linera_views`] to store application state.

mod system_api;

pub use self::system_api::ViewStorageContext;
pub use linera_views::*;

// Import the views system interface.
wit_bindgen_guest_rust::import!("view_system_api.wit");

/// An alias to [`collection_view::ByteCollectionView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type ByteCollectionView<V> = views::collection_view::ByteCollectionView<ViewStorageContext, V>;

/// An alias to [`map_view::ByteMapView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type ByteMapView<V> = views::map_view::ByteMapView<ViewStorageContext, V>;

/// An alias to [`set_view::ByteSetView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type ByteSetView = views::set_view::ByteSetView<ViewStorageContext>;

/// An alias to [`collection_view::CollectionView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type CollectionView<K, V> = views::collection_view::CollectionView<ViewStorageContext, K, V>;

/// An alias to [`collection_view::CustomCollectionView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type CustomCollectionView<K, V> =
    views::collection_view::CustomCollectionView<ViewStorageContext, K, V>;

/// An alias to [`map_view::CustomMapView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type CustomMapView<K, V> = views::map_view::CustomMapView<ViewStorageContext, K, V>;

/// An alias to [`set_view::CustomSetView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type CustomSetView<W> = views::set_view::CustomSetView<ViewStorageContext, W>;

/// An alias to [`log_view::LogView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type LogView<T> = views::log_view::LogView<ViewStorageContext, T>;

/// An alias to [`map_view::MapView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type MapView<K, V> = views::map_view::MapView<ViewStorageContext, K, V>;

/// An alias to [`queue_view::QueueView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type QueueView<T> = views::queue_view::QueueView<ViewStorageContext, T>;

/// An alias to [`collection_view::ReadGuardedView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type ReadGuardedView<'a, W> = views::collection_view::ReadGuardedView<'a, W>;

/// An alias to [`register_view::RegisterView`] that uses the WebAssembly specific
/// [`ViewStorageContext`].
pub type RegisterView<T> = views::register_view::RegisterView<ViewStorageContext, T>;

/// An alias to [`set_view::SetView`] that uses the WebAssembly specific [`ViewStorageContext`].
pub type SetView<W> = views::set_view::SetView<ViewStorageContext, W>;
