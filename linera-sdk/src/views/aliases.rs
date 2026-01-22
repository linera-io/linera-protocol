// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Aliases for views using the [`ViewStorageContext`].

use super::ViewStorageContext;

/// An alias to [`linera_views::sync_view::collection_view::SyncByteCollectionView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncByteCollectionView<V> =
    linera_views::sync_view::collection_view::SyncByteCollectionView<ViewStorageContext, V>;

/// An alias to [`linera_views::sync_view::map_view::SyncByteMapView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncByteMapView<V> =
    linera_views::sync_view::map_view::SyncByteMapView<ViewStorageContext, V>;

/// An alias to [`linera_views::sync_view::set_view::SyncByteSetView`] that uses the WebAssembly-specific
/// [`ViewStorageContext`].
pub type SyncByteSetView = linera_views::sync_view::set_view::SyncByteSetView<ViewStorageContext>;

/// An alias to [`linera_views::sync_view::collection_view::SyncCollectionView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncCollectionView<K, V> =
    linera_views::sync_view::collection_view::SyncCollectionView<ViewStorageContext, K, V>;

/// An alias to [`linera_views::sync_view::collection_view::SyncCustomCollectionView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncCustomCollectionView<K, V> =
    linera_views::sync_view::collection_view::SyncCustomCollectionView<ViewStorageContext, K, V>;

/// An alias to [`linera_views::sync_view::map_view::SyncCustomMapView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncCustomMapView<K, V> =
    linera_views::sync_view::map_view::SyncCustomMapView<ViewStorageContext, K, V>;

/// An alias to [`linera_views::sync_view::set_view::SyncCustomSetView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncCustomSetView<W> =
    linera_views::sync_view::set_view::SyncCustomSetView<ViewStorageContext, W>;

/// An alias to [`linera_views::sync_view::log_view::SyncLogView`] that uses the WebAssembly-specific
/// [`ViewStorageContext`].
pub type SyncLogView<T> = linera_views::sync_view::log_view::SyncLogView<ViewStorageContext, T>;

/// An alias to [`linera_views::sync_view::map_view::SyncMapView`] that uses the WebAssembly-specific
/// [`ViewStorageContext`].
pub type SyncMapView<K, V> = linera_views::sync_view::map_view::SyncMapView<ViewStorageContext, K, V>;

/// An alias to [`linera_views::sync_view::queue_view::SyncQueueView`] that uses the WebAssembly-specific
/// [`ViewStorageContext`].
pub type SyncQueueView<T> =
    linera_views::sync_view::queue_view::SyncQueueView<ViewStorageContext, T>;

/// An alias to [`linera_views::sync_view::collection_view::SyncReadGuardedView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncReadGuardedView<'a, W> =
    linera_views::sync_view::collection_view::SyncReadGuardedView<'a, W>;

/// An alias to [`linera_views::sync_view::register_view::SyncRegisterView`] that uses the
/// WebAssembly-specific [`ViewStorageContext`].
pub type SyncRegisterView<T> =
    linera_views::sync_view::register_view::SyncRegisterView<ViewStorageContext, T>;

/// An alias to [`linera_views::sync_view::set_view::SyncSetView`] that uses the WebAssembly-specific
/// [`ViewStorageContext`].
pub type SyncSetView<W> = linera_views::sync_view::set_view::SyncSetView<ViewStorageContext, W>;
