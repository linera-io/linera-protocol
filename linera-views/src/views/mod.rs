// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The following views implement the `View` trait:
//! * `RegisterView` implements the storing of a single data.
//! * `LogView` implements a log, which is a list of entries that can be expanded.
//! * `QueueView` implements a queue, which is a list of entries that can be expanded and reduced.
//! * `MapView` implements a map with keys and values.
//! * `SetView` implements a set with keys.
//! * `CollectionView` implements a map whose values are views themselves.
//! * `ReentrantCollectionView` implements a map for which different keys can be accessed independently.
//! * `ViewContainer<C>` implements a `KeyValueStore` and is used internally.
//!
//! The `LogView` can be seen as an analog of `VecDeque` while `MapView` is an analog of `BTreeMap`.

/// The `RegisterView` implements a register for a single value.
pub mod register_view;

/// The `LogView` implements a log list that can be pushed.
pub mod log_view;

/// The `QueueView` implements a queue that can push on the back and delete on the front.
pub mod queue_view;

/// The `MapView` implements a map with ordered keys.
pub mod map_view;

/// The `SetView` implements a set with ordered entries.
pub mod set_view;

/// The `CollectionView` implements a map structure whose keys are ordered and the values are views.
pub mod collection_view;

/// The `ReentrantCollectionView` implements a map structure whose keys are ordered and the values are views with concurrent access.
pub mod reentrant_collection_view;

/// The implementation of a key-value store view.
#[cfg(not(target_arch = "wasm32"))]
pub mod key_value_store_view;

