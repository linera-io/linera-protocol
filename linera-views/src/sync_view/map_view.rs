// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The `SyncMapView` implements a map that can be modified.
//!
//! This reproduces more or less the functionalities of the `BTreeMap`.
//! There are 3 different variants:
//! * The [`SyncByteMapView`][class1] whose keys are the `Vec<u8>` and the values are a serializable type `V`.
//!   The ordering of the entries is via the lexicographic order of the keys.
//! * The [`SyncMapView`][class2] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the BCS serialized keys.
//! * The [`SyncCustomMapView`][class3] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the custom serialized keys.
//!
//! [class1]: map_view::SyncByteMapView
//! [class2]: map_view::SyncMapView
//! [class3]: map_view::SyncCustomMapView

use std::{
    borrow::{Borrow, Cow},
    collections::{btree_map::Entry, BTreeMap},
    marker::PhantomData,
};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{
        from_bytes_option, get_key_range_for_prefix, CustomSerialize, DeletionSet,
        SuffixClosedSetIterator, Update,
    },
    context::{BaseKey, Context},
    store::ReadableSyncKeyValueStore as _,
    sync_view::{SyncReplaceContext, SyncView},
    ViewError,
};

/// A view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, V: Allocative")]
pub struct SyncByteMapView<C, V> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// Tracks deleted key prefixes.
    deletion_set: DeletionSet,
    /// Pending changes not yet persisted to storage.
    updates: BTreeMap<Vec<u8>, Update<V>>,
}

impl<C: Context, C2: Context, V> SyncReplaceContext<C2> for SyncByteMapView<C, V>
where
    V: Send + Sync + Serialize + Clone,
{
    type Target = SyncByteMapView<C2, V>;

    fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SyncByteMapView {
            context: ctx(&self.context),
            deletion_set: self.deletion_set.clone(),
            updates: self.updates.clone(),
        }
    }
}

/// Whether we have a value or its serialization.
enum ValueOrBytes<'a, T> {
    /// The value itself.
    Value(&'a T),
    /// The serialization.
    Bytes(Vec<u8>),
}

impl<'a, T> ValueOrBytes<'a, T>
where
    T: Clone + DeserializeOwned,
{
    /// Convert to a Cow.
    fn to_value(&self) -> Result<Cow<'a, T>, ViewError> {
        match self {
            ValueOrBytes::Value(value) => Ok(Cow::Borrowed(value)),
            ValueOrBytes::Bytes(bytes) => Ok(Cow::Owned(bcs::from_bytes(bytes)?)),
        }
    }
}

impl<T> ValueOrBytes<'_, T>
where
    T: Serialize,
{
    /// Convert to bytes.
    pub fn into_bytes(self) -> Result<Vec<u8>, ViewError> {
        match self {
            ValueOrBytes::Value(value) => Ok(bcs::to_bytes(value)?),
            ValueOrBytes::Bytes(bytes) => Ok(bytes),
        }
    }
}

impl<C, V> SyncView for SyncByteMapView<C, V>
where
    C: Context,
    V: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 0;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(Vec::new())
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            updates: BTreeMap::new(),
            deletion_set: DeletionSet::new(),
        })
    }

    fn rollback(&mut self) {
        self.updates.clear();
        self.deletion_set.rollback();
    }

    fn has_pending_changes(&self) -> bool {
        self.deletion_set.has_pending_changes() || !self.updates.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.deletion_set.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            for (index, update) in &self.updates {
                if let Update::Set(value) = update {
                    let key = self.context.base_key().base_index(index);
                    batch.put_key_value(key, value)?;
                    delete_view = false;
                }
            }
        } else {
            for index in &self.deletion_set.deleted_prefixes {
                let key = self.context.base_key().base_index(index);
                batch.delete_key_prefix(key);
            }
            for (index, update) in &self.updates {
                let key = self.context.base_key().base_index(index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value(key, value)?,
                }
            }
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        self.updates.clear();
        self.deletion_set.delete_storage_first = false;
        self.deletion_set.deleted_prefixes.clear();
    }

    fn clear(&mut self) {
        self.updates.clear();
        self.deletion_set.clear();
    }
}
