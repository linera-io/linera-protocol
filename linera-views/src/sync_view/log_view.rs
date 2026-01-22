// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    ops::{Bound, Range, RangeBounds},
};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option_or_default,
    context::Context,
    store::ReadableSyncKeyValueStore as _,
    sync_view::{SyncView, MIN_VIEW_TAG},
    ViewError,
};

/// Key tags to create the sub-keys of a `SyncLogView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable `stored_count`.
    Count = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// A view that supports logging values of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncLogView<C, T> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// The number of entries persisted in storage.
    stored_count: usize,
    /// New values not yet persisted to storage.
    new_values: Vec<T>,
}

impl<C, T> SyncView for SyncLogView<C, T>
where
    C: Context,
    T: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Count as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_count =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        Ok(Self {
            context,
            delete_storage_first: false,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.new_values.clear();
    }

    fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        !self.new_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            delete_view = true;
        }
        if !self.new_values.is_empty() {
            delete_view = false;
            let mut count = self.stored_count;
            for value in &self.new_values {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &count)?;
                batch.put_key_value(key, value)?;
                count += 1;
            }
            let key = self.context.base_key().base_tag(KeyTag::Count as u8);
            batch.put_key_value(key, &count)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.delete_storage_first {
            self.stored_count = 0;
        }
        self.stored_count += self.new_values.len();
        self.new_values.clear();
        self.delete_storage_first = false;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_values.clear();
    }
}
