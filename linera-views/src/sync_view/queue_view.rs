// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{vec_deque::IterMut, VecDeque},
    ops::Range,
};

use allocative::Allocative;
use linera_base::visit_allocative_simple;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option_or_default,
    context::SyncContext,
    store::ReadableSyncKeyValueStore as _,
    sync_view::{SyncView, MIN_VIEW_TAG},
    ViewError,
};

/// Key tags to create the sub-keys of a `SyncQueueView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable `stored_indices`.
    Store = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncQueueView<C, T> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The range of indices for entries persisted in storage.
    #[allocative(visit = visit_allocative_simple)]
    stored_indices: Range<usize>,
    /// The number of entries to delete from the front.
    front_delete_count: usize,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// New values added to the back, not yet persisted to storage.
    new_back_values: VecDeque<T>,
}

impl<C, T> SyncView for SyncQueueView<C, T>
where
    C: SyncContext,
    T: Serialize + Send + Sync,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Store as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_indices =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            delete_storage_first: false,
            new_back_values: VecDeque::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.front_delete_count = 0;
        self.new_back_values.clear();
    }

    fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if self.front_delete_count > 0 {
            return true;
        }
        !self.new_back_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            delete_view = true;
        }
        let mut new_stored_indices = self.stored_indices.clone();
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key().base_tag(KeyTag::Index as u8);
            batch.delete_key_prefix(key_prefix);
            new_stored_indices = Range::default();
        } else if self.front_delete_count > 0 {
            let deletion_range = self.stored_indices.clone().take(self.front_delete_count);
            new_stored_indices.start += self.front_delete_count;
            for index in deletion_range {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &index)?;
                batch.delete_key(key);
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            for value in &self.new_back_values {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &new_stored_indices.end)?;
                batch.put_key_value(key, value)?;
                new_stored_indices.end += 1;
            }
        }
        if !self.delete_storage_first || !new_stored_indices.is_empty() {
            let key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &new_stored_indices)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.stored_count() == 0 {
            self.stored_indices = Range::default();
        } else if self.front_delete_count > 0 {
            self.stored_indices.start += self.front_delete_count;
        }
        if !self.new_back_values.is_empty() {
            self.stored_indices.end += self.new_back_values.len();
            self.new_back_values.clear();
        }
        self.front_delete_count = 0;
        self.delete_storage_first = false;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_back_values.clear();
    }
}
