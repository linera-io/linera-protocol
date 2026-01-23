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

impl<C, T> SyncQueueView<C, T> {
    /// Returns the number of entries still stored (not yet deleted from storage).
    fn stored_count(&self) -> usize {
        if self.delete_storage_first {
            0
        } else {
            self.stored_indices.len() - self.front_delete_count
        }
    }
}

impl<C, T> SyncQueueView<C, T>
where
    C: SyncContext,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let key = self
            .context
            .base_key()
            .derive_tag_key(KeyTag::Index as u8, &index)?;
        Ok(self.context.store().read_value(&key)?)
    }

    /// Reads the front value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.front().unwrap(), Some(34));
    /// ```
    pub fn front(&self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_count();
        let value = if stored_remainder > 0 {
            self.get(self.stored_indices.end - stored_remainder)?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Reads the back value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.back().unwrap(), Some(42));
    /// ```
    pub fn back(&self) -> Result<Option<T>, ViewError> {
        Ok(match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_count() > 0 => self.get(self.stored_indices.end - 1)?,
            _ => None,
        })
    }

    /// Deletes the front value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34 as u128);
    /// queue.delete_front();
    /// assert_eq!(queue.elements().unwrap(), Vec::<u128>::new());
    /// ```
    pub fn delete_front(&mut self) {
        if self.stored_count() > 0 {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Pushes a value to the end of the queue.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().unwrap(), vec![34, 37]);
    /// ```
    pub fn push_back(&mut self, value: T) {
        self.new_back_values.push_back(value);
    }

    /// Reads the size of the queue.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        self.stored_count() + self.new_back_values.len()
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self
                .context
                .base_key()
                .derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key)
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.context.store().read_multi_values(&keys)? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries("SyncQueueView".into()));
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the `count` next values in the queue (including staged ones).
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.read_front(1).unwrap(), vec![34]);
    /// ```
    pub fn read_front(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(count);
        if !self.delete_storage_first {
            let stored_remainder = self.stored_count();
            let start = self.stored_indices.end - stored_remainder;
            if count <= stored_remainder {
                values.extend(self.read_context(start..(start + count))?);
            } else {
                values.extend(self.read_context(start..self.stored_indices.end)?);
                values.extend(
                    self.new_back_values
                        .range(0..(count - stored_remainder))
                        .cloned(),
                );
            }
        } else {
            values.extend(self.new_back_values.range(0..count).cloned());
        }
        Ok(values)
    }

    /// Reads the `count` last values in the queue (including staged ones).
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.read_back(1).unwrap(), vec![42]);
    /// ```
    pub fn read_back(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(count);
        let new_back_len = self.new_back_values.len();
        if count <= new_back_len || self.delete_storage_first {
            values.extend(
                self.new_back_values
                    .range((new_back_len - count)..new_back_len)
                    .cloned(),
            );
        } else {
            let start = self.stored_indices.end + new_back_len - count;
            values.extend(self.read_context(start..self.stored_indices.end)?);
            values.extend(self.new_back_values.iter().cloned());
        }
        Ok(values)
    }

    /// Reads all the elements
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().unwrap(), vec![34, 37]);
    /// ```
    pub fn elements(&self) -> Result<Vec<T>, ViewError> {
        let count = self.count();
        self.read_front(count)
    }

    fn load_all(&mut self) -> Result<(), ViewError> {
        if !self.delete_storage_first {
            let stored_remainder = self.stored_count();
            let start = self.stored_indices.end - stored_remainder;
            let elements = self.read_context(start..self.stored_indices.end)?;
            let shift = self.stored_indices.end - start;
            for elt in elements {
                self.new_back_values.push_back(elt);
            }
            self.new_back_values.rotate_right(shift);
            // All indices are being deleted at the next flush. This is because they are deleted either:
            // * Because a self.front_delete_count forces them to be removed
            // * Or because loading them means that their value can be changed which invalidates
            //   the entries on storage
            self.delete_storage_first = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::queue_view::SyncQueueView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// let mut iter = queue.iter_mut().unwrap();
    /// let value = iter.next().unwrap();
    /// *value = 42;
    /// assert_eq!(queue.elements().unwrap(), vec![42]);
    /// ```
    pub fn iter_mut(&mut self) -> Result<IterMut<'_, T>, ViewError> {
        self.load_all()?;
        Ok(self.new_back_values.iter_mut())
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::SyncQueueView;
    use crate::{
        context::SyncContext,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for SyncQueueView<C, T> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncQueueView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: SyncContext + Send + Sync, T: async_graphql::OutputType> SyncQueueView<C, T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count() as u32)
        }

        async fn entries(&self, count: Option<usize>) -> async_graphql::Result<Vec<T>> {
            Ok(self.read_front(count.unwrap_or_else(|| self.count()))?)
        }
    }
}
