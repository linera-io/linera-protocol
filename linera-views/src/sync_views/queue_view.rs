// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::vec_deque::IterMut, ops::Range};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    context::SyncContext,
    store::SyncReadableKeyValueStore as _,
    sync_views::SyncView,
    views::queue_view::{QueueView, KEY_TAG_INDEX},
    ViewError,
};

/// A synchronous view that supports a FIFO queue for values of type `T`.
///
/// This is a thin wrapper around [`QueueView`] that implements [`SyncView`]
/// instead of [`View`](crate::views::View).
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncQueueView<C, T>(
    /// The inner async queue view whose state and logic we reuse.
    pub(crate) QueueView<C, T>,
);

impl<C, T> SyncView for SyncQueueView<C, T>
where
    C: SyncContext,
    T: Serialize + Send + Sync,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.0.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(QueueView::<C, T>::base_pre_load(context.base_key()))
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(SyncQueueView(QueueView::base_post_load(context, values)?))
    }

    fn rollback(&mut self) {
        self.0.base_rollback();
    }

    fn has_pending_changes(&self) -> bool {
        self.0.base_has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.0.base_pre_save(batch, self.0.context.base_key())
    }

    fn post_save(&mut self) {
        self.0.base_post_save();
    }

    fn clear(&mut self) {
        self.0.base_clear();
    }
}

impl<C, T> SyncQueueView<C, T> {
    /// Deletes the front value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34 as u128);
    /// queue.delete_front();
    /// assert_eq!(queue.elements().unwrap(), Vec::<u128>::new());
    /// ```
    pub fn delete_front(&mut self) {
        self.0.delete_front();
    }

    /// Pushes a value to the end of the queue.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().unwrap(), vec![34, 37]);
    /// ```
    pub fn push_back(&mut self, value: T) {
        self.0.push_back(value);
    }

    /// Reads the size of the queue.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.count(), 1);
    /// ```
    pub fn count(&self) -> usize {
        self.0.count()
    }
}

impl<C, T> SyncQueueView<C, T>
where
    C: SyncContext,
{
    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.0.context.extra()
    }
}

impl<C, T> SyncQueueView<C, T>
where
    C: SyncContext,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let key = self
            .0
            .context
            .base_key()
            .derive_tag_key(KEY_TAG_INDEX, &index)?;
        Ok(self.0.context.store().read_value(&key)?)
    }

    /// Reads the front value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.front().unwrap(), Some(34));
    /// ```
    pub fn front(&self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.0.stored_count();
        let value = if stored_remainder > 0 {
            self.get(self.0.stored_indices.end - stored_remainder)?
        } else {
            self.0.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Reads the back value, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.back().unwrap(), Some(42));
    /// ```
    pub fn back(&self) -> Result<Option<T>, ViewError> {
        Ok(match self.0.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.0.stored_count() > 0 => self.get(self.0.stored_indices.end - 1)?,
            _ => None,
        })
    }

    fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self
                .0
                .context
                .base_key()
                .derive_tag_key(KEY_TAG_INDEX, &index)?;
            keys.push(key)
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.0.context.store().read_multi_values(&keys)? {
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
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
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
        if !self.0.delete_storage_first {
            let stored_remainder = self.0.stored_count();
            let start = self.0.stored_indices.end - stored_remainder;
            if count <= stored_remainder {
                values.extend(self.read_context(start..(start + count))?);
            } else {
                values.extend(self.read_context(start..self.0.stored_indices.end)?);
                values.extend(
                    self.0
                        .new_back_values
                        .range(0..(count - stored_remainder))
                        .cloned(),
                );
            }
        } else {
            values.extend(self.0.new_back_values.range(0..count).cloned());
        }
        Ok(values)
    }

    /// Reads the `count` last values in the queue (including staged ones).
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
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
        let new_back_len = self.0.new_back_values.len();
        if count <= new_back_len || self.0.delete_storage_first {
            values.extend(
                self.0
                    .new_back_values
                    .range((new_back_len - count)..new_back_len)
                    .cloned(),
            );
        } else {
            let start = self.0.stored_indices.end + new_back_len - count;
            values.extend(self.read_context(start..self.0.stored_indices.end)?);
            values.extend(self.0.new_back_values.iter().cloned());
        }
        Ok(values)
    }

    /// Reads all the elements
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
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
        if !self.0.delete_storage_first {
            let stored_remainder = self.0.stored_count();
            let start = self.0.stored_indices.end - stored_remainder;
            let elements = self.read_context(start..self.0.stored_indices.end)?;
            let shift = self.0.stored_indices.end - start;
            for elt in elements {
                self.0.new_back_values.push_back(elt);
            }
            self.0.new_back_values.rotate_right(shift);
            self.0.delete_storage_first = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::queue_view::SyncQueueView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut queue = SyncQueueView::load(context).unwrap();
    /// queue.push_back(34);
    /// let mut iter = queue.iter_mut().unwrap();
    /// let value = iter.next().unwrap();
    /// *value = 42;
    /// assert_eq!(queue.elements().unwrap(), vec![42]);
    /// ```
    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter_mut(&mut self) -> Result<IterMut<'_, T>, ViewError> {
        self.load_all()?;
        Ok(self.0.new_back_values.iter_mut())
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
