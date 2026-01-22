// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    ops::{Bound, Range, RangeBounds},
};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch, context::SyncContext, store::SyncReadableKeyValueStore as _,
    sync_views::SyncView, views::log_view::LogView, ViewError,
};

/// A synchronous view that supports logging values of type `T`.
///
/// This is a thin wrapper around [`LogView`] that implements [`SyncView`]
/// instead of [`View`](crate::views::View).
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncLogView<C, T>(
    /// The inner async log view whose state and logic we reuse.
    pub(crate) LogView<C, T>,
);

impl<C, T> SyncView for SyncLogView<C, T>
where
    C: SyncContext,
    T: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.0.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(LogView::<C, T>::base_pre_load(context.base_key()))
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(SyncLogView(LogView::base_post_load(context, values)?))
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

impl<C, T> SyncLogView<C, T> {
    /// Pushes a value to the end of the log.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// ```
    pub fn push(&mut self, value: T) {
        self.0.push(value);
    }

    /// Reads the size of the log.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(log.count(), 2);
    /// ```
    pub fn count(&self) -> usize {
        self.0.count()
    }
}

impl<C, T> SyncLogView<C, T>
where
    C: SyncContext,
{
    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.0.context.extra()
    }
}

impl<C, T> SyncLogView<C, T>
where
    C: SyncContext,
    T: Clone + DeserializeOwned + Serialize + Send + Sync,
{
    /// Reads the logged value with the given index (including staged ones).
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// assert_eq!(log.get(0).unwrap(), Some(34));
    /// ```
    pub fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.0.delete_storage_first {
            self.0.new_values.get(index).cloned()
        } else if index < self.0.stored_count {
            let key = self
                .0
                .context
                .base_key()
                .derive_tag_key(crate::views::log_view::KEY_TAG_INDEX, &index)?;
            self.0.context.store().read_value(&key)?
        } else {
            self.0.new_values.get(index - self.0.stored_count).cloned()
        };
        Ok(value)
    }

    /// Reads several logged keys (including staged ones)
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(log.multi_get(vec![0, 1]).unwrap(), vec![Some(34), Some(42)]);
    /// ```
    pub fn multi_get(&self, indices: Vec<usize>) -> Result<Vec<Option<T>>, ViewError> {
        let mut result = Vec::new();
        if self.0.delete_storage_first {
            for index in indices {
                result.push(self.0.new_values.get(index).cloned());
            }
        } else {
            let mut index_to_positions = BTreeMap::<usize, Vec<usize>>::new();
            for (pos, index) in indices.into_iter().enumerate() {
                if index < self.0.stored_count {
                    index_to_positions.entry(index).or_default().push(pos);
                    result.push(None);
                } else {
                    result.push(self.0.new_values.get(index - self.0.stored_count).cloned());
                }
            }
            let mut keys = Vec::new();
            let mut vec_positions = Vec::new();
            for (index, positions) in index_to_positions {
                let key = self
                    .0
                    .context
                    .base_key()
                    .derive_tag_key(crate::views::log_view::KEY_TAG_INDEX, &index)?;
                keys.push(key);
                vec_positions.push(positions);
            }
            let values = self.0.context.store().read_multi_values(&keys)?;
            for (positions, value) in vec_positions.into_iter().zip(values) {
                if let Some((&last, rest)) = positions.split_last() {
                    for &position in rest {
                        *result.get_mut(position).unwrap() = value.clone();
                    }
                    *result.get_mut(last).unwrap() = value;
                }
            }
        }
        Ok(result)
    }

    /// Reads the index-value pairs at the given positions.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(
    ///     log.multi_get_pairs(vec![0, 1, 5]).unwrap(),
    ///     vec![(0, Some(34)), (1, Some(42)), (5, None)]
    /// );
    /// ```
    pub fn multi_get_pairs(
        &self,
        indices: Vec<usize>,
    ) -> Result<Vec<(usize, Option<T>)>, ViewError> {
        let values = self.multi_get(indices.clone())?;
        Ok(indices.into_iter().zip(values).collect())
    }

    fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self
                .0
                .context
                .base_key()
                .derive_tag_key(crate::views::log_view::KEY_TAG_INDEX, &index)?;
            keys.push(key);
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.0.context.store().read_multi_values(&keys)? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries("SyncLogView".into()));
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the logged values in the given range (including staged ones).
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::log_view::SyncLogView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut log = SyncLogView::load(context).unwrap();
    /// log.push(34);
    /// log.push(42);
    /// log.push(56);
    /// assert_eq!(log.read(0..2).unwrap(), vec![34, 42]);
    /// ```
    pub fn read<R>(&self, range: R) -> Result<Vec<T>, ViewError>
    where
        R: RangeBounds<usize>,
    {
        let effective_stored_count = if self.0.delete_storage_first {
            0
        } else {
            self.0.stored_count
        };
        let end = match range.end_bound() {
            Bound::Included(end) => *end + 1,
            Bound::Excluded(end) => *end,
            Bound::Unbounded => self.count(),
        }
        .min(self.count());
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => *start + 1,
            Bound::Unbounded => 0,
        };
        if start >= end {
            return Ok(Vec::new());
        }
        if start < effective_stored_count {
            if end <= effective_stored_count {
                self.read_context(start..end)
            } else {
                let mut values = self.read_context(start..effective_stored_count)?;
                values.extend(
                    self.0.new_values[0..(end - effective_stored_count)]
                        .iter()
                        .cloned(),
                );
                Ok(values)
            }
        } else {
            Ok(
                self.0.new_values[(start - effective_stored_count)..(end - effective_stored_count)]
                    .to_vec(),
            )
        }
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::SyncLogView;
    use crate::{
        context::SyncContext,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for SyncLogView<C, T> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncLogView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: SyncContext + Send + Sync, T: async_graphql::OutputType> SyncLogView<C, T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count() as u32)
        }

        async fn entries(
            &self,
            start: Option<usize>,
            end: Option<usize>,
        ) -> async_graphql::Result<Vec<T>> {
            Ok(self.read(start.unwrap_or_default()..end.unwrap_or_else(|| self.count()))?)
        }
    }
}
