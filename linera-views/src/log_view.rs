// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, HasherOutput, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::Debug,
    ops::{Bound, Range, RangeBounds},
};

/// Key tags to create the sub-keys of a LogView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable stored_count.
    Store = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
    /// Prefix for the hash.
    Hash,
}

/// A view that supports logging values of type `T`.
#[derive(Debug)]
pub struct LogView<C, T> {
    context: C,
    was_cleared: bool,
    stored_count: usize,
    new_values: Vec<T>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Store as u8);
        let stored_count = context.read_key(&key).await?.unwrap_or_default();
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            stored_count,
            new_values: Vec::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.new_values.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            if self.stored_count > 0 {
                batch.delete_key_prefix(self.context.base_key());
                self.stored_count = 0;
            }
        }
        if !self.new_values.is_empty() {
            for value in &self.new_values {
                let key = self
                    .context
                    .derive_tag_key(KeyTag::Index as u8, &self.stored_count)?;
                batch.put_key_value(key, value)?;
                self.stored_count += 1;
            }
            let key = self.context.base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &self.stored_count)?;
            self.new_values.clear();
        }
        let hash = *self.hash.get_mut();
        if self.stored_hash != hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = hash;
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.new_values.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
{
    /// Pushes a value to the end of the log.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::log_view::LogView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut log = LogView::load(context).await.unwrap();
    ///   log.push(34);
    /// # })
    /// ```
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
        *self.hash.get_mut() = None;
    }

    /// Reads the size of the log.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::log_view::LogView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut log = LogView::load(context).await.unwrap();
    ///   log.push(34);
    ///   log.push(42);
    ///   assert_eq!(log.count(), 2);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        if self.was_cleared {
            self.new_values.len()
        } else {
            self.stored_count + self.new_values.len()
        }
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + DeserializeOwned + Serialize + Send,
{
    /// Reads the logged value with the given index (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::log_view::LogView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut log = LogView::load(context).await.unwrap();
    ///   log.push(34);
    ///   assert_eq!(log.get(0).await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.was_cleared {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            self.context.read_key(&key).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key);
        }
        let values_opt = self.context.read_multi_key(keys).await?;
        let mut values = Vec::with_capacity(count);
        for entry in values_opt {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries);
                },
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the logged values in the given range (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::log_view::LogView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut log = LogView::load(context).await.unwrap();
    ///   log.push(34);
    ///   log.push(42);
    ///   log.push(56);
    ///   assert_eq!(log.read(0..2).await.unwrap(), vec![34,42]);
    /// # })
    /// ```
    pub async fn read<R>(&self, range: R) -> Result<Vec<T>, ViewError>
    where
        R: RangeBounds<usize>,
    {
        let effective_stored_count = if self.was_cleared {
            0
        } else {
            self.stored_count
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
                self.read_context(start..end).await
            } else {
                let mut values = self.read_context(start..effective_stored_count).await?;
                values.extend(
                    self.new_values[0..(end - effective_stored_count)]
                        .iter()
                        .cloned(),
                );
                Ok(values)
            }
        } else {
            Ok(
                self.new_values[(start - effective_stored_count)..(end - effective_stored_count)]
                    .to_vec(),
            )
        }
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let elements = self.read(..).await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, T> HashableView<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
                let hash = self.hash.get_mut();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let mut hash = self.hash.lock().await;
        match *hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}
