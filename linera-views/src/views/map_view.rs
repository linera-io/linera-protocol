// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The `MapView` implements a map that can be modified.
//!
//! This reproduces more or less the functionalities of the `BTreeMap`.
//! There are 3 different variants:
//! * The [`ByteMapView`][class1] whose keys are the `Vec<u8>` and the values are a serializable type `V`.
//!   The ordering of the entries is via the lexicographic order of the keys.
//! * The [`MapView`][class2] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the BCS serialized keys.
//! * The [`CustomMapView`][class3] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the custom serialized keys.
//!
//! [class1]: map_view::ByteMapView
//! [class2]: map_view::MapView
//! [class3]: map_view::CustomMapView

#[cfg(with_metrics)]
use std::sync::LazyLock;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{bucket_latencies, register_histogram_vec, MeasureLatency},
    prometheus::HistogramVec,
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static MAP_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "map_view_hash_runtime",
        "MapView hash runtime",
        &[],
        bucket_latencies(5.0),
    )
});

use std::{
    borrow::{Borrow, Cow},
    collections::{btree_map::Entry, BTreeMap},
    marker::PhantomData,
    mem,
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{
        from_bytes_option, get_interval, CustomSerialize, DeletionSet, HasherOutput,
        SuffixClosedSetIterator, Update,
    },
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    store::{KeyIterable, KeyValueIterable},
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

/// A view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug)]
pub struct ByteMapView<C, V> {
    context: C,
    deletion_set: DeletionSet,
    updates: BTreeMap<Vec<u8>, Update<V>>,
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

impl<'a, T> ValueOrBytes<'a, T>
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

#[async_trait]
impl<C, V> View<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 0;

    fn context(&self) -> &C {
        &self.context
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

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.updates.clear();
        self.deletion_set.rollback();
    }

    async fn has_pending_changes(&self) -> bool {
        self.deletion_set.has_pending_changes() || !self.updates.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.deletion_set.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(value) = update {
                    let key = self.context.base_index(&index);
                    batch.put_key_value(key, &value)?;
                    delete_view = false;
                }
            }
        } else {
            for index in mem::take(&mut self.deletion_set.deleted_prefixes) {
                let key = self.context.base_index(&index);
                batch.delete_key_prefix(key);
            }
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_index(&index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value(key, &value)?,
                }
            }
        }
        self.deletion_set.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.updates.clear();
        self.deletion_set.clear();
    }
}

impl<C, V> ClonableView<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(ByteMapView {
            context: self.context.clone(),
            updates: self.updates.clone(),
            deletion_set: self.deletion_set.clone(),
        })
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Inserts or resets the value of a key of the map.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.keys().await.unwrap(), vec![vec![0, 1]]);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        self.updates.insert(short_key, Update::Set(value));
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], "Hello");
    /// map.remove(vec![0, 1]);
    /// # })
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        if self.deletion_set.contains_prefix_of(&short_key) {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove a range of keys containing it.
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![0, 2], String::from("Bonjour"));
    /// map.remove_by_prefix(vec![0]);
    /// assert!(map.keys().await.unwrap().is_empty());
    /// # })
    /// ```
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) {
        let key_list = self
            .updates
            .range(get_interval(key_prefix.clone()))
            .map(|x| x.0.to_vec())
            .collect::<Vec<_>>();
        for key in key_list {
            self.updates.remove(&key);
        }
        self.deletion_set.insert_key_prefix(key_prefix);
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert!(map.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!map.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        if let Some(update) = self.updates.get(short_key) {
            let test = match update {
                Update::Removed => false,
                Update::Set(_value) => true,
            };
            return Ok(test);
        }
        if self.deletion_set.contains_prefix_of(short_key) {
            return Ok(false);
        }
        let key = self.context.base_index(short_key);
        Ok(self.context.contains_key(&key).await?)
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.get(&[0, 1]).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub async fn get(&self, short_key: &[u8]) -> Result<Option<V>, ViewError> {
        if let Some(update) = self.updates.get(short_key) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value.clone()),
            };
            return Ok(value);
        }
        if self.deletion_set.contains_prefix_of(short_key) {
            return Ok(None);
        }
        let key = self.context.base_index(short_key);
        Ok(self.context.read_value(&key).await?)
    }

    /// Reads the values at the given positions, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let values = map.multi_get(vec![vec![0, 1], vec![0, 2]]).await.unwrap();
    /// assert_eq!(values, vec![Some(String::from("Hello")), None]);
    /// # })
    /// ```
    pub async fn multi_get(&self, short_keys: Vec<Vec<u8>>) -> Result<Vec<Option<V>>, ViewError> {
        let size = short_keys.len();
        let mut results = vec![None; size];
        let mut missed_indices = Vec::new();
        let mut vector_query = Vec::new();
        for (i, short_key) in short_keys.into_iter().enumerate() {
            if let Some(update) = self.updates.get(&short_key) {
                if let Update::Set(value) = update {
                    results[i] = Some(value.clone());
                }
            } else if !self.deletion_set.contains_prefix_of(&short_key) {
                missed_indices.push(i);
                let key = self.context.base_index(&short_key);
                vector_query.push(key);
            }
        }
        let values = self.context.read_multi_values_bytes(vector_query).await?;
        for (i, value) in missed_indices.into_iter().zip(values) {
            results[i] = from_bytes_option(&value)?;
        }
        Ok(results)
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let value = map.get_mut(&[0, 1]).await.unwrap().unwrap();
    /// assert_eq!(*value, String::from("Hello"));
    /// *value = String::from("Hola");
    /// assert_eq!(map.get(&[0, 1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut(&mut self, short_key: &[u8]) -> Result<Option<&mut V>, ViewError> {
        let update = match self.updates.entry(short_key.to_vec()) {
            Entry::Vacant(e) => {
                if self.deletion_set.contains_prefix_of(short_key) {
                    None
                } else {
                    let key = self.context.base_index(short_key);
                    let value = self.context.read_value(&key).await?;
                    value.map(|value| e.insert(Update::Set(value)))
                }
            }
            Entry::Occupied(e) => Some(e.into_mut()),
        };
        Ok(match update {
            Some(Update::Set(value)) => Some(value),
            _ => None,
        })
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Clone + Serialize + DeserializeOwned + 'static,
{
    /// Applies the function f on each index (aka key) which has the assigned prefix.
    /// Keys are visited in the lexicographic order. The shortened key is send to the
    /// function and if it returns false, then the loop exits
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![1, 3], String::from("Bonjour"));
    /// let prefix = vec![1];
    /// let mut count = 0;
    /// map.for_each_key_while(
    ///     |_key| {
    ///         count += 1;
    ///         Ok(count < 3)
    ///     },
    ///     prefix,
    /// )
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = self.updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !self.deletion_set.contains_prefix_of(&prefix) {
            let iter = self
                .deletion_set
                .deleted_prefixes
                .range(get_interval(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = self.context.base_index(&prefix);
            for index in self.context.find_keys_by_prefix(&base).await?.iterator() {
                let index = index?;
                loop {
                    match update {
                        Some((key, value)) if &key[prefix_len..] <= index => {
                            if let Update::Set(_) = value {
                                if !f(&key[prefix_len..])? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if &key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(index) && !f(index)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(_) = value {
                if !f(&key[prefix_len..])? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies the function f on each index (aka key) having the specified prefix.
    /// The shortened keys are sent to the function f. Keys are visited in the
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let mut count = 0;
    /// let prefix = Vec::new();
    /// map.for_each_key(
    ///     |_key| {
    ///         count += 1;
    ///         Ok(())
    ///     },
    ///     prefix,
    /// )
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(
            |key| {
                f(key)?;
                Ok(true)
            },
            prefix,
        )
        .await
    }

    /// Returns the list of keys of the map in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![2, 2], String::from("Hallo"));
    /// assert_eq!(
    ///     map.keys().await.unwrap(),
    ///     vec![vec![0, 1], vec![1, 2], vec![2, 2]]
    /// );
    /// # })
    /// ```
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        let prefix = Vec::new();
        self.for_each_key(
            |key| {
                keys.push(key.to_vec());
                Ok(())
            },
            prefix,
        )
        .await?;
        Ok(keys)
    }

    /// Returns the list of keys of the map having a specified prefix
    /// in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![1, 3], String::from("Hallo"));
    /// assert_eq!(
    ///     map.keys_by_prefix(vec![1]).await.unwrap(),
    ///     vec![vec![1, 2], vec![1, 3]]
    /// );
    /// # })
    /// ```
    pub async fn keys_by_prefix(&self, prefix: Vec<u8>) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        let prefix_clone = prefix.clone();
        self.for_each_key(
            |key| {
                let mut big_key = prefix.clone();
                big_key.extend(key);
                keys.push(big_key);
                Ok(())
            },
            prefix_clone,
        )
        .await?;
        Ok(keys)
    }

    /// Returns the number of keys of the map
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![2, 2], String::from("Hallo"));
    /// assert_eq!(map.count().await.unwrap(), 3);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        let prefix = Vec::new();
        self.for_each_key(
            |_key| {
                count += 1;
                Ok(())
            },
            prefix,
        )
        .await?;
        Ok(count)
    }

    /// Applies a function f on each key/value pair matching a prefix. The key is the
    /// shortened one by the prefix. The value is an enum that can be either a value
    /// or its serialization. This is needed in order to avoid a scenario where we
    /// deserialize something that was serialized. The key/value are send to the
    /// function f. If it returns false the loop ends prematurely. Keys and values
    /// are visited in the lexicographic order.
    async fn for_each_key_value_or_bytes_while<'a, F>(
        &'a self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], ValueOrBytes<'a, V>) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = self.updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !self.deletion_set.contains_prefix_of(&prefix) {
            let iter = self
                .deletion_set
                .deleted_prefixes
                .range(get_interval(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = self.context.base_index(&prefix);
            for entry in self
                .context
                .find_key_values_by_prefix(&base)
                .await?
                .into_iterator_owned()
            {
                let (index, bytes) = entry?;
                loop {
                    match update {
                        Some((key, value)) if key[prefix_len..] <= *index => {
                            if let Update::Set(value) = value {
                                let value = ValueOrBytes::Value(value);
                                if !f(&key[prefix_len..], value)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(&index) {
                                let value = ValueOrBytes::Bytes(bytes);
                                if !f(&index, value)? {
                                    return Ok(());
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                let value = ValueOrBytes::Value(value);
                if !f(&key[prefix_len..], value)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }
    /// Applies a function f on each index/value pair matching a prefix. Keys
    /// and values are visited in the lexicographic order. The shortened index
    /// is send to the function f and if it returns false then the loop ends
    /// prematurely
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![1, 3], String::from("Hallo"));
    /// let mut part_keys = Vec::new();
    /// let prefix = vec![1];
    /// map.for_each_key_value_while(
    ///     |key, _value| {
    ///         part_keys.push(key.to_vec());
    ///         Ok(part_keys.len() < 2)
    ///     },
    ///     prefix,
    /// )
    /// .await
    /// .unwrap();
    /// assert_eq!(part_keys.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_key_value_while<'a, F>(
        &'a self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        self.for_each_key_value_or_bytes_while(
            |key, value| {
                let value = value.to_value()?;
                f(key, value)
            },
            prefix,
        )
        .await
    }

    /// Applies a function f on each key/value pair matching a prefix. The key is the
    /// shortened one by the prefix. The value is an enum that can be either a value
    /// or its serialization. This is needed in order to avoid a scenario where we
    /// deserialize something that was serialized. The key/value are send to the
    /// function f. Keys and values are visited in the lexicographic order.
    async fn for_each_key_value_or_bytes<'a, F>(
        &'a self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], ValueOrBytes<'a, V>) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_value_or_bytes_while(
            |key, value| {
                f(key, value)?;
                Ok(true)
            },
            prefix,
        )
        .await
    }

    /// Applies a function f on each key/value pair matching a prefix. The shortened
    /// key and value are send to the function f. Keys and values are visited in the
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let mut count = 0;
    /// let prefix = Vec::new();
    /// map.for_each_key_value(
    ///     |_key, _value| {
    ///         count += 1;
    ///         Ok(())
    ///     },
    ///     prefix,
    /// )
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key_value<'a, F>(
        &'a self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_value_while(
            |key, value| {
                f(key, value)?;
                Ok(true)
            },
            prefix,
        )
        .await
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Clone + Send + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of keys and values of the map matching a prefix
    /// in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![1, 2], String::from("Hello"));
    /// let prefix = vec![1];
    /// assert_eq!(
    ///     map.key_values_by_prefix(prefix).await.unwrap(),
    ///     vec![(vec![1, 2], String::from("Hello"))]
    /// );
    /// # })
    /// ```
    pub async fn key_values_by_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
        let mut key_values = Vec::new();
        let prefix_copy = prefix.clone();
        self.for_each_key_value(
            |key, value| {
                let mut big_key = prefix.clone();
                big_key.extend(key);
                let value = value.into_owned();
                key_values.push((big_key, value));
                Ok(())
            },
            prefix_copy,
        )
        .await?;
        Ok(key_values)
    }

    /// Returns the list of keys and values of the map in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![1, 2], String::from("Hello"));
    /// assert_eq!(
    ///     map.key_values().await.unwrap(),
    ///     vec![(vec![1, 2], String::from("Hello"))]
    /// );
    /// # })
    /// ```
    pub async fn key_values(&self) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
        self.key_values_by_prefix(Vec::new()).await
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = ByteMapView::load(context).await.unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.get_mut_or_default(&[7]).await.unwrap(), "");
    /// let value = map.get_mut_or_default(&[0, 1]).await.unwrap();
    /// assert_eq!(*value, String::from("Hello"));
    /// *value = String::from("Hola");
    /// assert_eq!(map.get(&[0, 1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut_or_default(&mut self, short_key: &[u8]) -> Result<&mut V, ViewError> {
        let update = match self.updates.entry(short_key.to_vec()) {
            Entry::Vacant(e) if self.deletion_set.contains_prefix_of(short_key) => {
                e.insert(Update::Set(V::default()))
            }
            Entry::Vacant(e) => {
                let key = self.context.base_index(short_key);
                let value = self.context.read_value(&key).await?.unwrap_or_default();
                e.insert(Update::Set(value))
            }
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(_) => &mut *entry,
                    Update::Removed => {
                        *entry = Update::Set(V::default());
                        &mut *entry
                    }
                }
            }
        };
        let Update::Set(value) = update else {
            unreachable!()
        };
        Ok(value)
    }
}

#[async_trait]
impl<C, V> HashableView<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = MAP_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
        let prefix = Vec::new();
        self.for_each_key_value_or_bytes(
            |index, value| {
                count += 1;
                hasher.update_with_bytes(index)?;
                let bytes = value.into_bytes()?;
                hasher.update_with_bytes(&bytes)?;
                Ok(())
            },
            prefix,
        )
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

/// A `View` that has a type for keys. The ordering of the entries
/// is determined by the serialization of the context.
#[derive(Debug)]
pub struct MapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Sync,
    V: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = ByteMapView::<C, V>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = ByteMapView::post_load(context, values)?;
        Ok(MapView {
            map,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> ClonableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Sync,
    V: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(MapView {
            map: self.map.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Inserts or resets a value at an index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u32, _> = MapView::load(context).await.unwrap();
    /// map.insert(&(24 as u32), String::from("Hello"));
    /// assert_eq!(
    ///     map.get(&(24 as u32)).await.unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then the operation does nothing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = MapView::<_, u32, String>::load(context).await.unwrap();
    /// map.remove(&(37 as u32));
    /// assert_eq!(map.get(&(37 as u32)).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = MapView::<_, u32, String>::load(context).await.unwrap();
    /// map.insert(&(37 as u32), String::from("Hello"));
    /// assert!(map.contains_key(&(37 as u32)).await.unwrap());
    /// assert!(!map.contains_key(&(34 as u32)).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.contains_key(&short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u32, _> = MapView::load(context).await.unwrap();
    /// map.insert(&(37 as u32), String::from("Hello"));
    /// assert_eq!(
    ///     map.get(&(37 as u32)).await.unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// assert_eq!(map.get(&(34 as u32)).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get(&short_key).await
    }

    /// Obtains a mutable reference to a value at a given position if available
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u32, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(37 as u32), String::from("Hello"));
    /// assert_eq!(map.get_mut(&(34 as u32)).await.unwrap(), None);
    /// let value = map.get_mut(&(37 as u32)).await.unwrap().unwrap();
    /// *value = String::from("Hola");
    /// assert_eq!(
    ///     map.get(&(37 as u32)).await.unwrap(),
    ///     Some(String::from("Hola"))
    /// );
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut(&short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Send + DeserializeOwned,
    V: Clone + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u32, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(37 as u32), String::from("Hello"));
    /// assert_eq!(map.indices().await.unwrap(), vec![37 as u32]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false, then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u128, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Thanks"));
    /// map.insert(&(37 as u128), String::from("Spasiba"));
    /// map.insert(&(38 as u128), String::from("Merci"));
    /// let mut count = 0;
    /// map.for_each_index_while(|_index| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_while(
                |key| {
                    let index = C::deserialize_value(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in the order
    /// determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u128, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// let mut count = 0;
    /// map.for_each_index(|_index| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key(
                |key| {
                    let index = C::deserialize_value(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index/value pair. Indices and values are
    /// visited in an order determined by serialization.
    /// If the function returns false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u128, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Thanks"));
    /// map.insert(&(37 as u128), String::from("Spasiba"));
    /// map.insert(&(38 as u128), String::from("Merci"));
    /// let mut values = Vec::new();
    /// map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, value| {
                    let index = C::deserialize_value(key)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function on each index/value pair. Indices and values are
    /// visited in an order determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, Vec<u8>, _> = MapView::load(context).await.unwrap();
    /// map.insert(&vec![0, 1], String::from("Hello"));
    /// let mut count = 0;
    /// map.for_each_index_value(|_index, _value| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index_value<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, value| {
                    let index = C::deserialize_value(key)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Send + DeserializeOwned,
    V: Clone + Sync + Send + Serialize + DeserializeOwned + 'static,
{
    /// Obtains all the `(index,value)` pairs.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, String, _> = MapView::load(context).await.unwrap();
    /// map.insert("Italian", String::from("Ciao"));
    /// let index_values = map.index_values().await.unwrap();
    /// assert_eq!(
    ///     index_values,
    ///     vec![("Italian".to_string(), "Ciao".to_string())]
    /// );
    /// # })
    /// ```
    pub async fn index_values(&self) -> Result<Vec<(I, V)>, ViewError> {
        let mut key_values = Vec::new();
        self.for_each_index_value(|index, value| {
            let value = value.into_owned();
            key_values.push((index, value));
            Ok(())
        })
        .await?;
        Ok(key_values)
    }

    /// Obtains the number of entries in the map
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, String, _> = MapView::load(context).await.unwrap();
    /// map.insert("Italian", String::from("Ciao"));
    /// map.insert("French", String::from("Bonjour"));
    /// assert_eq!(map.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.map.count().await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u32, u128> = MapView::load(context).await.unwrap();
    /// let value = map.get_mut_or_default(&(34 as u32)).await.unwrap();
    /// assert_eq!(*value, 0 as u128);
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut_or_default(&short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// A Custom MapView that uses the custom serialization
#[derive(Debug)]
pub struct CustomMapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = ByteMapView::<C, V>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = ByteMapView::post_load(context, values)?;
        Ok(CustomMapView {
            map,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> ClonableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(CustomMapView {
            map: self.map.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Insert or resets a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u128, _> = MapView::load(context).await.unwrap();
    /// map.insert(&(24 as u128), String::from("Hello"));
    /// assert_eq!(
    ///     map.get(&(24 as u128)).await.unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then this does not do anything.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = MapView::<_, u128, String>::load(context).await.unwrap();
    /// map.remove(&(37 as u128));
    /// assert_eq!(map.get(&(37 as u128)).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = MapView::<_, u128, String>::load(context).await.unwrap();
    /// map.insert(&(37 as u128), String::from("Hello"));
    /// assert!(map.contains_key(&(37 as u128)).await.unwrap());
    /// assert!(!map.contains_key(&(34 as u128)).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.contains_key(&short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: CustomMapView<MemoryContext<()>, u128, String> =
    ///     CustomMapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// assert_eq!(
    ///     map.get(&(34 as u128)).await.unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// # })
    /// ```
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get(&short_key).await
    }

    /// Obtains a mutable reference to a value at a given position if available
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// let value = map.get_mut(&(34 as u128)).await.unwrap().unwrap();
    /// *value = String::from("Hola");
    /// assert_eq!(
    ///     map.get(&(34 as u128)).await.unwrap(),
    ///     Some(String::from("Hola"))
    /// );
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut(&short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Send + CustomSerialize,
    V: Clone + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined
    /// by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, u128, String> = MapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// map.insert(&(37 as u128), String::from("Bonjour"));
    /// assert_eq!(map.indices().await.unwrap(), vec![34 as u128, 37 as u128]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function returns false,
    /// then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = CustomMapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// map.insert(&(37 as u128), String::from("Hola"));
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index_while(|index| {
    ///     indices.push(index);
    ///     Ok(indices.len() < 5)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(indices.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_while(
                |key| {
                    let index = I::from_custom_bytes(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = CustomMapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// map.insert(&(37 as u128), String::from("Hola"));
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(indices, vec![34, 37]);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key(
                |key| {
                    let index = I::from_custom_bytes(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on the index/value pairs. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// If the function returns false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map = CustomMapView::<_, u128, String>::load(context)
    ///     .await
    ///     .unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// map.insert(&(37 as u128), String::from("Hola"));
    /// let mut values = Vec::new();
    /// map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 5)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, value| {
                    let index = I::from_custom_bytes(key)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index/value pair. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    /// map.insert(&(34 as u128), String::from("Hello"));
    /// map.insert(&(37 as u128), String::from("Hola"));
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index_value(|index, _value| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(indices, vec![34, 37]);
    /// # })
    /// ```
    pub async fn for_each_index_value<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, value| {
                    let index = I::from_custom_bytes(key)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Send + CustomSerialize,
    V: Clone + Sync + Send + Serialize + DeserializeOwned + 'static,
{
    /// Obtains all the `(index,value)` pairs.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, String, _> = MapView::load(context).await.unwrap();
    /// map.insert("Italian", String::from("Ciao"));
    /// let index_values = map.index_values().await.unwrap();
    /// assert_eq!(
    ///     index_values,
    ///     vec![("Italian".to_string(), "Ciao".to_string())]
    /// );
    /// # })
    /// ```
    pub async fn index_values(&self) -> Result<Vec<(I, V)>, ViewError> {
        let mut key_values = Vec::new();
        self.for_each_index_value(|index, value| {
            let value = value.into_owned();
            key_values.push((index, value));
            Ok(())
        })
        .await?;
        Ok(key_values)
    }

    /// Obtains the number of entries in the map
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: MapView<_, String, _> = MapView::load(context).await.unwrap();
    /// map.insert("Italian", String::from("Ciao"));
    /// map.insert("French", String::from("Bonjour"));
    /// assert_eq!(map.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.map.count().await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut map: CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    /// assert_eq!(
    ///     *map.get_mut_or_default(&(34 as u128)).await.unwrap(),
    ///     String::new()
    /// );
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Send + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut_or_default(&short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// Type wrapping `ByteMapView` while memoizing the hash.
pub type HashedByteMapView<C, V> = WrappedHashableContainerView<C, ByteMapView<C, V>, HasherOutput>;

/// Type wrapping `MapView` while memoizing the hash.
pub type HashedMapView<C, I, V> = WrappedHashableContainerView<C, MapView<C, I, V>, HasherOutput>;

/// Type wrapping `CustomMapView` while memoizing the hash.
pub type HashedCustomMapView<C, I, V> =
    WrappedHashableContainerView<C, CustomMapView<C, I, V>, HasherOutput>;

mod graphql {
    use std::borrow::Cow;

    use super::{ByteMapView, CustomMapView, MapView};
    use crate::{
        context::Context,
        graphql::{hash_name, mangle, Entry, MapInput},
    };

    impl<C: Send + Sync, V: async_graphql::OutputType> async_graphql::TypeName for ByteMapView<C, V> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "ByteMapView_{}_{:08x}",
                mangle(V::type_name()),
                hash_name::<V>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, V> ByteMapView<C, V>
    where
        C: Context + Send + Sync,
        V: async_graphql::OutputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + 'static,
    {
        #[graphql(derived(name = "keys"))]
        async fn keys_(&self, count: Option<usize>) -> Result<Vec<Vec<u8>>, async_graphql::Error> {
            let keys = self.keys().await?;
            let it = keys.iter().cloned();
            Ok(if let Some(count) = count {
                it.take(count).collect()
            } else {
                it.collect()
            })
        }

        async fn entry(
            &self,
            key: Vec<u8>,
        ) -> Result<Entry<Vec<u8>, Option<V>>, async_graphql::Error> {
            Ok(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        async fn entries(
            &self,
            input: Option<MapInput<Vec<u8>>>,
        ) -> Result<Vec<Entry<Vec<u8>, Option<V>>>, async_graphql::Error> {
            let keys = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys);
            let keys = if let Some(keys) = keys {
                keys
            } else {
                self.keys().await?
            };

            let mut entries = vec![];
            for key in keys {
                entries.push(Entry {
                    value: self.get(&key).await?,
                    key,
                })
            }

            Ok(entries)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for MapView<C, I, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "MapView_{}_{}_{:08x}",
                mangle(I::type_name()),
                mangle(V::type_name()),
                hash_name::<(I, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I, V> MapView<C, I, V>
    where
        C: Context + Send + Sync,
        I: async_graphql::OutputType
            + async_graphql::InputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::fmt::Debug
            + Clone
            + Send
            + Sync
            + 'static,
        V: async_graphql::OutputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + 'static,
    {
        async fn keys(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let indices = self.indices().await?;
            let it = indices.iter().cloned();
            Ok(if let Some(count) = count {
                it.take(count).collect()
            } else {
                it.collect()
            })
        }

        async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
            Ok(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        async fn entries(
            &self,
            input: Option<MapInput<I>>,
        ) -> Result<Vec<Entry<I, Option<V>>>, async_graphql::Error> {
            let keys = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys);
            let keys = if let Some(keys) = keys {
                keys
            } else {
                self.indices().await?
            };

            let mut values = vec![];
            for key in keys {
                values.push(Entry {
                    value: self.get(&key).await?,
                    key,
                })
            }

            Ok(values)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for CustomMapView<C, I, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!("CustomMapView_{}_{}", I::type_name(), V::type_name()).into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I, V> CustomMapView<C, I, V>
    where
        C: Context + Send + Sync,
        I: async_graphql::OutputType
            + async_graphql::InputType
            + crate::common::CustomSerialize
            + std::fmt::Debug
            + Clone
            + Send
            + Sync
            + 'static,
        V: async_graphql::OutputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + 'static,
    {
        async fn keys(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let indices = self.indices().await?;
            let it = indices.iter().cloned();
            Ok(if let Some(count) = count {
                it.take(count).collect()
            } else {
                it.collect()
            })
        }

        async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
            Ok(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        async fn entries(
            &self,
            input: Option<MapInput<I>>,
        ) -> Result<Vec<Entry<I, Option<V>>>, async_graphql::Error> {
            let keys = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys);
            let keys = if let Some(keys) = keys {
                keys
            } else {
                self.indices().await?
            };

            let mut values = vec![];
            for key in keys {
                values.push(Entry {
                    value: self.get(&key).await?,
                    key,
                })
            }

            Ok(values)
        }
    }
}

/// The tests for `Borrow` and `bcs`.
#[cfg(test)]
pub mod tests {
    use std::borrow::Borrow;

    fn check_str<T: Borrow<str>>(s: T) {
        let ser1 = bcs::to_bytes("Hello").unwrap();
        let ser2 = bcs::to_bytes(s.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    fn check_array_u8<T: Borrow<[u8]>>(v: T) {
        let ser1 = bcs::to_bytes(&vec![23_u8, 67_u8, 123_u8]).unwrap();
        let ser2 = bcs::to_bytes(&v.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    #[test]
    fn test_serialization_borrow() {
        check_str("Hello".to_string());
        check_str("Hello");
        //
        check_array_u8(vec![23, 67, 123]);
        check_array_u8([23, 67, 123]);
    }
}
