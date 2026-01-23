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
    context::{BaseKey, SyncContext},
    store::ReadableSyncKeyValueStore as _,
    sync_view::SyncView,
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

impl<C, V> SyncView for SyncByteMapView<C, V>
where
    C: SyncContext,
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

impl<C, V> SyncByteMapView<C, V>
where
    C: SyncContext,
{
    /// Inserts or resets the value of a key of the map.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.keys().unwrap(), vec![vec![0, 1]]);
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        self.updates.insert(short_key, Update::Set(value));
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], "Hello");
    /// map.remove(vec![0, 1]);
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        if self.deletion_set.contains_prefix_of(&short_key) {
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Removes values by prefix. If absent then nothing is done.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![0, 2], String::from("Bonjour"));
    /// map.remove_by_prefix(vec![0]);
    /// assert!(map.keys().unwrap().is_empty());
    /// ```
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) {
        let key_list = self
            .updates
            .range(get_key_range_for_prefix(key_prefix.clone()))
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert!(map.contains_key(&[0, 1]).unwrap());
    /// assert!(!map.contains_key(&[0, 2]).unwrap());
    /// ```
    pub fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
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
        let key = self.context.base_key().base_index(short_key);
        Ok(self.context.store().contains_key(&key)?)
    }
}

impl<C, V> SyncByteMapView<C, V>
where
    C: SyncContext,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.get(&[0, 1]).unwrap(), Some(String::from("Hello")));
    /// ```
    pub fn get(&self, short_key: &[u8]) -> Result<Option<V>, ViewError> {
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
        let key = self.context.base_key().base_index(short_key);
        Ok(self.context.store().read_value(&key)?)
    }

    /// Reads the values at the given positions, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let values = map.multi_get(vec![vec![0, 1], vec![0, 2]]).unwrap();
    /// assert_eq!(values, vec![Some(String::from("Hello")), None]);
    /// ```
    pub fn multi_get(&self, short_keys: Vec<Vec<u8>>) -> Result<Vec<Option<V>>, ViewError> {
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
                let key = self.context.base_key().base_index(&short_key);
                vector_query.push(key);
            }
        }
        let values = self.context.store().read_multi_values_bytes(&vector_query)?;
        for (i, value) in missed_indices.into_iter().zip(values) {
            results[i] = from_bytes_option(&value)?;
        }
        Ok(results)
    }

    /// Reads the key-value pairs at the given positions, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let pairs = map.multi_get_pairs(vec![vec![0, 1], vec![0, 2]]).unwrap();
    /// assert_eq!(
    ///     pairs,
    ///     vec![
    ///         (vec![0, 1], Some(String::from("Hello"))),
    ///         (vec![0, 2], None)
    ///     ]
    /// );
    /// ```
    pub fn multi_get_pairs(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<V>)>, ViewError> {
        let values = self.multi_get(short_keys.clone())?;
        Ok(short_keys.into_iter().zip(values).collect())
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// let value = map.get_mut(&[0, 1]).unwrap().unwrap();
    /// assert_eq!(*value, String::from("Hello"));
    /// *value = String::from("Hola");
    /// assert_eq!(map.get(&[0, 1]).unwrap(), Some(String::from("Hola")));
    /// ```
    pub fn get_mut(&mut self, short_key: &[u8]) -> Result<Option<&mut V>, ViewError> {
        let update = match self.updates.entry(short_key.to_vec()) {
            Entry::Vacant(e) => {
                if self.deletion_set.contains_prefix_of(short_key) {
                    None
                } else {
                    let key = self.context.base_key().base_index(short_key);
                    let value = self.context.store().read_value(&key)?;
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

impl<C, V> SyncByteMapView<C, V>
where
    C: SyncContext,
    V: Clone + Serialize + DeserializeOwned + 'static,
{
    /// Applies the function f on each index (aka key) which has the assigned prefix.
    /// Keys are visited in the lexicographic order. The shortened key is sent to the
    /// function and if it returns false, then the loop exits.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
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
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_key_while<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = self.updates.range(get_key_range_for_prefix(prefix.clone()));
        let mut update = updates.next();
        if !self.deletion_set.contains_prefix_of(&prefix) {
            let iter = self
                .deletion_set
                .deleted_prefixes
                .range(get_key_range_for_prefix(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = self.context.base_key().base_index(&prefix);
            for index in self.context.store().find_keys_by_prefix(&base)? {
                loop {
                    match update {
                        Some((key, value)) if &key[prefix_len..] <= index.as_slice() => {
                            if let Update::Set(_) = value {
                                if !f(&key[prefix_len..])? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(&index) && !f(&index)? {
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
    /// Keys are visited in the lexicographic order.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
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
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_key<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
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
    }

    /// Returns the list of keys of the map in lexicographic order.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![2, 2], String::from("Hallo"));
    /// assert_eq!(
    ///     map.keys().unwrap(),
    ///     vec![vec![0, 1], vec![1, 2], vec![2, 2]]
    /// );
    /// ```
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        let prefix = Vec::new();
        self.for_each_key(
            |key| {
                keys.push(key.to_vec());
                Ok(())
            },
            prefix,
        )?;
        Ok(keys)
    }

    /// Returns the list of keys of the map having a specified prefix in lexicographic order.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![1, 3], String::from("Hallo"));
    /// assert_eq!(
    ///     map.keys_by_prefix(vec![1]).unwrap(),
    ///     vec![vec![1, 2], vec![1, 3]]
    /// );
    /// ```
    pub fn keys_by_prefix(&self, prefix: Vec<u8>) -> Result<Vec<Vec<u8>>, ViewError> {
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
        )?;
        Ok(keys)
    }

    /// Returns the number of keys of the map.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// map.insert(vec![1, 2], String::from("Bonjour"));
    /// map.insert(vec![2, 2], String::from("Hallo"));
    /// assert_eq!(map.count().unwrap(), 3);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        let prefix = Vec::new();
        self.for_each_key(
            |_key| {
                count += 1;
                Ok(())
            },
            prefix,
        )?;
        Ok(count)
    }

    /// Applies a function f on each key/value pair matching a prefix.
    fn for_each_key_value_or_bytes_while<'a, F>(
        &'a self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], ValueOrBytes<'a, V>) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = self.updates.range(get_key_range_for_prefix(prefix.clone()));
        let mut update = updates.next();
        if !self.deletion_set.contains_prefix_of(&prefix) {
            let iter = self
                .deletion_set
                .deleted_prefixes
                .range(get_key_range_for_prefix(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = self.context.base_key().base_index(&prefix);
            for (index, bytes) in self.context.store().find_key_values_by_prefix(&base)? {
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

    /// Applies a function f on each index/value pair matching a prefix.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
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
    /// .unwrap();
    /// assert_eq!(part_keys.len(), 2);
    /// ```
    pub fn for_each_key_value_while<'a, F>(
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
    }

    /// Applies a function f on each key/value pair matching a prefix.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
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
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_key_value<'a, F>(&'a self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
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
    }
}

impl<C, V> SyncByteMapView<C, V>
where
    C: SyncContext,
    V: Clone + Send + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of keys and values of the map matching a prefix in lexicographic order.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![1, 2], String::from("Hello"));
    /// let prefix = vec![1];
    /// assert_eq!(
    ///     map.key_values_by_prefix(prefix).unwrap(),
    ///     vec![(vec![1, 2], String::from("Hello"))]
    /// );
    /// ```
    pub fn key_values_by_prefix(&self, prefix: Vec<u8>) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
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
        )?;
        Ok(key_values)
    }

    /// Returns the list of keys and values of the map in lexicographic order.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![1, 2], String::from("Hello"));
    /// assert_eq!(
    ///     map.key_values().unwrap(),
    ///     vec![(vec![1, 2], String::from("Hello"))]
    /// );
    /// ```
    pub fn key_values(&self) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
        self.key_values_by_prefix(Vec::new())
    }
}

impl<C, V> SyncByteMapView<C, V>
where
    C: SyncContext,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position. Default value if the index is missing.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncByteMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncByteMapView::load(context).unwrap();
    /// map.insert(vec![0, 1], String::from("Hello"));
    /// assert_eq!(map.get_mut_or_default(&[7]).unwrap(), "");
    /// let value = map.get_mut_or_default(&[0, 1]).unwrap();
    /// assert_eq!(*value, String::from("Hello"));
    /// *value = String::from("Hola");
    /// assert_eq!(map.get(&[0, 1]).unwrap(), Some(String::from("Hola")));
    /// ```
    pub fn get_mut_or_default(&mut self, short_key: &[u8]) -> Result<&mut V, ViewError> {
        let update = match self.updates.entry(short_key.to_vec()) {
            Entry::Vacant(e) if self.deletion_set.contains_prefix_of(short_key) => {
                e.insert(Update::Set(V::default()))
            }
            Entry::Vacant(e) => {
                let key = self.context.base_key().base_index(short_key);
                let value = self.context.store().read_value(&key)?.unwrap_or_default();
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

/// A `SyncView` that has a type for keys. The ordering of the entries
/// is determined by the serialization of the context.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, V: Allocative")]
pub struct SyncMapView<C, I, V> {
    /// The underlying map storing entries with serialized keys.
    map: SyncByteMapView<C, V>,
    /// Phantom data for the key type.
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<C, I, V> SyncView for SyncMapView<C, I, V>
where
    C: SyncContext,
    I: Send + Sync,
    V: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = SyncByteMapView::<C, V>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = SyncByteMapView::post_load(context, values)?;
        Ok(SyncMapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.map.post_save()
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> SyncMapView<C, I, V>
where
    C: SyncContext,
    I: Serialize,
{
    /// Inserts or resets a value at an index.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, _> = SyncMapView::load(context).unwrap();
    /// map.insert(&(24 as u32), String::from("Hello")).unwrap();
    /// assert_eq!(
    ///     map.get(&(24 as u32)).unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then the operation does nothing.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncMapView::<_, u32, String>::load(context).unwrap();
    /// map.remove(&(37 as u32)).unwrap();
    /// assert_eq!(map.get(&(37 as u32)).unwrap(), None);
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncMapView::<_, u32, String>::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// assert!(map.contains_key(&(37 as u32)).unwrap());
    /// assert!(!map.contains_key(&(34 as u32)).unwrap());
    /// ```
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.contains_key(&short_key)
    }
}

impl<C, I, V> SyncMapView<C, I, V>
where
    C: SyncContext,
    I: Serialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, _> = SyncMapView::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// assert_eq!(
    ///     map.get(&(37 as u32)).unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// assert_eq!(map.get(&(34 as u32)).unwrap(), None);
    /// ```
    pub fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.get(&short_key)
    }

    /// Reads values at given positions, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, _> = SyncMapView::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// map.insert(&(49 as u32), String::from("Bonjour")).unwrap();
    /// assert_eq!(
    ///     map.multi_get(&[37 as u32, 49 as u32, 64 as u32]).unwrap(),
    ///     [
    ///         Some(String::from("Hello")),
    ///         Some(String::from("Bonjour")),
    ///         None
    ///     ]
    /// );
    /// ```
    pub fn multi_get<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<V>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| BaseKey::derive_short_key(index))
            .collect::<Result<_, _>>()?;
        self.map.multi_get(short_keys)
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, String> = SyncMapView::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// assert_eq!(map.get_mut(&(34 as u32)).unwrap(), None);
    /// let value = map.get_mut(&(37 as u32)).unwrap().unwrap();
    /// *value = String::from("Hola");
    /// assert_eq!(
    ///     map.get(&(37 as u32)).unwrap(),
    ///     Some(String::from("Hola"))
    /// );
    /// ```
    pub fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.get_mut(&short_key)
    }
}

impl<C, I, V> SyncMapView<C, I, V>
where
    C: SyncContext,
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, u128> = SyncMapView::load(context).unwrap();
    /// let value = map.get_mut_or_default(&(34 as u32)).unwrap();
    /// assert_eq!(*value, 0 as u128);
    /// ```
    pub fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.map.get_mut_or_default(&short_key)
    }
}

impl<C, I, V> SyncMapView<C, I, V>
where
    C: SyncContext,
    I: Serialize + DeserializeOwned + Send,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    /// Applies a function on each index/value pair. Indices and values are
    /// visited in an order determined by serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, Vec<u8>, _> = SyncMapView::load(context).unwrap();
    /// map.insert(&vec![0, 1], String::from("Hello")).unwrap();
    /// let mut count = 0;
    /// map.for_each_index_value(|_index, _value| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_index_value<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map.for_each_key_value(
            |key, value| {
                let index = BaseKey::deserialize_value(key)?;
                f(index, value)
            },
            prefix,
        )?;
        Ok(())
    }

    /// Obtains all the `(index,value)` pairs.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, String, _> = SyncMapView::load(context).unwrap();
    /// map.insert("Italian", String::from("Ciao")).unwrap();
    /// let index_values = map.index_values().unwrap();
    /// assert_eq!(
    ///     index_values,
    ///     vec![("Italian".to_string(), "Ciao".to_string())]
    /// );
    /// ```
    pub fn index_values(&self) -> Result<Vec<(I, V)>, ViewError> {
        let mut key_values = Vec::new();
        self.for_each_index_value(|index, value| {
            let value = value.into_owned();
            key_values.push((index, value));
            Ok(())
        })?;
        Ok(key_values)
    }

    /// Returns the list of keys of the map in the order determined by the serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, String> = SyncMapView::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// map.insert(&(49 as u32), String::from("Bonjour")).unwrap();
    /// assert_eq!(map.indices().unwrap(), vec![37, 49]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.map.for_each_key(
            |key| {
                indices.push(bcs::from_bytes(key)?);
                Ok(())
            },
            Vec::new(),
        )?;
        Ok(indices)
    }

    /// Returns the number of keys of the map.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncMapView<_, u32, String> = SyncMapView::load(context).unwrap();
    /// map.insert(&(37 as u32), String::from("Hello")).unwrap();
    /// map.insert(&(49 as u32), String::from("Bonjour")).unwrap();
    /// assert_eq!(map.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.map.count()
    }
}

/// A map view that uses custom serialization for keys.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, V: Allocative")]
pub struct SyncCustomMapView<C, I, V> {
    /// The underlying map storing entries with custom-serialized keys.
    map: SyncByteMapView<C, V>,
    /// Phantom data for the key type.
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<C, I, V> SyncView for SyncCustomMapView<C, I, V>
where
    C: SyncContext,
    I: CustomSerialize + Send + Sync,
    V: Serialize + Clone + Send + Sync,
{
    const NUM_INIT_KEYS: usize = SyncByteMapView::<C, V>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = SyncByteMapView::post_load(context, values)?;
        Ok(SyncCustomMapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.map.post_save()
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C: SyncContext, I: CustomSerialize, V> SyncCustomMapView<C, I, V> {
    /// Inserts or resets a value.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map: SyncCustomMapView<_, u128, _> = SyncCustomMapView::load(context).unwrap();
    /// map.insert(&(24 as u128), String::from("Hello")).unwrap();
    /// assert_eq!(
    ///     map.get(&(24 as u128)).unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then this does not do anything.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(37 as u128), String::from("Hello")).unwrap();
    /// map.remove(&(37 as u128)).unwrap();
    /// assert_eq!(map.get(&(37 as u128)).unwrap(), None);
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(24 as u128), String::from("Hello")).unwrap();
    /// assert!(map.contains_key(&(24 as u128)).unwrap());
    /// assert!(!map.contains_key(&(23 as u128)).unwrap());
    /// ```
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.contains_key(&short_key)
    }
}

impl<C, I, V> SyncCustomMapView<C, I, V>
where
    C: SyncContext,
    I: CustomSerialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// assert_eq!(
    ///     map.get(&(34 as u128)).unwrap(),
    ///     Some(String::from("Hello"))
    /// );
    /// ```
    pub fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get(&short_key)
    }

    /// Read values at several positions, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(12 as u128), String::from("Hi")).unwrap();
    /// assert_eq!(
    ///     map.multi_get(&[34 as u128, 12 as u128, 89 as u128])
    ///         .unwrap(),
    ///     [Some(String::from("Hello")), Some(String::from("Hi")), None]
    /// );
    /// ```
    pub fn multi_get<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<V>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| index.to_custom_bytes())
            .collect::<Result<_, _>>()?;
        self.map.multi_get(short_keys)
    }

    /// Read index-value pairs at several positions, if any.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(12 as u128), String::from("Hi")).unwrap();
    /// assert_eq!(
    ///     map.multi_get_pairs([34 as u128, 12 as u128, 89 as u128])
    ///         .unwrap(),
    ///     vec![
    ///         (34 as u128, Some(String::from("Hello"))),
    ///         (12 as u128, Some(String::from("Hi"))),
    ///         (89 as u128, None)
    ///     ]
    /// );
    /// ```
    pub fn multi_get_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<V>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + Clone,
    {
        let indices_vec = indices.into_iter().collect::<Vec<Q>>();
        let values = self.multi_get(indices_vec.iter())?;
        Ok(indices_vec.into_iter().zip(values).collect())
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// let value = map.get_mut(&(34 as u128)).unwrap().unwrap();
    /// *value = String::from("Hola");
    /// assert_eq!(
    ///     map.get(&(34 as u128)).unwrap(),
    ///     Some(String::from("Hola"))
    /// );
    /// ```
    pub fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut(&short_key)
    }
}

impl<C, I, V> SyncCustomMapView<C, I, V>
where
    C: SyncContext,
    I: CustomSerialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, String>::load(context).unwrap();
    /// map.insert(&(24 as u128), String::from("Hello")).unwrap();
    /// assert_eq!(
    ///     *map.get_mut_or_default(&(34 as u128)).unwrap(),
    ///     String::new()
    /// );
    /// ```
    pub fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut_or_default(&short_key)
    }
}

impl<C, I, V> SyncCustomMapView<C, I, V>
where
    C: SyncContext,
    I: Send + CustomSerialize,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    /// Returns the list of indices in the map. The order is determined
    /// by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(37 as u128), String::from("Bonjour")).unwrap();
    /// assert_eq!(map.indices().unwrap(), vec![34 as u128, 37 as u128]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function returns false,
    /// then the loop ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(37 as u128), String::from("Hola")).unwrap();
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index_while(|index| {
    ///     indices.push(index);
    ///     Ok(indices.len() < 5)
    /// })
    /// .unwrap();
    /// assert_eq!(indices.len(), 2);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map.for_each_key_while(
            |key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            },
            prefix,
        )?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(37 as u128), String::from("Hola")).unwrap();
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(indices, vec![34, 37]);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map.for_each_key(
            |key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            },
            prefix,
        )?;
        Ok(())
    }

    /// Applies a function f on the index/value pairs. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// If the function returns false, then the loop ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, String>::load(context)
    ///     .unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(37 as u128), String::from("Hola")).unwrap();
    /// let mut values = Vec::new();
    /// map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 5)
    /// })
    /// .unwrap();
    /// assert_eq!(values.len(), 2);
    /// ```
    pub fn for_each_index_value_while<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map.for_each_key_value_while(
            |key, value| {
                let index = I::from_custom_bytes(key)?;
                f(index, value)
            },
            prefix,
        )?;
        Ok(())
    }

    /// Applies a function f on each index/value pair. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(34 as u128), String::from("Hello")).unwrap();
    /// map.insert(&(37 as u128), String::from("Hola")).unwrap();
    /// let mut indices = Vec::<u128>::new();
    /// map.for_each_index_value(|index, _value| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(indices, vec![34, 37]);
    /// ```
    pub fn for_each_index_value<'a, F>(&'a self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map.for_each_key_value(
            |key, value| {
                let index = I::from_custom_bytes(key)?;
                f(index, value)
            },
            prefix,
        )?;
        Ok(())
    }

    /// Obtains all the `(index,value)` pairs.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, _>::load(context).unwrap();
    /// map.insert(&(24 as u128), String::from("Ciao")).unwrap();
    /// let index_values = map.index_values().unwrap();
    /// assert_eq!(index_values, vec![(24 as u128, "Ciao".to_string())]);
    /// ```
    pub fn index_values(&self) -> Result<Vec<(I, V)>, ViewError> {
        let mut key_values = Vec::new();
        self.for_each_index_value(|index, value| {
            let value = value.into_owned();
            key_values.push((index, value));
            Ok(())
        })?;
        Ok(key_values)
    }

    /// Obtains the number of entries in the map.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::map_view::SyncCustomMapView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut map = SyncCustomMapView::<_, u128, String>::load(context).unwrap();
    /// map.insert(&(24 as u128), String::from("Ciao")).unwrap();
    /// assert_eq!(map.count().unwrap(), 1);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.map.count()
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::{SyncByteMapView, SyncCustomMapView, SyncMapView};
    use crate::{
        context::SyncContext,
        graphql::{hash_name, mangle, Entry, MapInput},
    };

    impl<C: Send + Sync, V: async_graphql::OutputType> async_graphql::TypeName
        for SyncByteMapView<C, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncByteMapView_{}_{:08x}",
                mangle(V::type_name()),
                hash_name::<V>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, V> SyncByteMapView<C, V>
    where
        C: SyncContext + Send + Sync,
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
            let keys = self.keys()?;
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
                value: self.get(&key)?,
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
                self.keys()?
            };

            let mut entries = vec![];
            for key in keys {
                entries.push(Entry {
                    value: self.get(&key)?,
                    key,
                })
            }

            Ok(entries)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for SyncMapView<C, I, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncMapView_{}_{}_{:08x}",
                mangle(I::type_name()),
                mangle(V::type_name()),
                hash_name::<(I, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I, V> SyncMapView<C, I, V>
    where
        C: SyncContext + Send + Sync,
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
            let indices = self.indices()?;
            let it = indices.iter().cloned();
            Ok(if let Some(count) = count {
                it.take(count).collect()
            } else {
                it.collect()
            })
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }

        async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
            Ok(Entry {
                value: self.get(&key)?,
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
                self.indices()?
            };

            let values = self.multi_get(&keys)?;
            Ok(values
                .into_iter()
                .zip(keys)
                .map(|(value, key)| Entry { value, key })
                .collect())
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for SyncCustomMapView<C, I, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncCustomMapView_{}_{}_{:08x}",
                mangle(I::type_name()),
                mangle(V::type_name()),
                hash_name::<(I, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I, V> SyncCustomMapView<C, I, V>
    where
        C: SyncContext + Send + Sync,
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
            let indices = self.indices()?;
            let it = indices.iter().cloned();
            Ok(if let Some(count) = count {
                it.take(count).collect()
            } else {
                it.collect()
            })
        }

        async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
            Ok(Entry {
                value: self.get(&key)?,
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
                self.indices()?
            };

            let values = self.multi_get(&keys)?;
            Ok(values
                .into_iter()
                .zip(keys)
                .map(|(value, key)| Entry { value, key })
                .collect())
        }
    }
}
