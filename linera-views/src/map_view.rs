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

use crate::{
    batch::Batch,
    common::{
        contains_key, get_interval, insert_key_prefix, Context, CustomSerialize, HasherOutput,
        KeyIterable, KeyValueIterable, SuffixClosedSetIterator, Update, MIN_VIEW_TAG,
    },
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    marker::PhantomData,
    mem,
};

/// Key tags to create the sub-keys of a MapView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

/// A view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug)]
pub struct ByteMapView<C, V> {
    context: C,
    delete_storage_first: bool,
    updates: BTreeMap<Vec<u8>, Update<V>>,
    deleted_prefixes: BTreeSet<Vec<u8>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C, V> View<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_value(&key).await?;
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: BTreeMap::new(),
            deleted_prefixes: BTreeSet::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
        self.deleted_prefixes.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value(key, &value)?;
                }
            }
            self.stored_hash = None;
        } else {
            for index in mem::take(&mut self.deleted_prefixes) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                batch.delete_key_prefix(key);
            }
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value(key, &value)?,
                }
            }
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
        self.delete_storage_first = false;
        Ok(())
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.clear();
        self.deleted_prefixes.clear();
        *self.hash.get_mut() = None;
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.keys().await.unwrap(), vec![vec![0,1]]);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        *self.hash.get_mut() = None;
        self.updates.insert(short_key, Update::Set(value));
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], "Hello");
    ///   map.remove(vec![0,1]);
    /// # })
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![0,2], String::from("Bonjour"));
    ///   map.remove_by_prefix(vec![0]);
    ///   assert!(map.keys().await.unwrap().is_empty());
    /// # })
    /// ```
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) {
        *self.hash.get_mut() = None;
        let key_list = self
            .updates
            .range(get_interval(key_prefix.clone()))
            .map(|x| x.0.to_vec())
            .collect::<Vec<_>>();
        for key in key_list {
            self.updates.remove(&key);
        }
        if !self.delete_storage_first {
            insert_key_prefix(&mut self.deleted_prefixes, key_prefix);
        }
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hello")));
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
        if self.delete_storage_first {
            return Ok(None);
        }
        if contains_key(&self.deleted_prefixes, short_key) {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
        Ok(self.context.read_value(&key).await?)
    }

    /// Loads the value in updates if that is at all possible.
    async fn load_value(&mut self, short_key: &[u8]) -> Result<(), ViewError> {
        if !self.delete_storage_first && !self.updates.contains_key(short_key) {
            let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
            let value = self.context.read_value(&key).await?;
            if let Some(value) = value {
                self.updates.insert(short_key.to_vec(), Update::Set(value));
            }
        }
        Ok(())
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let value = map.get_mut(vec![0,1]).await.unwrap().unwrap();
    ///   assert_eq!(*value, String::from("Hello"));
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut(&mut self, short_key: Vec<u8>) -> Result<Option<&mut V>, ViewError> {
        self.load_value(&short_key).await?;
        if let Some(update) = self.updates.get_mut(&short_key) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value),
            };
            return Ok(value);
        }
        Ok(None)
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Applies the function f on each index (aka key) which has the assigned prefix.
    /// Keys are visited in the lexicographic order. The shortened key is send to the
    /// function and if it returns false, then the loop exits
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Bonjour"));
    ///   let prefix = vec![1];
    ///   let mut count = 0;
    ///   map.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 3)
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let iter = self.deleted_prefixes.range(get_interval(prefix.clone()));
        let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
        let mut updates = self.updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !self.delete_storage_first && !contains_key(&self.deleted_prefixes, &prefix) {
            let base = self.context.base_tag_index(KeyTag::Index as u8, &prefix);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   let prefix = Vec::new();
    ///   map.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 1);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![2,2], String::from("Hallo"));
    ///   assert_eq!(map.keys().await.unwrap(), vec![vec![0,1], vec![1,2], vec![2,2]]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Hallo"));
    ///   assert_eq!(map.keys_by_prefix(vec![1]).await.unwrap(), vec![vec![1,2], vec![1,3]]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![2,2], String::from("Hallo"));
    ///   assert_eq!(map.count().await.unwrap(), 3);
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

    /// Applies a function f on each index/value pair matching a prefix. Keys
    /// and values are visited in the lexicographic order. The shortened index
    /// is send to the function f and if it returns false then the loop ends
    /// prematurely
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Hallo"));
    ///   let mut part_keys = Vec::new();
    ///   let prefix = vec![1];
    ///   map.for_each_key_value_while(|key, _value| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 2)
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(part_keys.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_key_value_while<F>(
        &self,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let iter = self.deleted_prefixes.range(get_interval(prefix.clone()));
        let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
        let mut updates = self.updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !self.delete_storage_first && !contains_key(&self.deleted_prefixes, &prefix) {
            let base = self.context.base_tag_index(KeyTag::Index as u8, &prefix);
            for entry in self
                .context
                .find_key_values_by_prefix(&base)
                .await?
                .iterator()
            {
                let (index, bytes) = entry?;
                loop {
                    match update {
                        Some((key, value)) if &key[prefix_len..] <= index => {
                            if let Update::Set(value) = value {
                                let bytes = bcs::to_bytes(value)?;
                                if !f(&key[prefix_len..], &bytes)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if &key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(index) && !f(index, bytes)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                let bytes = bcs::to_bytes(value)?;
                if !f(&key[prefix_len..], &bytes)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies a function f on each key/value pair matching a prefix. The shortened
    /// key and value are send to the function f. Keys and values are visited in the
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   let prefix = Vec::new();
    ///   map.for_each_key_value(|_key, _value| {
    ///     count += 1;
    ///     Ok(())
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key_value<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
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

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        let prefix = Vec::new();
        self.for_each_key_value(
            |index, value| {
                count += 1;
                hasher.update_with_bytes(index)?;
                hasher.update_with_bytes(value)?;
                Ok(())
            },
            prefix,
        )
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Sync + Send + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of keys and values of the map matching a prefix
    /// in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![1,2], String::from("Hello"));
    ///   let prefix = vec![1];
    ///   assert_eq!(map.key_values_by_prefix(prefix).await.unwrap(), vec![(vec![1,2], String::from("Hello"))]);
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
                let value = bcs::from_bytes(value)?;
                let mut big_key = prefix.clone();
                big_key.extend(key);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![1,2], String::from("Hello"));
    ///   assert_eq!(map.key_values().await.unwrap(), vec![(vec![1,2], String::from("Hello"))]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.get_mut_or_default(vec![7]).await.unwrap(), "");
    ///   let value = map.get_mut_or_default(vec![0,1]).await.unwrap();
    ///   assert_eq!(*value, String::from("Hello"));
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut_or_default(&mut self, short_key: Vec<u8>) -> Result<&mut V, ViewError> {
        use std::collections::btree_map::Entry;

        let update = match self.updates.entry(short_key.clone()) {
            Entry::Vacant(e) if self.delete_storage_first => e.insert(Update::Set(V::default())),
            Entry::Vacant(e) => {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
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
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
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
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.map.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let map = ByteMapView::load(context).await?;
        Ok(MapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u32,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(24 as u32), String::from("Hello"));
    ///   assert_eq!(map.get(&(24 as u32)).await.unwrap(), Some(String::from("Hello")));
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u32,String>::load(context).await.unwrap();
    ///   map.remove(&(37 as u32));
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), None);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u32,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), Some(String::from("Hello")));
    ///   assert_eq!(map.get(&(34 as u32)).await.unwrap(), None);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u32,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.get_mut(&(34 as u32)).await.unwrap(), None);
    ///   let value = map.get_mut(&(37 as u32)).await.unwrap().unwrap();
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut(short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Sync + Send + Serialize + DeserializeOwned,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u32,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.indices().await.unwrap(), vec![37 as u32]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Thanks"));
    ///   map.insert(&(37 as u128), String::from("Spasiba"));
    ///   map.insert(&(38 as u128), String::from("Merci"));
    ///   let mut count = 0;
    ///   map.for_each_index_while(|_index| {
    ///     count += 1;
    ///     Ok(count < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   let mut count = 0;
    ///   map.for_each_index(|_index| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Thanks"));
    ///   map.insert(&(37 as u128), String::from("Spasiba"));
    ///   map.insert(&(38 as u128), String::from("Merci"));
    ///   let mut values = Vec::new();
    ///   map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, bytes| {
                    let index = C::deserialize_value(key)?;
                    let value = C::deserialize_value(bytes)?;
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,Vec<u8>,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   map.for_each_index_value(|_index, _value| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, bytes| {
                    let index = C::deserialize_value(key)?;
                    let value = C::deserialize_value(bytes)?;
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
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u32,u128> = MapView::load(context).await.unwrap();
    ///   let value = map.get_mut_or_default(&(34 as u32)).await.unwrap();
    ///   assert_eq!(*value, 0 as u128);
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut_or_default(short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
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
    V: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.map.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let map = ByteMapView::load(context).await?;
        Ok(CustomMapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u128,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(24 as u128), String::from("Hello"));
    ///   assert_eq!(map.get(&(24 as u128)).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then this does not do anything.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u128,String>::load(context).await.unwrap();
    ///   map.remove(&(37 as u128));
    ///   assert_eq!(map.get(&(37 as u128)).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::memory::MemoryContext;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<MemoryContext<()>, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   assert_eq!(map.get(&(34 as u128)).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get(&short_key).await
    }

    /// Obtains a mutable reference to a value at a given position if available
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   let value = map.get_mut(&(34 as u128)).await.unwrap().unwrap();
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&(34 as u128)).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut(short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Sync + Send + CustomSerialize,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined
    /// by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u128,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Bonjour"));
    ///   assert_eq!(map.indices().await.unwrap(), vec![34 as u128,37 as u128]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index_while(|index| {
    ///     indices.push(index);
    ///     Ok(indices.len() < 5)
    ///   }).await.unwrap();
    ///   assert_eq!(indices.len(), 2);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![34,37]);
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::<_,u128,String>::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut values = Vec::new();
    ///   map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 5)
    ///   }).await.unwrap();
    ///   assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, bytes| {
                    let index = I::from_custom_bytes(key)?;
                    let value = C::deserialize_value(bytes)?;
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index_value(|index, _value| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![34,37]);
    /// # })
    /// ```
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, bytes| {
                    let index = I::from_custom_bytes(key)?;
                    let value = C::deserialize_value(bytes)?;
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
    I: CustomSerialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   assert_eq!(*map.get_mut_or_default(&(34 as u128)).await.unwrap(), String::new());
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut_or_default(short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

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
