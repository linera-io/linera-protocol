// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    common::CustomSerialize,
    context::Context,
    sync_view::{block_on, SyncClonableView, SyncHashableView, SyncReplaceContext, SyncView},
    views::{ClonableView as _, HashableView as _, ReplaceContext as _, View as _},
    ViewError,
};

/// A synchronous view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug)]
pub struct ByteMapView<C, V> {
    inner: crate::views::map_view::ByteMapView<C, V>,
}

impl<C, V> ByteMapView<C, V> {
    /// Inserts or updates a value.
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        self.inner.insert(short_key, value);
    }

    /// Removes a value.
    pub fn remove(&mut self, short_key: Vec<u8>) {
        self.inner.remove(short_key);
    }

    /// Removes all values with a common prefix.
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) {
        self.inner.remove_by_prefix(key_prefix);
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.contains_key(short_key))
    }

    /// Reads a value.
    pub fn get(&self, short_key: &[u8]) -> Result<Option<V>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get(short_key))
    }

    /// Reads multiple values.
    pub fn multi_get(&self, short_keys: Vec<Vec<u8>>) -> Result<Vec<Option<V>>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get(short_keys))
    }

    /// Reads multiple values and keys.
    pub fn multi_get_pairs(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<V>)>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get_pairs(short_keys))
    }

    /// Reads a mutable value.
    pub fn get_mut(&mut self, short_key: &[u8]) -> Result<Option<&mut V>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut(short_key))
    }

    /// Iterates over keys while `f` returns `true`.
    pub fn for_each_key_while<F>(&self, f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_key_while(f, prefix))
    }

    /// Iterates over keys.
    pub fn for_each_key<F>(&self, f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_key(f, prefix))
    }

    /// Reads all keys.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.keys())
    }

    /// Reads all keys with a prefix.
    pub fn keys_by_prefix(&self, prefix: Vec<u8>) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.keys_by_prefix(prefix))
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.count())
    }

    /// Iterates over key/value pairs while `f` returns `true`.
    pub fn for_each_key_value_while<'a, F>(
        &'a self,
        f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(&[u8], std::borrow::Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_key_value_while(f, prefix))
    }

    /// Iterates over key/value pairs.
    pub fn for_each_key_value<'a, F>(
        &'a self,
        f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(&[u8], std::borrow::Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_key_value(f, prefix))
    }

    /// Reads all key/value pairs for a prefix.
    pub fn key_values_by_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, V)>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.key_values_by_prefix(prefix))
    }

    /// Reads all key/value pairs.
    pub fn key_values(&self) -> Result<Vec<(Vec<u8>, V)>, ViewError>
    where
        C: Context,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.key_values())
    }

    /// Returns a mutable value, inserting the default if missing.
    pub fn get_mut_or_default(&mut self, short_key: &[u8]) -> Result<&mut V, ViewError>
    where
        C: Context,
        V: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut_or_default(short_key))
    }
}

impl<C, V> SyncView for ByteMapView<C, V>
where
    C: Context,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::map_view::ByteMapView<C, V> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::map_view::ByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::map_view::ByteMapView::<C, V>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::map_view::ByteMapView::<C, V>::load(context))?;
        Ok(Self { inner })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    fn has_pending_changes(&self) -> bool {
        block_on(self.inner.has_pending_changes())
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn pre_save(&self, batch: &mut crate::batch::Batch) -> Result<bool, ViewError> {
        self.inner.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.inner.post_save();
    }
}

impl<C, V, C2> SyncReplaceContext<C2> for ByteMapView<C, V>
where
    C: Context,
    C2: Context,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = ByteMapView<C2, V>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        ByteMapView { inner }
    }
}

impl<C, V> SyncClonableView for ByteMapView<C, V>
where
    C: Context,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, V> SyncHashableView for ByteMapView<C, V>
where
    C: Context,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Hasher = <crate::views::map_view::ByteMapView<C, V> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, V> Deref for ByteMapView<C, V> {
    type Target = crate::views::map_view::ByteMapView<C, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, V> DerefMut for ByteMapView<C, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous map view with BCS-serialized keys.
#[derive(Debug)]
pub struct MapView<C, K, V> {
    inner: crate::views::map_view::MapView<C, K, V>,
}

impl<C, K, V> MapView<C, K, V> {
    /// Inserts or updates a value.
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        K: Serialize,
        Q: Serialize + ?Sized,
    {
        self.inner.insert(index, value)
    }

    /// Removes a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        K: Serialize,
        Q: Serialize + ?Sized,
    {
        self.inner.remove(index)
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.contains_key(index))
    }

    /// Reads a value.
    pub fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get(index))
    }

    /// Reads multiple values.
    pub fn multi_get<'a, Q>(&self, indices: Vec<&'a Q>) -> Result<Vec<Option<V>>, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + ?Sized + 'a,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get(indices))
    }

    /// Reads multiple values and keys.
    pub fn multi_get_pairs<Q>(
        &self,
        indices: Vec<Q>,
    ) -> Result<Vec<(Q, Option<V>)>, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + Clone,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get_pairs(indices))
    }

    /// Reads a mutable value.
    pub fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut(index))
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<K>, ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.indices())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }

    /// Iterates over index/value pairs while `f` returns `true`.
    pub fn for_each_index_value_while<'a, F>(&'a self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K, std::borrow::Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value_while(f))
    }

    /// Iterates over index/value pairs.
    pub fn for_each_index_value<'a, F>(&'a self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K, std::borrow::Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value(f))
    }

    /// Reads all index/value pairs.
    pub fn index_values(&self) -> Result<Vec<(K, V)>, ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.index_values())
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        K: Serialize + DeserializeOwned,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.count())
    }

    /// Returns a mutable value, inserting the default if missing.
    pub fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        C: Context,
        K: Serialize,
        Q: Serialize + ?Sized,
        V: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut_or_default(index))
    }
}

impl<C, K, V> SyncView for MapView<C, K, V>
where
    C: Context,
    K: Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::map_view::MapView<C, K, V> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::map_view::MapView::<C, K, V>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::map_view::MapView::<C, K, V>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::map_view::MapView::<C, K, V>::load(context))?;
        Ok(Self { inner })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    fn has_pending_changes(&self) -> bool {
        block_on(self.inner.has_pending_changes())
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn pre_save(&self, batch: &mut crate::batch::Batch) -> Result<bool, ViewError> {
        self.inner.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.inner.post_save();
    }
}

impl<C, K, V, C2> SyncReplaceContext<C2> for MapView<C, K, V>
where
    C: Context,
    C2: Context,
    K: Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = MapView<C2, K, V>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        MapView { inner }
    }
}

impl<C, K, V> SyncClonableView for MapView<C, K, V>
where
    C: Context,
    K: Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, K, V> SyncHashableView for MapView<C, K, V>
where
    C: Context,
    K: Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Hasher = <crate::views::map_view::MapView<C, K, V> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, K, V> Deref for MapView<C, K, V> {
    type Target = crate::views::map_view::MapView<C, K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, K, V> DerefMut for MapView<C, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous map view with custom key serialization.
#[derive(Debug)]
pub struct CustomMapView<C, K, V> {
    inner: crate::views::map_view::CustomMapView<C, K, V>,
}

impl<C, K, V> CustomMapView<C, K, V> {
    /// Inserts or updates a value.
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
    {
        self.inner.insert(index, value)
    }

    /// Removes a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
    {
        self.inner.remove(index)
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.contains_key(index))
    }

    /// Reads a value.
    pub fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get(index))
    }

    /// Reads multiple values.
    pub fn multi_get<'a, Q>(&self, indices: Vec<&'a Q>) -> Result<Vec<Option<V>>, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized + 'a,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get(indices))
    }

    /// Reads multiple values and keys.
    pub fn multi_get_pairs<Q>(
        &self,
        indices: Vec<Q>,
    ) -> Result<Vec<(Q, Option<V>)>, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + Clone,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.multi_get_pairs(indices))
    }

    /// Reads a mutable value.
    pub fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut(index))
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<K>, ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.indices())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }

    /// Iterates over index/value pairs while `f` returns `true`.
    pub fn for_each_index_value_while<'a, F>(&'a self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K, std::borrow::Cow<'a, V>) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value_while(f))
    }

    /// Iterates over index/value pairs.
    pub fn for_each_index_value<'a, F>(&'a self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
        F: FnMut(K, std::borrow::Cow<'a, V>) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value(f))
    }

    /// Reads all index/value pairs.
    pub fn index_values(&self) -> Result<Vec<(K, V)>, ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.index_values())
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        K: CustomSerialize + DeserializeOwned + Send + Sync,
        V: Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.count())
    }

    /// Returns a mutable value, inserting the default if missing.
    pub fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        C: Context,
        K: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
        V: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
    {
        block_on(self.inner.get_mut_or_default(index))
    }
}

impl<C, K, V> SyncView for CustomMapView<C, K, V>
where
    C: Context,
    K: CustomSerialize + DeserializeOwned + Send + Sync,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::map_view::CustomMapView<C, K, V> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::map_view::CustomMapView::<C, K, V>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::map_view::CustomMapView::<C, K, V>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::map_view::CustomMapView::<C, K, V>::load(context))?;
        Ok(Self { inner })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    fn has_pending_changes(&self) -> bool {
        block_on(self.inner.has_pending_changes())
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn pre_save(&self, batch: &mut crate::batch::Batch) -> Result<bool, ViewError> {
        self.inner.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.inner.post_save();
    }
}

impl<C, K, V, C2> SyncReplaceContext<C2> for CustomMapView<C, K, V>
where
    C: Context,
    C2: Context,
    K: CustomSerialize + DeserializeOwned + Send + Sync,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = CustomMapView<C2, K, V>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        CustomMapView { inner }
    }
}

impl<C, K, V> SyncClonableView for CustomMapView<C, K, V>
where
    C: Context,
    K: CustomSerialize + DeserializeOwned + Send + Sync,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, K, V> SyncHashableView for CustomMapView<C, K, V>
where
    C: Context,
    K: CustomSerialize + DeserializeOwned + Send + Sync,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Hasher = <crate::views::map_view::CustomMapView<C, K, V> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, K, V> Deref for CustomMapView<C, K, V> {
    type Target = crate::views::map_view::CustomMapView<C, K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, K, V> DerefMut for CustomMapView<C, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for maps with a memoized hash.
pub type HashedMapView<C, K, V> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, MapView<C, K, V>, crate::common::HasherOutput>;

/// A view for byte maps with a memoized hash.
pub type HashedByteMapView<C, V> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, ByteMapView<C, V>, crate::common::HasherOutput>;

/// A view for maps with custom key serialization and a memoized hash.
pub type HashedCustomMapView<C, K, V> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    CustomMapView<C, K, V>,
    crate::common::HasherOutput,
>;
