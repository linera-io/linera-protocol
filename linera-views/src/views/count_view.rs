// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Count-tracking variants of map/set/collection views.

use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option_or_default,
    context::Context,
    views::{
        collection_view::CollectionView, map_view::MapView, set_view::SetView, ClonableView,
        HashableView, Hasher, ReplaceContext, View, ViewError, MIN_VIEW_TAG,
    },
};

/// Key tags used by count views.
#[repr(u8)]
enum KeyTag {
    /// Stores the persisted count.
    Count = MIN_VIEW_TAG,
    /// Prefix for the inner subview.
    Subview,
}

/// A map view variant with O(1) `count`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, V: Allocative")]
pub struct MapCountView<C, I, V> {
    map: MapView<C, I, V>,
    stored_count: usize,
    count: usize,
}

impl<C, C2, I, V> ReplaceContext<C2> for MapCountView<C, I, V>
where
    C: Context,
    C2: Context,
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize + Clone,
{
    type Target = MapCountView<C2, I, V>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        let old_context = self.context();
        let context = ctx(&old_context);
        let subview_base = context.base_key().base_tag(KeyTag::Subview as u8);
        let subview_context = context.clone_with_base_key(subview_base);
        MapCountView {
            map: self.map.with_context(|_| subview_context.clone()).await,
            stored_count: self.stored_count,
            count: self.count,
        }
    }
}

impl<C, I, V> View for MapCountView<C, I, V>
where
    C: Context,
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        // The inner context has our base key + the KeyTag::Subview byte.
        self.map.context().clone_with_trimmed_key(1)
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Count as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_count =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let subview_base = context.base_key().base_tag(KeyTag::Subview as u8);
        let subview_context = context.clone_with_base_key(subview_base);
        let map = MapView::post_load(subview_context, &[])?;
        Ok(Self {
            map,
            stored_count,
            count: stored_count,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback();
        self.count = self.stored_count;
    }

    async fn has_pending_changes(&self) -> bool {
        self.count != self.stored_count || self.map.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let delete_subview = self.map.pre_save(batch)?;
        if self.count != self.stored_count {
            let count_key = self.context().base_key().base_tag(KeyTag::Count as u8);
            if self.count == 0 {
                batch.delete_key(count_key);
            } else {
                batch.put_key_value(count_key, &self.count)?;
            }
        }
        Ok(delete_subview && self.count == 0)
    }

    fn post_save(&mut self) {
        self.map.post_save();
        self.stored_count = self.count;
    }

    fn clear(&mut self) {
        self.map.clear();
        self.count = 0;
    }
}

impl<C, I, V> ClonableView for MapCountView<C, I, V>
where
    C: Context,
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize + Clone,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(Self {
            map: self.map.clone_unchecked()?,
            stored_count: self.stored_count,
            count: self.count,
        })
    }
}

impl<C, I, V> Deref for MapCountView<C, I, V> {
    type Target = MapView<C, I, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<C, I, V> DerefMut for MapCountView<C, I, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<C, I, V> MapCountView<C, I, V>
where
    C: Context,
    I: Serialize,
{
    /// Inserts or resets a value at an index and updates the count in O(1).
    pub async fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        if !self.map.contains_key(index).await? {
            self.count += 1;
        }
        self.map.insert(index, value)
    }

    /// Removes a value and updates the count in O(1).
    pub async fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        if self.map.contains_key(index).await? {
            self.count -= 1;
        }
        self.map.remove(index)
    }

    /// Returns the number of entries in O(1).
    pub async fn count(&self) -> Result<usize, ViewError> {
        Ok(self.count)
    }
}

impl<C, I, V> MapCountView<C, I, V>
where
    C: Context,
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position, creating it with default
    /// if missing, and updates the count in O(1).
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized,
    {
        if !self.map.contains_key(index).await? {
            self.count += 1;
        }
        self.map.get_mut_or_default(index).await
    }
}

impl<C, I, V> HashableView for MapCountView<C, I, V>
where
    Self: View,
    MapView<C, I, V>: HashableView,
{
    type Hasher = <MapView<C, I, V> as HashableView>::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// A set view variant with O(1) `count`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I")]
pub struct SetCountView<C, I> {
    set: SetView<C, I>,
    stored_count: usize,
    count: usize,
}

impl<C: Context, C2: Context, I: Send + Sync + Serialize> ReplaceContext<C2>
    for SetCountView<C, I>
{
    type Target = SetCountView<C2, I>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        let old_context = self.context();
        let context = ctx(&old_context);
        let subview_base = context.base_key().base_tag(KeyTag::Subview as u8);
        let subview_context = context.clone_with_base_key(subview_base);
        SetCountView {
            set: self.set.with_context(|_| subview_context.clone()).await,
            stored_count: self.stored_count,
            count: self.count,
        }
    }
}

impl<C: Context, I: Send + Sync + Serialize> View for SetCountView<C, I> {
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        // The inner context has our base key + the KeyTag::Subview byte.
        self.set.context().clone_with_trimmed_key(1)
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Count as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_count =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let subview_base = context.base_key().base_tag(KeyTag::Subview as u8);
        let subview_context = context.clone_with_base_key(subview_base);
        let set = SetView::post_load(subview_context, &[])?;
        Ok(Self {
            set,
            stored_count,
            count: stored_count,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback();
        self.count = self.stored_count;
    }

    async fn has_pending_changes(&self) -> bool {
        self.count != self.stored_count || self.set.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let delete_subview = self.set.pre_save(batch)?;
        if self.count != self.stored_count {
            let count_key = self.context().base_key().base_tag(KeyTag::Count as u8);
            if self.count == 0 {
                batch.delete_key(count_key);
            } else {
                batch.put_key_value(count_key, &self.count)?;
            }
        }
        Ok(delete_subview && self.count == 0)
    }

    fn post_save(&mut self) {
        self.set.post_save();
        self.stored_count = self.count;
    }

    fn clear(&mut self) {
        self.set.clear();
        self.count = 0;
    }
}

impl<C, I> ClonableView for SetCountView<C, I>
where
    C: Context,
    I: Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(Self {
            set: self.set.clone_unchecked()?,
            stored_count: self.stored_count,
            count: self.count,
        })
    }
}

impl<C, I> Deref for SetCountView<C, I> {
    type Target = SetView<C, I>;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}

impl<C, I> DerefMut for SetCountView<C, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.set
    }
}

impl<C: Context, I: Serialize> SetCountView<C, I> {
    /// Inserts a value and updates the count in O(1).
    pub async fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        if !self.set.contains(index).await? {
            self.count += 1;
        }
        self.set.insert(index)
    }

    /// Removes a value and updates the count in O(1).
    pub async fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        if self.set.contains(index).await? {
            self.count -= 1;
        }
        self.set.remove(index)
    }

    /// Returns the number of entries in O(1).
    pub async fn count(&self) -> Result<usize, ViewError> {
        Ok(self.count)
    }
}

impl<C, I> HashableView for SetCountView<C, I>
where
    Self: View,
    SetView<C, I>: HashableView,
{
    type Hasher = <SetView<C, I> as HashableView>::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}

/// A collection view variant with O(1) `count`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, W: Allocative")]
pub struct CollectionCountView<C, I, W> {
    collection: CollectionView<C, I, W>,
    stored_count: usize,
    count: usize,
}

impl<C, I, W> View for CollectionCountView<C, I, W>
where
    C: Context,
    I: Send + Sync + Serialize + DeserializeOwned,
    W: View<Context = C>,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        // The inner context has our base key + the KeyTag::Subview byte.
        self.collection.context().clone_with_trimmed_key(1)
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Count as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_count =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let subview_base = context.base_key().base_tag(KeyTag::Subview as u8);
        let subview_context = context.clone_with_base_key(subview_base);
        let collection = CollectionView::post_load(subview_context, &[])?;
        Ok(Self {
            collection,
            stored_count,
            count: stored_count,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback();
        self.count = self.stored_count;
    }

    async fn has_pending_changes(&self) -> bool {
        self.count != self.stored_count || self.collection.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let delete_subview = self.collection.pre_save(batch)?;
        if self.count != self.stored_count {
            let count_key = self.context().base_key().base_tag(KeyTag::Count as u8);
            if self.count == 0 {
                batch.delete_key(count_key);
            } else {
                batch.put_key_value(count_key, &self.count)?;
            }
        }
        Ok(delete_subview && self.count == 0)
    }

    fn post_save(&mut self) {
        self.collection.post_save();
        self.stored_count = self.count;
    }

    fn clear(&mut self) {
        self.collection.clear();
        self.count = 0;
    }
}

impl<C, I, W> ClonableView for CollectionCountView<C, I, W>
where
    C: Context,
    I: Send + Sync + Serialize + DeserializeOwned,
    W: ClonableView<Context = C>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(Self {
            collection: self.collection.clone_unchecked()?,
            stored_count: self.stored_count,
            count: self.count,
        })
    }
}

impl<C, I, W> Deref for CollectionCountView<C, I, W> {
    type Target = CollectionView<C, I, W>;

    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}

impl<C, I, W> DerefMut for CollectionCountView<C, I, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.collection
    }
}

impl<C, I, W> CollectionCountView<C, I, W>
where
    C: Context,
    I: Serialize,
    W: View<Context = C>,
{
    /// Loads a mutable entry and updates the count if a new entry is created.
    pub async fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let existed = self.collection.try_load_entry(index).await?.is_some();
        let view = self.collection.load_entry_mut(index).await?;
        if !existed {
            self.count += 1;
        }
        Ok(view)
    }

    /// Resets an entry and updates the count if a new entry is created.
    pub async fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let existed = self.collection.try_load_entry(index).await?.is_some();
        self.collection.reset_entry_to_default(index)?;
        if !existed {
            self.count += 1;
        }
        Ok(())
    }

    /// Removes an entry and updates the count in O(1).
    pub async fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let existed = self.collection.try_load_entry(index).await?.is_some();
        self.collection.remove_entry(index)?;
        if existed {
            self.count -= 1;
        }
        Ok(())
    }

    /// Returns the number of entries in O(1).
    pub async fn count(&self) -> Result<usize, ViewError> {
        Ok(self.count)
    }
}

impl<C, I, W> HashableView for CollectionCountView<C, I, W>
where
    Self: View,
    CollectionView<C, I, W>: HashableView,
{
    type Hasher = <CollectionView<C, I, W> as HashableView>::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash().await
    }
}
