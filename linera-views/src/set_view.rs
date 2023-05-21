// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

/// Key tags to create the sub-keys of a SetView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the setview
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash
    Hash,
}

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct ByteSetView<C> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<()>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C> View<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(_) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, Vec::new());
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(_) => batch.put_key_value_bytes(key, Vec::new()),
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
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.updates.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Insert a value. If already present then it has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   assert_eq!(set.contains(vec![0,1]).await.unwrap(), true);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        self.updates.insert(short_key, Update::Set(()));
    }

    /// Removes a value from the set. If absent then no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.remove(vec![0,1]);
    ///   assert_eq!(set.contains(vec![0,1]).await.unwrap(), false);
    /// # })
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        if self.was_cleared {
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   assert_eq!(set.contains(vec![34]).await.unwrap(), false);
    ///   assert_eq!(set.contains(vec![0,1]).await.unwrap(), true);
    /// # })
    /// ```
    pub async fn contains(&self, short_key: Vec<u8>) -> Result<bool, ViewError> {
        if let Some(update) = self.updates.get(&short_key) {
            let value = match update {
                Update::Removed => false,
                Update::Set(()) => true,
            };
            return Ok(value);
        }
        if self.was_cleared {
            return Ok(false);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
        match self.context.read_key_bytes(&key).await? {
            None => Ok(false),
            Some(_) => Ok(true),
        }
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Returns the list of keys in the set. The order is lexicographic.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   set.insert(vec![0,2]);
    ///   assert_eq!(set.keys().await.unwrap(), vec![vec![0,1], vec![0,2]]);
    /// # })
    /// ```
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })
        .await?;
        Ok(keys)
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false, then the loop ends
    /// prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   set.insert(vec![0,2]);
    ///   set.insert(vec![3]);
    ///   let mut count = 0;
    ///   set.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.was_cleared {
            let base = self.context.base_tag(KeyTag::Index as u8);
            for index in self.context.find_keys_by_prefix(&base).await?.iterator() {
                let index = index?;
                loop {
                    match update {
                        Some((key, value)) if key.as_slice() <= index => {
                            if let Update::Set(_) = value {
                                if !f(key)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            if !f(index)? {
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
                if !f(key)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies a function f on each serialized index (aka key). Keys are visited in a
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   set.insert(vec![0,2]);
    ///   set.insert(vec![3]);
    ///   let mut count = 0;
    ///   set.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(|key| {
            f(key)?;
            Ok(true)
        })
        .await
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        self.for_each_key(|key| {
            count += 1;
            hasher.update_with_bytes(key)?;
            Ok(())
        })
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C> HashableView<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
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

/// A ['View'] implementing the set functionality with the index I being a non-trivial type.
#[derive(Debug)]
pub struct SetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I> View<C> for SetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.set.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let set = ByteSetView::load(context).await?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.set.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.set.delete(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Inserts a value. If already present then no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = SetView::<_,u32>::load(context).await.unwrap();
    ///   set.insert(&(34 as u32));
    ///   assert_eq!(set.indices().await.unwrap().len(), 1);
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = SetView::<_,u32>::load(context).await.unwrap();
    ///   set.remove(&(34 as u32));
    ///   assert_eq!(set.indices().await.unwrap().len(), 0);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.set.extra()
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set : SetView<_,u32> = SetView::load(context).await.unwrap();
    ///   set.insert(&(34 as u32));
    ///   assert_eq!(set.contains(&(34 as u32)).await.unwrap(), true);
    ///   assert_eq!(set.contains(&(45 as u32)).await.unwrap(), false);
    /// # })
    /// ```
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.contains(short_key).await
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
{
    /// Returns the list of indices in the set. The order is determined by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_test_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set : SetView<_,u32> = SetView::load(context).await.unwrap();
    ///   set.insert(&(34 as u32));
    ///   assert_eq!(set.indices().await.unwrap(), vec![34 as u32]);
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
    /// determined by the serialization. If the function returns false, then the
    /// loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = SetView::<_,u32>::load(context).await.unwrap();
    ///   set.insert(&(34 as u32));
    ///   set.insert(&(37 as u32));
    ///   set.insert(&(42 as u32));
    ///   let mut count = 0;
    ///   set.for_each_index_while(|_key| {
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
        self.set
            .for_each_key_while(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = SetView::<_,u32>::load(context).await.unwrap();
    ///   set.insert(&(34 as u32));
    ///   set.insert(&(37 as u32));
    ///   set.insert(&(42 as u32));
    ///   let mut count = 0;
    ///   set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I> HashableView<C> for SetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}

/// A ['View'] implementing the set functionality with the index I being a non-trivial type.
#[derive(Debug)]
pub struct CustomSetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I> View<C> for CustomSetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
{
    fn context(&self) -> &C {
        self.set.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let set = ByteSetView::load(context).await?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.set.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.set.delete(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Inserts a value. If present then it has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.insert(&(34 as u128));
    ///   assert_eq!(set.indices().await.unwrap().len(), 1);
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.remove(&(34 as u128));
    ///   assert_eq!(set.indices().await.unwrap().len(), 0);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.set.extra()
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.insert(&(34 as u128));
    ///   assert_eq!(set.contains(&(34 as u128)).await.unwrap(), true);
    ///   assert_eq!(set.contains(&(37 as u128)).await.unwrap(), false);
    /// # })
    /// ```
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.contains(short_key).await
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + CustomSerialize,
{
    /// Returns the list of indices in the set. The order is determined by the custom
    /// serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.insert(&(34 as u128));
    ///   set.insert(&(37 as u128));
    ///   assert_eq!(set.indices().await.unwrap(), vec![34 as u128,37 as u128]);
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
    /// determined by the custom serialization. If the function does return
    /// false, then the loop prematurely ends.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.insert(&(34 as u128));
    ///   set.insert(&(37 as u128));
    ///   set.insert(&(42 as u128));
    ///   let mut count = 0;
    ///   set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 5)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut set = CustomSetView::<_,u128>::load(context).await.unwrap();
    ///   set.insert(&(34 as u128));
    ///   set.insert(&(37 as u128));
    ///   set.insert(&(42 as u128));
    ///   let mut count = 0;
    ///   set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I> HashableView<C> for CustomSetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + CustomSerialize,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}
