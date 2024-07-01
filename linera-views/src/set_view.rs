// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    linera_base::sync::Lazy,
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update},
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static SET_VIEW_HASH_RUNTIME: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "set_view_hash_runtime",
        "SetView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct ByteSetView<C> {
    context: C,
    delete_storage_first: bool,
    updates: BTreeMap<Vec<u8>, Update<()>>,
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
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        !self.updates.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(_) = update {
                    let key = self.context.base_index(&index);
                    batch.put_key_value_bytes(key, Vec::new());
                    delete_view = false;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_index(&index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(_) => batch.put_key_value_bytes(key, Vec::new()),
                }
            }
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.clear();
    }
}

impl<C> ClonableView<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(ByteSetView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
        })
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
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   assert_eq!(set.contains(&[0,1]).await.unwrap(), true);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>) {
        self.updates.insert(short_key, Update::Set(()));
    }

    /// Removes a value from the set. If absent then no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.remove(vec![0,1]);
    ///   assert_eq!(set.contains(&[0,1]).await.unwrap(), false);
    /// # })
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
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
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut set = ByteSetView::load(context).await.unwrap();
    ///   set.insert(vec![0,1]);
    ///   assert_eq!(set.contains(&[34]).await.unwrap(), false);
    ///   assert_eq!(set.contains(&[0,1]).await.unwrap(), true);
    /// # })
    /// ```
    pub async fn contains(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        if let Some(update) = self.updates.get(short_key) {
            let value = match update {
                Update::Removed => false,
                Update::Set(()) => true,
            };
            return Ok(value);
        }
        if self.delete_storage_first {
            return Ok(false);
        }
        let key = self.context.base_index(short_key);
        Ok(self.context.contains_key(&key).await?)
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
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
        if !self.delete_storage_first {
            let base = self.context.base_key();
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
    /// # use linera_views::{memory::create_memory_context, set_view::ByteSetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
}

#[async_trait]
impl<C> HashableView<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = SET_VIEW_HASH_RUNTIME.measure_latency();
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

    async fn has_pending_changes(&self) -> bool {
        self.set.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.set.flush(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> ClonableView<C> for SetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SetView {
            set: self.set.clone_unchecked()?,
            _phantom: PhantomData,
        })
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::{memory::create_memory_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::{memory::create_memory_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
        self.set.contains(&short_key).await
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
{
    /// Returns the list of indices in the set. The order is determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{memory::create_memory_context, set_view::SetView};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::SetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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

    async fn has_pending_changes(&self) -> bool {
        self.set.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.set.flush(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> ClonableView<C> for CustomSetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(CustomSetView {
            set: self.set.clone_unchecked()?,
            _phantom: PhantomData,
        })
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
        self.set.contains(&short_key).await
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::set_view::CustomSetView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
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

/// Type wrapping `ByteSetView` while memoizing the hash.
pub type HashedByteSetView<C> = WrappedHashableContainerView<C, ByteSetView<C>, HasherOutput>;

/// Type wrapping `SetView` while memoizing the hash.
pub type HashedSetView<C, I> = WrappedHashableContainerView<C, SetView<C, I>, HasherOutput>;

/// Type wrapping `CustomSetView` while memoizing the hash.
pub type HashedCustomSetView<C, I> =
    WrappedHashableContainerView<C, CustomSetView<C, I>, HasherOutput>;
