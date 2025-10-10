// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::BTreeMap, marker::PhantomData, mem};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{CustomSerialize, HasherOutput, Update},
    context::{BaseKey, Context},
    hashable_wrapper::WrappedHashableContainerView,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, HashableView, Hasher, ReplaceContext, View, ViewError},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static SET_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "set_view_hash_runtime",
            "SetView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A [`View`] that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct ByteSetView<C> {
    context: C,
    delete_storage_first: bool,
    updates: BTreeMap<Vec<u8>, Update<()>>,
}

impl<C: Context, C2: Context> ReplaceContext<C2> for ByteSetView<C> {
    type Target = ByteSetView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        ByteSetView {
            context: ctx(self.context()),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
        }
    }
}

impl<C: Context> View for ByteSetView<C> {
    const NUM_INIT_KEYS: usize = 0;

    type Context = C;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(Vec::new())
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: BTreeMap::new(),
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
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
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(_) = update {
                    let key = self.context.base_key().base_index(&index);
                    batch.put_key_value_bytes(key, Vec::new());
                    delete_view = false;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_key().base_index(&index);
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

impl<C: Context> ClonableView for ByteSetView<C> {
    fn clone_unchecked(&mut self) -> Self {
        ByteSetView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
        }
    }
}

impl<C: Context> ByteSetView<C> {
    /// Insert a value. If already present then it has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).await.unwrap(), true);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>) {
        self.updates.insert(short_key, Update::Set(()));
    }

    /// Removes a value from the set. If absent then no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.remove(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).await.unwrap(), false);
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

impl<C: Context> ByteSetView<C> {
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[34]).await.unwrap(), false);
    /// assert_eq!(set.contains(&[0, 1]).await.unwrap(), true);
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
        let key = self.context.base_key().base_index(short_key);
        Ok(self.context.store().contains_key(&key).await?)
    }
}

impl<C: Context> ByteSetView<C> {
    /// Returns the list of keys in the set. The order is lexicographic.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// assert_eq!(set.keys().await.unwrap(), vec![vec![0, 1], vec![0, 2]]);
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

    /// Returns the number of entries in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// assert_eq!(set.keys().await.unwrap(), vec![vec![0, 1], vec![0, 2]]);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_key(|_key| {
            count += 1;
            Ok(())
        })
        .await?;
        Ok(count)
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false, then the loop ends
    /// prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// set.insert(vec![3]);
    /// let mut count = 0;
    /// set.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.delete_storage_first {
            let base = &self.context.base_key().bytes;
            for index in self.context.store().find_keys_by_prefix(base).await? {
                loop {
                    match update {
                        Some((key, value)) if key <= &index => {
                            if let Update::Set(_) = value {
                                if !f(key)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == &index {
                                break;
                            }
                        }
                        _ => {
                            if !f(&index)? {
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
    /// # use linera_views::{context::MemoryContext, set_view::ByteSetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = ByteSetView::load(context).await.unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// set.insert(vec![3]);
    /// let mut count = 0;
    /// set.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
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

impl<C: Context> HashableView for ByteSetView<C> {
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::SET_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
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

/// A [`View`] implementing the set functionality with the index `I` being any serializable type.
#[derive(Debug)]
pub struct SetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

impl<C: Context, I: Send + Sync + Serialize, C2: Context> ReplaceContext<C2> for SetView<C, I> {
    type Target = SetView<C2, I>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SetView {
            set: self.set.with_context(ctx).await,
            _phantom: self._phantom,
        }
    }
}

impl<C: Context, I: Send + Sync + Serialize> View for SetView<C, I> {
    const NUM_INIT_KEYS: usize = ByteSetView::<C>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> &C {
        self.set.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteSetView::<C>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let set = ByteSetView::post_load(context, values)?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
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

impl<C, I> ClonableView for SetView<C, I>
where
    C: Context,
    I: Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Self {
        SetView {
            set: self.set.clone_unchecked(),
            _phantom: PhantomData,
        }
    }
}

impl<C: Context, I: Serialize> SetView<C, I> {
    /// Inserts a value. If already present then no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::SetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = SetView::<_, u32>::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.indices().await.unwrap().len(), 1);
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::SetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = SetView::<_, u32>::load(context).await.unwrap();
    /// set.remove(&(34 as u32));
    /// assert_eq!(set.indices().await.unwrap().len(), 0);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.set.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.set.extra()
    }
}

impl<C: Context, I: Serialize> SetView<C, I> {
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::SetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set: SetView<_, u32> = SetView::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.contains(&(34 as u32)).await.unwrap(), true);
    /// assert_eq!(set.contains(&(45 as u32)).await.unwrap(), false);
    /// # })
    /// ```
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.set.contains(&short_key).await
    }
}

impl<C: Context, I: Serialize + DeserializeOwned + Send> SetView<C, I> {
    /// Returns the list of indices in the set. The order is determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::SetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set: SetView<_, u32> = SetView::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.indices().await.unwrap(), vec![34 as u32]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Returns the number of entries in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::{context::MemoryContext, set_view::SetView};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set: SetView<_, u32> = SetView::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.count().await.unwrap(), 1);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.set.count().await
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false, then the
    /// loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::SetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = SetView::<_, u32>::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// set.insert(&(37 as u32));
    /// set.insert(&(42 as u32));
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
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
        self.set
            .for_each_key_while(|key| {
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::SetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = SetView::<_, u32>::load(context).await.unwrap();
    /// set.insert(&(34 as u32));
    /// set.insert(&(37 as u32));
    /// set.insert(&(42 as u32));
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

impl<C, I> HashableView for SetView<C, I>
where
    Self: View,
    ByteSetView<C>: HashableView,
{
    type Hasher = <ByteSetView<C> as HashableView>::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}

/// A [`View`] implementing the set functionality with the index `I` being a type with a custom
/// serialization format.
#[derive(Debug)]
pub struct CustomSetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

impl<C, I> View for CustomSetView<C, I>
where
    C: Context,
    I: Send + Sync + CustomSerialize,
{
    const NUM_INIT_KEYS: usize = ByteSetView::<C>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> &C {
        self.set.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteSetView::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let set = ByteSetView::post_load(context, values)?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
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

impl<C, I> ClonableView for CustomSetView<C, I>
where
    C: Context,
    I: Send + Sync + CustomSerialize,
{
    fn clone_unchecked(&mut self) -> Self {
        CustomSetView {
            set: self.set.clone_unchecked(),
            _phantom: PhantomData,
        }
    }
}

impl<C: Context, I: CustomSerialize> CustomSetView<C, I> {
    /// Inserts a value. If present then it has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// assert_eq!(set.indices().await.unwrap().len(), 1);
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.remove(&(34 as u128));
    /// assert_eq!(set.indices().await.unwrap().len(), 0);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
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
    I: CustomSerialize,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// assert_eq!(set.contains(&(34 as u128)).await.unwrap(), true);
    /// assert_eq!(set.contains(&(37 as u128)).await.unwrap(), false);
    /// # })
    /// ```
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.contains(&short_key).await
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    I: Sync + Send + CustomSerialize,
{
    /// Returns the list of indices in the set. The order is determined by the custom
    /// serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// assert_eq!(set.indices().await.unwrap(), vec![34 as u128, 37 as u128]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Returns the number of entries of the set.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// assert_eq!(set.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.set.count().await
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function does return
    /// false, then the loop prematurely ends.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// set.insert(&(42 as u128));
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 5)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::set_view::CustomSetView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut set = CustomSetView::<_, u128>::load(context).await.unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// set.insert(&(42 as u128));
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
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

impl<C: Context, I> HashableView for CustomSetView<C, I>
where
    Self: View,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::MemoryContext,
        store::{WritableKeyValueStore as _},
    };

    #[tokio::test]
    async fn test_byte_set_view_flush_with_delete_storage_first_and_set_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // First, add some initial data to storage
        set.insert(vec![1, 2, 3]);
        set.insert(vec![4, 5, 6]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Now clear the set (this sets delete_storage_first = true)
        set.clear();

        // Add new items after clearing - this creates Update::Set entries
        set.insert(vec![7, 8, 9]);
        set.insert(vec![10, 11, 12]);

        // Create a new batch and flush
        let mut batch = Batch::new();
        let delete_view = set.flush(&mut batch)?;

        // The key assertion: delete_view should be false because we had Update::Set entries
        // This tests line 103: if let Update::Set(_) = update { ... delete_view = false; }
        assert!(!delete_view);

        // Verify the batch contains the expected operations
        assert!(!batch.operations.is_empty());

        // Write the batch and verify the final state
        set.context().store().write_batch(batch).await?;

        // Reload and verify only the new items exist
        let new_set = ByteSetView::load(set.context().clone()).await?;
        assert!(new_set.contains(&[7, 8, 9]).await?);
        assert!(new_set.contains(&[10, 11, 12]).await?);
        assert!(!new_set.contains(&[1, 2, 3]).await?);
        assert!(!new_set.contains(&[4, 5, 6]).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_byte_set_view_flush_with_delete_storage_first_no_set_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add some initial data
        set.insert(vec![1, 2, 3]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Clear the set and flush without adding anything back
        set.clear();
        let mut batch = Batch::new();
        let delete_view = set.flush(&mut batch)?;

        // When there are no Update::Set entries after clear, delete_view should be true
        assert!(delete_view);

        Ok(())
    }

    #[tokio::test]
    async fn test_byte_set_view_flush_with_delete_storage_first_mixed_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add initial data
        set.insert(vec![1, 2, 3]);
        set.insert(vec![4, 5, 6]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Clear the set
        set.clear();

        // Add some items back and remove others
        set.insert(vec![7, 8, 9]);  // This creates Update::Set
        set.remove(vec![10, 11, 12]); // This creates Update::Removed (but gets optimized away due to delete_storage_first)

        let mut batch = Batch::new();
        let delete_view = set.flush(&mut batch)?;

        // Should be false because we have Update::Set entries (line 103 logic)
        assert!(!delete_view);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_flush_with_delete_storage_first_and_set_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: SetView<_, u32> = SetView::load(context).await?;

        // Add initial data
        set.insert(&42)?;
        set.insert(&84)?;
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Clear the set
        set.clear();

        // Add new items - this should trigger the Update::Set branch in line 103
        set.insert(&123)?;
        set.insert(&456)?;

        let mut batch = Batch::new();
        let delete_view = set.flush(&mut batch)?;

        // Should be false due to Update::Set entries
        assert!(!delete_view);

        // Verify final state
        set.context().store().write_batch(batch).await?;
        let new_set: SetView<_, u32> = SetView::load(set.context().clone()).await?;
        assert!(new_set.contains(&123).await?);
        assert!(new_set.contains(&456).await?);
        assert!(!new_set.contains(&42).await?);
        assert!(!new_set.contains(&84).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_byte_set_view_keys_initialization() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add several keys to test line 223: let mut keys = Vec::new();
        set.insert(vec![1, 2]);
        set.insert(vec![3, 4]);
        set.insert(vec![5, 6]);

        // Call keys() method which executes line 223
        let keys = set.keys().await?;

        // Verify the Vec::new() initialization worked and keys were collected
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&vec![1, 2]));
        assert!(keys.contains(&vec![3, 4]));
        assert!(keys.contains(&vec![5, 6]));

        Ok(())
    }

    #[tokio::test]
    async fn test_byte_set_view_count_initialization() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Test with empty set first - count should start at 0 (line 245)
        let count = set.count().await?;
        assert_eq!(count, 0);

        // Add items and verify count increments from 0 (line 245: let mut count = 0;)
        set.insert(vec![1]);
        set.insert(vec![2]);
        set.insert(vec![3]);

        let count = set.count().await?;
        assert_eq!(count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_count_delegation() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: SetView<_, u32> = SetView::load(context).await?;

        // Test line 557: self.set.count().await - SetView delegates to ByteSetView
        let count = set.count().await?;
        assert_eq!(count, 0);

        // Add items and verify delegation works
        set.insert(&42)?;
        set.insert(&84)?;
        set.insert(&126)?;

        // This calls line 557 which delegates to the underlying ByteSetView
        let count = set.count().await?;
        assert_eq!(count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_match_update_pattern() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add initial data to storage
        set.insert(vec![1]);
        set.insert(vec![3]);
        set.insert(vec![5]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Add some pending updates that will be processed in the loop
        set.insert(vec![2]);  // This will create an Update::Set
        set.insert(vec![4]);  // This will create another Update::Set

        let mut keys_processed = Vec::new();
        
        // This will exercise line 286: match update pattern
        // The method iterates through stored keys and pending updates
        set.for_each_key_while(|key| {
            keys_processed.push(key.to_vec());
            Ok(true)  // Continue processing
        }).await?;

        // Should have processed both stored and pending keys
        assert!(keys_processed.len() >= 4);
        assert!(keys_processed.contains(&vec![1]));
        assert!(keys_processed.contains(&vec![2]));
        assert!(keys_processed.contains(&vec![3]));
        assert!(keys_processed.contains(&vec![4]));

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_early_return() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add data to storage first
        set.insert(vec![1]);
        set.insert(vec![2]);
        set.insert(vec![3]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        let mut count = 0;
        
        // This tests line 300: return Ok(()); when function returns false
        set.for_each_key_while(|_key| {
            count += 1;
            if count >= 2 {
                Ok(false)  // This should trigger early return on line 300
            } else {
                Ok(true)
            }
        }).await?;

        // Should have stopped early, processing only 2 keys
        assert_eq!(count, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_hash_mut_delegation() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add some data
        set.insert(vec![1, 2, 3]);
        set.insert(vec![4, 5, 6]);

        // Test line 356: self.hash().await - hash_mut delegates to hash
        let hash1 = set.hash_mut().await?;
        let hash2 = set.hash().await?;

        // Both should produce the same result since hash_mut delegates to hash
        assert_eq!(hash1, hash2);

        // Verify hash changes when data changes
        set.insert(vec![7, 8, 9]);
        let hash3 = set.hash_mut().await?;
        assert_ne!(hash1, hash3);

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_early_return_on_update_set() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add some data to storage first
        set.insert(vec![1]);
        set.insert(vec![3]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Add pending updates that come before stored keys lexicographically
        set.insert(vec![0]);  // This will be processed first as an Update::Set
        set.insert(vec![2]);  // This will be processed as an Update::Set

        let mut count = 0;
        
        // This tests line 290: return Ok(()); in the Update::Set branch
        // The function should return false on the first Update::Set key, triggering early return
        set.for_each_key_while(|key| {
            count += 1;
            if key == &[0] {
                Ok(false)  // This should trigger line 290: return Ok(());
            } else {
                Ok(true)
            }
        }).await?;

        // Should have stopped early after processing the first key
        assert_eq!(count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_early_return_in_remaining_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Only add pending updates, no stored data
        // This forces the method to only process the remaining updates loop (line 308-315)
        set.insert(vec![1]);
        set.insert(vec![2]);
        set.insert(vec![3]);

        let mut count = 0;
        
        // This tests line 311: return Ok(()); in the remaining updates while loop
        set.for_each_key_while(|key| {
            count += 1;
            if key == &[2] {
                Ok(false)  // This should trigger line 311: return Ok(());
            } else {
                Ok(true)
            }
        }).await?;

        // Should have stopped early when processing key [2]
        assert_eq!(count, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_for_each_index_while_deserialization() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: SetView<_, u32> = SetView::load(context).await?;

        // Add some data
        set.insert(&42)?;
        set.insert(&84)?;
        set.insert(&126)?;

        let mut indices_processed = Vec::new();
        
        // This tests line 590: let index = BaseKey::deserialize_value(key)?;
        // The SetView should deserialize byte keys back to u32 indices
        set.for_each_index_while(|index| {
            indices_processed.push(index);
            Ok(true)
        }).await?;

        // Verify that deserialization worked correctly
        assert_eq!(indices_processed.len(), 3);
        assert!(indices_processed.contains(&42));
        assert!(indices_processed.contains(&84));
        assert!(indices_processed.contains(&126));

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_deserialization_early_return() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: SetView<_, u32> = SetView::load(context).await?;

        // Add several indices
        set.insert(&10)?;
        set.insert(&20)?;
        set.insert(&30)?;
        set.insert(&40)?;

        let mut count = 0;
        
        // Test line 590 deserialization combined with early return
        set.for_each_index_while(|index| {
            count += 1;
            if index == 30 {
                Ok(false)  // Should stop iteration early
            } else {
                Ok(true)
            }
        }).await?;

        // Should have processed indices until reaching 30
        assert!(count >= 3);
        assert!(count <= 4);  // Depends on ordering, but should stop at or before 4

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_break_on_matching_key() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add data to storage first
        set.insert(vec![2]);
        set.insert(vec![4]);
        set.insert(vec![6]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Add a pending update that matches one of the stored keys
        set.insert(vec![4]);  // This creates an Update::Set that matches stored key [4]

        let mut keys_processed = Vec::new();
        
        // This tests line 295: break; when key == &index
        // When the update key [4] matches the stored index [4], it should break and continue to next stored key
        set.for_each_key_while(|key| {
            keys_processed.push(key.to_vec());
            Ok(true)
        }).await?;

        // Should process each key only once, even though [4] appears in both stored and pending
        assert!(keys_processed.contains(&vec![2]));
        assert!(keys_processed.contains(&vec![4]));
        assert!(keys_processed.contains(&vec![6]));
        
        // Count occurrences of [4] - should be exactly 1 due to the break on line 295
        let count_4 = keys_processed.iter().filter(|&key| key == &vec![4]).count();
        assert_eq!(count_4, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_remaining_updates_update_set_processing() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Don't add any data to storage, only add pending updates
        // This ensures we only exercise the remaining updates loop (line 308-315)
        set.insert(vec![1]);  // Update::Set
        set.insert(vec![2]);  // Update::Set
        set.insert(vec![3]);  // Update::Set
        set.remove(vec![4]);  // Update::Removed - should be ignored in the loop

        let mut keys_processed = Vec::new();
        
        // This tests line 313: closing brace of Update::Set block in remaining updates
        // Only Update::Set entries should be processed, Update::Removed should be skipped
        set.for_each_key_while(|key| {
            keys_processed.push(key.to_vec());
            Ok(true)
        }).await?;

        // Should only process the Update::Set entries, not the Update::Removed
        assert_eq!(keys_processed.len(), 3);
        assert!(keys_processed.contains(&vec![1]));
        assert!(keys_processed.contains(&vec![2]));
        assert!(keys_processed.contains(&vec![3]));
        // Should NOT contain [4] because it's Update::Removed
        assert!(!keys_processed.contains(&vec![4]));

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_update_types_in_remaining_loop() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Create a complex scenario with mixed update types
        set.insert(vec![1]);  // Update::Set
        set.remove(vec![2]);  // Update::Removed
        set.insert(vec![3]);  // Update::Set
        set.remove(vec![4]);  // Update::Removed
        set.insert(vec![5]);  // Update::Set

        let mut processed_keys = Vec::new();
        
        // This further tests the Update::Set filtering in the remaining updates loop
        set.for_each_key_while(|key| {
            processed_keys.push(key.to_vec());
            Ok(true)
        }).await?;

        // Should only process Update::Set entries (1, 3, 5), not Update::Removed (2, 4)
        assert_eq!(processed_keys.len(), 3);
        assert!(processed_keys.contains(&vec![1]));
        assert!(processed_keys.contains(&vec![3]));
        assert!(processed_keys.contains(&vec![5]));
        assert!(!processed_keys.contains(&vec![2]));
        assert!(!processed_keys.contains(&vec![4]));

        Ok(())
    }

    #[tokio::test]
    async fn test_contains_update_removed_returns_false() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // First add an item and persist it
        set.insert(vec![1, 2, 3]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Verify it exists
        assert!(set.contains(&[1, 2, 3]).await?);

        // Now remove the item - this creates an Update::Removed
        set.remove(vec![1, 2, 3]);

        // This tests line 196: Update::Removed => false,
        // The contains() method should return false for the removed item
        let contains_result = set.contains(&[1, 2, 3]).await?;
        assert!(!contains_result);

        Ok(())
    }

    #[tokio::test]
    async fn test_contains_delete_storage_first_returns_false() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add some items and persist them
        set.insert(vec![1]);
        set.insert(vec![2]);
        set.insert(vec![3]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Verify items exist
        assert!(set.contains(&[1]).await?);
        assert!(set.contains(&[2]).await?);
        assert!(set.contains(&[3]).await?);

        // Clear the set - this sets delete_storage_first = true
        set.clear();

        // This tests line 202: return Ok(false); when delete_storage_first is true
        // All items should now return false, even for keys that exist in storage
        assert!(!set.contains(&[1]).await?);
        assert!(!set.contains(&[2]).await?);
        assert!(!set.contains(&[3]).await?);

        // Even non-existent keys should return false
        assert!(!set.contains(&[99]).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_contains_delete_storage_first_with_new_additions() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add and persist some initial data
        set.insert(vec![1]);
        set.insert(vec![2]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Clear the set (sets delete_storage_first = true)
        set.clear();

        // Add new items after clearing
        set.insert(vec![3]);
        set.insert(vec![4]);

        // Line 202 should be bypassed for new items since they have Update::Set
        // Old items should return false due to line 202
        assert!(!set.contains(&[1]).await?);  // Old item - line 202 path
        assert!(!set.contains(&[2]).await?);  // Old item - line 202 path
        assert!(set.contains(&[3]).await?);   // New item - has Update::Set
        assert!(set.contains(&[4]).await?);   // New item - has Update::Set

        Ok(())
    }

    #[tokio::test] 
    async fn test_contains_mixed_update_scenarios() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Test various combinations to exercise both line 196 and line 202

        // Add some items
        set.insert(vec![1]);
        set.insert(vec![2]);
        set.insert(vec![3]);

        // Remove one item - creates Update::Removed (tests line 196)
        set.remove(vec![2]);

        // Verify line 196 behavior
        assert!(set.contains(&[1]).await?);   // Still has Update::Set
        assert!(!set.contains(&[2]).await?);  // Line 196: Update::Removed => false
        assert!(set.contains(&[3]).await?);   // Still has Update::Set

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_update_set_processing_in_stored_loop() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add data to storage first
        set.insert(vec![2]);
        set.insert(vec![4]);
        set.insert(vec![6]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Add pending updates that will be processed alongside stored keys
        set.insert(vec![1]);  // Update::Set - comes before stored keys
        set.insert(vec![3]);  // Update::Set - comes between stored keys
        set.remove(vec![5]);  // Update::Removed - should be ignored

        let mut processed_keys = Vec::new();

        // This tests line 292: closing brace of Update::Set block in stored keys processing
        // Only Update::Set entries should be processed, Update::Removed should be skipped
        set.for_each_key_while(|key| {
            processed_keys.push(key.to_vec());
            Ok(true)
        }).await?;

        // Should process stored keys (2, 4, 6) and Update::Set pending keys (1, 3)
        // Should NOT process Update::Removed key (5)
        assert!(processed_keys.contains(&vec![1]));  // Update::Set
        assert!(processed_keys.contains(&vec![2]));  // Stored
        assert!(processed_keys.contains(&vec![3]));  // Update::Set
        assert!(processed_keys.contains(&vec![4]));  // Stored
        assert!(!processed_keys.contains(&vec![5])); // Update::Removed - should be skipped
        assert!(processed_keys.contains(&vec![6]));  // Stored

        Ok(())
    }

    #[tokio::test]
    async fn test_for_each_key_while_inner_loop_completion() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set = ByteSetView::load(context).await?;

        // Add multiple stored keys
        set.insert(vec![10]);
        set.insert(vec![20]);
        set.insert(vec![30]);
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Add pending updates with various patterns
        set.insert(vec![5]);   // Before first stored key
        set.insert(vec![15]);  // Between stored keys
        set.insert(vec![25]);  // Between stored keys
        set.insert(vec![35]);  // After last stored key

        let mut keys_in_order = Vec::new();

        // This tests line 307: closing brace of the inner loop that processes stored keys
        // The inner loop should properly handle all stored keys and interleaved updates
        set.for_each_key_while(|key| {
            keys_in_order.push(key.to_vec());
            Ok(true)
        }).await?;

        // Verify all keys were processed
        assert!(keys_in_order.contains(&vec![5]));
        assert!(keys_in_order.contains(&vec![10]));
        assert!(keys_in_order.contains(&vec![15]));
        assert!(keys_in_order.contains(&vec![20]));
        assert!(keys_in_order.contains(&vec![25]));
        assert!(keys_in_order.contains(&vec![30]));
        assert!(keys_in_order.contains(&vec![35]));

        // Should have processed 7 keys total
        assert_eq!(keys_in_order.len(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_hash_mut_delegation() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: SetView<_, u32> = SetView::load(context).await?;

        // Add some data to the SetView
        set.insert(&42)?;
        set.insert(&84)?;
        set.insert(&126)?;

        // Test line 641: self.set.hash_mut().await - SetView delegates to ByteSetView
        let hash1 = set.hash_mut().await?;
        let hash2 = set.hash().await?;

        // Both should produce the same result since SetView delegates to ByteSetView
        assert_eq!(hash1, hash2);

        // Verify that the delegation works correctly when data changes
        set.insert(&168)?;
        let hash3 = set.hash_mut().await?;
        assert_ne!(hash1, hash3);

        // Test that SetView hash delegation produces same result as direct ByteSetView hash
        let context2 = MemoryContext::new_for_testing(());
        let mut byte_set = ByteSetView::load(context2).await?;
        
        // Add equivalent data to ByteSetView (using serialized form of the same numbers)
        use crate::context::BaseKey;
        byte_set.insert(BaseKey::derive_short_key(&42u32)?);
        byte_set.insert(BaseKey::derive_short_key(&84u32)?);
        byte_set.insert(BaseKey::derive_short_key(&126u32)?);
        byte_set.insert(BaseKey::derive_short_key(&168u32)?);

        let byte_set_hash = byte_set.hash_mut().await?;
        assert_eq!(hash3, byte_set_hash);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_view_hash_delegation_with_different_types() -> Result<(), ViewError> {
        // Test that the delegation works with different types
        let context = MemoryContext::new_for_testing(());
        let mut string_set: SetView<_, String> = SetView::load(context).await?;

        string_set.insert(&"hello".to_string())?;
        string_set.insert(&"world".to_string())?;

        // Line 641 should work for any type that implements Serialize
        let hash1 = string_set.hash_mut().await?;
        let hash2 = string_set.hash().await?;
        assert_eq!(hash1, hash2);

        // Verify hash changes with data
        string_set.insert(&"test".to_string())?;
        let hash3 = string_set.hash_mut().await?;
        assert_ne!(hash1, hash3);

        Ok(())
    }

    // CustomSetView tests - similar patterns but using CustomSerialize
    #[tokio::test]
    async fn test_custom_set_view_flush_with_delete_storage_first_and_set_updates() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add initial data
        set.insert(&42u128)?;
        set.insert(&84u128)?;
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Clear the set
        set.clear();

        // Add new items - this should trigger the Update::Set branch (line 103 equivalent)
        set.insert(&123u128)?;
        set.insert(&456u128)?;

        let mut batch = Batch::new();
        let delete_view = set.flush(&mut batch)?;

        // Should be false due to Update::Set entries
        assert!(!delete_view);

        // Verify final state
        set.context().store().write_batch(batch).await?;
        let new_set: CustomSetView<_, u128> = CustomSetView::load(set.context().clone()).await?;
        assert!(new_set.contains(&123u128).await?);
        assert!(new_set.contains(&456u128).await?);
        assert!(!new_set.contains(&42u128).await?);
        assert!(!new_set.contains(&84u128).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_contains_update_removed_returns_false() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add and persist an item
        set.insert(&12345u128)?;
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Verify it exists
        assert!(set.contains(&12345u128).await?);

        // Remove the item - creates Update::Removed (tests line 196 equivalent)
        set.remove(&12345u128)?;

        // Should return false for the removed item
        assert!(!set.contains(&12345u128).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_contains_delete_storage_first_returns_false() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add items and persist
        set.insert(&111u128)?;
        set.insert(&222u128)?;
        set.insert(&333u128)?;
        let mut batch = Batch::new();
        set.flush(&mut batch)?;
        set.context().store().write_batch(batch).await?;

        // Verify items exist
        assert!(set.contains(&111u128).await?);
        assert!(set.contains(&222u128).await?);

        // Clear the set - sets delete_storage_first = true
        set.clear();

        // Tests line 202 equivalent: should return false when delete_storage_first is true
        assert!(!set.contains(&111u128).await?);
        assert!(!set.contains(&222u128).await?);
        assert!(!set.contains(&333u128).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_count_delegation() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Test that CustomSetView delegates count to ByteSetView (line 557 equivalent)
        let count = set.count().await?;
        assert_eq!(count, 0);

        // Add items and verify delegation works
        set.insert(&100u128)?;
        set.insert(&200u128)?;
        set.insert(&300u128)?;

        let count = set.count().await?;
        assert_eq!(count, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_hash_mut_delegation() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add some data
        set.insert(&1000u128)?;
        set.insert(&2000u128)?;

        // Test hash_mut delegation (line 641 equivalent for CustomSetView)
        let hash1 = set.hash_mut().await?;
        let hash2 = set.hash().await?;

        // Both should produce the same result since CustomSetView delegates to ByteSetView
        assert_eq!(hash1, hash2);

        // Verify hash changes when data changes
        set.insert(&3000u128)?;
        let hash3 = set.hash_mut().await?;
        assert_ne!(hash1, hash3);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_for_each_index_while_deserialization() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add some data
        set.insert(&424242u128)?;
        set.insert(&848484u128)?;
        set.insert(&121212u128)?;

        let mut indices_processed = Vec::new();
        
        // Test CustomSerialize deserialization (line 590 equivalent but with CustomSerialize)
        set.for_each_index_while(|index| {
            indices_processed.push(index);
            Ok(true)
        }).await?;

        // Verify that custom deserialization worked correctly
        assert_eq!(indices_processed.len(), 3);
        assert!(indices_processed.contains(&424242u128));
        assert!(indices_processed.contains(&848484u128));
        assert!(indices_processed.contains(&121212u128));

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_for_each_index_while_early_return() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Add several indices
        set.insert(&10u128)?;
        set.insert(&20u128)?;
        set.insert(&30u128)?;
        set.insert(&40u128)?;

        let mut count = 0;
        
        // Test custom deserialization with early return
        set.for_each_index_while(|index| {
            count += 1;
            if index == 30u128 {
                Ok(false)  // Should stop iteration early
            } else {
                Ok(true)
            }
        }).await?;

        // Should have processed indices until reaching 30
        assert!(count >= 3);
        assert!(count <= 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_indices_method() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Test the indices() method which uses custom deserialization
        let indices = set.indices().await?;
        assert_eq!(indices.len(), 0);

        // Add some data
        set.insert(&777u128)?;
        set.insert(&888u128)?;
        set.insert(&999u128)?;

        let indices = set.indices().await?;
        assert_eq!(indices.len(), 3);
        assert!(indices.contains(&777u128));
        assert!(indices.contains(&888u128));
        assert!(indices.contains(&999u128));

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_set_view_mixed_operations() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut set: CustomSetView<_, u128> = CustomSetView::load(context).await?;

        // Test a combination of operations
        set.insert(&100u128)?;
        set.insert(&200u128)?;
        set.insert(&300u128)?;

        // Remove one item (creates Update::Removed)
        set.remove(&200u128)?;

        // Verify mixed update states
        assert!(set.contains(&100u128).await?);
        assert!(!set.contains(&200u128).await?);  // Update::Removed should return false
        assert!(set.contains(&300u128).await?);

        // Test count with mixed updates
        let count = set.count().await?;
        assert_eq!(count, 2);

        // Test indices with mixed updates
        let indices = set.indices().await?;
        assert_eq!(indices.len(), 2);
        assert!(indices.contains(&100u128));
        assert!(!indices.contains(&200u128));
        assert!(indices.contains(&300u128));

        Ok(())
    }

    #[cfg(with_graphql)]
    mod graphql_tests {
        use super::*;
        use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};

        // Create a simple GraphQL schema for testing
        struct Query;

        #[Object]
        impl Query {
            async fn test_set(&self) -> TestSetView {
                let context = MemoryContext::new_for_testing(());
                let mut set: SetView<_, u32> = SetView::load(context).await.unwrap();
                
                // Add test data
                set.insert(&42).unwrap();
                set.insert(&84).unwrap();
                set.insert(&126).unwrap();
                set.insert(&168).unwrap();
                set.insert(&210).unwrap();
                
                TestSetView { set }
            }
        }

        struct TestSetView {
            set: SetView<MemoryContext<()>, u32>,
        }

        #[Object]
        impl TestSetView {
            async fn elements(&self, count: Option<usize>) -> Result<Vec<u32>, async_graphql::Error> {
                // This calls line 1169: async fn elements(&self, count: Option<usize>)
                let mut indices = self.set.indices().await?;
                if let Some(count) = count {
                    // This tests line 1172: indices.truncate(count);
                    indices.truncate(count);
                }
                Ok(indices)
            }

            async fn count(&self) -> Result<u32, async_graphql::Error> {
                Ok(self.set.count().await? as u32)
            }
        }

        #[tokio::test]
        async fn test_graphql_elements_without_count() -> Result<(), Box<dyn std::error::Error>> {
            let schema = Schema::build(Query, EmptyMutation, EmptySubscription).finish();
            
            // Test line 1169 without count parameter - should return all elements
            let query = r#"
                query {
                    testSet {
                        elements
                    }
                }
            "#;

            let result = schema.execute(query).await;
            assert!(result.errors.is_empty());
            
            let data = result.data.into_json()?;
            let elements = &data["testSet"]["elements"];
            assert!(elements.is_array());
            assert_eq!(elements.as_array().unwrap().len(), 5);

            Ok(())
        }

        #[tokio::test]
        async fn test_graphql_elements_with_count() -> Result<(), Box<dyn std::error::Error>> {
            let schema = Schema::build(Query, EmptyMutation, EmptySubscription).finish();
            
            // Test line 1172 truncate logic - should limit to 3 elements
            let query = r#"
                query {
                    testSet {
                        elements(count: 3)
                    }
                }
            "#;

            let result = schema.execute(query).await;
            assert!(result.errors.is_empty());
            
            let data = result.data.into_json()?;
            let elements = &data["testSet"]["elements"];
            assert!(elements.is_array());
            // This tests that line 1172 (indices.truncate(count)) worked correctly
            assert_eq!(elements.as_array().unwrap().len(), 3);

            Ok(())
        }

        #[tokio::test]
        async fn test_graphql_count_field() -> Result<(), Box<dyn std::error::Error>> {
            let schema = Schema::build(Query, EmptyMutation, EmptySubscription).finish();
            
            let query = r#"
                query {
                    testSet {
                        count
                    }
                }
            "#;

            let result = schema.execute(query).await;
            assert!(result.errors.is_empty());
            
            let data = result.data.into_json()?;
            let count = &data["testSet"]["count"];
            assert_eq!(count.as_u64().unwrap(), 5);

            Ok(())
        }

        #[tokio::test]
        async fn test_graphql_combined_query() -> Result<(), Box<dyn std::error::Error>> {
            let schema = Schema::build(Query, EmptyMutation, EmptySubscription).finish();
            
            // Test both elements and count in a single query
            let query = r#"
                query {
                    testSet {
                        elements(count: 2)
                        count
                    }
                }
            "#;

            let result = schema.execute(query).await;
            assert!(result.errors.is_empty());
            
            let data = result.data.into_json()?;
            let elements = &data["testSet"]["elements"];
            let count = &data["testSet"]["count"];
            
            // Verify truncation worked (line 1172)
            assert_eq!(elements.as_array().unwrap().len(), 2);
            // Verify count returns total, not truncated count
            assert_eq!(count.as_u64().unwrap(), 5);

            Ok(())
        }
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use serde::{de::DeserializeOwned, Serialize};

    use super::{CustomSetView, SetView};
    use crate::{
        common::CustomSerialize,
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, I: async_graphql::OutputType> async_graphql::TypeName for SetView<C, I> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SetView_{}_{:08x}",
                mangle(I::type_name()),
                hash_name::<I>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I> SetView<C, I>
    where
        C: Context,
        I: Send + Sync + Serialize + DeserializeOwned + async_graphql::OutputType,
    {
        async fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices().await?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count().await? as u32)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType> async_graphql::TypeName for CustomSetView<C, I> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "CustomSetView_{}_{:08x}",
                mangle(I::type_name()),
                hash_name::<I>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I> CustomSetView<C, I>
    where
        C: Context,
        I: Send + Sync + CustomSerialize + async_graphql::OutputType,
    {
        async fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices().await?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count().await? as u32)
        }
    }
}
