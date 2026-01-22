// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::BTreeMap, marker::PhantomData};

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{CustomSerialize, HasherOutput, Update},
    context::{BaseKey, Context},
    store::ReadableSyncKeyValueStore as _,
    sync_view::{
        hashable_wrapper::SyncWrappedHashableContainerView,
        historical_hash_wrapper::SyncHistoricallyHashableView,
        Hasher, SyncClonableView, SyncHashableView, SyncReplaceContext, SyncView,
    },
    ViewError,
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
            "SyncSetView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A [`SyncView`] that supports inserting and removing values indexed by a key.
#[derive(Debug, Allocative)]
#[allocative(bound = "C")]
pub struct SyncByteSetView<C> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// Pending changes not yet persisted to storage.
    updates: BTreeMap<Vec<u8>, Update<()>>,
}

impl<C: Context, C2: Context> SyncReplaceContext<C2> for SyncByteSetView<C> {
    type Target = SyncByteSetView<C2>;

    fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SyncByteSetView {
            context: ctx(&self.context),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
        }
    }
}

impl<C: Context> SyncView for SyncByteSetView<C> {
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
            delete_storage_first: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
    }

    fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        !self.updates.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            for (index, update) in self.updates.iter() {
                if let Update::Set(_) = update {
                    let key = self.context.base_key().base_index(index);
                    batch.put_key_value_bytes(key, Vec::new());
                    delete_view = false;
                }
            }
        } else {
            for (index, update) in self.updates.iter() {
                let key = self.context.base_key().base_index(index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(_) => batch.put_key_value_bytes(key, Vec::new()),
                }
            }
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.clear();
    }
}

impl<C: Context> SyncClonableView for SyncByteSetView<C> {
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SyncByteSetView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
        })
    }
}

impl<C: Context> SyncByteSetView<C> {
    /// Inserts a value. If already present then it has no effect.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), true);
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>) {
        self.updates.insert(short_key, Update::Set(()));
    }

    /// Removes a value from the set. If absent then no effect.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.remove(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), false);
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

impl<C: Context> SyncByteSetView<C> {
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[34]).unwrap(), false);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), true);
    /// ```
    pub fn contains(&self, short_key: &[u8]) -> Result<bool, ViewError> {
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
        Ok(self.context.store().contains_key(&key)?)
    }
}

impl<C: Context> SyncByteSetView<C> {
    /// Returns the list of keys in the set. The order is lexicographic.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// assert_eq!(set.keys().unwrap(), vec![vec![0, 1], vec![0, 2]]);
    /// ```
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })
        ?;
        Ok(keys)
    }

    /// Returns the number of entries in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// assert_eq!(set.keys().unwrap(), vec![vec![0, 1], vec![0, 2]]);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_key(|_key| {
            count += 1;
            Ok(())
        })
        ?;
        Ok(count)
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false, then the loop ends
    /// prematurely.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// set.insert(vec![3]);
    /// let mut count = 0;
    /// set.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.delete_storage_first {
            let base = &self.context.base_key().bytes;
            for index in self.context.store().find_keys_by_prefix(base)? {
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
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncByteSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// set.insert(vec![3]);
    /// let mut count = 0;
    /// set.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(|key| {
            f(key)?;
            Ok(true)
        })
        
    }
}

impl<C: Context> SyncHashableView for SyncByteSetView<C> {
    type Hasher = sha3::Sha3_256;

    fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash()
    }

    fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::SET_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
        self.for_each_key(|key| {
            count += 1;
            hasher.update_with_bytes(key)?;
            Ok(())
        })
        ?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

/// A [`SyncView`] implementing the set functionality with the index `I` being any serializable type.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I")]
pub struct SyncSetView<C, I> {
    /// The underlying set storing entries with serialized keys.
    set: SyncByteSetView<C>,
    /// Phantom data for the key type.
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<C: Context, I: Send + Sync + Serialize, C2: Context> SyncReplaceContext<C2> for SyncSetView<C, I> {
    type Target = SyncSetView<C2, I>;

    fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SyncSetView {
            set: self.set.with_context(ctx),
            _phantom: self._phantom,
        }
    }
}

impl<C: Context, I: Send + Sync + Serialize> SyncView for SyncSetView<C, I> {
    const NUM_INIT_KEYS: usize = SyncByteSetView::<C>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> C {
        self.set.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteSetView::<C>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let set = SyncByteSetView::post_load(context, values)?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.set.has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.set.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.set.post_save()
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> SyncClonableView for SyncSetView<C, I>
where
    C: Context,
    I: Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SyncSetView {
            set: self.set.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C: Context, I: Serialize> SyncSetView<C, I> {
    /// Inserts a value. If already present then no effect.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.indices().unwrap().len(), 1);
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
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.remove(&(34 as u32));
    /// assert_eq!(set.indices().unwrap().len(), 0);
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

impl<C: Context, I: Serialize> SyncSetView<C, I> {
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.contains(&(34 as u32)).unwrap(), true);
    /// assert_eq!(set.contains(&(45 as u32)).unwrap(), false);
    /// ```
    pub fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.set.contains(&short_key)
    }
}

impl<C: Context, I: Serialize + DeserializeOwned + Send> SyncSetView<C, I> {
    /// Returns the list of indices in the set. The order is determined by serialization.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.indices().unwrap(), vec![34 as u32]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        ?;
        Ok(indices)
    }

    /// Returns the number of entries in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// assert_eq!(set.count().unwrap(), 1);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.set.count()
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false, then the
    /// loop ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// set.insert(&(37 as u32));
    /// set.insert(&(42 as u32));
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set
            .for_each_key_while(|key| {
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            ?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32));
    /// set.insert(&(37 as u32));
    /// set.insert(&(42 as u32));
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            ?;
        Ok(())
    }
}

impl<C, I> SyncHashableView for SyncSetView<C, I>
where
    Self: SyncView,
    SyncByteSetView<C>: SyncHashableView,
{
    type Hasher = <SyncByteSetView<C> as SyncHashableView>::Hasher;

    fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut()
    }

    fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash()
    }
}

/// A [`SyncView`] implementing the set functionality with the index `I` being a type with a custom
/// serialization format.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I")]
pub struct SyncCustomSetView<C, I> {
    /// The underlying set storing entries with custom-serialized keys.
    set: SyncByteSetView<C>,
    /// Phantom data for the key type.
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<C, I> SyncView for SyncCustomSetView<C, I>
where
    C: Context,
    I: Send + Sync + CustomSerialize,
{
    const NUM_INIT_KEYS: usize = SyncByteSetView::<C>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> C {
        self.set.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteSetView::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let set = SyncByteSetView::post_load(context, values)?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.set.has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.set.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.set.post_save()
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> SyncClonableView for SyncCustomSetView<C, I>
where
    C: Context,
    I: Send + Sync + CustomSerialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SyncCustomSetView {
            set: self.set.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C: Context, I: CustomSerialize> SyncCustomSetView<C, I> {
    /// Inserts a value. If present then it has no effect.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// assert_eq!(set.indices().unwrap().len(), 1);
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.remove(&(34 as u128));
    /// assert_eq!(set.indices().unwrap().len(), 0);
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

impl<C, I> SyncCustomSetView<C, I>
where
    C: Context,
    I: CustomSerialize,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// assert_eq!(set.contains(&(34 as u128)).unwrap(), true);
    /// assert_eq!(set.contains(&(37 as u128)).unwrap(), false);
    /// ```
    pub fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.set.contains(&short_key)
    }
}

impl<C, I> SyncCustomSetView<C, I>
where
    C: Context,
    I: Sync + Send + CustomSerialize,
{
    /// Returns the list of indices in the set. The order is determined by the custom
    /// serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// assert_eq!(set.indices().unwrap(), vec![34 as u128, 37 as u128]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        ?;
        Ok(indices)
    }

    /// Returns the number of entries of the set.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// assert_eq!(set.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.set.count()
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function does return
    /// false, then the loop prematurely ends.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// set.insert(&(42 as u128));
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 5)
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            ?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128));
    /// set.insert(&(37 as u128));
    /// set.insert(&(42 as u128));
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// 
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            ?;
        Ok(())
    }
}

impl<C: Context, I> SyncHashableView for SyncCustomSetView<C, I>
where
    Self: SyncView,
{
    type Hasher = sha3::Sha3_256;

    fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut()
    }

    fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash()
    }
}

/// Type wrapping `SyncByteSetView` while memoizing the hash.
pub type SyncHashedByteSetView<C> = SyncWrappedHashableContainerView<C, SyncByteSetView<C>, HasherOutput>;

/// Wrapper around `SyncByteSetView` to compute hashes based on the history of changes.
pub type SyncHistoricallyHashedByteSetView<C> = SyncHistoricallyHashableView<C, SyncByteSetView<C>>;

/// Type wrapping `SyncSetView` while memoizing the hash.
pub type SyncHashedSetView<C, I> = SyncWrappedHashableContainerView<C, SyncSetView<C, I>, HasherOutput>;

/// Wrapper around `SyncSetView` to compute hashes based on the history of changes.
pub type SyncHistoricallyHashedSetView<C, I> = SyncHistoricallyHashableView<C, SyncSetView<C, I>>;

/// Type wrapping `SyncCustomSetView` while memoizing the hash.
pub type SyncHashedCustomSetView<C, I> =
    SyncWrappedHashableContainerView<C, SyncCustomSetView<C, I>, HasherOutput>;

/// Wrapper around `SyncCustomSetView` to compute hashes based on the history of changes.
pub type SyncHistoricallyHashedCustomSetView<C, I> = SyncHistoricallyHashableView<C, SyncCustomSetView<C, I>>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use serde::{de::DeserializeOwned, Serialize};

    use super::{SyncCustomSetView, SyncSetView};
    use crate::{
        common::CustomSerialize,
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, I: async_graphql::OutputType> async_graphql::TypeName for SyncSetView<C, I> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncSetView_{}_{:08x}",
                mangle(I::type_name()),
                hash_name::<I>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I> SyncSetView<C, I>
    where
        C: Context,
        I: Send + Sync + Serialize + DeserializeOwned + async_graphql::OutputType,
    {
        fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices()?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType> async_graphql::TypeName for SyncCustomSetView<C, I> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncCustomSetView_{}_{:08x}",
                mangle(I::type_name()),
                hash_name::<I>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, I> SyncCustomSetView<C, I>
    where
        C: Context,
        I: Send + Sync + CustomSerialize + async_graphql::OutputType,
    {
        fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices()?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }
    }
}
