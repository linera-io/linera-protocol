// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::BTreeMap, marker::PhantomData};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{CustomSerialize, Update},
    context::{BaseKey, SyncContext},
    store::ReadableSyncKeyValueStore as _,
    sync_view::SyncView,
    ViewError,
};

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

impl<C: SyncContext> SyncView for SyncByteSetView<C> {
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


impl<C: SyncContext> SyncByteSetView<C> {
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

impl<C: SyncContext> SyncByteSetView<C> {
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

impl<C: SyncContext> SyncByteSetView<C> {
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

impl<C: SyncContext, I: Send + Sync + Serialize> SyncView for SyncSetView<C, I> {
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

impl<C: SyncContext, I: Serialize> SyncSetView<C, I> {
    /// Inserts a value. If already present then no effect.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
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
    /// set.remove(&(34 as u32)).unwrap();
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

impl<C: SyncContext, I: Serialize> SyncSetView<C, I> {
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
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

impl<C: SyncContext, I: Serialize + DeserializeOwned + Send> SyncSetView<C, I> {
    /// Returns the list of indices in the set. The order is determined by serialization.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// assert_eq!(set.indices().unwrap(), vec![34 as u32]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })?;
        Ok(indices)
    }

    /// Returns the number of entries in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_view::set_view::SyncSetView};
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set: SyncSetView<_, u32> = SyncSetView::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
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
    /// set.insert(&(34 as u32)).unwrap();
    /// set.insert(&(37 as u32)).unwrap();
    /// set.insert(&(42 as u32)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set.for_each_key_while(|key| {
            let index = BaseKey::deserialize_value(key)?;
            f(index)
        })?;
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
    /// set.insert(&(34 as u32)).unwrap();
    /// set.insert(&(37 as u32)).unwrap();
    /// set.insert(&(42 as u32)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set.for_each_key(|key| {
            let index = BaseKey::deserialize_value(key)?;
            f(index)
        })?;
        Ok(())
    }
}

/// A view that supports a set of values with custom serialization.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I: Allocative")]
pub struct SyncCustomSetView<C, I> {
    /// The underlying set storing entries with custom-serialized keys.
    set: SyncByteSetView<C>,
    /// Phantom data for the key type.
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<C, I> SyncView for SyncCustomSetView<C, I>
where
    C: SyncContext,
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

impl<C: SyncContext, I: CustomSerialize> SyncCustomSetView<C, I> {
    /// Inserts a value. If present then it has no effect.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
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
    /// set.remove(&(34 as u128)).unwrap();
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
    C: SyncContext,
    I: CustomSerialize,
{
    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
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
    C: SyncContext,
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
    /// set.insert(&(34 as u128)).unwrap();
    /// set.insert(&(37 as u128)).unwrap();
    /// assert_eq!(set.indices().unwrap(), vec![34 as u128, 37 as u128]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })?;
        Ok(indices)
    }

    /// Returns the number of entries of the set.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::set_view::SyncCustomSetView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// set.insert(&(37 as u128)).unwrap();
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
    /// set.insert(&(34 as u128)).unwrap();
    /// set.insert(&(37 as u128)).unwrap();
    /// set.insert(&(42 as u128)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 5)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set.for_each_key_while(|key| {
            let index = I::from_custom_bytes(key)?;
            f(index)
        })?;
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
    /// set.insert(&(34 as u128)).unwrap();
    /// set.insert(&(37 as u128)).unwrap();
    /// set.insert(&(42 as u128)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set.for_each_key(|key| {
            let index = I::from_custom_bytes(key)?;
            f(index)
        })?;
        Ok(())
    }
}
