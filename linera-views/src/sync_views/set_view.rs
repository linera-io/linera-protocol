// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, marker::PhantomData};

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::CustomSerialize,
    context::{BaseKey, SyncContext},
    store::SyncReadableKeyValueStore as _,
    sync_views::SyncView,
    views::set_view::ByteSetView,
    ViewError,
};

/// A synchronous [`SyncView`] that supports inserting and removing values indexed by a key.
///
/// This is a thin wrapper around [`ByteSetView`] that implements [`SyncView`]
/// instead of [`View`](crate::views::View).
#[derive(Debug, Allocative)]
#[allocative(bound = "C")]
pub struct SyncByteSetView<C>(
    /// The inner async byte set view whose state and logic we reuse.
    pub(crate) ByteSetView<C>,
);

impl<C: SyncContext> SyncView for SyncByteSetView<C> {
    const NUM_INIT_KEYS: usize = 0;

    type Context = C;

    fn context(&self) -> C {
        self.0.context.clone()
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(Vec::new())
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(SyncByteSetView(ByteSetView::base_post_load(context)))
    }

    fn rollback(&mut self) {
        self.0.base_rollback();
    }

    fn has_pending_changes(&self) -> bool {
        self.0.base_has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        Ok(self.0.base_pre_save(batch, self.0.context.base_key()))
    }

    fn post_save(&mut self) {
        self.0.base_post_save();
    }

    fn clear(&mut self) {
        self.0.base_clear();
    }
}

impl<C> SyncByteSetView<C> {
    /// Inserts a value. If already present then it has no effect.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), true);
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>) {
        self.0.insert(short_key);
    }

    /// Removes a value from the set. If absent then no effect.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.remove(vec![0, 1]);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), false);
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        self.0.remove(short_key);
    }
}

impl<C: SyncContext> SyncByteSetView<C> {
    /// Gets the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.0.context.extra()
    }

    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// assert_eq!(set.contains(&[34]).unwrap(), false);
    /// assert_eq!(set.contains(&[0, 1]).unwrap(), true);
    /// ```
    pub fn contains(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        if let Some(update) = self.0.updates.get(short_key) {
            let value = match update {
                crate::common::Update::Removed => false,
                crate::common::Update::Set(()) => true,
            };
            return Ok(value);
        }
        if self.0.delete_storage_first {
            return Ok(false);
        }
        let key = self.0.context.base_key().base_index(short_key);
        Ok(self.0.context.store().contains_key(&key)?)
    }

    /// Returns the list of keys in the set. The order is lexicographic.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
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
        })?;
        Ok(keys)
    }

    /// Returns the number of entries in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// assert_eq!(set.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_key(|_key| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false, then the loop ends
    /// prematurely.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// set.insert(vec![0, 3]);
    /// let mut count = 0;
    /// set.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.0.updates.iter();
        let mut update = updates.next();
        if !self.0.delete_storage_first {
            let base = &self.0.context.base_key().bytes;
            for index in self.0.context.store().find_keys_by_prefix(base)? {
                loop {
                    match update {
                        Some((key, value)) if key <= &index => {
                            if let crate::common::Update::Set(_) = value {
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
            if let crate::common::Update::Set(_) = value {
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncByteSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncByteSetView::load(context).unwrap();
    /// set.insert(vec![0, 1]);
    /// set.insert(vec![0, 2]);
    /// let mut count = 0;
    /// set.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
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

/// A synchronous [`SyncView`] implementing the set functionality with the index `I` being
/// any serializable type.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I")]
pub struct SyncSetView<C, I> {
    /// The underlying sync byte set view.
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// assert!(set.contains(&(34 as u32)).unwrap());
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// set.remove(&(34 as u32)).unwrap();
    /// assert!(!set.contains(&(34 as u32)).unwrap());
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

    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// assert!(set.contains(&(34 as u32)).unwrap());
    /// assert!(!set.contains(&(37 as u32)).unwrap());
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// set.insert(&(37 as u32)).unwrap();
    /// assert_eq!(set.indices().unwrap(), vec![34, 37]);
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// set.insert(&(37 as u32)).unwrap();
    /// assert_eq!(set.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.set.count()
    }

    /// Applies a function f on each index. If the function returns false, then the
    /// loop ends prematurely.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// set.insert(&(37 as u32)).unwrap();
    /// set.insert(&(38 as u32)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index_while(|_index| {
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

    /// Applies a function f on each index.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncSetView::<_, u32>::load(context).unwrap();
    /// set.insert(&(34 as u32)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index(|_index| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 1);
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

/// A synchronous view that supports a set of values with custom serialization.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I: Allocative")]
pub struct SyncCustomSetView<C, I> {
    /// The underlying sync byte set view.
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// assert!(set.contains(&(34 as u128)).unwrap());
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// set.remove(&(34 as u128)).unwrap();
    /// assert!(!set.contains(&(34 as u128)).unwrap());
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

    /// Returns true if the given index exists in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// assert!(set.contains(&(34 as u128)).unwrap());
    /// assert!(!set.contains(&(37 as u128)).unwrap());
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
    /// Returns the list of indices in the set.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
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
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// assert_eq!(set.count().unwrap(), 1);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.set.count()
    }

    /// Applies a function f on each index. If the function returns false, then the
    /// loop prematurely ends.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// set.insert(&(37 as u128)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index_while(|_index| {
    ///     count += 1;
    ///     Ok(count < 5)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
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

    /// Applies a function f on each index.
    /// ```rust
    /// # use linera_views::{context::SyncMemoryContext, sync_views::set_view::SyncCustomSetView};
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut set = SyncCustomSetView::<_, u128>::load(context).unwrap();
    /// set.insert(&(34 as u128)).unwrap();
    /// let mut count = 0;
    /// set.for_each_index(|_index| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 1);
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

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use serde::{de::DeserializeOwned, Serialize};

    use super::{SyncCustomSetView, SyncSetView};
    use crate::{
        common::CustomSerialize,
        context::SyncContext,
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
        C: SyncContext + Send + Sync,
        I: Send + Sync + Serialize + DeserializeOwned + async_graphql::OutputType,
    {
        async fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices()?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }
    }

    impl<C: Send + Sync, I: async_graphql::OutputType> async_graphql::TypeName
        for SyncCustomSetView<C, I>
    {
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
        C: SyncContext + Send + Sync,
        I: Send + Sync + CustomSerialize + async_graphql::OutputType,
    {
        async fn elements(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
            let mut indices = self.indices()?;
            if let Some(count) = count {
                indices.truncate(count);
            }
            Ok(indices)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }
    }
}
