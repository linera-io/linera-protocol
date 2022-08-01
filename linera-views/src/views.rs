// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use std::{
    borrow::Borrow,
    cmp::Eq,
    collections::{hash_map, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
};

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database.
pub trait Context {
    /// The error type in use.
    type Error: Debug;

    /// How to create derived keys. By default, we concatenate the BCS-serialization of
    /// the index on top of the base key.
    ///
    /// By default, we assume that only one type `I` is possible for the index after a given base key.
    /// Otherwise, the type of `I` should be also serialized to make the encoding non-ambiguous.
    fn derive_key_bytes<I: serde::Serialize>(
        &mut self,
        base_key_bytes: &[u8],
        index: &I,
    ) -> Vec<u8> {
        let mut bytes = base_key_bytes.to_vec();
        bcs::serialize_into(&mut bytes, index).unwrap();
        bytes
    }
}

/// A view gives an exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C: Context>: Sized {
    /// The base address of the view.
    type Key;

    /// Create a view or a subview.
    async fn load(context: C, key: Self::Key) -> Result<Self, C::Error>;

    /// Discard all pending changes. After that `commit` should have no effect to storage.
    fn reset(&mut self);

    /// Persist changes to storage. This consumes the view. If the view is dropped without
    /// calling `commit`, staged changes are simply lost.
    async fn commit(self) -> Result<(), C::Error>;
}

/// Common traits for addresses. We typically wrap the bytes of a key in a dedicated
/// structure to enforce the schema.
pub trait Key: From<Vec<u8>> + AsRef<[u8]> {}

/// Create a new type of key implementing the trait Key.
macro_rules! declare_key {
    ($name:ident < $( $param:ident ),+ >, $doc:literal) => {

#[doc = $doc]
#[derive(Debug, Clone)]
pub struct $name< $( $param ),+ > {
    bytes: Vec<u8>,
    #[allow(unused_parens)]
    _marker: PhantomData<( $( $param ),+ )>,
}

impl< $( $param ),+ > From<Vec<u8>> for $name< $( $param ),+ > {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            _marker: PhantomData,
        }
    }
}

impl< $( $param ),+ > AsRef<[u8]> for $name< $( $param ),+ >
{
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl< $( $param ),+ > Key for $name< $( $param ),+ > {}

    }
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Clone)]
pub struct RegisterView<C, T> {
    context: C,
    key: RegisterKey<T>,
    old_value: T,
    new_value: Option<T>,
}

declare_key!(RegisterKey<T>, "The address of a [`RegisterView`]");

/// The context operations supporting [`RegisterView`].
#[async_trait]
pub trait RegisterOperations<T>: Context {
    async fn get(&mut self, key: &RegisterKey<T>) -> Result<T, Self::Error>;

    async fn set(&mut self, key: &RegisterKey<T>, value: T) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    T: Send + Sync,
{
    type Key = RegisterKey<T>;

    async fn load(mut context: C, key: Self::Key) -> Result<Self, C::Error> {
        let old_value = context.get(&key).await?;
        Ok(Self {
            context,
            key,
            old_value,
            new_value: None,
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if let Some(value) = self.new_value {
            self.context.set(&self.key, value).await?;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.new_value = None;
    }
}

impl<C, T> RegisterView<C, T> {
    /// Read the value in the register.
    pub fn get(&self) -> &T {
        match &self.new_value {
            Some(value) => value,
            None => &self.old_value,
        }
    }

    /// Set the value in the register.
    pub fn set(&mut self, value: T) {
        self.new_value = Some(value);
    }
}

/// A view that supports logging values of type `T`.
#[derive(Debug, Clone)]
pub struct AppendOnlyLogView<C, T> {
    context: C,
    key: AppendOnlyLogKey<T>,
    old_count: usize,
    new_values: Vec<T>,
}

declare_key!(
    AppendOnlyLogKey<T>,
    "The address of a [`AppendOnlyLogView`]"
);

/// The context operations supporting [`AppendOnlyLogView`].
#[async_trait]
pub trait AppendOnlyLogOperations<T>: Context {
    async fn count(&mut self, key: &AppendOnlyLogKey<T>) -> Result<usize, Self::Error>;

    async fn read(
        &mut self,
        key: &AppendOnlyLogKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<T>, Self::Error>;

    async fn append(
        &mut self,
        key: &AppendOnlyLogKey<T>,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    type Key = AppendOnlyLogKey<T>;

    async fn load(mut context: C, key: Self::Key) -> Result<Self, C::Error> {
        let old_count = context.count(&key).await?;
        Ok(Self {
            context,
            key,
            old_count,
            new_values: Vec::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if !self.new_values.is_empty() {
            self.context.append(&self.key, self.new_values).await?;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.new_values = Vec::new();
    }
}

impl<C, T> AppendOnlyLogView<C, T> {
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        self.old_count + self.new_values.len()
    }
}

impl<C, T> AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&mut self, mut range: std::ops::Range<usize>) -> Result<Vec<T>, C::Error> {
        if range.end > self.count() {
            range.end = self.count();
        }
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(range.end - range.start);
        if range.start < self.old_count {
            if range.end <= self.old_count {
                values.extend(self.context.read(&self.key, range.start..range.end).await?);
            } else {
                values.extend(
                    self.context
                        .read(&self.key, range.start..self.old_count)
                        .await?,
                );
                values.extend(self.new_values[0..(range.end - self.old_count)].to_vec());
            }
        } else {
            values.extend(
                self.new_values[(range.start - self.old_count)..(range.end - self.old_count)]
                    .to_vec(),
            );
        }
        Ok(values)
    }
}

/// A view that supports inserting and removing individual values indexed by their keys.
#[derive(Debug, Clone)]
pub struct MapView<C, K, V> {
    context: C,
    key: MapKey<K, V>,
    updates: HashMap<K, Option<V>>,
}

declare_key!(MapKey<K, V>, "The address of a [`MapView`]");

/// The context operations supporting [`MapView`].
#[async_trait]
pub trait MapOperations<K, V>: Context {
    async fn get<T>(&mut self, key: &MapKey<K, V>, index: &T) -> Result<Option<V>, Self::Error>
    where
        K: Borrow<T>,
        T: Eq + Hash + Sync + ?Sized;

    async fn insert(&mut self, key: &MapKey<K, V>, index: K, value: V) -> Result<(), Self::Error>;

    async fn remove(&mut self, key: &MapKey<K, V>, index: K) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, K, V> View<C> for MapView<C, K, V>
where
    C: MapOperations<K, V> + Send,
    K: Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    type Key = MapKey<K, V>;

    async fn load(context: C, key: Self::Key) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            key,
            updates: HashMap::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for (index, update) in self.updates {
            match update {
                None => {
                    self.context.remove(&self.key, index).await?;
                }
                Some(value) => {
                    self.context.insert(&self.key, index, value).await?;
                }
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.updates = HashMap::new();
    }
}

impl<C, K, V> MapView<C, K, V>
where
    K: Eq + Hash,
{
    /// Set or insert a value.
    pub fn insert(&mut self, index: K, value: V) {
        self.updates.insert(index, Some(value));
    }

    /// Remove a value.
    pub fn remove(&mut self, index: K) {
        self.updates.insert(index, None);
    }
}

impl<C, K, V> MapView<C, K, V>
where
    C: MapOperations<K, V>,
    K: Eq + Hash + Sync,
    V: Clone,
{
    /// Read the value at hte given postition, if any.
    pub async fn get<T>(&mut self, index: &T) -> Result<Option<V>, C::Error>
    where
        K: Borrow<T>,
        T: Eq + Hash + Sync + ?Sized,
    {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        self.context.get(&self.key, index).await
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key.
#[derive(Debug, Clone)]
pub struct CollectionView<C, I, W> {
    context: C,
    key: CollectionKey<I, W>,
    active_views: HashMap<I, W>,
}

declare_key!(CollectionKey<I, W>, "The address of a [`CollectionView`]");

/// The context operations supporting [`AppendOnlyLogView`].
pub trait CollectionOperations<I, W>: Context + Clone
where
    W: View<Self>,
{
    fn entry(&mut self, key: &CollectionKey<I, W>, index: &I) -> W::Key;
}

impl<C, I, W> CollectionOperations<I, W> for C
where
    C: Context + Clone,
    I: serde::Serialize,
    W: View<Self>,
    W::Key: From<Vec<u8>>,
{
    fn entry(&mut self, key: &CollectionKey<I, W>, index: &I) -> W::Key {
        let bytes = self.derive_key_bytes(&key.bytes, index);
        bytes.into()
    }
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I, W> + Send,
    I: Send,
    W: View<C> + Send,
    W::Key: Send,
{
    type Key = CollectionKey<I, W>;

    async fn load(context: C, key: Self::Key) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            key,
            active_views: HashMap::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for view in self.active_views.into_values() {
            view.commit().await?;
        }
        // TODO: clean up empty entries
        Ok(())
    }

    fn reset(&mut self) {
        self.active_views = HashMap::new();
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I, W> + Send,
    I: Eq + Hash + Sync + Clone + serde::Serialize,
    W: View<C>,
    W::Key: From<Vec<u8>>,
{
    /// Obtain a subview to access the data at the given index in the collection.
    pub async fn view(&mut self, index: I) -> Result<&mut W, C::Error> {
        match self.active_views.entry(index.clone()) {
            hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
            hash_map::Entry::Vacant(e) => {
                let inner_key = self.context.entry(&self.key, &index);
                let view = W::load(self.context.clone(), inner_key).await?;
                Ok(e.insert(view))
            }
        }
    }
}
