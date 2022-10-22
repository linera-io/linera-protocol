// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::views::{
    CollectionOperations, CollectionView, Context, LogOperations, LogView, MapOperations, MapView,
    QueueOperations, QueueView, ReentrantCollectionView, RegisterOperations, RegisterView,
    ScopedOperations, ScopedView, View, ViewError,
};
use async_trait::async_trait;
use serde::Serialize;
use std::{fmt::Debug, io::Write};

#[async_trait]
pub trait HashView<C: HashingContext>: View<C> {
    /// Compute the hash of the values.
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError>;
}

pub trait HashingContext: Context {
    type Hasher: Hasher;
}

pub trait Hasher: Default + Write + Send + Sync + 'static {
    type Output: Debug + Clone + Eq + AsRef<[u8]> + 'static;

    fn finalize(self) -> Self::Output;

    fn update_with_bcs_bytes(&mut self, value: &impl Serialize) -> Result<(), ViewError> {
        bcs::serialize_into(self, value)?;
        Ok(())
    }
}

impl Hasher for sha2::Sha512 {
    type Output = generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>;

    fn finalize(self) -> Self::Output {
        <sha2::Sha512 as sha2::Digest>::finalize(self)
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> HashView<C> for ScopedView<INDEX, W>
where
    C: HashingContext + Send + Sync + ScopedOperations + 'static,
    ViewError: From<C::Error>,
    W: HashView<C> + Send,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        self.view.hash().await
    }
}

#[async_trait]
impl<C, T> HashView<C> for RegisterView<C, T>
where
    C: HashingContext + RegisterOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Default + Send + Sync + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(self.get())?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, T> HashView<C> for LogView<C, T>
where
    C: HashingContext + LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let count = self.count();
        let elements = self.read(0..count).await?;
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, T> HashView<C> for QueueView<C, T>
where
    C: HashingContext + QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let count = self.count();
        let elements = self.read_front(count).await?;
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, I, V> HashView<C> for MapView<C, I, V>
where
    C: HashingContext + MapOperations<I, V> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Send + Sync + Serialize,
    V: Clone + Send + Sync + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        let indices = self.indices().await?;
        hasher.update_with_bcs_bytes(&indices.len())?;

        for index in indices {
            let value = self
                .get(&index)
                .await?
                .expect("The value for the returned index should be present");
            hasher.update_with_bcs_bytes(&index)?;
            hasher.update_with_bcs_bytes(&value)?;
        }
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, I, W> HashView<C> for CollectionView<C, I, W>
where
    C: HashingContext + CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Debug + Send + Sync + Serialize + 'static,
    W: HashView<C> + Send + 'static,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        let indices = self.indices().await?;
        hasher.update_with_bcs_bytes(&indices.len())?;
        for index in indices {
            hasher.update_with_bcs_bytes(&index)?;
            let view = self.load_entry(index).await?;
            let hash = view.hash().await?;
            hasher.write_all(hash.as_ref())?;
        }
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, I, W> HashView<C> for ReentrantCollectionView<C, I, W>
where
    C: HashingContext + CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Debug + Send + Sync + Serialize + 'static,
    W: HashView<C> + Send + 'static,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        let indices = self.indices().await?;
        hasher.update_with_bcs_bytes(&indices.len())?;
        for index in indices {
            hasher.update_with_bcs_bytes(&index)?;
            let mut view = self.try_load_entry(index).await?;
            let hash = view.hash().await?;
            hasher.write_all(hash.as_ref())?;
        }
        Ok(hasher.finalize())
    }
}
