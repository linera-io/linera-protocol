// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::views::*;
use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha512};
use std::{fmt::Debug, io::Write};

pub type Sha512Value = generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>;

#[async_trait]
pub trait HashView<C: Context>: View<C> {
    /// Compute the hash of the values.
    async fn hash(&mut self) -> Result<Sha512Value, C::Error>;
}

#[async_trait]
impl<C, W, const INDEX: u64> HashView<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + ScopedOperations + 'static,
    W: HashView<C> + Send,
{
    async fn hash(&mut self) -> Result<Sha512Value, C::Error> {
        self.view.hash().await
    }
}

#[async_trait]
impl<C, T> HashView<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    T: Default + Send + Sync + Serialize,
{
    async fn hash(&mut self) -> Result<Sha512Value, C::Error> {
        let mut hasher = Sha512::default();
        bcs::serialize_into(&mut hasher, self.get())?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, T> HashView<C> for AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone + Serialize,
{
    async fn hash(&mut self) -> Result<Sha512Value, C::Error> {
        let count = self.count();
        let elements = self.read(0..count).await?;
        let mut hasher = Sha512::default();
        bcs::serialize_into(&mut hasher, &elements)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, I, V> HashView<C> for MapView<C, I, V>
where
    C: MapOperations<I, V> + Send,
    I: Eq + Ord + Clone + Send + Sync + Serialize,
    V: Clone + Send + Sync + Serialize,
{
    async fn hash(&mut self) -> Result<Sha512Value, C::Error> {
        let mut hasher = Sha512::default();
        let indices = self.indices().await?;
        bcs::serialize_into(&mut hasher, &indices.len())?;
        for index in indices {
            let value = self
                .get(&index)
                .await?
                .expect("The value for the returned index should be present");
            bcs::serialize_into(&mut hasher, &index)?;
            bcs::serialize_into(&mut hasher, &value)?;
        }
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, I, W> HashView<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Eq + Ord + Clone + Debug + Send + Sync + Serialize + 'static,
    W: HashView<C> + Send + 'static,
{
    async fn hash(&mut self) -> Result<Sha512Value, C::Error> {
        let mut hasher = Sha512::default();
        let indices = self.indices().await?;
        bcs::serialize_into(&mut hasher, &indices.len())?;
        for index in indices {
            bcs::serialize_into(&mut hasher, &index)?;
            let view = self.load_entry(index).await?;
            let hash = view.hash().await?;
            hasher.write_all(&hash)?;
        }
        Ok(hasher.finalize())
    }
}
