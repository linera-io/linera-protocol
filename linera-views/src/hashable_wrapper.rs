// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, MIN_VIEW_TAG},
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use futures::join;
use serde::{de::DeserializeOwned, Serialize};
use std::ops::{Deref, DerefMut};

/// A hash for ContainerView and storing of the hash for memoization purposes
#[derive(Debug)]
pub struct WrappedHashableContainerView<C, W, O> {
    context: C,
    stored_hash: Option<O>,
    hash: Mutex<Option<O>>,
    inner: W,
}

/// Key tags to create the sub-keys of a MapView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

/// That is used for accessing whether we need to delete the storage first before
/// writing
pub(crate) trait DeleteStorageFirst {
    fn delete_storage_first(&self) -> bool;
}

#[async_trait]
impl<C, W, O> View<C> for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C> + DeleteStorageFirst + Send,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: Hasher<Output = O>,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let hash_key = context.base_tag(KeyTag::Hash as u8);
        let base_key = context.base_tag(KeyTag::Index as u8);
        let (hash, inner) = join!(
            context.read_value(&hash_key),
            W::load(context.clone_with_base_key(base_key))
        );
        let hash = hash?;
        let inner = inner?;
        Ok(Self {
            context,
            stored_hash: hash,
            hash: Mutex::new(hash),
            inner,
        })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.delete_storage_first() {
            self.stored_hash = None;
        }
        self.inner.flush(batch)?;
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

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C, W, O> ClonableView<C> for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C> + ClonableView<C> + DeleteStorageFirst + Send,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: Hasher<Output = O>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(WrappedHashableContainerView {
            context: self.context.clone(),
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut()),
            inner: self.inner.clone_unchecked()?,
        })
    }
}

#[async_trait]
impl<C, W, O> HashableView<C> for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C> + DeleteStorageFirst + Send + Sync,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: Hasher<Output = O>,
{
    type Hasher = W::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.inner.hash_mut().await?;
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
                let new_hash = self.inner.hash().await?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}

impl<C, W, O> Deref for WrappedHashableContainerView<C, W, O> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.inner
    }
}

impl<C, W, O> DerefMut for WrappedHashableContainerView<C, W, O> {
    fn deref_mut(&mut self) -> &mut W {
        *self.hash.get_mut() = None;
        &mut self.inner
    }
}
