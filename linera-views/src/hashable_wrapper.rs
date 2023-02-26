use crate::{
    common::{Batch, Context},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
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
    /// Prefix for the indices of the view
    Index = 0,
    /// Prefix for the hash
    Hash = 1,
}

#[async_trait]
impl<C, W, O> View<C> for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: Hasher<Output = O>,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        let base_key = context.base_tag(KeyTag::Index as u8);
        let inner = W::load(context.clone_with_base_key(base_key)).await?;
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

    fn delete(self, batch: &mut Batch) {
        self.inner.delete(batch);
    }

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut() = None;
    }
}

#[async_trait]
impl<C, W, O> HashableView<C> for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C> + Send + Sync,
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
