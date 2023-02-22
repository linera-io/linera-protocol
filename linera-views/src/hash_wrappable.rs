use async_lock::Mutex;
use crate::common::Batch;
use crate::views::ViewError;
use crate::views::{HashableView, Hasher};
use crate::views::View;
use crate::common::Context;
use crate::common::HashOutput;
use async_trait::async_trait;

pub struct WrappedHashableContainerView<C,W>
{
    context: C,
    stored_hash: Option<HashOutput>,
    hash: Mutex<Option<HashOutput>>,
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
impl<C,W> View<C> for WrappedHashableContainerView<C,W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: View<C>,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self,ViewError> {
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
        self.inner.flush(&mut batch)?;
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
        self.inner.delete(&mut batch);
    }

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut() = None;
    }
}

#[async_trait]
impl<C, W> HashableView<C> for WrappedHashableContainerView<C,W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C>
{
    type Hasher = sha2::Sha512;

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
