use crate::{
    common::{Batch, Context, HashOutput},
    views::{HashView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// prefix used.
///
/// 0 : for the storing of the value
/// 1 : for the hash
enum KeyTag {
    Value = 0,
    Hash = 1,
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug)]
pub struct RegisterView<C, T> {
    context: C,
    stored_value: T,
    update: Option<T>,
    hash: Option<HashOutput>,
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Value as u8);
        let stored_value = context.read_key(&key).await?.unwrap_or_default();
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            stored_value,
            update: None,
            hash,
        })
    }

    fn rollback(&mut self) {
        self.update = None;
        self.hash = None;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            let key = self.context.base_tag(KeyTag::Value as u8);
            batch.put_key_value(key, &value)?;
            self.stored_value = value;
        }
        let key = self.context.base_tag(KeyTag::Hash as u8);
        match self.hash {
            None => batch.delete_key(key),
            Some(hash) => batch.put_key_value(key, &hash)?,
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.update = Some(T::default());
        self.hash = None;
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
    pub fn get(&self) -> &T {
        match &self.update {
            None => &self.stored_value,
            Some(value) => value,
        }
    }

    /// Set the value in the register.
    pub fn set(&mut self, value: T) {
        self.update = Some(value);
        self.hash = None;
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
    T: Clone,
{
    /// Obtain a mutable reference to the value in the register.
    pub fn get_mut(&mut self) -> &mut T {
        self.hash = None;
        match &mut self.update {
            Some(value) => value,
            update => {
                *update = Some(self.stored_value.clone());
                update.as_mut().unwrap()
            }
        }
    }
}

#[async_trait]
impl<C, T> HashView<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                hasher.update_with_bcs_bytes(self.get())?;
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}
