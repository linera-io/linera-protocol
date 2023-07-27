// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{from_bytes_opt, Context, HasherOutput, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// Key tags to create the sub-keys of a RegisterView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the value
    Value = MIN_VIEW_TAG,
    /// Prefix for the hash
    Hash,
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug)]
pub struct RegisterView<C, T> {
    context: C,
    stored_value: Box<T>,
    update: Option<Box<T>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
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
        let key1 = context.base_tag(KeyTag::Value as u8);
        let key2 = context.base_tag(KeyTag::Hash as u8);
        let keys = vec![key1, key2];
        let values_bytes = context.read_multi_key_bytes(keys).await?;
        let stored_value = Box::new(from_bytes_opt(values_bytes[0].clone())?.unwrap_or_default());
        let hash = from_bytes_opt(values_bytes[1].clone())?;
        Ok(Self {
            context,
            stored_value,
            update: None,
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.update = None;
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            let key = self.context.base_tag(KeyTag::Value as u8);
            batch.put_key_value(key, &value)?;
            self.stored_value = value;
        }
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
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.update = Some(Box::default());
        *self.hash.get_mut() = None;
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut register = RegisterView::<_,u32>::load(context).await.unwrap();
    ///   let value = register.get();
    ///   assert_eq!(*value, 0);
    /// # })
    /// ```
    pub fn get(&self) -> &T {
        match &self.update {
            None => &self.stored_value,
            Some(value) => value,
        }
    }

    /// Sets the value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut register = RegisterView::load(context).await.unwrap();
    ///   register.set(5);
    ///   let value = register.get();
    ///   assert_eq!(*value, 5);
    /// # })
    /// ```
    pub fn set(&mut self, value: T) {
        self.update = Some(Box::new(value));
        *self.hash.get_mut() = None;
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
    T: Clone + Serialize,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut register : RegisterView<_,u32> = RegisterView::load(context).await.unwrap();
    ///   let value = register.get_mut();
    ///   assert_eq!(*value, 0);
    /// # })
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        *self.hash.get_mut() = None;
        match &mut self.update {
            Some(value) => value,
            update => {
                *update = Some(self.stored_value.clone());
                update.as_mut().unwrap()
            }
        }
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(self.get())?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C, T> HashableView<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
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
                let new_hash = self.compute_hash().await?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}
