// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{from_bytes_opt, Context, HasherOutput, MIN_VIEW_TAG},
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    linera_base::sync::Lazy,
    prometheus::HistogramVec,
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static REGISTER_VIEW_HASH_RUNTIME: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "register_view_hash_runtime",
        "RegisterView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

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
    delete_storage_first: bool,
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
        let values_bytes = context.read_multi_values_bytes(keys).await?;
        let stored_value = Box::new(from_bytes_opt(&values_bytes[0])?.unwrap_or_default());
        let hash = from_bytes_opt(&values_bytes[1])?;
        Ok(Self {
            delete_storage_first: false,
            context,
            stored_value,
            update: None,
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.update = None;
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key());
            self.stored_value = Box::default();
            self.stored_hash = None;
        } else if let Some(value) = self.update.take() {
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
        self.delete_storage_first = false;
        Ok(())
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
        *self.hash.get_mut() = None;
    }
}

impl<C, T> ClonableView<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(RegisterView {
            delete_storage_first: self.delete_storage_first,
            context: self.context.clone(),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut()),
        })
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
        self.delete_storage_first = false;
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
        self.delete_storage_first = false;
        match &mut self.update {
            Some(value) => value,
            update => {
                *update = Some(self.stored_value.clone());
                update.as_mut().unwrap()
            }
        }
    }

    fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = REGISTER_VIEW_HASH_RUNTIME.measure_latency();
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
                let new_hash = self.compute_hash()?;
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
                let new_hash = self.compute_hash()?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}
