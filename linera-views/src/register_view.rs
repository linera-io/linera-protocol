// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    linera_base::sync::Lazy,
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{from_bytes_option, Context, HasherOutput},
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
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

/// A view that supports modifying a single value of type `T`.
#[derive(Debug)]
pub struct RegisterView<C, T> {
    delete_storage_first: bool,
    context: C,
    stored_value: Box<T>,
    update: Option<Box<T>>,
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 1;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key()])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value = from_bytes_option(values.first().unwrap())?;
        let stored_value = Box::new(value.unwrap_or_default());
        Ok(Self {
            delete_storage_first: false,
            context,
            stored_value,
            update: None,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.update = None;
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        self.update.is_some()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key(self.context.base_key());
            self.stored_value = Box::default();
            delete_view = true;
        } else if let Some(value) = self.update.take() {
            let key = self.context.base_key();
            batch.put_key_value(key, &value)?;
            self.stored_value = value;
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
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
        self.compute_hash()
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash()
    }
}

/// Type wrapping `RegisterView` while memoizing the hash.
pub type HashedRegisterView<C, T> =
    WrappedHashableContainerView<C, RegisterView<C, T>, HasherOutput>;
