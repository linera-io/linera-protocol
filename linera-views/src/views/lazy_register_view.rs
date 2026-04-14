// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{from_bytes_option_or_default, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    store::ReadableKeyValueStore,
    views::{ClonableView, HashableView, Hasher, ReplaceContext, View},
    ViewError,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static LAZY_REGISTER_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "lazy_register_view_hash_runtime",
            "LazyRegisterView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A view that supports modifying a single value of type `T`.
/// Unlike [`crate::register_view::RegisterView`], the value is not loaded from storage until it is first accessed.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct LazyRegisterView<C, T> {
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The value persisted in storage, loaded lazily on first access.
    /// `OnceLock` replaces both the `Mutex` and the `Option` that were
    /// previously used: empty means not yet loaded, set means loaded.
    #[allocative(skip)]
    stored_value: OnceLock<Box<T>>,
    /// Pending update not yet persisted to storage.
    update: Option<Box<T>>,
}

impl<C, T, C2> ReplaceContext<C2> for LazyRegisterView<C, T>
where
    C: Context,
    C2: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = LazyRegisterView<C2, T>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        let stored_value = self.stored_value.clone();
        LazyRegisterView {
            delete_storage_first: self.delete_storage_first,
            context: ctx(&self.context),
            stored_value,
            update: self.update.clone(),
        }
    }
}

impl<C, T> View for LazyRegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 0;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![])
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(Self {
            delete_storage_first: false,
            context,
            stored_value: OnceLock::new(),
            update: None,
        })
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

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key(self.context.base_key().bytes.clone());
            delete_view = true;
        } else if let Some(value) = &self.update {
            let key = self.context.base_key().bytes.clone();
            batch.put_key_value(key, value)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.delete_storage_first {
            self.stored_value = OnceLock::from(Box::<T>::default());
        } else if let Some(value) = self.update.take() {
            self.stored_value = OnceLock::from(value);
        }
        self.delete_storage_first = false;
        self.update = None;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
    }
}

impl<C, T> ClonableView for LazyRegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let stored_value = self.stored_value.clone();
        Ok(LazyRegisterView {
            delete_storage_first: self.delete_storage_first,
            context: self.context.clone(),
            stored_value,
            update: self.update.clone(),
        })
    }
}

impl<C, T> LazyRegisterView<C, T>
where
    C: Context,
    T: Default + DeserializeOwned,
{
    /// Access the current value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::lazy_register_view::LazyRegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let register = LazyRegisterView::<_, u32>::load(context).await.unwrap();
    /// let value = register.get().await.unwrap();
    /// assert_eq!(*value, 0);
    /// # })
    /// ```
    pub async fn get(&self) -> Result<&T, ViewError> {
        if let Some(value) = &self.update {
            return Ok(value);
        }
        if self.stored_value.get().is_none() {
            let key = self.context.base_key().bytes.clone();
            let bytes = self.context.store().read_value_bytes(&key).await?;
            let value = from_bytes_option_or_default(&bytes)?;
            let _ = self.stored_value.set(Box::new(value));
        }
        Ok(self.stored_value.get().unwrap())
    }

    /// Sets the value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::lazy_register_view::LazyRegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut register = LazyRegisterView::load(context).await.unwrap();
    /// register.set(5);
    /// let value = register.get().await.unwrap();
    /// assert_eq!(*value, 5);
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

impl<C, T> LazyRegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Serialize + DeserializeOwned,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::lazy_register_view::LazyRegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut register: LazyRegisterView<_, u32> = LazyRegisterView::load(context).await.unwrap();
    /// let value = register.get_mut().await.unwrap();
    /// assert_eq!(*value, 0);
    /// # })
    /// ```
    pub async fn get_mut(&mut self) -> Result<&mut T, ViewError> {
        self.delete_storage_first = false;
        if self.update.is_none() {
            if self.stored_value.get().is_none() {
                let key = self.context.base_key().bytes.clone();
                let bytes = self.context.store().read_value_bytes(&key).await?;
                let value = from_bytes_option_or_default(&bytes)?;
                let _ = self.stored_value.set(Box::new(value));
            }
            self.update = Some(self.stored_value.get().unwrap().clone());
        }
        Ok(self.update.as_mut().unwrap())
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::LAZY_REGISTER_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(self.get().await?)?;
        Ok(hasher.finalize())
    }
}

impl<C, T> HashableView for LazyRegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }
}

/// Type wrapping `LazyRegisterView` while memoizing the hash.
pub type HashedLazyRegisterView<C, T> =
    WrappedHashableContainerView<C, LazyRegisterView<C, T>, HasherOutput>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::LazyRegisterView;
    use crate::context::Context;

    impl<C, T> async_graphql::OutputType for LazyRegisterView<C, T>
    where
        C: Context,
        T: async_graphql::OutputType + Default + Send + Sync + serde::de::DeserializeOwned,
    {
        fn type_name() -> Cow<'static, str> {
            T::type_name()
        }

        fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
            T::create_type_info(registry)
        }

        async fn resolve(
            &self,
            ctx: &async_graphql::ContextSelectionSet<'_>,
            field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
        ) -> async_graphql::ServerResult<async_graphql::Value> {
            self.get()
                .await
                .map_err(|e| async_graphql::ServerError::new(e.to_string(), Some(field.pos)))?
                .resolve(ctx, field)
                .await
        }
    }
}
