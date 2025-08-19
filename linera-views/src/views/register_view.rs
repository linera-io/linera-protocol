// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{from_bytes_option_or_default, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, HashableView, Hasher, ReplaceContext, View},
    ViewError,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static REGISTER_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "register_view_hash_runtime",
            "RegisterView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug)]
pub struct RegisterView<C, T> {
    delete_storage_first: bool,
    context: C,
    stored_value: Box<T>,
    update: Option<Box<T>>,
}

impl<C, T, C2> ReplaceContext<C2> for RegisterView<C, T>
where
    C: Context,
    C2: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = RegisterView<C2, T>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        RegisterView {
            delete_storage_first: self.delete_storage_first,
            context: ctx(self.context()),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
        }
    }
}

impl<C, T> View for RegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().bytes.clone()])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let stored_value = Box::new(value);
        Ok(Self {
            delete_storage_first: false,
            context,
            stored_value,
            update: None,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
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
            batch.delete_key(self.context.base_key().bytes.clone());
            self.stored_value = Box::default();
            delete_view = true;
        } else if let Some(value) = self.update.take() {
            let key = self.context.base_key().bytes.clone();
            batch.put_key_value(key, &value)?;
            self.stored_value = value;
        }
        self.delete_storage_first = false;
        self.update = None;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
    }
}

impl<C, T> ClonableView for RegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Self {
        RegisterView {
            delete_storage_first: self.delete_storage_first,
            context: self.context.clone(),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
        }
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut register = RegisterView::<_, u32>::load(context).await.unwrap();
    /// let value = register.get();
    /// assert_eq!(*value, 0);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut register = RegisterView::load(context).await.unwrap();
    /// register.set(5);
    /// let value = register.get();
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

impl<C, T> RegisterView<C, T>
where
    C: Context,
    T: Clone + Serialize,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut register: RegisterView<_, u32> = RegisterView::load(context).await.unwrap();
    /// let value = register.get_mut();
    /// assert_eq!(*value, 0);
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
        let _hash_latency = metrics::REGISTER_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(self.get())?;
        Ok(hasher.finalize())
    }
}

impl<C, T> HashableView for RegisterView<C, T>
where
    C: Context,
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

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::RegisterView;
    use crate::context::Context;

    impl<C, T> async_graphql::OutputType for RegisterView<C, T>
    where
        C: Context,
        T: async_graphql::OutputType + Send + Sync,
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
            self.get().resolve(ctx, field).await
        }
    }
}
