// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{from_bytes_option_or_default, HasherOutput},
    context::Context,
    sync_view::{
        hashable_wrapper::SyncWrappedHashableContainerView, Hasher, SyncClonableView,
        SyncHashableView, SyncReplaceContext, SyncView,
    },
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
            "SyncRegisterView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncRegisterView<C, T> {
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The value persisted in storage.
    stored_value: Box<T>,
    /// Pending update not yet persisted to storage.
    update: Option<Box<T>>,
}

impl<C, T, C2> SyncReplaceContext<C2> for SyncRegisterView<C, T>
where
    C: Context,
    C2: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = SyncRegisterView<C2, T>;

    fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        SyncRegisterView {
            delete_storage_first: self.delete_storage_first,
            context: ctx(&self.context),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
        }
    }
}

impl<C, T> SyncView for SyncRegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
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

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.update = None;
    }

    fn has_pending_changes(&self) -> bool {
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
            *self.stored_value = Default::default();
        } else if let Some(value) = self.update.take() {
            self.stored_value = value;
        }
        self.delete_storage_first = false;
        self.update = None;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.update = Some(Box::default());
    }
}

impl<C, T> SyncClonableView for SyncRegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SyncRegisterView {
            delete_storage_first: self.delete_storage_first,
            context: self.context.clone(),
            stored_value: self.stored_value.clone(),
            update: self.update.clone(),
        })
    }
}

impl<C, T> SyncRegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register = SyncRegisterView::<_, u32>::load(context).unwrap();
    /// let value = register.get();
    /// assert_eq!(*value, 0);
    /// ```
    pub fn get(&self) -> &T {
        match &self.update {
            None => &self.stored_value,
            Some(value) => value,
        }
    }

    /// Sets the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register = SyncRegisterView::load(context).unwrap();
    /// register.set(5);
    /// let value = register.get();
    /// assert_eq!(*value, 5);
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

impl<C, T> SyncRegisterView<C, T>
where
    C: Context,
    T: Clone + Serialize,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register: SyncRegisterView<_, u32> = SyncRegisterView::load(context).unwrap();
    /// let value = register.get_mut();
    /// assert_eq!(*value, 0);
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

impl<C, T> SyncHashableView for SyncRegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash()
    }

    fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash()
    }
}

/// Type wrapping `SyncRegisterView` while memoizing the hash.
pub type SyncHashedRegisterView<C, T> =
    SyncWrappedHashableContainerView<C, SyncRegisterView<C, T>, HasherOutput>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::SyncRegisterView;
    use crate::context::Context;

    impl<C, T> async_graphql::OutputType for SyncRegisterView<C, T>
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

        fn resolve(
            &self,
            ctx: &async_graphql::ContextSelectionSet<'_>,
            field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
        ) -> async_graphql::ServerResult<async_graphql::Value> {
            self.get().resolve(ctx, field)
        }
    }
}
