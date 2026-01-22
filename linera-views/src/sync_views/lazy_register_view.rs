// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::OnceLock;

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch, common::from_bytes_option_or_default, context::SyncContext,
    store::SyncReadableKeyValueStore as _, sync_views::SyncView,
    views::lazy_register_view::LazyRegisterView, ViewError,
};

/// A synchronous view that supports modifying a single value of type `T`,
/// loaded lazily on first access.
///
/// This is a thin wrapper around [`LazyRegisterView`] that implements [`SyncView`]
/// instead of [`View`](crate::views::View).
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncLazyRegisterView<C, T>(
    /// The inner async lazy register view whose state and logic we reuse.
    pub(crate) LazyRegisterView<C, T>,
);

impl<C, T> SyncView for SyncLazyRegisterView<C, T>
where
    C: SyncContext,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 0;

    type Context = C;

    fn context(&self) -> C {
        self.0.context.clone()
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![])
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(SyncLazyRegisterView(LazyRegisterView {
            delete_storage_first: false,
            context,
            stored_value: OnceLock::new(),
            update: None,
        }))
    }

    fn rollback(&mut self) {
        self.0.base_rollback();
    }

    fn has_pending_changes(&self) -> bool {
        self.0.base_has_pending_changes()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.0
            .base_pre_save(batch, self.0.context.base_key().bytes.clone())
    }

    fn post_save(&mut self) {
        self.0.base_post_save();
    }

    fn clear(&mut self) {
        self.0.base_clear();
    }
}

impl<C, T> SyncLazyRegisterView<C, T>
where
    C: SyncContext,
    T: Default + DeserializeOwned,
{
    /// Access the current value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::lazy_register_view::SyncLazyRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let register = SyncLazyRegisterView::<_, u32>::load(context).unwrap();
    /// let value = register.get().unwrap();
    /// assert_eq!(*value, 0);
    /// ```
    pub fn get(&self) -> Result<&T, ViewError> {
        if let Some(value) = &self.0.update {
            return Ok(value);
        }
        if let Some(value) = self.0.stored_value.get() {
            return Ok(value);
        }
        let key = self.0.context.base_key().bytes.clone();
        let bytes = self.0.context.store().read_value_bytes(&key)?;
        let value = from_bytes_option_or_default(&bytes)?;
        Ok(self.0.stored_value.get_or_init(|| Box::new(value)))
    }

    /// Sets the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::lazy_register_view::SyncLazyRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register = SyncLazyRegisterView::load(context).unwrap();
    /// register.set(5);
    /// let value = register.get().unwrap();
    /// assert_eq!(*value, 5);
    /// ```
    pub fn set(&mut self, value: T) {
        self.0.delete_storage_first = false;
        self.0.update = Some(Box::new(value));
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.0.context.extra()
    }
}

impl<C, T> SyncLazyRegisterView<C, T>
where
    C: SyncContext,
    T: Clone + Default + Serialize + DeserializeOwned,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::lazy_register_view::SyncLazyRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register: SyncLazyRegisterView<_, u32> = SyncLazyRegisterView::load(context).unwrap();
    /// let value = register.get_mut().unwrap();
    /// assert_eq!(*value, 0);
    /// ```
    pub fn get_mut(&mut self) -> Result<&mut T, ViewError> {
        if self.0.update.is_none() {
            let update = self.get()?.clone();
            self.0.update = Some(Box::new(update));
        }
        self.0.delete_storage_first = false;
        Ok(self.0.update.as_mut().unwrap())
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::SyncLazyRegisterView;
    use crate::context::SyncContext;

    impl<C, T> async_graphql::OutputType for SyncLazyRegisterView<C, T>
    where
        C: SyncContext + Send + Sync,
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
                .map_err(|e| async_graphql::ServerError::new(e.to_string(), Some(field.pos)))?
                .resolve(ctx, field)
                .await
        }
    }
}
