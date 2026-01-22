// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use allocative::Allocative;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch, context::SyncContext, sync_views::SyncView, views::register_view::RegisterView,
    ViewError,
};

/// A synchronous view that supports modifying a single value of type `T`.
///
/// This is a thin wrapper around [`RegisterView`] that implements [`SyncView`]
/// instead of [`View`](crate::views::View).
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct SyncRegisterView<C, T>(
    /// The inner async register view whose state and logic we reuse.
    pub(crate) RegisterView<C, T>,
);

impl<C, T> SyncView for SyncRegisterView<C, T>
where
    C: SyncContext,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.0.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(RegisterView::<C, T>::base_pre_load(
            &context.base_key().bytes,
        ))
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(SyncRegisterView(RegisterView::base_post_load(
            context, values,
        )?))
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

impl<C, T> SyncRegisterView<C, T> {
    /// Access the current value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::register_view::SyncRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register = SyncRegisterView::<_, u32>::load(context).unwrap();
    /// let value = register.get();
    /// assert_eq!(*value, 0);
    /// ```
    pub fn get(&self) -> &T {
        self.0.get()
    }

    /// Sets the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::register_view::SyncRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register = SyncRegisterView::load(context).unwrap();
    /// register.set(5);
    /// let value = register.get();
    /// assert_eq!(*value, 5);
    /// ```
    pub fn set(&mut self, value: T) {
        self.0.set(value);
    }
}

impl<C, T> SyncRegisterView<C, T>
where
    C: SyncContext,
{
    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.0.context.extra()
    }
}

impl<C, T> SyncRegisterView<C, T>
where
    T: Clone,
{
    /// Obtains a mutable reference to the value in the register.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_views::register_view::SyncRegisterView;
    /// # use linera_views::sync_views::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut register: SyncRegisterView<_, u32> = SyncRegisterView::load(context).unwrap();
    /// let value = register.get_mut();
    /// assert_eq!(*value, 0);
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::SyncRegisterView;
    use crate::context::SyncContext;

    impl<C, T> async_graphql::OutputType for SyncRegisterView<C, T>
    where
        C: SyncContext + Send + Sync,
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
