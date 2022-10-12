// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[macro_export]
macro_rules! impl_view {

    ($name: ident { $($field:ident),* $(,)? }; $( $ops_trait:path ),* $(,)? ) => {

#[$crate::async_trait]
impl<C> $crate::views::View<C> for $name<C>
where
    C: $crate::views::Context
        + Send
        + Sync
        + Clone
        + 'static
        + $crate::views::ScopedOperations
        $( + $ops_trait )*
{
    #[allow(unreachable_code)]
    fn context(&self) -> &C {
        $( return self.$field.context(); )*
    }

    async fn load(context: C) -> Result<Self, C::Error> {
        $( let $field = ScopedView::load(context.clone()).await?; )*
        Ok(Self {
            $( $field ),*
        })
    }

    fn rollback(&mut self) {
        $( self.$field.rollback(); )*
    }

    async fn commit(self, batch: &mut C::Batch) -> Result<(), C::Error> {
        use $crate::views::View;

        $( self.$field.commit(batch).await?; )*
        Ok(())
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        use $crate::views::View;

        $( self.$field.commit_and_reset(batch).await?; )*
        Ok(())
    }

    async fn delete(self, batch: &mut C::Batch) -> Result<(), C::Error> {
        use $crate::views::View;

        $( self.$field.delete(batch).await?; )*
        Ok(())
    }

    fn reset_to_default(&mut self) {
        $( self.$field.reset_to_default(); )*
    }
}

#[$crate::async_trait]
impl<C> $crate::hash::HashView<C> for $name<C>
where
    C: $crate::hash::HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        + $crate::views::ScopedOperations
        $( + $ops_trait )*
{
    async fn hash(&mut self) -> Result<<C::Hasher as $crate::hash::Hasher>::Output, C::Error> {
        use $crate::hash::{Hasher, HashView};
        use std::io::Write;

        let mut hasher = C::Hasher::default();
        $( hasher.write_all(self.$field.hash().await?.as_ref())?; )*
        Ok(hasher.finalize())
    }
}

impl<C> $name<C>
where
    C: $crate::views::Context
        + Send
        + Sync
        + Clone
        + 'static
        + $crate::views::ScopedOperations
        $( + $ops_trait )*
{
    pub async fn write_commit(self) -> Result<(), C::Error> {
        use $crate::views::View;

        let context = self.context().clone();
        context.run_with_batch(move |batch| {
            Box::pin(async move {
                $( self.$field.commit(batch).await?; )*
                Ok(())
            })
        }).await
    }

    pub async fn write_commit_and_reset(&mut self) -> Result<(), C::Error> {
        use $crate::views::View;

        let context = self.context().clone();
        context.run_with_batch(move |batch| {
            Box::pin(async move {
                $( self.$field.commit_and_reset(batch).await?; )*
                Ok(())
            })
        }).await
    }

    pub async fn write_delete(self) -> Result<(), C::Error> {
        use $crate::views::View;

        let context = self.context().clone();
        context.run_with_batch(move |batch| {
            Box::pin(async move {
                $( self.$field.delete(batch).await?; )*
                Ok(())
            })
        }).await
    }
}

linera_views::paste! {

pub trait [< $name Context >]: $crate::hash::HashingContext
    + Send
    + Sync
    + Clone
    + 'static
    + $crate::views::ScopedOperations
    $( + $ops_trait )*
{}

impl<AnyContext> [< $name Context >] for AnyContext
where
    AnyContext: $crate::hash::HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        + $crate::views::ScopedOperations
        $( + $ops_trait )*
{}

}

    }
}
