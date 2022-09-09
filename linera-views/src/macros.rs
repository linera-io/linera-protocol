// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[macro_export]
macro_rules! impl_view {

    ($name: ident { $($field:ident),* $(,)? }; $($op_name:ident < $($op_param:ty),+ >),* $(,)? ) => {

#[$crate::async_trait]
impl<C> $crate::views::View<C> for $name<C>
where
    C: $crate::views::Context
        + Send
        + Sync
        + Clone
        + 'static
        + $crate::views::ScopedOperations
        $(+ $op_name < $($op_param),* >)*
{
    async fn load(context: C) -> Result<Self, C::Error> {
        $( let $field = ScopedView::load(context.clone()).await?; )*
        Ok(Self {
            $( $field ),*
        })
    }

    fn rollback(&mut self) {
        $( self.$field.rollback(); )*
    }

    async fn commit(self) -> Result<(), C::Error> {
        $( self.$field.commit().await?; )*
        Ok(())
    }

    async fn delete(self) -> Result<(), C::Error> {
        $( self.$field.delete().await?; )*
        Ok(())
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
        $(+ $op_name < $($op_param),* >)*
{
    async fn hash(&mut self) -> Result<<C::Hasher as $crate::hash::Hasher>::Output, C::Error> {
        use $crate::hash::{Hasher, HashView};
        use std::io::Write;

        let mut hasher = C::Hasher::default();
        $( hasher.write_all(self.$field.hash().await?.as_ref())?; )*
        Ok(hasher.finalize())
    }
}

linera_views::paste! {
pub trait [< $name Context >]: $crate::hash::HashingContext
    + Send
    + Sync
    + Clone
    + 'static
    + $crate::views::ScopedOperations
    $(+ $op_name < $($op_param),* >)*
{}
}

    }
}
