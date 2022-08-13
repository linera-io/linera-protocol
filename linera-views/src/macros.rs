// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[macro_export]
macro_rules! impl_view {

    ($name: ident { $($field:ident),* }; $($op_name:ident < $($op_param:ty),+ >),* ) => {

#[async_trait]
impl<C> View<C> for $name<C>
where
    C: Context
        + Send
        + Sync
        + Clone
        + 'static
        + ScopedOperations
        $(+ $op_name < $($op_param),* >)*
{
    async fn load(context: C) -> Result<Self, C::Error> {
        $( let $field = ScopedView::load(context.clone()).await?; )*
        Ok(Self {
            $( $field ),*
        })
    }

    fn reset_changes(&mut self) {
        $( self.$field.reset_changes(); )*
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

#[async_trait]
impl<C> HashView<C> for $name<C>
where
    C: HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        + ScopedOperations
        $(+ $op_name < $($op_param),* >)*
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, C::Error> {
        use std::io::Write;

        let mut hasher = C::Hasher::default();
        $( hasher.write_all(self.$field.hash().await?.as_ref())?; )*
        Ok(hasher.finalize())
    }
}

    }
}
