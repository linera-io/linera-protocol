// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[macro_export]
macro_rules! impl_view {

    ($name: ident { $($field:ident),* $(,)? }; $( $ops_trait:path ),* $(,)? ) => {

#[$crate::async_trait]
impl<C> $crate::views::View<C> for $name<C>
where
    C: $crate::common::Context
        + Send
        + Sync
        + Clone
        + 'static
        $( + $ops_trait )*,
    $crate::views::ViewError: From<C::Error>,
{
    #[allow(unreachable_code)]
    fn context(&self) -> &C {
        $( return self.$field.context(); )*
    }

    async fn load(context: C) -> Result<Self, $crate::views::ViewError> {
        $( let $field = $crate::scoped_view::ScopedView::load(context.clone()).await?; )*
        Ok(Self {
            $( $field ),*
        })
    }

    fn rollback(&mut self) {
        $( self.$field.rollback(); )*
    }

    fn flush(&mut self, batch: &mut $crate::common::Batch) -> Result<(), $crate::views::ViewError> {
        use $crate::views::View;

        $( self.$field.flush(batch)?; )*
        Ok(())
    }

    fn delete(self, batch: &mut $crate::common::Batch) {
        use $crate::views::View;

        $( self.$field.delete(batch); )*
    }

    fn clear(&mut self) {
        $( self.$field.clear(); )*
    }
}

#[$crate::async_trait]
impl<C> $crate::views::HashView<C> for $name<C>
where
    C: $crate::views::HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        $( + $ops_trait )*,
    $crate::views::ViewError: From<C::Error>,
{
    async fn hash(&mut self) -> Result<<C::Hasher as $crate::views::Hasher>::Output, $crate::views::ViewError> {
        use $crate::views::{Hasher, HashView};
        use std::io::Write;

        let mut hasher = C::Hasher::default();
        $( hasher.write_all(self.$field.hash().await?.as_ref())?; )*
        Ok(hasher.finalize())
    }
}

impl<C> $name<C>
where
    C: $crate::common::Context
        + Send
        + Sync
        + Clone
        + 'static
        $( + $ops_trait )*,
    $crate::views::ViewError: From<C::Error>,
{
    pub async fn save(&mut self) -> Result<(), $crate::views::ViewError> {
        use $crate::views::View;

        let mut batch = $crate::common::Batch::default();
        $( self.$field.flush(&mut batch)?; )*
        self.context().write_batch(batch).await?;
        Ok(())
     }

    pub async fn write_delete(self) -> Result<(), $crate::views::ViewError> {
        use $crate::views::View;
        use $crate::common::Batch;

        let context = self.context().clone();
        let batch = Batch::build(move |batch| {
            Box::pin(async move {
                $( self.$field.delete(batch); )*
                Ok(())
            })
        }).await?;
        context.write_batch(batch).await?;
        Ok(())
    }
}

linera_views::paste! {

pub trait [< $name Context >]: $crate::views::HashingContext<Hasher = $crate::sha2::Sha512>
    + Send
    + Sync
    + Clone
    + 'static
    $( + $ops_trait )*
{}

impl<AnyContext> [< $name Context >] for AnyContext
where
    AnyContext: $crate::views::HashingContext<Hasher = $crate::sha2::Sha512>
        + Send
        + Sync
        + Clone
        + 'static
        $( + $ops_trait )*,
    $crate::views::ViewError: From<AnyContext::Error>,
{}

impl<C> $name<C>
where
    C: [< $name Context >],
    $crate::views::ViewError: From<C::Error>,
{
    pub async fn hash_value(&mut self) -> Result<$crate::crypto::HashValue, $crate::views::ViewError> {
        use $crate::crypto::{BcsSignable, HashValue};
        use $crate::generic_array::GenericArray;
        use $crate::views::HashView;
        use $crate::serde::{Serialize, Deserialize};
        use $crate::sha2::{Sha512, Digest};

        #[derive(Serialize, Deserialize)]
        struct [< $name Hash >](GenericArray<u8, <Sha512 as Digest>::OutputSize>);
        impl BcsSignable for [< $name Hash >] {}

        let hash = self.hash().await?;
        Ok(HashValue::new(&[< $name Hash >](hash)))
    }
}

}

    }
}
