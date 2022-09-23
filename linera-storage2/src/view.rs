use crate::chain::{ChainStateView, InnerChainStateView, InnerChainStateViewContext};
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    impl_view,
    views::{CollectionOperations, MapOperations, MapView, ScopedView, SharedCollectionView, View},
};

/// A view accessing the validator's storage.
#[derive(Debug)]
pub struct StorageView<C> {
    pub chain_states: ScopedView<0, SharedCollectionView<C, ChainId, InnerChainStateView<C>>>,
    pub certificates: ScopedView<1, MapView<C, HashValue, Certificate>>,
}

impl_view! {
    StorageView {
        chain_states,
        certificates,
    };
    CollectionOperations<ChainId>,
    MapOperations<HashValue, Certificate>,
    InnerChainStateViewContext,
}

impl<C> StorageView<C>
where
    C: StorageViewContext,
{
    pub async fn load_chain(&mut self, id: ChainId) -> Result<ChainStateView<C>, C::Error> {
        Ok(self.chain_states.load_entry(id).await?.into())
    }

    pub async fn read_certificate(
        &mut self,
        hash: HashValue,
    ) -> Result<Option<Certificate>, C::Error> {
        self.certificates.get(&hash).await
    }

    pub async fn write_certificate(&mut self, certificate: Certificate) -> Result<(), C::Error> {
        let context = self.context().clone();
        context
            .run_with_batch(|batch| {
                Box::pin(async {
                    self.certificates.insert(certificate.hash, certificate);
                    self.certificates.commit(batch).await
                })
            })
            .await
    }
}
