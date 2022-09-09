use crate::{
    chain::{ChainStateView, InnerChainStateView, InnerChainStateViewContext},
    Store,
};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{Certificate, ChainId},
};
use linera_views::{
    impl_view,
    views::{CollectionOperations, MapOperations, MapView, ScopedView, SharedCollectionView, View},
};
use std::{fmt::Display, sync::Arc};
use thiserror::Error;
use tokio::sync::Mutex;

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

#[async_trait]
impl<C> Store for Arc<Mutex<StorageView<C>>>
where
    C: StorageViewContext,
{
    type Context = C;
    type Error = StoreError<C::Error>;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<C>, Self::Error> {
        let mut storage = self.lock().await;
        let chain_state = storage.chain_states.load_entry(id).await?;
        Ok(chain_state.into())
    }

    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, Self::Error> {
        let mut storage = self.lock().await;
        let maybe_certificate = storage.certificates.get(&hash).await?;
        maybe_certificate.ok_or_else(|| StoreError::MissingCertificate { hash })
    }

    async fn write_certificate(&self, certificate: Certificate) -> Result<(), Self::Error> {
        let context = self.lock().await.context().clone();
        let context_for_scope = context.clone();
        let cloned_self = self.clone();
        context
            .run_with_batch(move |batch| {
                Box::pin(async move {
                    let mut storage = cloned_self.lock().await;
                    storage.certificates.insert(certificate.hash, certificate);
                    storage.certificates.commit(batch).await?;
                    storage.certificates = ScopedView::load(context_for_scope).await?;
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }
}

/// Storage access error.
#[derive(Debug, Error)]
pub enum StoreError<E> {
    #[error("Certificate with hash {hash} is missing from the storage")]
    MissingCertificate { hash: HashValue },

    #[error(transparent)]
    Context(E),
}

impl<E> From<E> for StoreError<E> {
    fn from(context_error: E) -> Self {
        StoreError::Context(context_error)
    }
}

impl<E> From<StoreError<E>> for linera_base::error::Error
where
    E: Display,
    linera_base::error::Error: From<E>,
{
    fn from(error: StoreError<E>) -> Self {
        match error {
            StoreError::MissingCertificate { .. } => Self::StorageError {
                backend: String::new(),
                error: error.to_string(),
            },
            StoreError::Context(context_error) => context_error.into(),
        }
    }
}
