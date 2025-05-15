// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use dashmap::DashMap;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::{ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate};
use linera_client::config::{DestinationId, LimitsConfig};
use linera_sdk::{
    ensure,
    views::{View, ViewError},
};
use linera_storage::Storage;
use linera_views::{batch::Batch, context::Context, log_view::LogView, views::ClonableView};
use mini_moka::unsync::Cache as LfuCache;
use quick_cache::{sync::Cache as FifoCache, Weighter};

use crate::{
    common::{BlockId, ExporterError, LiteBlockId},
    state::{BlockExporterStateView, DestinationStates},
};

pub(super) struct ExporterStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    shared_storage: SharedStorage<<S as Storage>::BlockExporterContext, S>,
}

struct SharedStorage<C, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    storage: S,
    destination_states: DestinationStates,
    shared_canonical_state: CanonicalState<C>,
    blobs_cache: Arc<FifoCache<BlobId, Arc<Blob>, BlobCacheWeighter>>,
    blocks_cache: Arc<FifoCache<CryptoHash, Arc<ConfirmedBlockCertificate>, BlockCacheWeighter>>,
}

pub(super) struct BlockProcessorStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    shared_storage: SharedStorage<S::BlockExporterContext, S>,
    chain_states_cache: LfuCache<ChainId, LiteBlockId>,
    exporter_state_view: BlockExporterStateView<<S as Storage>::BlockExporterContext>,
}

impl<C, S> SharedStorage<C, S>
where
    C: Context + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        storage: S,
        state_context: LogView<C, CryptoHash>,
        destination_states: DestinationStates,
        limits: LimitsConfig,
    ) -> Self {
        let shared_canonical_state =
            CanonicalState::new((limits.auxialiary_cache_size / 2).into(), state_context);
        let blobs_cache = Arc::new(FifoCache::with_weighter(
            limits.blob_cache_items_capacity as usize,
            limits.blob_cache_weight as u64,
            CacheWeighter::default(),
        ));
        let blocks_cache = Arc::new(FifoCache::with_weighter(
            limits.block_cache_items_capacity as usize,
            limits.block_cache_weight as u64,
            CacheWeighter::default(),
        ));

        Self {
            storage,
            shared_canonical_state,
            blobs_cache,
            blocks_cache,
            destination_states,
        }
    }

    async fn get_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<ConfirmedBlockCertificate>, ExporterError> {
        match self.blocks_cache.get_value_or_guard_async(&hash).await {
            Ok(value) => Ok(value),
            Err(guard) => {
                let block = self.download_block(hash).await?;
                let heaped_block = Arc::new(block);
                let _ = guard.insert(heaped_block.clone());
                Ok(heaped_block)
            }
        }
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<Arc<Blob>, ExporterError> {
        match self.blobs_cache.get_value_or_guard_async(&blob_id).await {
            Ok(blob) => Ok(blob),
            Err(guard) => {
                let blob = self.download_blob(blob_id).await?;
                let heaped_blob = Arc::new(blob);
                let _ = guard.insert(heaped_blob.clone());
                Ok(heaped_blob)
            }
        }
    }

    async fn download_block(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, ExporterError> {
        let cert = self.storage.read_certificate(hash).await?;
        Ok(cert)
    }

    async fn download_blob(&self, blob_id: BlobId) -> Result<Blob, ExporterError> {
        let blob = self.storage.read_blob(blob_id).await?;
        Ok(blob)
    }

    async fn push_block(&mut self, block_hash: CryptoHash) -> Result<(), ExporterError> {
        self.shared_canonical_state.push(block_hash).await
    }

    fn clone(&mut self) -> Result<Self, ExporterError> {
        Ok(Self {
            storage: self.storage.clone(),
            shared_canonical_state: self.shared_canonical_state.clone()?,
            blobs_cache: self.blobs_cache.clone(),
            blocks_cache: self.blocks_cache.clone(),
            destination_states: self.destination_states.clone(),
        })
    }
}

impl<S> ExporterStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(shared_storage: SharedStorage<S::BlockExporterContext, S>) -> Self {
        Self { shared_storage }
    }

    pub(crate) async fn get_block(
        &self,
        index: usize,
    ) -> Result<Arc<ConfirmedBlockCertificate>, ExporterError> {
        let hash = self
            .shared_storage
            .shared_canonical_state
            .get(index)
            .await?;
        self.shared_storage.get_block(hash).await
    }

    pub(crate) async fn get_blob(&self, blob_id: BlobId) -> Result<Arc<Blob>, ExporterError> {
        self.shared_storage.get_blob(blob_id).await
    }

    pub(crate) fn increment_destination(&self, id: DestinationId) {
        self.shared_storage
            .destination_states
            .increment_destination(id);
    }

    pub(crate) fn load_destination_state(&self, id: DestinationId) -> u64 {
        self.shared_storage.destination_states.load_state(id)
    }

    pub(crate) fn clone(&mut self) -> Result<Self, ExporterError> {
        Ok(ExporterStorage::new(self.shared_storage.clone()?))
    }
}

impl<S> BlockProcessorStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(super) async fn load(
        storage: S,
        id: u32,
        number_of_destinaions: u16,
        limits: LimitsConfig,
    ) -> Result<(Self, ExporterStorage<S>), ExporterError> {
        let context = storage.block_exporter_context(id).await?;
        let (view, canonical_state, destination_states) =
            BlockExporterStateView::initiate(context, number_of_destinaions).await?;

        let chain_states_cache_capacity = ((limits.auxialiary_cache_size / 2) as u64 * 1024 * 1024)
            / (size_of::<CryptoHash>() + size_of::<LiteBlockId>()) as u64;
        let chain_states_cache = LfuCache::builder()
            .max_capacity(chain_states_cache_capacity)
            .build();

        let mut shared_storage =
            SharedStorage::new(storage, canonical_state, destination_states, limits);
        let exporter_storage = ExporterStorage::new(shared_storage.clone()?);

        Ok((
            Self {
                shared_storage,
                chain_states_cache,
                exporter_state_view: view,
            },
            exporter_storage,
        ))
    }

    pub(super) async fn get_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<GenericCertificate<ConfirmedBlock>>, ExporterError> {
        self.shared_storage.get_block(hash).await
    }

    pub(super) async fn is_block_indexed(
        &mut self,
        block_id: &BlockId,
    ) -> Result<bool, ExporterError> {
        if let Some(status) = self.chain_states_cache.get(&block_id.chain_id) {
            return Ok(status.height >= block_id.height);
        }

        if let Some(status) = self
            .exporter_state_view
            .get_chain_status(&block_id.chain_id)
            .await?
        {
            let result = status.height >= block_id.height;
            self.chain_states_cache.insert(block_id.chain_id, status);
            return Ok(result);
        }

        Err(ExporterError::UnprocessedChain)
    }

    pub(super) async fn index_chain(&mut self, block_id: &BlockId) -> Result<(), ExporterError> {
        ensure!(
            block_id.height == BlockHeight::ZERO,
            ExporterError::BadInitialization
        );
        self.shared_storage.push_block(block_id.hash).await?;
        self.exporter_state_view
            .initialize_chain(block_id.clone())
            .await?;
        self.chain_states_cache
            .insert(block_id.chain_id, block_id.clone().into());

        Ok(())
    }

    pub(super) async fn index_block(&mut self, block_id: &BlockId) -> Result<(), ExporterError> {
        if block_id.height == BlockHeight::ZERO {
            self.index_chain(block_id).await?;
            return Ok(());
        }

        if self
            .exporter_state_view
            .index_block(block_id.clone())
            .await?
        {
            self.shared_storage.push_block(block_id.hash).await?;
            self.chain_states_cache
                .insert(block_id.chain_id, block_id.clone().into());
        }

        Ok(())
    }

    pub(super) async fn save(&mut self) -> Result<(), ExporterError> {
        let mut batch = Batch::new();
        self.shared_storage
            .shared_canonical_state
            .flush(&mut batch)?;
        self.exporter_state_view
            .set_destination_states(self.shared_storage.destination_states.clone());
        self.exporter_state_view.flush(&mut batch)?;
        self.exporter_state_view.rollback();
        if let Err(e) = self.exporter_state_view.context().write_batch(batch).await {
            Err(ExporterError::GenericError(Box::new(e)))?;
        };

        Ok(())
    }
}

struct CanonicalState<C> {
    count: usize,
    state_cache: Arc<FifoCache<usize, CryptoHash>>,
    state_updates_buffer: Arc<DashMap<usize, CryptoHash>>,
    state_context: LogView<C, CryptoHash>,
}

impl<C> CanonicalState<C>
where
    C: Context + Send + Sync + 'static,
{
    fn new(cache_size: usize, state_context: LogView<C, CryptoHash>) -> Self {
        let items_capacity =
            (cache_size * 1024 * 1024) / (size_of::<usize>() + size_of::<CryptoHash>());

        Self {
            count: state_context.count(),
            state_cache: Arc::new(FifoCache::new(items_capacity)),
            state_updates_buffer: Arc::new(DashMap::new()),
            state_context,
        }
    }

    fn clone(&mut self) -> Result<Self, ViewError> {
        Ok(Self {
            count: self.count,
            state_cache: self.state_cache.clone(),
            state_updates_buffer: self.state_updates_buffer.clone(),
            state_context: self.state_context.clone_unchecked()?,
        })
    }

    async fn get(&self, index: usize) -> Result<CryptoHash, ExporterError> {
        match self.state_cache.get_value_or_guard_async(&index).await {
            Ok(value) => Ok(value),
            Err(guard) => {
                let hash = if let Some(entry) = self
                    .state_updates_buffer
                    .get(&index)
                    .map(|entry| *entry.value())
                {
                    entry
                } else {
                    self.state_context
                        .get(index)
                        .await?
                        .ok_or(ExporterError::UnprocessedBlock)?
                };

                let _ = guard.insert(hash);
                Ok(hash)
            }
        }
    }

    async fn push(&mut self, value: CryptoHash) -> Result<(), ExporterError> {
        let index = self.next_index();
        let _ = self.state_updates_buffer.insert(index, value);
        self.state_cache.insert(index, value);

        Ok(())
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ExporterError> {
        for (_, value) in self
            .state_updates_buffer
            .iter()
            .map(|r| (*r.key(), *r.value()))
            .collect::<BTreeMap<_, _>>()
        {
            self.state_context.push(value);
        }

        let _ = self.state_context.flush(batch)?;

        self.state_updates_buffer.clear();
        self.state_context.rollback();

        Ok(())
    }

    fn next_index(&mut self) -> usize {
        let next = self.count;
        self.count += 1;
        next
    }
}

#[derive(Clone)]
struct CacheWeighter<Q, V> {
    key: PhantomData<Q>,
    value: PhantomData<V>,
}

impl Weighter<BlobId, Arc<Blob>> for CacheWeighter<BlobId, Arc<Blob>> {
    fn weight(&self, _key: &BlobId, val: &Arc<Blob>) -> u64 {
        (size_of::<BlobId>()
            + size_of::<Arc<Blob>>()
            + 2 * size_of::<usize>()
            + size_of::<Blob>()
            + val.bytes().len()) as u64
    }
}

impl Weighter<CryptoHash, Arc<ConfirmedBlockCertificate>>
    for CacheWeighter<CryptoHash, Arc<ConfirmedBlockCertificate>>
{
    fn weight(&self, _key: &CryptoHash, val: &Arc<ConfirmedBlockCertificate>) -> u64 {
        (size_of::<CryptoHash>()
            + size_of::<Arc<ConfirmedBlockCertificate>>()
            + bcs::serialized_size(val).unwrap()) as u64
    }
}

impl<Q, V> Default for CacheWeighter<Q, V> {
    fn default() -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
        }
    }
}

type BlobCacheWeighter = CacheWeighter<BlobId, Arc<Blob>>;
type BlockCacheWeighter = CacheWeighter<CryptoHash, Arc<ConfirmedBlockCertificate>>;
