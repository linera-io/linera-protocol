// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
};

use futures::future::try_join_all;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_sdk::{ensure, views::View};
use linera_service::config::{DestinationId, LimitsConfig};
use linera_storage::Storage;
use linera_views::{
    batch::Batch, context::Context, log_view::LogView, store::WritableKeyValueStore as _,
    views::ClonableView,
};
use mini_moka::unsync::Cache as LfuCache;
use quick_cache::{sync::Cache as FifoCache, Weighter};

#[cfg(with_metrics)]
use crate::metrics;
use crate::{
    common::{BlockId, CanonicalBlock, ExporterError, LiteBlockId},
    state::{BlockExporterStateView, DestinationStates},
};

const NUM_OF_BLOBS: usize = 20;

pub(super) struct ExporterStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    shared_storage: SharedStorage<S::BlockExporterContext, S>,
}

type BlobCache = FifoCache<BlobId, Arc<Blob>, BlobCacheWeighter>;
type BlockCache = FifoCache<CryptoHash, Arc<ConfirmedBlockCertificate>, BlockCacheWeighter>;

struct SharedStorage<C, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    storage: S,
    destination_states: DestinationStates,
    shared_canonical_state: CanonicalState<C>,
    blobs_cache: Arc<BlobCache>,
    blocks_cache: Arc<BlockCache>,
}

pub(super) struct BlockProcessorStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    blob_state_cache: LfuCache<BlobId, ()>,
    chain_states_cache: LfuCache<ChainId, LiteBlockId>,
    shared_storage: SharedStorage<S::BlockExporterContext, S>,
    // Handle on the persistent storage where the exporter state is pushed to periodically.
    exporter_state_view: BlockExporterStateView<<S as Storage>::BlockExporterContext>,
}

impl<C, S> SharedStorage<C, S>
where
    C: Context + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        storage: S,
        state_context: LogView<C, CanonicalBlock>,
        destination_states: DestinationStates,
        limits: LimitsConfig,
    ) -> Self {
        let shared_canonical_state =
            CanonicalState::new((limits.auxiliary_cache_size_mb / 3).into(), state_context);
        let blobs_cache = Arc::new(FifoCache::with_weighter(
            limits.blob_cache_items_capacity as usize,
            (limits.blob_cache_weight_mb as u64) * 1024 * 1024,
            CacheWeighter::default(),
        ));
        let blocks_cache = Arc::new(FifoCache::with_weighter(
            limits.block_cache_items_capacity as usize,
            (limits.block_cache_weight_mb as u64) * 1024 * 1024,
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
                #[cfg(with_metrics)]
                metrics::GET_CERTIFICATE_HISTOGRAM.measure_latency();
                let block = self.storage.read_certificate(hash).await?;
                let block = block.ok_or_else(|| ExporterError::ReadCertificateError(hash))?;
                let heaped_block = Arc::new(block);
                guard.insert(heaped_block.clone()).ok();
                Ok(heaped_block)
            }
        }
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<Arc<Blob>, ExporterError> {
        match self.blobs_cache.get_value_or_guard_async(&blob_id).await {
            Ok(blob) => Ok(blob),
            Err(guard) => {
                #[cfg(with_metrics)]
                metrics::GET_BLOB_HISTOGRAM.measure_latency();
                let blob = self.storage.read_blob(blob_id).await?.unwrap();
                let heaped_blob = Arc::new(blob);
                guard.insert(heaped_blob.clone()).ok();
                Ok(heaped_blob)
            }
        }
    }

    async fn get_blobs(&self, blobs: &[BlobId]) -> Result<Vec<Arc<Blob>>, ExporterError> {
        let tasks = blobs.iter().map(|id| self.get_blob(*id));
        let results = try_join_all(tasks).await?;
        Ok(results)
    }

    fn push_block(&mut self, block: CanonicalBlock) {
        self.shared_canonical_state.push(block)
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

    pub(crate) async fn get_block_with_blob_ids(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>), ExporterError> {
        let block = self
            .shared_storage
            .shared_canonical_state
            .get(index)
            .await?;

        Ok((
            self.shared_storage.get_block(block.block_hash).await?,
            block.blobs.into(),
        ))
    }

    pub(crate) async fn get_block_with_blobs(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<Arc<Blob>>), ExporterError> {
        let canonical_block = self
            .shared_storage
            .shared_canonical_state
            .get(index)
            .await?;

        let block_task = self.shared_storage.get_block(canonical_block.block_hash);
        let blobs_task = self.shared_storage.get_blobs(&canonical_block.blobs);

        let (block, blobs) = tokio::try_join!(block_task, blobs_task)?;
        Ok((block, blobs))
    }

    pub(crate) async fn get_blob(&self, blob_id: BlobId) -> Result<Arc<Blob>, ExporterError> {
        self.shared_storage.get_blob(blob_id).await
    }

    pub(crate) fn load_destination_state(&self, id: &DestinationId) -> Arc<AtomicU64> {
        self.shared_storage.destination_states.load_state(id)
    }

    pub(crate) fn clone(&mut self) -> Result<Self, ExporterError> {
        Ok(ExporterStorage::new(self.shared_storage.clone()?))
    }

    #[allow(unused)]
    pub(crate) fn get_latest_index(&self) -> usize {
        self.shared_storage.shared_canonical_state.latest_index()
    }
}

impl<S> BlockProcessorStorage<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(super) async fn load(
        storage: S,
        id: u32,
        destinations: Vec<DestinationId>,
        limits: LimitsConfig,
    ) -> Result<(Self, ExporterStorage<S>), ExporterError> {
        let context = storage.block_exporter_context(id).await?;
        let (view, canonical_state, destination_states) =
            BlockExporterStateView::initiate(context, destinations).await?;

        let chain_states_cache_capacity =
            ((limits.auxiliary_cache_size_mb / 3) as u64 * 1024 * 1024)
                / (size_of::<CryptoHash>() + size_of::<LiteBlockId>()) as u64;
        let chain_states_cache = LfuCache::builder()
            .max_capacity(chain_states_cache_capacity)
            .build();

        let blob_state_cache_capacity = ((limits.auxiliary_cache_size_mb / 3) as u64 * 1024 * 1024)
            / (size_of::<BlobId>() as u64);
        let blob_state_cache = LfuCache::builder()
            .max_capacity(blob_state_cache_capacity)
            .build();

        let mut shared_storage =
            SharedStorage::new(storage, canonical_state, destination_states, limits);
        let exporter_storage = ExporterStorage::new(shared_storage.clone()?);

        Ok((
            Self {
                shared_storage,
                chain_states_cache,
                exporter_state_view: view,
                blob_state_cache,
            },
            exporter_storage,
        ))
    }

    pub(super) async fn get_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<ConfirmedBlockCertificate>, ExporterError> {
        self.shared_storage.get_block(hash).await
    }

    pub(super) async fn get_blob(&self, blob: BlobId) -> Result<Arc<Blob>, ExporterError> {
        self.shared_storage.get_blob(blob).await
    }

    pub(super) async fn is_blob_indexed(&mut self, blob: BlobId) -> Result<bool, ExporterError> {
        match self.blob_state_cache.get(&blob) {
            Some(_) => Ok(true),
            None => self.exporter_state_view.is_blob_indexed(blob).await,
        }
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
        self.exporter_state_view.initialize_chain(*block_id).await?;
        self.chain_states_cache
            .insert(block_id.chain_id, (*block_id).into());

        Ok(())
    }

    pub(super) async fn index_block(&mut self, block_id: &BlockId) -> Result<bool, ExporterError> {
        if block_id.height == BlockHeight::ZERO {
            self.index_chain(block_id).await?;
            return Ok(true);
        }

        if self.exporter_state_view.index_block(*block_id).await? {
            self.chain_states_cache
                .insert(block_id.chain_id, (*block_id).into());
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) fn index_blob(&mut self, blob: BlobId) -> Result<(), ExporterError> {
        self.exporter_state_view.index_blob(blob)?;
        self.blob_state_cache.insert(blob, ());
        Ok(())
    }

    pub(super) fn push_block(&mut self, block: CanonicalBlock) {
        self.shared_storage.push_block(block)
    }

    pub(super) fn new_committee(&mut self, committee_destinations: HashSet<DestinationId>) {
        committee_destinations.into_iter().for_each(|id| {
            let state = match self.shared_storage.destination_states.get(&id) {
                None => {
                    tracing::info!(id=?id, "adding new committee member");
                    #[cfg(with_metrics)]
                    {
                        metrics::DESTINATION_STATE_COUNTER
                            .with_label_values(&[id.address()])
                            .reset();
                    }
                    Arc::new(AtomicU64::new(0))
                }
                Some(state) => state.clone(),
            };
            self.shared_storage.destination_states.insert(id, state);
        });
    }

    pub(super) fn set_latest_committee_blob(&mut self, blob_id: BlobId) {
        self.exporter_state_view.set_latest_committee_blob(blob_id);
    }

    pub(super) fn get_latest_committee_blob(&self) -> Option<BlobId> {
        self.exporter_state_view.get_latest_committee_blob()
    }

    pub(super) async fn save(&mut self) -> Result<(), ExporterError> {
        let mut batch = Batch::new();

        self.shared_storage
            .shared_canonical_state
            .flush(&mut batch)?;

        self.exporter_state_view
            .set_destination_states(self.shared_storage.destination_states.clone());

        self.exporter_state_view.pre_save(&mut batch)?;
        #[cfg(with_metrics)]
        metrics::SAVE_HISTOGRAM.measure_latency();
        if let Err(e) = self
            .exporter_state_view
            .context()
            .store()
            .write_batch(batch)
            .await
        {
            Err(ExporterError::ViewError(e.into()))?;
        };
        self.exporter_state_view.post_save();

        // clear the shared state only after persisting it
        // only matters for the shared updates buffer
        self.shared_storage.shared_canonical_state.clear();

        Ok(())
    }
}

/// A view of the canonical state that is used to store blocks in the exporter.
struct CanonicalState<C> {
    /// The number of blocks in the canonical state.
    count: usize,
    /// The (persistent) storage view that is used to access the canonical state.
    state_context: LogView<C, CanonicalBlock>,
    /// A cache that stores the canonical blocks.
    /// This cache is used to speed up access to the canonical state.
    /// It uses a FIFO eviction policy and is limited by the size of the cache.
    /// The cache is used to avoid reading the canonical state from the persistent storage
    /// for every request.
    state_cache: Arc<FifoCache<usize, CanonicalBlock, CanonicalStateCacheWeighter>>,
    /// A buffer that stores the updates to the canonical state.
    ///
    /// This buffer is used to temporarily hold updates to the canonical state before they are
    /// flushed to the persistent storage.
    state_updates_buffer: Arc<papaya::HashMap<usize, CanonicalBlock>>,
}

impl<C> CanonicalState<C>
where
    C: Context + Send + Sync + 'static,
{
    fn new(cache_size: usize, state_context: LogView<C, CanonicalBlock>) -> Self {
        let cache_size = cache_size * 1024 * 1024;
        let items_capacity = cache_size
            / (size_of::<usize>()
                + size_of::<CanonicalBlock>()
                + NUM_OF_BLOBS * size_of::<BlobId>());

        Self {
            count: state_context.count(),
            state_cache: Arc::new(FifoCache::with_weighter(
                items_capacity,
                cache_size as u64,
                CacheWeighter::default(),
            )),
            state_updates_buffer: Arc::new(papaya::HashMap::new()),
            state_context,
        }
    }

    fn clone(&mut self) -> Result<Self, ExporterError> {
        Ok(Self {
            count: self.count,
            state_cache: self.state_cache.clone(),
            state_updates_buffer: self.state_updates_buffer.clone(),
            state_context: self.state_context.clone_unchecked()?,
        })
    }

    /// Returns the latest index of the canonical state.
    fn latest_index(&self) -> usize {
        self.count
    }

    async fn get(&self, index: usize) -> Result<CanonicalBlock, ExporterError> {
        match self.state_cache.get_value_or_guard_async(&index).await {
            Ok(value) => Ok(value),
            Err(guard) => {
                let block = if let Some(entry) = {
                    let pinned = self.state_updates_buffer.pin();
                    pinned.get(&index).cloned()
                } {
                    entry
                } else {
                    #[cfg(with_metrics)]
                    metrics::GET_CANONICAL_BLOCK_HISTOGRAM.measure_latency();
                    self.state_context
                        .get(index)
                        .await?
                        .ok_or(ExporterError::UnprocessedBlock)?
                };

                guard.insert(block.clone()).ok();
                Ok(block)
            }
        }
    }

    fn push(&mut self, value: CanonicalBlock) {
        let index = self.next_index();
        self.state_updates_buffer.pin().insert(index, value.clone());
        self.state_cache.insert(index, value);
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ExporterError> {
        for (_, value) in self
            .state_updates_buffer
            .pin()
            .iter()
            .map(|(key, value)| (*key, value.clone()))
            .collect::<BTreeMap<_, _>>()
        {
            self.state_context.push(value);
        }

        self.state_context.pre_save(batch)?;
        self.state_context.post_save();

        Ok(())
    }

    fn clear(&mut self) {
        self.state_updates_buffer.pin().clear();
        self.state_context.rollback();
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

impl Weighter<BlobId, Arc<Blob>> for BlobCacheWeighter {
    fn weight(&self, _key: &BlobId, val: &Arc<Blob>) -> u64 {
        (size_of::<BlobId>()
            + size_of::<Arc<Blob>>()
            + 2 * size_of::<usize>() // two reference counts in Arc, just a micro-optimization
            + size_of::<Blob>()
            + val.bytes().len()) as u64
    }
}

impl Weighter<CryptoHash, Arc<ConfirmedBlockCertificate>> for BlockCacheWeighter {
    fn weight(&self, _key: &CryptoHash, _val: &Arc<ConfirmedBlockCertificate>) -> u64 {
        (size_of::<CryptoHash>()
            + 2 * size_of::<usize>()
            + size_of::<Arc<ConfirmedBlockCertificate>>()
            + 1_000_000) as u64 // maximum block size in testnet resource control policy
    }
}

impl Weighter<usize, CanonicalBlock> for CanonicalStateCacheWeighter {
    fn weight(&self, _key: &usize, _val: &CanonicalBlock) -> u64 {
        (size_of::<usize>() + size_of::<CanonicalBlock>() + NUM_OF_BLOBS * size_of::<BlobId>())
            as u64
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
type CanonicalStateCacheWeighter = CacheWeighter<usize, CanonicalBlock>;
