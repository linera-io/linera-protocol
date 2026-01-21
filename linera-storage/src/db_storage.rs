// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::Arc,
};

use async_trait::async_trait;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight, NetworkDescription, TimeDelta, Timestamp},
    identifiers::{ApplicationId, BlobId, ChainId, EventId, IndexAndEvent, StreamId},
};
use linera_chain::{
    types::{CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, LiteCertificate},
    ChainStateView,
};
use linera_execution::{
    BlobState, ExecutionRuntimeConfig, UserContractCode, UserServiceCode, WasmRuntime,
};
use linera_views::{
    backends::dual::{DualStoreRootKeyAssignment, StoreInUse},
    batch::Batch,
    context::ViewContext,
    store::{
        KeyValueDatabase, KeyValueStore, ReadableKeyValueStore as _, WritableKeyValueStore as _,
    },
    views::View,
    ViewError,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
#[cfg(with_testing)]
use {
    futures::channel::oneshot::{self, Receiver},
    linera_views::{random::generate_test_namespace, store::TestKeyValueDatabase},
    std::cmp::Reverse,
};

use crate::{ChainRuntimeContext, Clock, Storage};

#[cfg(with_metrics)]
pub mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounterVec};

    /// The metric counting how often a blob is tested for existence from storage
    pub(super) static CONTAINS_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "contains_blob",
            "The metric counting how often a blob is tested for existence from storage",
            &[],
        )
    });

    /// The metric counting how often multiple blobs are tested for existence from storage
    pub(super) static CONTAINS_BLOBS_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "contains_blobs",
            "The metric counting how often multiple blobs are tested for existence from storage",
            &[],
        )
    });

    /// The metric counting how often a blob state is tested for existence from storage
    pub(super) static CONTAINS_BLOB_STATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "contains_blob_state",
            "The metric counting how often a blob state is tested for existence from storage",
            &[],
        )
    });

    /// The metric counting how often a certificate is tested for existence from storage.
    pub(super) static CONTAINS_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "contains_certificate",
            "The metric counting how often a certificate is tested for existence from storage",
            &[],
        )
    });

    /// The metric counting how often a hashed certificate value is read from storage.
    #[doc(hidden)]
    pub static READ_CONFIRMED_BLOCK_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_confirmed_block",
            "The metric counting how often a hashed confirmed block is read from storage",
            &[],
        )
    });

    /// The metric counting how often confirmed blocks are read from storage.
    #[doc(hidden)]
    pub(super) static READ_CONFIRMED_BLOCKS_COUNTER: LazyLock<IntCounterVec> =
        LazyLock::new(|| {
            register_int_counter_vec(
                "read_confirmed_blocks",
                "The metric counting how often confirmed blocks are read from storage",
                &[],
            )
        });

    /// The metric counting how often a blob is read from storage.
    #[doc(hidden)]
    pub(super) static READ_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_blob",
            "The metric counting how often a blob is read from storage",
            &[],
        )
    });

    /// The metric counting how often a blob state is read from storage.
    #[doc(hidden)]
    pub(super) static READ_BLOB_STATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_blob_state",
            "The metric counting how often a blob state is read from storage",
            &[],
        )
    });

    /// The metric counting how often blob states are read from storage.
    #[doc(hidden)]
    pub(super) static READ_BLOB_STATES_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_blob_states",
            "The metric counting how often blob states are read from storage",
            &[],
        )
    });

    /// The metric counting how often a blob is written to storage.
    #[doc(hidden)]
    pub(super) static WRITE_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_blob",
            "The metric counting how often a blob is written to storage",
            &[],
        )
    });

    /// The metric counting how often a certificate is read from storage.
    #[doc(hidden)]
    pub static READ_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_certificate",
            "The metric counting how often a certificate is read from storage",
            &[],
        )
    });

    /// The metric counting how often certificates are read from storage.
    #[doc(hidden)]
    pub(super) static READ_CERTIFICATES_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_certificates",
            "The metric counting how often certificate are read from storage",
            &[],
        )
    });

    /// The metric counting how often a certificate is written to storage.
    #[doc(hidden)]
    pub static WRITE_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_certificate",
            "The metric counting how often a certificate is written to storage",
            &[],
        )
    });

    /// The latency to load a chain state.
    #[doc(hidden)]
    pub(crate) static LOAD_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "load_chain_latency",
            "The latency to load a chain state",
            &[],
            exponential_bucket_latencies(1000.0),
        )
    });

    /// The metric counting how often an event is read from storage.
    #[doc(hidden)]
    pub(super) static READ_EVENT_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "read_event",
            "The metric counting how often an event is read from storage",
            &[],
        )
    });

    /// The metric counting how often an event is tested for existence from storage
    pub(super) static CONTAINS_EVENT_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "contains_event",
            "The metric counting how often an event is tested for existence from storage",
            &[],
        )
    });

    /// The metric counting how often an event is written to storage.
    #[doc(hidden)]
    pub(super) static WRITE_EVENT_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_event",
            "The metric counting how often an event is written to storage",
            &[],
        )
    });

    /// The metric counting how often the network description is read from storage.
    #[doc(hidden)]
    pub(super) static READ_NETWORK_DESCRIPTION: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "network_description",
            "The metric counting how often the network description is read from storage",
            &[],
        )
    });

    /// The metric counting how often the network description is written to storage.
    #[doc(hidden)]
    pub(super) static WRITE_NETWORK_DESCRIPTION: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_network_description",
            "The metric counting how often the network description is written to storage",
            &[],
        )
    });
}

/// The key used for blobs. The Blob ID itself is contained in the root key.
const BLOB_KEY: &[u8] = &[0];

/// The key used for blob states. The Blob ID itself is contained in the root key.
const BLOB_STATE_KEY: &[u8] = &[1];

/// The key used for lite certificates. The cryptohash itself is contained in the root key.
const LITE_CERTIFICATE_KEY: &[u8] = &[2];

/// The key used for confirmed blocks. The cryptohash itself is contained in the root key.
const BLOCK_KEY: &[u8] = &[3];

/// The key used for the network description.
const NETWORK_DESCRIPTION_KEY: &[u8] = &[4];

fn get_block_keys() -> Vec<Vec<u8>> {
    vec![LITE_CERTIFICATE_KEY.to_vec(), BLOCK_KEY.to_vec()]
}

#[derive(Default)]
#[allow(clippy::type_complexity)]
struct MultiPartitionBatch {
    keys_value_bytes: BTreeMap<Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>>,
}

impl MultiPartitionBatch {
    fn new() -> Self {
        Self::default()
    }

    fn put_key_values(&mut self, root_key: Vec<u8>, key_values: Vec<(Vec<u8>, Vec<u8>)>) {
        let entry = self.keys_value_bytes.entry(root_key).or_default();
        entry.extend(key_values);
    }

    fn put_key_value(&mut self, root_key: Vec<u8>, key: Vec<u8>, value: Vec<u8>) {
        self.put_key_values(root_key, vec![(key, value)]);
    }

    fn add_blob(&mut self, blob: &Blob) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_BLOB_COUNTER.with_label_values(&[]).inc();
        let root_key = RootKey::BlobId(blob.id()).bytes();
        let key = BLOB_KEY.to_vec();
        self.put_key_value(root_key, key, blob.bytes().to_vec());
        Ok(())
    }

    fn add_blob_state(&mut self, blob_id: BlobId, blob_state: &BlobState) -> Result<(), ViewError> {
        let root_key = RootKey::BlobId(blob_id).bytes();
        let key = BLOB_STATE_KEY.to_vec();
        let value = bcs::to_bytes(blob_state)?;
        self.put_key_value(root_key, key, value);
        Ok(())
    }

    /// Adds a certificate to the batch.
    ///
    /// Writes both the certificate data (indexed by hash) and a height index
    /// (mapping chain_id + height to hash).
    ///
    /// Note: If called multiple times with the same `(chain_id, height)`, the height
    /// index will be overwritten. The caller is responsible for ensuring that
    /// certificates at the same height have the same hash.
    fn add_certificate(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        let hash = certificate.hash();

        // Write certificate data by hash
        let root_key = RootKey::BlockHash(hash).bytes();
        let mut key_values = Vec::new();
        let key = LITE_CERTIFICATE_KEY.to_vec();
        let value = bcs::to_bytes(&certificate.lite_certificate())?;
        key_values.push((key, value));
        let key = BLOCK_KEY.to_vec();
        let value = bcs::to_bytes(&certificate.value())?;
        key_values.push((key, value));
        self.put_key_values(root_key, key_values);

        // Write height index: chain_id -> height -> hash
        let chain_id = certificate.value().block().header.chain_id;
        let height = certificate.value().block().header.height;
        let index_root_key = RootKey::BlockByHeight(chain_id).bytes();
        let height_key = to_height_key(height);
        let index_value = bcs::to_bytes(&hash)?;
        self.put_key_value(index_root_key, height_key, index_value);

        Ok(())
    }

    fn add_event(&mut self, event_id: EventId, value: Vec<u8>) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_EVENT_COUNTER.with_label_values(&[]).inc();
        let key = to_event_key(&event_id);
        let root_key = RootKey::Event(event_id.chain_id).bytes();
        self.put_key_value(root_key, key, value);
        Ok(())
    }

    fn add_network_description(
        &mut self,
        information: &NetworkDescription,
    ) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_NETWORK_DESCRIPTION
            .with_label_values(&[])
            .inc();
        let root_key = RootKey::NetworkDescription.bytes();
        let key = NETWORK_DESCRIPTION_KEY.to_vec();
        let value = bcs::to_bytes(information)?;
        self.put_key_value(root_key, key, value);
        Ok(())
    }
}

/// Main implementation of the [`Storage`] trait.
#[derive(Clone)]
pub struct DbStorage<Database, Clock = WallClock> {
    database: Arc<Database>,
    clock: Clock,
    thread_pool: Arc<linera_execution::ThreadPool>,
    wasm_runtime: Option<WasmRuntime>,
    user_contracts: Arc<papaya::HashMap<ApplicationId, UserContractCode>>,
    user_services: Arc<papaya::HashMap<ApplicationId, UserServiceCode>>,
    execution_runtime_config: ExecutionRuntimeConfig,
}

#[derive(Debug, Serialize, Deserialize)]
enum RootKey {
    NetworkDescription,
    BlockExporterState(u32),
    ChainState(ChainId),
    BlockHash(CryptoHash),
    BlobId(BlobId),
    Event(ChainId),
    BlockByHeight(ChainId),
}

const CHAIN_ID_TAG: u8 = 2;
const BLOB_ID_TAG: u8 = 4;
const EVENT_ID_TAG: u8 = 5;

impl RootKey {
    fn bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RestrictedEventId {
    pub stream_id: StreamId,
    pub index: u32,
}

fn to_event_key(event_id: &EventId) -> Vec<u8> {
    let restricted_event_id = RestrictedEventId {
        stream_id: event_id.stream_id.clone(),
        index: event_id.index,
    };
    bcs::to_bytes(&restricted_event_id).unwrap()
}

pub(crate) fn to_height_key(height: BlockHeight) -> Vec<u8> {
    bcs::to_bytes(&height).unwrap()
}

fn is_chain_state(root_key: &[u8]) -> bool {
    if root_key.is_empty() {
        return false;
    }
    root_key[0] == CHAIN_ID_TAG
}

/// An implementation of [`DualStoreRootKeyAssignment`] that stores the
/// chain states into the first store.
#[derive(Clone, Copy)]
pub struct ChainStatesFirstAssignment;

impl DualStoreRootKeyAssignment for ChainStatesFirstAssignment {
    fn assigned_store(root_key: &[u8]) -> Result<StoreInUse, bcs::Error> {
        if root_key.is_empty() {
            return Ok(StoreInUse::Second);
        }
        let store = match is_chain_state(root_key) {
            true => StoreInUse::First,
            false => StoreInUse::Second,
        };
        Ok(store)
    }
}

/// A `Clock` implementation using the system clock.
#[derive(Clone)]
pub struct WallClock;

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl Clock for WallClock {
    fn current_time(&self) -> Timestamp {
        Timestamp::now()
    }

    async fn sleep(&self, delta: TimeDelta) {
        linera_base::time::timer::sleep(delta.as_duration()).await
    }

    async fn sleep_until(&self, timestamp: Timestamp) {
        let delta = timestamp.delta_since(Timestamp::now());
        if delta > TimeDelta::ZERO {
            self.sleep(delta).await
        }
    }
}

#[cfg(with_testing)]
#[derive(Default)]
struct TestClockInner {
    time: Timestamp,
    sleeps: BTreeMap<Reverse<Timestamp>, Vec<oneshot::Sender<()>>>,
    /// Optional callback that decides whether to auto-advance for a given target timestamp.
    /// Returns `true` if the clock should auto-advance to that time.
    sleep_callback: Option<Box<dyn Fn(Timestamp) -> bool + Send + Sync>>,
}

#[cfg(with_testing)]
impl TestClockInner {
    fn set(&mut self, time: Timestamp) {
        self.time = time;
        let senders = self.sleeps.split_off(&Reverse(time));
        for sender in senders.into_values().flatten() {
            // Receiver may have been dropped if the sleep was cancelled.
            sender.send(()).ok();
        }
    }

    fn add_sleep(&mut self, delta: TimeDelta) -> Receiver<()> {
        let target_time = self.time.saturating_add(delta);
        self.add_sleep_until(target_time)
    }

    fn add_sleep_until(&mut self, time: Timestamp) -> Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let should_auto_advance = self
            .sleep_callback
            .as_ref()
            .is_some_and(|callback| callback(time));
        if should_auto_advance && time > self.time {
            // Auto-advance mode: immediately advance the clock and complete the sleep.
            self.set(time);
            // Receiver may have been dropped if the sleep was cancelled.
            sender.send(()).ok();
        } else if self.time >= time {
            // Receiver may have been dropped if the sleep was cancelled.
            sender.send(()).ok();
        } else {
            self.sleeps.entry(Reverse(time)).or_default().push(sender);
        }
        receiver
    }
}

/// A clock implementation that uses a stored number of microseconds and that can be updated
/// explicitly. All clones share the same time, and setting it in one clone updates all the others.
#[cfg(with_testing)]
#[derive(Clone, Default)]
pub struct TestClock(Arc<std::sync::Mutex<TestClockInner>>);

#[cfg(with_testing)]
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl Clock for TestClock {
    fn current_time(&self) -> Timestamp {
        self.lock().time
    }

    async fn sleep(&self, delta: TimeDelta) {
        if delta == TimeDelta::ZERO {
            return;
        }
        let receiver = self.lock().add_sleep(delta);
        // Sender may have been dropped if the clock was dropped; just stop waiting.
        receiver.await.ok();
    }

    async fn sleep_until(&self, timestamp: Timestamp) {
        let receiver = self.lock().add_sleep_until(timestamp);
        // Sender may have been dropped if the clock was dropped; just stop waiting.
        receiver.await.ok();
    }
}

#[cfg(with_testing)]
impl TestClock {
    /// Creates a new clock with its time set to 0, i.e. the Unix epoch.
    pub fn new() -> Self {
        TestClock(Arc::default())
    }

    /// Sets the current time.
    pub fn set(&self, time: Timestamp) {
        self.lock().set(time);
    }

    /// Advances the current time by the specified delta.
    pub fn add(&self, delta: TimeDelta) {
        let mut guard = self.lock();
        let time = guard.time.saturating_add(delta);
        guard.set(time);
    }

    /// Returns the current time according to the test clock.
    pub fn current_time(&self) -> Timestamp {
        self.lock().time
    }

    /// Sets a callback that decides whether to auto-advance for each sleep call.
    ///
    /// The callback receives the target timestamp and should return `true` if the clock
    /// should auto-advance to that time, or `false` if the sleep should block normally.
    pub fn set_sleep_callback<F>(&self, callback: F)
    where
        F: Fn(Timestamp) -> bool + Send + Sync + 'static,
    {
        self.lock().sleep_callback = Some(Box::new(callback));
    }

    /// Clears the sleep callback.
    pub fn clear_sleep_callback(&self) {
        self.lock().sleep_callback = None;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TestClockInner> {
        self.0.lock().expect("poisoned TestClock mutex")
    }
}

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl<Database, C> Storage for DbStorage<Database, C>
where
    Database: KeyValueDatabase<
            Store: KeyValueStore + Clone + linera_base::util::traits::AutoTraits + 'static,
            Error: Send + Sync,
        > + Clone
        + linera_base::util::traits::AutoTraits
        + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    type Context = ViewContext<ChainRuntimeContext<Self>, Database::Store>;
    type Clock = C;
    type BlockExporterContext = ViewContext<u32, Database::Store>;

    fn clock(&self) -> &C {
        &self.clock
    }

    fn thread_pool(&self) -> &Arc<linera_execution::ThreadPool> {
        &self.thread_pool
    }

    #[instrument(level = "trace", skip_all, fields(chain_id = %chain_id))]
    async fn load_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, ViewError> {
        #[cfg(with_metrics)]
        let _metric = metrics::LOAD_CHAIN_LATENCY.measure_latency();
        let runtime_context = ChainRuntimeContext {
            storage: self.clone(),
            thread_pool: self.thread_pool.clone(),
            chain_id,
            execution_runtime_config: self.execution_runtime_config,
            user_contracts: self.user_contracts.clone(),
            user_services: self.user_services.clone(),
        };
        let root_key = RootKey::ChainState(chain_id).bytes();
        let store = self.database.open_exclusive(&root_key)?;
        let context = ViewContext::create_root_context(store, runtime_context).await?;
        ChainStateView::load(context).await
    }

    #[instrument(level = "trace", skip_all, fields(%blob_id))]
    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let root_key = RootKey::BlobId(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let test = store.contains_key(BLOB_KEY).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOB_COUNTER.with_label_values(&[]).inc();
        Ok(test)
    }

    #[instrument(skip_all, fields(blob_count = blob_ids.len()))]
    async fn missing_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<BlobId>, ViewError> {
        let mut missing_blobs = Vec::new();
        for blob_id in blob_ids {
            let root_key = RootKey::BlobId(*blob_id).bytes();
            let store = self.database.open_shared(&root_key)?;
            if !store.contains_key(BLOB_KEY).await? {
                missing_blobs.push(*blob_id);
            }
        }
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOBS_COUNTER.with_label_values(&[]).inc();
        Ok(missing_blobs)
    }

    #[instrument(skip_all, fields(%blob_id))]
    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let root_key = RootKey::BlobId(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let test = store.contains_key(BLOB_STATE_KEY).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOB_STATE_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(test)
    }

    #[instrument(skip_all, fields(%hash))]
    async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlock>, ViewError> {
        let root_key = RootKey::BlockHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let value = store.read_value(BLOCK_KEY).await?;
        #[cfg(with_metrics)]
        metrics::READ_CONFIRMED_BLOCK_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(value)
    }

    #[instrument(skip_all)]
    async fn read_confirmed_blocks<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Option<ConfirmedBlock>>, ViewError> {
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let root_keys = Self::get_root_keys_for_certificates(&hashes);
        let mut blocks = Vec::new();
        for root_key in root_keys {
            let store = self.database.open_shared(&root_key)?;
            blocks.push(store.read_value(BLOCK_KEY).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_CONFIRMED_BLOCKS_COUNTER
            .with_label_values(&[])
            .inc_by(hashes.len() as u64);
        Ok(blocks)
    }

    #[instrument(skip_all, fields(%blob_id))]
    async fn read_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError> {
        let root_key = RootKey::BlobId(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let maybe_blob_bytes = store.read_value_bytes(BLOB_KEY).await?;
        #[cfg(with_metrics)]
        metrics::READ_BLOB_COUNTER.with_label_values(&[]).inc();
        Ok(maybe_blob_bytes.map(|blob_bytes| Blob::new_with_id_unchecked(blob_id, blob_bytes)))
    }

    #[instrument(skip_all, fields(blob_ids_len = %blob_ids.len()))]
    async fn read_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Option<Blob>>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut blobs = Vec::new();
        for blob_id in blob_ids {
            blobs.push(self.read_blob(*blob_id).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_BLOB_COUNTER
            .with_label_values(&[])
            .inc_by(blob_ids.len() as u64);
        Ok(blobs)
    }

    #[instrument(skip_all, fields(%blob_id))]
    async fn read_blob_state(&self, blob_id: BlobId) -> Result<Option<BlobState>, ViewError> {
        let root_key = RootKey::BlobId(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let blob_state = store.read_value::<BlobState>(BLOB_STATE_KEY).await?;
        #[cfg(with_metrics)]
        metrics::READ_BLOB_STATE_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(blob_state)
    }

    #[instrument(skip_all, fields(blob_ids_len = %blob_ids.len()))]
    async fn read_blob_states(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<Option<BlobState>>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut blob_states = Vec::new();
        for blob_id in blob_ids {
            blob_states.push(self.read_blob_state(*blob_id).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_BLOB_STATES_COUNTER
            .with_label_values(&[])
            .inc_by(blob_ids.len() as u64);
        Ok(blob_states)
    }

    #[instrument(skip_all, fields(blob_id = %blob.id()))]
    async fn write_blob(&self, blob: &Blob) -> Result<(), ViewError> {
        let mut batch = MultiPartitionBatch::new();
        batch.add_blob(blob)?;
        self.write_batch(batch).await?;
        Ok(())
    }

    #[instrument(skip_all, fields(blob_ids_len = %blob_ids.len()))]
    async fn maybe_write_blob_states(
        &self,
        blob_ids: &[BlobId],
        blob_state: BlobState,
    ) -> Result<(), ViewError> {
        if blob_ids.is_empty() {
            return Ok(());
        }
        let mut maybe_blob_states = Vec::new();
        for blob_id in blob_ids {
            let root_key = RootKey::BlobId(*blob_id).bytes();
            let store = self.database.open_shared(&root_key)?;
            let maybe_blob_state = store.read_value::<BlobState>(BLOB_STATE_KEY).await?;
            maybe_blob_states.push(maybe_blob_state);
        }
        let mut batch = MultiPartitionBatch::new();
        for (maybe_blob_state, blob_id) in maybe_blob_states.iter().zip(blob_ids) {
            match maybe_blob_state {
                None => {
                    batch.add_blob_state(*blob_id, &blob_state)?;
                }
                Some(state) => {
                    if state.epoch < blob_state.epoch {
                        batch.add_blob_state(*blob_id, &blob_state)?;
                    }
                }
            }
        }
        // We tolerate race conditions because two active chains are likely to
        // be both from the latest epoch, and otherwise failing to pick the
        // more recent blob state has limited impact.
        self.write_batch(batch).await?;
        Ok(())
    }

    #[instrument(skip_all, fields(blobs_len = %blobs.len()))]
    async fn maybe_write_blobs(&self, blobs: &[Blob]) -> Result<Vec<bool>, ViewError> {
        if blobs.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch = MultiPartitionBatch::new();
        let mut blob_states = Vec::new();
        for blob in blobs {
            let root_key = RootKey::BlobId(blob.id()).bytes();
            let store = self.database.open_shared(&root_key)?;
            let has_state = store.contains_key(BLOB_STATE_KEY).await?;
            blob_states.push(has_state);
            if has_state {
                batch.add_blob(blob)?;
            }
        }
        self.write_batch(batch).await?;
        Ok(blob_states)
    }

    #[instrument(skip_all, fields(blobs_len = %blobs.len()))]
    async fn write_blobs(&self, blobs: &[Blob]) -> Result<(), ViewError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let mut batch = MultiPartitionBatch::new();
        for blob in blobs {
            batch.add_blob(blob)?;
        }
        self.write_batch(batch).await
    }

    #[instrument(skip_all, fields(blobs_len = %blobs.len()))]
    async fn write_blobs_and_certificate(
        &self,
        blobs: &[Blob],
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError> {
        let mut batch = MultiPartitionBatch::new();
        for blob in blobs {
            batch.add_blob(blob)?;
        }
        batch.add_certificate(certificate)?;
        self.write_batch(batch).await
    }

    #[instrument(skip_all, fields(%hash))]
    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        let root_key = RootKey::BlockHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let results = store.contains_keys(&get_block_keys()).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(results[0] && results[1])
    }

    #[instrument(skip_all, fields(%hash))]
    async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlockCertificate>, ViewError> {
        let root_key = RootKey::BlockHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let values = store.read_multi_values_bytes(&get_block_keys()).await?;
        #[cfg(with_metrics)]
        metrics::READ_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        Self::deserialize_certificate(&values, hash)
    }

    #[instrument(skip_all)]
    async fn read_certificates(
        &self,
        hashes: &[CryptoHash],
    ) -> Result<Vec<Option<ConfirmedBlockCertificate>>, ViewError> {
        let raw_certs = self.read_certificates_raw(hashes).await?;

        raw_certs
            .into_iter()
            .zip(hashes)
            .map(|(maybe_raw, hash)| {
                let Some((lite_cert_bytes, confirmed_block_bytes)) = maybe_raw else {
                    return Ok(None);
                };
                let cert = bcs::from_bytes::<LiteCertificate>(&lite_cert_bytes)?;
                let value = bcs::from_bytes::<ConfirmedBlock>(&confirmed_block_bytes)?;
                assert_eq!(&value.hash(), hash);
                let certificate = cert
                    .with_value(value)
                    .ok_or(ViewError::InconsistentEntries)?;
                Ok(Some(certificate))
            })
            .collect()
    }

    #[instrument(skip_all)]
    async fn read_certificates_raw(
        &self,
        hashes: &[CryptoHash],
    ) -> Result<Vec<Option<(Vec<u8>, Vec<u8>)>>, ViewError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let root_keys = Self::get_root_keys_for_certificates(hashes);
        let mut values = Vec::new();
        for root_key in root_keys {
            let store = self.database.open_shared(&root_key)?;
            values.extend(store.read_multi_values_bytes(&get_block_keys()).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_CERTIFICATES_COUNTER
            .with_label_values(&[])
            .inc_by(hashes.len() as u64);
        Ok(values
            .chunks_exact(2)
            .map(|chunk| {
                let lite_cert_bytes = chunk[0].as_ref()?;
                let confirmed_block_bytes = chunk[1].as_ref()?;
                Some((lite_cert_bytes.clone(), confirmed_block_bytes.clone()))
            })
            .collect())
    }

    async fn read_certificate_hashes_by_heights(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<Option<CryptoHash>>, ViewError> {
        if heights.is_empty() {
            return Ok(Vec::new());
        }

        let index_root_key = RootKey::BlockByHeight(chain_id).bytes();
        let store = self.database.open_shared(&index_root_key)?;
        let height_keys: Vec<Vec<u8>> = heights.iter().map(|h| to_height_key(*h)).collect();
        let hash_bytes = store.read_multi_values_bytes(&height_keys).await?;
        let hash_options: Vec<Option<CryptoHash>> = hash_bytes
            .into_iter()
            .map(|opt| {
                opt.map(|bytes| bcs::from_bytes::<CryptoHash>(&bytes))
                    .transpose()
            })
            .collect::<Result<_, _>>()?;

        Ok(hash_options)
    }

    #[instrument(skip_all)]
    async fn read_certificates_by_heights_raw(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<Option<(Vec<u8>, Vec<u8>)>>, ViewError> {
        let hashes: Vec<Option<CryptoHash>> = self
            .read_certificate_hashes_by_heights(chain_id, heights)
            .await?;

        // Map from hash to all indices in the heights array (handles duplicates)
        let mut indices: HashMap<CryptoHash, Vec<usize>> = HashMap::new();
        for (index, maybe_hash) in hashes.iter().enumerate() {
            if let Some(hash) = maybe_hash {
                indices.entry(*hash).or_default().push(index);
            }
        }

        // Deduplicate hashes for the storage query
        let unique_hashes = indices.keys().copied().collect::<Vec<_>>();

        let mut result = vec![None; heights.len()];

        for (raw_cert, hash) in self
            .read_certificates_raw(&unique_hashes)
            .await?
            .into_iter()
            .zip(unique_hashes)
        {
            if let Some(idx_list) = indices.get(&hash) {
                for &index in idx_list {
                    result[index] = raw_cert.clone();
                }
            } else {
                // This should not happen, but log a warning if it does.
                tracing::error!(?hash, "certificate hash not found in indices map",);
            }
        }

        Ok(result)
    }

    #[instrument(skip_all, fields(%chain_id, heights_len = heights.len()))]
    async fn read_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<Option<ConfirmedBlockCertificate>>, ViewError> {
        self.read_certificates_by_heights_raw(chain_id, heights)
            .await?
            .into_iter()
            .map(|maybe_raw| match maybe_raw {
                None => Ok(None),
                Some((lite_cert_bytes, confirmed_block_bytes)) => {
                    let cert = bcs::from_bytes::<LiteCertificate>(&lite_cert_bytes)?;
                    let value = bcs::from_bytes::<ConfirmedBlock>(&confirmed_block_bytes)?;
                    let certificate = cert
                        .with_value(value)
                        .ok_or(ViewError::InconsistentEntries)?;
                    Ok(Some(certificate))
                }
            })
            .collect()
    }

    #[instrument(skip_all, fields(event_id = ?event_id))]
    async fn read_event(&self, event_id: EventId) -> Result<Option<Vec<u8>>, ViewError> {
        let event_key = to_event_key(&event_id);
        let root_key = RootKey::Event(event_id.chain_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let event = store.read_value_bytes(&event_key).await?;
        #[cfg(with_metrics)]
        metrics::READ_EVENT_COUNTER.with_label_values(&[]).inc();
        Ok(event)
    }

    #[instrument(skip_all, fields(event_id = ?event_id))]
    async fn contains_event(&self, event_id: EventId) -> Result<bool, ViewError> {
        let event_key = to_event_key(&event_id);
        let root_key = RootKey::Event(event_id.chain_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let exists = store.contains_key(&event_key).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_EVENT_COUNTER.with_label_values(&[]).inc();
        Ok(exists)
    }

    #[instrument(skip_all, fields(chain_id = %chain_id, stream_id = %stream_id, start_index = %start_index))]
    async fn read_events_from_index(
        &self,
        chain_id: &ChainId,
        stream_id: &StreamId,
        start_index: u32,
    ) -> Result<Vec<IndexAndEvent>, ViewError> {
        let root_key = RootKey::Event(*chain_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let mut keys = Vec::new();
        let mut indices = Vec::new();
        let prefix = bcs::to_bytes(stream_id).unwrap();
        for short_key in store.find_keys_by_prefix(&prefix).await? {
            let index = bcs::from_bytes::<u32>(&short_key)?;
            if index >= start_index {
                let mut key = prefix.clone();
                key.extend(short_key);
                keys.push(key);
                indices.push(index);
            }
        }
        let values = store.read_multi_values_bytes(&keys).await?;
        let mut returned_values = Vec::new();
        for (index, value) in indices.into_iter().zip(values) {
            let event = value.unwrap();
            returned_values.push(IndexAndEvent { index, event });
        }
        Ok(returned_values)
    }

    #[instrument(skip_all)]
    async fn write_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError> {
        let mut batch = MultiPartitionBatch::new();
        for (event_id, value) in events {
            batch.add_event(event_id, value)?;
        }
        self.write_batch(batch).await
    }

    #[instrument(skip_all)]
    async fn read_network_description(&self) -> Result<Option<NetworkDescription>, ViewError> {
        let root_key = RootKey::NetworkDescription.bytes();
        let store = self.database.open_shared(&root_key)?;
        let maybe_value = store.read_value(NETWORK_DESCRIPTION_KEY).await?;
        #[cfg(with_metrics)]
        metrics::READ_NETWORK_DESCRIPTION
            .with_label_values(&[])
            .inc();
        Ok(maybe_value)
    }

    #[instrument(skip_all)]
    async fn write_network_description(
        &self,
        information: &NetworkDescription,
    ) -> Result<(), ViewError> {
        let mut batch = MultiPartitionBatch::new();
        batch.add_network_description(information)?;
        self.write_batch(batch).await?;
        Ok(())
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.wasm_runtime
    }

    #[instrument(skip_all)]
    async fn block_exporter_context(
        &self,
        block_exporter_id: u32,
    ) -> Result<Self::BlockExporterContext, ViewError> {
        let root_key = RootKey::BlockExporterState(block_exporter_id).bytes();
        let store = self.database.open_exclusive(&root_key)?;
        Ok(ViewContext::create_root_context(store, block_exporter_id).await?)
    }

    async fn list_blob_ids(&self) -> Result<Vec<BlobId>, ViewError> {
        let root_keys = self.database.list_root_keys().await?;
        let mut blob_ids = Vec::new();
        for root_key in root_keys {
            if !root_key.is_empty() && root_key[0] == BLOB_ID_TAG {
                let root_key_red = &root_key[1..];
                let blob_id = bcs::from_bytes(root_key_red)?;
                blob_ids.push(blob_id);
            }
        }
        Ok(blob_ids)
    }

    async fn list_chain_ids(&self) -> Result<Vec<ChainId>, ViewError> {
        let root_keys = self.database.list_root_keys().await?;
        let mut chain_ids = Vec::new();
        for root_key in root_keys {
            if !root_key.is_empty() && root_key[0] == CHAIN_ID_TAG {
                let root_key_red = &root_key[1..];
                let chain_id = bcs::from_bytes(root_key_red)?;
                chain_ids.push(chain_id);
            }
        }
        Ok(chain_ids)
    }

    async fn list_event_ids(&self) -> Result<Vec<EventId>, ViewError> {
        let root_keys = self.database.list_root_keys().await?;
        let mut event_ids = Vec::new();
        for root_key in root_keys {
            if !root_key.is_empty() && root_key[0] == EVENT_ID_TAG {
                let root_key_red = &root_key[1..];
                let chain_id = bcs::from_bytes(root_key_red)?;
                let store = self.database.open_shared(&root_key)?;
                let keys = store.find_keys_by_prefix(&[]).await?;
                for key in keys {
                    let restricted_event_id = bcs::from_bytes::<RestrictedEventId>(&key)?;
                    let event_id = EventId {
                        chain_id,
                        stream_id: restricted_event_id.stream_id,
                        index: restricted_event_id.index,
                    };
                    event_ids.push(event_id);
                }
            }
        }
        Ok(event_ids)
    }
}

impl<Database, C> DbStorage<Database, C>
where
    Database: KeyValueDatabase + Clone,
    Database::Store: KeyValueStore + Clone,
    C: Clock,
    Database::Error: Send + Sync,
{
    #[instrument(skip_all)]
    fn get_root_keys_for_certificates(hashes: &[CryptoHash]) -> Vec<Vec<u8>> {
        hashes
            .iter()
            .map(|hash| RootKey::BlockHash(*hash).bytes())
            .collect()
    }

    #[instrument(skip_all)]
    fn deserialize_certificate(
        pair: &[Option<Vec<u8>>],
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlockCertificate>, ViewError> {
        let Some(cert_bytes) = pair[0].as_ref() else {
            return Ok(None);
        };
        let Some(value_bytes) = pair[1].as_ref() else {
            return Ok(None);
        };
        let cert = bcs::from_bytes::<LiteCertificate>(cert_bytes)?;
        let value = bcs::from_bytes::<ConfirmedBlock>(value_bytes)?;
        assert_eq!(value.hash(), hash);
        let certificate = cert
            .with_value(value)
            .ok_or(ViewError::InconsistentEntries)?;
        Ok(Some(certificate))
    }

    #[instrument(skip_all)]
    async fn write_entry(
        store: &Database::Store,
        key_values: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        for (key, value) in key_values {
            batch.put_key_value_bytes(key, value);
        }
        store.write_batch(batch).await?;
        Ok(())
    }

    #[instrument(skip_all, fields(batch_size = batch.keys_value_bytes.len()))]
    async fn write_batch(&self, batch: MultiPartitionBatch) -> Result<(), ViewError> {
        if batch.keys_value_bytes.is_empty() {
            return Ok(());
        }
        let mut futures = Vec::new();
        for (root_key, key_values) in batch.keys_value_bytes {
            let store = self.database.open_shared(&root_key)?;
            futures.push(async move { Self::write_entry(&store, key_values).await });
        }
        futures::future::try_join_all(futures).await?;
        Ok(())
    }
}

impl<Database, C> DbStorage<Database, C> {
    fn new(database: Database, wasm_runtime: Option<WasmRuntime>, clock: C) -> Self {
        Self {
            database: Arc::new(database),
            clock,
            // The `Arc` here is required on native but useless on the Web.
            #[cfg_attr(web, expect(clippy::arc_with_non_send_sync))]
            thread_pool: Arc::new(linera_execution::ThreadPool::new(20)),
            wasm_runtime,
            user_contracts: Arc::new(papaya::HashMap::new()),
            user_services: Arc::new(papaya::HashMap::new()),
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        }
    }

    /// Sets whether contract log messages should be output.
    pub fn with_allow_application_logs(mut self, allow: bool) -> Self {
        self.execution_runtime_config.allow_application_logs = allow;
        self
    }
}

impl<Database> DbStorage<Database, WallClock>
where
    Database: KeyValueDatabase + Clone + 'static,
    Database::Error: Send + Sync,
    Database::Store: KeyValueStore + Clone + 'static,
{
    pub async fn maybe_create_and_connect(
        config: &Database::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, Database::Error> {
        let database = Database::maybe_create_and_connect(config, namespace).await?;
        Ok(Self::new(database, wasm_runtime, WallClock))
    }

    pub async fn connect(
        config: &Database::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, Database::Error> {
        let database = Database::connect(config, namespace).await?;
        Ok(Self::new(database, wasm_runtime, WallClock))
    }
}

#[cfg(with_testing)]
impl<Database> DbStorage<Database, TestClock>
where
    Database: TestKeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Store: KeyValueStore + Clone + Send + Sync + 'static,
    Database::Error: Send + Sync,
{
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let config = Database::new_test_config().await.unwrap();
        let namespace = generate_test_namespace();
        DbStorage::<Database, TestClock>::new_for_testing(
            config,
            &namespace,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .unwrap()
    }

    pub async fn new_for_testing(
        config: Database::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, Database::Error> {
        let database = Database::recreate_and_connect(&config, namespace).await?;
        Ok(Self::new(database, wasm_runtime, clock))
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{BlockHeight, Epoch, Round, Timestamp},
        identifiers::{
            ApplicationId, BlobId, BlobType, ChainId, EventId, GenericApplicationId, StreamId,
            StreamName,
        },
    };
    use linera_chain::{
        block::{Block, BlockBody, BlockHeader, ConfirmedBlock},
        types::ConfirmedBlockCertificate,
    };
    use linera_views::{
        memory::MemoryDatabase,
        store::{KeyValueDatabase, ReadableKeyValueStore as _},
    };

    use crate::{
        db_storage::{
            to_event_key, to_height_key, MultiPartitionBatch, RootKey, BLOB_ID_TAG, CHAIN_ID_TAG,
            EVENT_ID_TAG,
        },
        DbStorage, Storage, TestClock,
    };

    // Several functionalities of the storage rely on the way that the serialization
    // is done. Thus we need to check that the serialization works in the way that
    // we expect.

    // The listing of the blobs in `list_blob_ids` depends on the serialization
    // of `RootKey::Blob`.
    #[test]
    fn test_root_key_blob_serialization() {
        let hash = CryptoHash::default();
        let blob_type = BlobType::default();
        let blob_id = BlobId::new(hash, blob_type);
        let root_key = RootKey::BlobId(blob_id).bytes();
        assert_eq!(root_key[0], BLOB_ID_TAG);
        assert_eq!(bcs::from_bytes::<BlobId>(&root_key[1..]).unwrap(), blob_id);
    }

    // The listing of the chains in `list_chain_ids` depends on the serialization
    // of `RootKey::ChainState`.
    #[test]
    fn test_root_key_chainstate_serialization() {
        let hash = CryptoHash::default();
        let chain_id = ChainId(hash);
        let root_key = RootKey::ChainState(chain_id).bytes();
        assert_eq!(root_key[0], CHAIN_ID_TAG);
        assert_eq!(
            bcs::from_bytes::<ChainId>(&root_key[1..]).unwrap(),
            chain_id
        );
    }

    // The listing of the events in `read_events_from_index` depends on the
    // serialization of `RootKey::Event`.
    #[test]
    fn test_root_key_event_serialization() {
        let hash = CryptoHash::test_hash("49");
        let chain_id = ChainId(hash);
        let application_description_hash = CryptoHash::test_hash("42");
        let application_id = ApplicationId::new(application_description_hash);
        let application_id = GenericApplicationId::User(application_id);
        let stream_name = StreamName(bcs::to_bytes("linera_stream").unwrap());
        let stream_id = StreamId {
            application_id,
            stream_name,
        };
        let prefix = bcs::to_bytes(&stream_id).unwrap();

        let index = 1567;
        let event_id = EventId {
            chain_id,
            stream_id,
            index,
        };
        let root_key = RootKey::Event(chain_id).bytes();
        assert_eq!(root_key[0], EVENT_ID_TAG);
        let key = to_event_key(&event_id);
        assert!(key.starts_with(&prefix));
    }

    // The height index lookup depends on the serialization of RootKey::BlockByHeight
    // and to_height_key, following the same pattern as Event.
    #[test]
    fn test_root_key_block_by_height_serialization() {
        use linera_base::data_types::BlockHeight;

        let hash = CryptoHash::default();
        let chain_id = ChainId(hash);
        let height = BlockHeight(42);

        // RootKey::BlockByHeight uses only ChainId for partitioning (like Event)
        let root_key = RootKey::BlockByHeight(chain_id).bytes();
        let deserialized_chain_id: ChainId = bcs::from_bytes(&root_key[1..]).unwrap();
        assert_eq!(deserialized_chain_id, chain_id);

        // Height is encoded as a key (like index in Event)
        let height_key = to_height_key(height);
        let deserialized_height: BlockHeight = bcs::from_bytes(&height_key).unwrap();
        assert_eq!(deserialized_height, height);
    }

    #[cfg(with_testing)]
    #[tokio::test]
    async fn test_add_certificate_creates_height_index() {
        // Create test storage
        let storage = DbStorage::<MemoryDatabase, TestClock>::make_test_storage(None).await;

        // Create a test certificate at a specific height
        let chain_id = ChainId(CryptoHash::test_hash("test_chain"));
        let height = BlockHeight(5);
        let block = Block {
            header: BlockHeader {
                chain_id,
                epoch: Epoch::ZERO,
                height,
                timestamp: Timestamp::from(0),
                state_hash: CryptoHash::new(&TestString::new("state_hash")),
                previous_block_hash: None,
                authenticated_owner: None,
                transactions_hash: CryptoHash::new(&TestString::new("transactions_hash")),
                messages_hash: CryptoHash::new(&TestString::new("messages_hash")),
                previous_message_blocks_hash: CryptoHash::new(&TestString::new(
                    "prev_msg_blocks_hash",
                )),
                previous_event_blocks_hash: CryptoHash::new(&TestString::new(
                    "prev_event_blocks_hash",
                )),
                oracle_responses_hash: CryptoHash::new(&TestString::new("oracle_responses_hash")),
                events_hash: CryptoHash::new(&TestString::new("events_hash")),
                blobs_hash: CryptoHash::new(&TestString::new("blobs_hash")),
                operation_results_hash: CryptoHash::new(&TestString::new("operation_results_hash")),
            },
            body: BlockBody {
                transactions: vec![],
                messages: vec![],
                previous_message_blocks: Default::default(),
                previous_event_blocks: Default::default(),
                oracle_responses: vec![],
                events: vec![],
                blobs: vec![],
                operation_results: vec![],
            },
        };
        let confirmed_block = ConfirmedBlock::new(block);
        let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);

        // Write certificate
        let mut batch = MultiPartitionBatch::new();
        batch.add_certificate(&certificate).unwrap();
        storage.write_batch(batch).await.unwrap();

        // Verify height index was created (following Event pattern)
        let hash = certificate.hash();
        let index_root_key = RootKey::BlockByHeight(chain_id).bytes();
        let store = storage.database.open_shared(&index_root_key).unwrap();
        let height_key = to_height_key(height);
        let value_bytes = store.read_value_bytes(&height_key).await.unwrap();

        assert!(value_bytes.is_some(), "Height index was not created");
        let stored_hash: CryptoHash = bcs::from_bytes(&value_bytes.unwrap()).unwrap();
        assert_eq!(stored_hash, hash, "Height index contains wrong hash");
    }

    #[cfg(with_testing)]
    #[tokio::test]
    async fn test_read_certificates_by_heights() {
        let storage = DbStorage::<MemoryDatabase, TestClock>::make_test_storage(None).await;
        let chain_id = ChainId(CryptoHash::test_hash("test_chain"));

        // Write certificates at heights 1, 3, 5
        let mut batch = MultiPartitionBatch::new();
        let mut expected_certs = vec![];

        for height in [1, 3, 5] {
            let block = Block {
                header: BlockHeader {
                    chain_id,
                    epoch: Epoch::ZERO,
                    height: BlockHeight(height),
                    timestamp: Timestamp::from(0),
                    state_hash: CryptoHash::new(&TestString::new("state_hash_{height}")),
                    previous_block_hash: None,
                    authenticated_owner: None,
                    transactions_hash: CryptoHash::new(&TestString::new("tx_hash_{height}")),
                    messages_hash: CryptoHash::new(&TestString::new("msg_hash_{height}")),
                    previous_message_blocks_hash: CryptoHash::new(&TestString::new(
                        "pmb_hash_{height}",
                    )),
                    previous_event_blocks_hash: CryptoHash::new(&TestString::new(
                        "peb_hash_{height}",
                    )),
                    oracle_responses_hash: CryptoHash::new(&TestString::new(
                        "oracle_hash_{height}",
                    )),
                    events_hash: CryptoHash::new(&TestString::new("events_hash_{height}")),
                    blobs_hash: CryptoHash::new(&TestString::new("blobs_hash_{height}")),
                    operation_results_hash: CryptoHash::new(&TestString::new(
                        "op_results_hash_{height}",
                    )),
                },
                body: BlockBody {
                    transactions: vec![],
                    messages: vec![],
                    previous_message_blocks: Default::default(),
                    previous_event_blocks: Default::default(),
                    oracle_responses: vec![],
                    events: vec![],
                    blobs: vec![],
                    operation_results: vec![],
                },
            };
            let confirmed_block = ConfirmedBlock::new(block);
            let cert = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
            expected_certs.push((height, cert.clone()));
            batch.add_certificate(&cert).unwrap();
        }
        storage.write_batch(batch).await.unwrap();

        // Test: Read in order [1, 3, 5]
        let heights = vec![BlockHeight(1), BlockHeight(3), BlockHeight(5)];
        let result = storage
            .read_certificates_by_heights(chain_id, &heights)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0].as_ref().unwrap().hash(),
            expected_certs[0].1.hash()
        );
        assert_eq!(
            result[1].as_ref().unwrap().hash(),
            expected_certs[1].1.hash()
        );
        assert_eq!(
            result[2].as_ref().unwrap().hash(),
            expected_certs[2].1.hash()
        );

        // Test: Read out of order [5, 1, 3]
        let heights = vec![BlockHeight(5), BlockHeight(1), BlockHeight(3)];
        let result = storage
            .read_certificates_by_heights(chain_id, &heights)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0].as_ref().unwrap().hash(),
            expected_certs[2].1.hash()
        );
        assert_eq!(
            result[1].as_ref().unwrap().hash(),
            expected_certs[0].1.hash()
        );
        assert_eq!(
            result[2].as_ref().unwrap().hash(),
            expected_certs[1].1.hash()
        );

        // Test: Read with missing heights [1, 2, 3]
        let heights = vec![
            BlockHeight(1),
            BlockHeight(2),
            BlockHeight(3),
            BlockHeight(3),
        ];
        let result = storage
            .read_certificates_by_heights(chain_id, &heights)
            .await
            .unwrap();
        assert_eq!(result.len(), 4); // BlockHeight(3) was duplicated.
        assert!(result[0].is_some());
        assert!(result[1].is_none()); // Height 2 doesn't exist
        assert!(result[2].is_some());
        assert!(result[3].is_some());
        assert_eq!(
            result[2].as_ref().unwrap().hash(),
            result[3].as_ref().unwrap().hash()
        ); // Both correspond to height 3

        // Test: Empty heights
        let heights = vec![];
        let result = storage
            .read_certificates_by_heights(chain_id, &heights)
            .await
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[cfg(with_testing)]
    #[tokio::test]
    async fn test_read_certificates_by_heights_multiple_chains() {
        let storage = DbStorage::<MemoryDatabase, TestClock>::make_test_storage(None).await;

        // Create certificates for two different chains at same heights
        let chain_a = ChainId(CryptoHash::test_hash("chain_a"));
        let chain_b = ChainId(CryptoHash::test_hash("chain_b"));

        let mut batch = MultiPartitionBatch::new();

        let block_a = Block {
            header: BlockHeader {
                chain_id: chain_a,
                epoch: Epoch::ZERO,
                height: BlockHeight(10),
                timestamp: Timestamp::from(0),
                state_hash: CryptoHash::new(&TestString::new("state_hash_a")),
                previous_block_hash: None,
                authenticated_owner: None,
                transactions_hash: CryptoHash::new(&TestString::new("tx_hash_a")),
                messages_hash: CryptoHash::new(&TestString::new("msg_hash_a")),
                previous_message_blocks_hash: CryptoHash::new(&TestString::new("pmb_hash_a")),
                previous_event_blocks_hash: CryptoHash::new(&TestString::new("peb_hash_a")),
                oracle_responses_hash: CryptoHash::new(&TestString::new("oracle_hash_a")),
                events_hash: CryptoHash::new(&TestString::new("events_hash_a")),
                blobs_hash: CryptoHash::new(&TestString::new("blobs_hash_a")),
                operation_results_hash: CryptoHash::new(&TestString::new("op_results_hash_a")),
            },
            body: BlockBody {
                transactions: vec![],
                messages: vec![],
                previous_message_blocks: Default::default(),
                previous_event_blocks: Default::default(),
                oracle_responses: vec![],
                events: vec![],
                blobs: vec![],
                operation_results: vec![],
            },
        };
        let confirmed_block_a = ConfirmedBlock::new(block_a);
        let cert_a = ConfirmedBlockCertificate::new(confirmed_block_a, Round::Fast, vec![]);
        batch.add_certificate(&cert_a).unwrap();

        let block_b = Block {
            header: BlockHeader {
                chain_id: chain_b,
                epoch: Epoch::ZERO,
                height: BlockHeight(10),
                timestamp: Timestamp::from(0),
                state_hash: CryptoHash::new(&TestString::new("state_hash_b")),
                previous_block_hash: None,
                authenticated_owner: None,
                transactions_hash: CryptoHash::new(&TestString::new("tx_hash_b")),
                messages_hash: CryptoHash::new(&TestString::new("msg_hash_b")),
                previous_message_blocks_hash: CryptoHash::new(&TestString::new("pmb_hash_b")),
                previous_event_blocks_hash: CryptoHash::new(&TestString::new("peb_hash_b")),
                oracle_responses_hash: CryptoHash::new(&TestString::new("oracle_hash_b")),
                events_hash: CryptoHash::new(&TestString::new("events_hash_b")),
                blobs_hash: CryptoHash::new(&TestString::new("blobs_hash_b")),
                operation_results_hash: CryptoHash::new(&TestString::new("op_results_hash_b")),
            },
            body: BlockBody {
                transactions: vec![],
                messages: vec![],
                previous_message_blocks: Default::default(),
                previous_event_blocks: Default::default(),
                oracle_responses: vec![],
                events: vec![],
                blobs: vec![],
                operation_results: vec![],
            },
        };
        let confirmed_block_b = ConfirmedBlock::new(block_b);
        let cert_b = ConfirmedBlockCertificate::new(confirmed_block_b, Round::Fast, vec![]);
        batch.add_certificate(&cert_b).unwrap();

        storage.write_batch(batch).await.unwrap();

        // Read from chain A - should get cert A
        let result = storage
            .read_certificates_by_heights(chain_a, &[BlockHeight(10)])
            .await
            .unwrap();
        assert_eq!(result[0].as_ref().unwrap().hash(), cert_a.hash());

        // Read from chain B - should get cert B
        let result = storage
            .read_certificates_by_heights(chain_b, &[BlockHeight(10)])
            .await
            .unwrap();
        assert_eq!(result[0].as_ref().unwrap().hash(), cert_b.hash());

        // Read from chain A for height that only chain B has - should get None
        let result = storage
            .read_certificates_by_heights(chain_a, &[BlockHeight(20)])
            .await
            .unwrap();
        assert!(result[0].is_none());
    }

    #[cfg(with_testing)]
    #[tokio::test]
    async fn test_read_certificates_by_heights_consistency() {
        let storage = DbStorage::<MemoryDatabase, TestClock>::make_test_storage(None).await;
        let chain_id = ChainId(CryptoHash::test_hash("test_chain"));

        // Write certificate
        let mut batch = MultiPartitionBatch::new();
        let block = Block {
            header: BlockHeader {
                chain_id,
                epoch: Epoch::ZERO,
                height: BlockHeight(7),
                timestamp: Timestamp::from(0),
                state_hash: CryptoHash::new(&TestString::new("state_hash")),
                previous_block_hash: None,
                authenticated_owner: None,
                transactions_hash: CryptoHash::new(&TestString::new("tx_hash")),
                messages_hash: CryptoHash::new(&TestString::new("msg_hash")),
                previous_message_blocks_hash: CryptoHash::new(&TestString::new("pmb_hash")),
                previous_event_blocks_hash: CryptoHash::new(&TestString::new("peb_hash")),
                oracle_responses_hash: CryptoHash::new(&TestString::new("oracle_hash")),
                events_hash: CryptoHash::new(&TestString::new("events_hash")),
                blobs_hash: CryptoHash::new(&TestString::new("blobs_hash")),
                operation_results_hash: CryptoHash::new(&TestString::new("op_results_hash")),
            },
            body: BlockBody {
                transactions: vec![],
                messages: vec![],
                previous_message_blocks: Default::default(),
                previous_event_blocks: Default::default(),
                oracle_responses: vec![],
                events: vec![],
                blobs: vec![],
                operation_results: vec![],
            },
        };
        let confirmed_block = ConfirmedBlock::new(block);
        let cert = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
        let hash = cert.hash();
        batch.add_certificate(&cert).unwrap();
        storage.write_batch(batch).await.unwrap();

        // Read by hash
        let cert_by_hash = storage.read_certificate(hash).await.unwrap().unwrap();

        // Read by height
        let certs_by_height = storage
            .read_certificates_by_heights(chain_id, &[BlockHeight(7)])
            .await
            .unwrap();
        let cert_by_height = certs_by_height[0].as_ref().unwrap();

        // Should be identical
        assert_eq!(cert_by_hash.hash(), cert_by_height.hash());
        assert_eq!(
            cert_by_hash.value().block().header,
            cert_by_height.value().block().header
        );
    }
}
