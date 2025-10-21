// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, NetworkDescription, TimeDelta, Timestamp},
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
    std::{cmp::Reverse, collections::BTreeMap},
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
            exponential_bucket_latencies(10.0),
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

/// The default key used when the root_key contains the information.
const DEFAULT_KEY: &[u8] = &[0];

/// The second key used when the root_key contains the information.
const ONE_KEY: &[u8] = &[1];

fn get_01_keys() -> Vec<Vec<u8>> {
    vec![vec![0], vec![1]]
}

#[derive(Default)]
struct MultiPartitionBatch {
    keys_value_bytes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
}

impl MultiPartitionBatch {
    fn new() -> Self {
        Self::default()
    }

    fn put_key_value_bytes(&mut self, root_key: Vec<u8>, key: Vec<u8>, value: Vec<u8>) {
        self.keys_value_bytes.push((root_key, key, value));
    }

    fn put_key_value<T: Serialize>(
        &mut self,
        root_key: Vec<u8>,
        value: &T,
    ) -> Result<(), ViewError> {
        let bytes = bcs::to_bytes(value)?;
        let key = DEFAULT_KEY.to_vec();
        self.keys_value_bytes.push((root_key, key, bytes));
        Ok(())
    }

    fn add_blob(&mut self, blob: &Blob) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_BLOB_COUNTER.with_label_values(&[]).inc();
        let root_key = RootKey::Blob(blob.id()).bytes();
        let key = DEFAULT_KEY.to_vec();
        self.put_key_value_bytes(root_key, key, blob.bytes().to_vec());
        Ok(())
    }

    fn add_blob_state(&mut self, blob_id: BlobId, blob_state: &BlobState) -> Result<(), ViewError> {
        let root_key = RootKey::Blob(blob_id).bytes();
        let key = ONE_KEY.to_vec();
        let value = bcs::to_bytes(blob_state)?;
        self.put_key_value_bytes(root_key, key, value);
        Ok(())
    }

    fn add_certificate(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        let hash = certificate.hash();
        let root_key = RootKey::CryptoHash(hash).bytes();
        self.put_key_value(root_key.clone(), &certificate.lite_certificate())?;
        let value = bcs::to_bytes(&certificate.value())?;
        self.put_key_value_bytes(root_key, ONE_KEY.to_vec(), value);
        Ok(())
    }

    fn add_event(&mut self, event_id: EventId, value: Vec<u8>) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        metrics::WRITE_EVENT_COUNTER.with_label_values(&[]).inc();
        let key = event_key(&event_id);
        let root_key = RootKey::Event(event_id.chain_id).bytes();
        self.put_key_value_bytes(root_key, key, value);
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
        self.put_key_value(root_key, information)?;
        Ok(())
    }
}

/// Main implementation of the [`Storage`] trait.
#[derive(Clone)]
pub struct DbStorage<Database, Clock = WallClock> {
    database: Arc<Database>,
    clock: Clock,
    wasm_runtime: Option<WasmRuntime>,
    user_contracts: Arc<papaya::HashMap<ApplicationId, UserContractCode>>,
    user_services: Arc<papaya::HashMap<ApplicationId, UserServiceCode>>,
    execution_runtime_config: ExecutionRuntimeConfig,
}

#[derive(Debug, Serialize, Deserialize)]
enum RootKey {
    ChainState(ChainId),
    CryptoHash(CryptoHash),
    Blob(BlobId),
    Event(ChainId),
    BlockExporterState(u32),
    NetworkDescription,
}

impl RootKey {
    fn bytes(&self) -> Vec<u8> {
        bcs::to_bytes(self).unwrap()
    }
}

fn event_key(event_id: &EventId) -> Vec<u8> {
    let mut key = bcs::to_bytes(&event_id.stream_id).unwrap();
    key.extend(bcs::to_bytes(&event_id.index).unwrap());
    key
}

fn is_chain_state(root_key: &[u8]) -> bool {
    if root_key.is_empty() {
        return false;
    }
    root_key[0] == CHAIN_ID_TAG
}

const CHAIN_ID_TAG: u8 = 0;
const BLOB_ID_TAG: u8 = 2;
const CHAIN_ID_LENGTH: usize = std::mem::size_of::<ChainId>();
const BLOB_ID_LENGTH: usize = std::mem::size_of::<BlobId>();

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::CryptoHash,
        identifiers::{
            ApplicationId, BlobId, BlobType, ChainId, EventId, GenericApplicationId, StreamId,
            StreamName,
        },
    };

    use crate::db_storage::{
        event_key, RootKey, BLOB_ID_LENGTH, BLOB_ID_TAG, CHAIN_ID_LENGTH, CHAIN_ID_TAG,
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
        let root_key = RootKey::Blob(blob_id).bytes();
        assert_eq!(root_key[0], BLOB_ID_TAG);
        assert_eq!(root_key.len(), 1 + BLOB_ID_LENGTH);
    }

    // The listing of the chains in `list_chain_ids` depends on the serialization
    // of `RootKey::ChainState`.
    #[test]
    fn test_root_key_chainstate_serialization() {
        let hash = CryptoHash::default();
        let chain_id = ChainId(hash);
        let root_key = RootKey::ChainState(chain_id).bytes();
        assert_eq!(root_key[0], CHAIN_ID_TAG);
        assert_eq!(root_key.len(), 1 + CHAIN_ID_LENGTH);
    }

    // The listing of the events in `read_events_from_index` depends on the
    // serialization of `BaseKey::Event`.
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
        let key = event_key(&event_id);
        assert!(key.starts_with(&prefix));
    }
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
}

#[cfg(with_testing)]
impl TestClockInner {
    fn set(&mut self, time: Timestamp) {
        self.time = time;
        let senders = self.sleeps.split_off(&Reverse(time));
        for sender in senders.into_values().flatten() {
            let _ = sender.send(());
        }
    }

    fn add_sleep(&mut self, delta: TimeDelta) -> Receiver<()> {
        self.add_sleep_until(self.time.saturating_add(delta))
    }

    fn add_sleep_until(&mut self, time: Timestamp) -> Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        if self.time >= time {
            let _ = sender.send(());
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
        let _ = receiver.await;
    }

    async fn sleep_until(&self, timestamp: Timestamp) {
        let receiver = self.lock().add_sleep_until(timestamp);
        let _ = receiver.await;
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

    fn lock(&self) -> std::sync::MutexGuard<TestClockInner> {
        self.0.lock().expect("poisoned TestClock mutex")
    }
}

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl<Database, C> Storage for DbStorage<Database, C>
where
    Database: KeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Store: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
    Database::Error: Send + Sync,
{
    type Context = ViewContext<ChainRuntimeContext<Self>, Database::Store>;
    type Clock = C;
    type BlockExporterContext = ViewContext<u32, Database::Store>;

    fn clock(&self) -> &C {
        &self.clock
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

    #[instrument(level = "trace", skip_all, fields(blob_id = %blob_id))]
    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let root_key = RootKey::Blob(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let test = store.contains_key(DEFAULT_KEY).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOB_COUNTER.with_label_values(&[]).inc();
        Ok(test)
    }

    #[instrument(skip_all, fields(blob_count = blob_ids.len()))]
    async fn missing_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<BlobId>, ViewError> {
        let mut missing_blobs = Vec::new();
        for blob_id in blob_ids {
            let root_key = RootKey::Blob(*blob_id).bytes();
            let store = self.database.open_shared(&root_key)?;
            let test = store.contains_key(DEFAULT_KEY).await?;
            if !test {
                missing_blobs.push(*blob_id);
            }
        }
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOBS_COUNTER.with_label_values(&[]).inc();
        Ok(missing_blobs)
    }

    #[instrument(skip_all, fields(blob_id = %blob_id))]
    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let root_key = RootKey::Blob(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let test = store.contains_key(ONE_KEY).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_BLOB_STATE_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(test)
    }

    #[instrument(skip_all, fields(hash = %hash))]
    async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlock>, ViewError> {
        let root_key = RootKey::CryptoHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let value = store.read_value(ONE_KEY).await?;
        #[cfg(with_metrics)]
        metrics::READ_CONFIRMED_BLOCK_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(value)
    }

    #[instrument(skip_all, fields(blob_id = %blob_id))]
    async fn read_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError> {
        let root_key = RootKey::Blob(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let maybe_blob_bytes = store.read_value_bytes(DEFAULT_KEY).await?;
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

    #[instrument(skip_all, fields(blob_id = %blob_id))]
    async fn read_blob_state(&self, blob_id: BlobId) -> Result<Option<BlobState>, ViewError> {
        let root_key = RootKey::Blob(blob_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let blob_state = store.read_value::<BlobState>(ONE_KEY).await?;
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
            let root_key = RootKey::Blob(*blob_id).bytes();
            let store = self.database.open_shared(&root_key)?;
            let maybe_blob_state = store.read_value::<BlobState>(ONE_KEY).await?;
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
            let root_key = RootKey::Blob(blob.id()).bytes();
            let store = self.database.open_shared(&root_key)?;
            let has_state = store.contains_key(ONE_KEY).await?;
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

    #[instrument(skip_all, fields(hash = %hash))]
    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        let root_key = RootKey::CryptoHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let results = store.contains_keys(get_01_keys()).await?;
        #[cfg(with_metrics)]
        metrics::CONTAINS_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        Ok(results[0] && results[1])
    }

    #[instrument(skip_all, fields(hash = %hash))]
    async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlockCertificate>, ViewError> {
        let root_key = RootKey::CryptoHash(hash).bytes();
        let store = self.database.open_shared(&root_key)?;
        let values = store.read_multi_values_bytes(get_01_keys()).await?;
        #[cfg(with_metrics)]
        metrics::READ_CERTIFICATE_COUNTER
            .with_label_values(&[])
            .inc();
        Self::deserialize_certificate(&values, hash)
    }

    #[instrument(skip_all)]
    async fn read_certificates<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Option<ConfirmedBlockCertificate>>, ViewError> {
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let root_keys = Self::get_root_keys_for_certificates(&hashes);
        let mut values = Vec::new();
        for root_key in root_keys {
            let store = self.database.open_shared(&root_key)?;
            values.extend(store.read_multi_values_bytes(get_01_keys()).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_CERTIFICATES_COUNTER
            .with_label_values(&[])
            .inc_by(hashes.len() as u64);
        let mut certificates = Vec::new();
        for (pair, hash) in values.chunks_exact(2).zip(hashes) {
            let certificate = Self::deserialize_certificate(pair, hash)?;
            certificates.push(certificate);
        }
        Ok(certificates)
    }

    /// Reads certificates by hashes.
    ///
    /// Returns a vector of tuples where the first element is a lite certificate
    /// and the second element is confirmed block.
    ///
    /// It does not check if all hashes all returned.
    #[instrument(skip_all)]
    async fn read_certificates_raw<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let root_keys = Self::get_root_keys_for_certificates(&hashes);
        let mut values = Vec::new();
        for root_key in root_keys {
            let store = self.database.open_shared(&root_key)?;
            values.extend(store.read_multi_values_bytes(get_01_keys()).await?);
        }
        #[cfg(with_metrics)]
        metrics::READ_CERTIFICATES_COUNTER
            .with_label_values(&[])
            .inc_by(hashes.len() as u64);
        Ok(values
            .chunks_exact(2)
            .filter_map(|chunk| {
                let lite_cert_bytes = chunk[0].as_ref()?;
                let confirmed_block_bytes = chunk[1].as_ref()?;
                Some((lite_cert_bytes.clone(), confirmed_block_bytes.clone()))
            })
            .collect())
    }

    #[instrument(skip_all, fields(event_id = ?event_id))]
    async fn read_event(&self, event_id: EventId) -> Result<Option<Vec<u8>>, ViewError> {
        let event_key = event_key(&event_id);
        let root_key = RootKey::Event(event_id.chain_id).bytes();
        let store = self.database.open_shared(&root_key)?;
        let event = store.read_value_bytes(&event_key).await?;
        #[cfg(with_metrics)]
        metrics::READ_EVENT_COUNTER.with_label_values(&[]).inc();
        Ok(event)
    }

    #[instrument(skip_all, fields(event_id = ?event_id))]
    async fn contains_event(&self, event_id: EventId) -> Result<bool, ViewError> {
        let event_key = event_key(&event_id);
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
        let values = store.read_multi_values_bytes(keys).await?;
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
        let maybe_value = store.read_value(DEFAULT_KEY).await?;
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
}

impl<Database, C> DbStorage<Database, C>
where
    Database: KeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Store: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock,
    Database::Error: Send + Sync,
{
    #[instrument(skip_all)]
    fn get_root_keys_for_certificates(hashes: &[CryptoHash]) -> Vec<Vec<u8>> {
        hashes
            .iter()
            .map(|hash| RootKey::CryptoHash(*hash).bytes())
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
        key: Vec<u8>,
        bytes: Vec<u8>,
    ) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.put_key_value_bytes(key, bytes);
        store.write_batch(batch).await?;
        Ok(())
    }

    #[instrument(skip_all, fields(batch_size = batch.key_value_bytes.len()))]
    async fn write_batch(&self, batch: MultiPartitionBatch) -> Result<(), ViewError> {
        if batch.keys_value_bytes.is_empty() {
            return Ok(());
        }
        let mut futures = Vec::new();
        for (root_key, key, bytes) in batch.keys_value_bytes {
            let store = self.database.open_shared(&root_key)?;
            futures.push(async move { Self::write_entry(&store, key, bytes).await });
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
            wasm_runtime,
            user_contracts: Arc::new(papaya::HashMap::new()),
            user_services: Arc::new(papaya::HashMap::new()),
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        }
    }
}

impl<Database> DbStorage<Database, WallClock>
where
    Database: KeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Error: Send + Sync,
    Database::Store: KeyValueStore + Clone + Send + Sync + 'static,
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

    /// Lists the blob IDs of the storage.
    pub async fn list_blob_ids(
        config: &Database::Config,
        namespace: &str,
    ) -> Result<Vec<BlobId>, ViewError> {
        let root_keys = Database::list_root_keys(config, namespace).await?;
        let mut blob_ids = Vec::new();
        for root_key in root_keys {
            if root_key.len() == 1 + BLOB_ID_LENGTH && root_key[0] == BLOB_ID_TAG {
                let root_key_red = &root_key[1..=BLOB_ID_LENGTH];
                let blob_id = bcs::from_bytes(root_key_red)?;
                blob_ids.push(blob_id);
            }
        }
        Ok(blob_ids)
    }
}

impl<Database> DbStorage<Database, WallClock>
where
    Database: KeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Error: Send + Sync,
{
    /// Lists the chain IDs of the storage.
    pub async fn list_chain_ids(
        config: &Database::Config,
        namespace: &str,
    ) -> Result<Vec<ChainId>, ViewError> {
        let root_keys = Database::list_root_keys(config, namespace).await?;
        let mut chain_ids = Vec::new();
        for root_key in root_keys {
            if root_key.len() == 1 + CHAIN_ID_LENGTH && root_key[0] == CHAIN_ID_TAG {
                let root_key_red = &root_key[1..=CHAIN_ID_LENGTH];
                let chain_id = bcs::from_bytes(root_key_red)?;
                chain_ids.push(chain_id);
            }
        }
        Ok(chain_ids)
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
