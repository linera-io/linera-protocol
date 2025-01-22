// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, TimeDelta, Timestamp},
    hashed::Hashed,
    identifiers::{BlobId, ChainId, UserApplicationId},
};
use linera_chain::{
    types::{ConfirmedBlock, ConfirmedBlockCertificate, LiteCertificate},
    ChainStateView,
};
use linera_execution::{
    committee::Epoch, BlobState, ExecutionRuntimeConfig, UserContractCode, UserServiceCode,
    WasmRuntime,
};
use linera_views::{
    backends::dual::{DualStoreRootKeyAssignment, StoreInUse},
    batch::Batch,
    context::ViewContext,
    store::KeyValueStore,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
#[cfg(with_testing)]
use {
    futures::channel::oneshot::{self, Receiver},
    linera_views::{random::generate_test_namespace, store::TestKeyValueStore},
    std::{cmp::Reverse, collections::BTreeMap},
};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        bucket_latencies, register_histogram_vec, register_int_counter_vec, MeasureLatency,
    },
    prometheus::{HistogramVec, IntCounterVec},
};

use crate::{ChainRuntimeContext, Clock, Storage};

/// The metric counting how often a blob is tested for existence from storage
#[cfg(with_metrics)]
static CONTAINS_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "contains_blob",
        "The metric counting how often a blob is tested for existence from storage",
        &[],
    )
});

/// The metric counting how often multiple blobs are tested for existence from storage
#[cfg(with_metrics)]
static CONTAINS_BLOBS_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "contains_blobs",
        "The metric counting how often multiple blobs are tested for existence from storage",
        &[],
    )
});

/// The metric counting how often a blob state is tested for existence from storage
#[cfg(with_metrics)]
static CONTAINS_BLOB_STATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "contains_blob_state",
        "The metric counting how often a blob state is tested for existence from storage",
        &[],
    )
});

/// The metric counting how often a certificate is tested for existence from storage.
#[cfg(with_metrics)]
static CONTAINS_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "contains_certificate",
        "The metric counting how often a certificate is tested for existence from storage",
        &[],
    )
});

/// The metric counting how often a hashed certificate value is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_HASHED_CONFIRMED_BLOCK_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_hashed_confirmed_block",
        "The metric counting how often a hashed confirmed block is read from storage",
        &[],
    )
});

/// The metric counting how often a blob is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_blob",
        "The metric counting how often a blob is read from storage",
        &[],
    )
});

/// The metric counting how often a blob state is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_BLOB_STATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_blob_state",
        "The metric counting how often a blob state is read from storage",
        &[],
    )
});

/// The metric counting how often blob states are read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_BLOB_STATES_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_blob_states",
        "The metric counting how often blob states are read from storage",
        &[],
    )
});

/// The metric counting how often a blob is written to storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static WRITE_BLOB_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "write_blob",
        "The metric counting how often a blob is written to storage",
        &[],
    )
});

/// The metric counting how often a certificate is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_certificate",
        "The metric counting how often a certificate is read from storage",
        &[],
    )
});

/// The metric counting how often certificates are read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_CERTIFICATES_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "read_certificates",
        "The metric counting how often certificate are read from storage",
        &[],
    )
});

/// The metric counting how often a certificate is written to storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static WRITE_CERTIFICATE_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "write_certificate",
        "The metric counting how often a certificate is written to storage",
        &[],
    )
});

/// The latency to load a chain state.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static LOAD_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "load_chain_latency",
        "The latency to load a chain state",
        &[],
        bucket_latencies(1.0),
    )
});

trait BatchExt {
    fn add_blob(&mut self, blob: &Blob) -> Result<(), ViewError>;

    fn add_blob_state(&mut self, blob_id: BlobId, blob_state: &BlobState) -> Result<(), ViewError>;

    fn add_certificate(&mut self, certificate: &ConfirmedBlockCertificate)
        -> Result<(), ViewError>;
}

impl BatchExt for Batch {
    fn add_blob(&mut self, blob: &Blob) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        WRITE_BLOB_COUNTER.with_label_values(&[]).inc();
        let blob_key = bcs::to_bytes(&BaseKey::Blob(blob.id()))?;
        self.put_key_value(blob_key.to_vec(), &blob.bytes())?;
        Ok(())
    }

    fn add_blob_state(&mut self, blob_id: BlobId, blob_state: &BlobState) -> Result<(), ViewError> {
        let blob_state_key = bcs::to_bytes(&BaseKey::BlobState(blob_id))?;
        self.put_key_value(blob_state_key.to_vec(), blob_state)?;
        Ok(())
    }

    fn add_certificate(
        &mut self,
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        WRITE_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        let hash = certificate.hash();
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::ConfirmedBlock(hash))?;
        self.put_key_value(cert_key.to_vec(), &certificate.lite_certificate())?;
        self.put_key_value(value_key.to_vec(), certificate.value())?;
        Ok(())
    }
}

/// Main implementation of the [`Storage`] trait.
#[derive(Clone)]
pub struct DbStorage<Store, Clock> {
    store: Arc<Store>,
    clock: Clock,
    wasm_runtime: Option<WasmRuntime>,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    execution_runtime_config: ExecutionRuntimeConfig,
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(CryptoHash),
    ConfirmedBlock(CryptoHash),
    Blob(BlobId),
    BlobState(BlobId),
}

/// An implementation of [`DualStoreRootKeyAssignment`] that stores the
/// chain states into the first store.
pub struct ChainStatesFirstAssignment;

impl DualStoreRootKeyAssignment for ChainStatesFirstAssignment {
    fn assigned_store(root_key: &[u8]) -> Result<StoreInUse, bcs::Error> {
        let store = match bcs::from_bytes(root_key)? {
            BaseKey::ChainState(_) => StoreInUse::First,
            _ => StoreInUse::Second,
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
impl<Store, C> Storage for DbStorage<Store, C>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
    Store::Error: Send + Sync,
{
    type Context = ViewContext<ChainRuntimeContext<Self>, Store>;
    type Clock = C;

    fn clock(&self) -> &C {
        &self.clock
    }

    async fn load_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, ViewError> {
        #[cfg(with_metrics)]
        let _metric = LOAD_CHAIN_LATENCY.measure_latency();
        let runtime_context = ChainRuntimeContext {
            storage: self.clone(),
            chain_id,
            execution_runtime_config: self.execution_runtime_config,
            user_contracts: self.user_contracts.clone(),
            user_services: self.user_services.clone(),
        };
        let root_key = bcs::to_bytes(&BaseKey::ChainState(chain_id))?;
        let store = self.store.clone_with_root_key(&root_key)?;
        let context = ViewContext::create_root_context(store, runtime_context).await?;
        ChainStateView::load(context).await
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let blob_key = bcs::to_bytes(&BaseKey::Blob(blob_id))?;
        let test = self.store.contains_key(&blob_key).await?;
        #[cfg(with_metrics)]
        CONTAINS_BLOB_COUNTER.with_label_values(&[]).inc();
        Ok(test)
    }

    async fn missing_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<BlobId>, ViewError> {
        let mut keys = Vec::new();
        for blob_id in blob_ids {
            let key = bcs::to_bytes(&BaseKey::Blob(*blob_id))?;
            keys.push(key);
        }
        let results = self.store.contains_keys(keys).await?;
        let mut missing_blobs = Vec::new();
        for (blob_id, result) in blob_ids.iter().zip(results) {
            if !result {
                missing_blobs.push(*blob_id);
            }
        }
        #[cfg(with_metrics)]
        CONTAINS_BLOBS_COUNTER.with_label_values(&[]).inc();
        Ok(missing_blobs)
    }

    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let blob_key = bcs::to_bytes(&BaseKey::BlobState(blob_id))?;
        let test = self.store.contains_key(&blob_key).await?;
        #[cfg(with_metrics)]
        CONTAINS_BLOB_STATE_COUNTER.with_label_values(&[]).inc();
        Ok(test)
    }

    async fn read_hashed_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Hashed<ConfirmedBlock>, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::ConfirmedBlock(hash))?;
        let maybe_value = self.store.read_value::<ConfirmedBlock>(&value_key).await?;
        #[cfg(with_metrics)]
        READ_HASHED_CONFIRMED_BLOCK_COUNTER
            .with_label_values(&[])
            .inc();
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
    }

    async fn read_blob(&self, blob_id: BlobId) -> Result<Blob, ViewError> {
        let blob_key = bcs::to_bytes(&BaseKey::Blob(blob_id))?;
        let maybe_blob_bytes = self.store.read_value::<Vec<u8>>(&blob_key).await?;
        #[cfg(with_metrics)]
        READ_BLOB_COUNTER.with_label_values(&[]).inc();
        let blob_bytes = maybe_blob_bytes.ok_or_else(|| ViewError::BlobsNotFound(vec![blob_id]))?;
        Ok(Blob::new_with_id_unchecked(blob_id, blob_bytes))
    }

    async fn read_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Option<Blob>>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let blob_keys = blob_ids
            .iter()
            .map(|blob_id| bcs::to_bytes(&BaseKey::Blob(*blob_id)))
            .collect::<Result<Vec<_>, _>>()?;
        let maybe_blob_bytes = self.store.read_multi_values::<Vec<u8>>(blob_keys).await?;
        #[cfg(with_metrics)]
        READ_BLOB_COUNTER
            .with_label_values(&[])
            .inc_by(blob_ids.len() as u64);

        Ok(blob_ids
            .iter()
            .zip(maybe_blob_bytes)
            .map(|(blob_id, maybe_blob_bytes)| {
                maybe_blob_bytes.map(|blob_bytes| Blob::new_with_id_unchecked(*blob_id, blob_bytes))
            })
            .collect())
    }

    async fn read_blob_state(&self, blob_id: BlobId) -> Result<BlobState, ViewError> {
        let blob_state_key = bcs::to_bytes(&BaseKey::BlobState(blob_id))?;
        let maybe_blob_state = self.store.read_value::<BlobState>(&blob_state_key).await?;
        #[cfg(with_metrics)]
        READ_BLOB_STATE_COUNTER.with_label_values(&[]).inc();
        let blob_state = maybe_blob_state
            .ok_or_else(|| ViewError::not_found("blob state for blob ID", blob_id))?;
        Ok(blob_state)
    }

    async fn read_blob_states(&self, blob_ids: &[BlobId]) -> Result<Vec<BlobState>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let blob_state_keys = blob_ids
            .iter()
            .map(|blob_id| bcs::to_bytes(&BaseKey::BlobState(*blob_id)))
            .collect::<Result<_, _>>()?;
        let maybe_blob_states = self
            .store
            .read_multi_values::<BlobState>(blob_state_keys)
            .await?;
        #[cfg(with_metrics)]
        READ_BLOB_STATES_COUNTER.with_label_values(&[]).inc();
        let blob_states = maybe_blob_states
            .into_iter()
            .zip(blob_ids)
            .map(|(blob_state, blob_id)| {
                blob_state.ok_or_else(|| ViewError::not_found("blob state for blob ID", blob_id))
            })
            .collect::<Result<_, _>>()?;
        Ok(blob_states)
    }

    async fn read_hashed_confirmed_blocks_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<Hashed<ConfirmedBlock>>, ViewError> {
        let mut hash = Some(from);
        let mut values = Vec::new();
        for _ in 0..limit {
            let Some(next_hash) = hash else {
                break;
            };
            let value = self.read_hashed_confirmed_block(next_hash).await?;
            hash = value.inner().block().header.previous_block_hash;
            values.push(value);
        }
        Ok(values)
    }

    async fn write_blob(&self, blob: &Blob) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.add_blob(blob)?;
        self.write_batch(batch).await?;
        Ok(())
    }

    async fn maybe_write_blob_state(
        &self,
        blob_id: BlobId,
        blob_state: BlobState,
    ) -> Result<Epoch, ViewError> {
        let current_blob_state = self.read_blob_state(blob_id).await;
        let (should_write, latest_epoch) = match current_blob_state {
            Ok(current_blob_state) => (
                current_blob_state.epoch < blob_state.epoch,
                current_blob_state.epoch.max(blob_state.epoch),
            ),
            Err(ViewError::NotFound(_)) => (true, blob_state.epoch),
            Err(err) => return Err(err),
        };

        if should_write {
            self.write_blob_state(blob_id, &blob_state).await?;
        }

        Ok(latest_epoch)
    }

    async fn maybe_write_blob_states(
        &self,
        blob_ids: &[BlobId],
        blob_state: BlobState,
        overwrite: bool,
    ) -> Result<Vec<Epoch>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let blob_state_keys = blob_ids
            .iter()
            .map(|blob_id| bcs::to_bytes(&BaseKey::BlobState(*blob_id)))
            .collect::<Result<_, _>>()?;
        let maybe_blob_states = self
            .store
            .read_multi_values::<BlobState>(blob_state_keys)
            .await?;
        let mut latest_epoches = Vec::new();
        let mut batch = Batch::new();
        let mut need_write = false;
        for (maybe_blob_state, blob_id) in maybe_blob_states.iter().zip(blob_ids) {
            let (should_write, latest_epoch) = match maybe_blob_state {
                None => (true, blob_state.epoch),
                Some(current_blob_state) => (
                    overwrite && current_blob_state.epoch < blob_state.epoch,
                    current_blob_state.epoch.max(blob_state.epoch),
                ),
            };
            if should_write {
                batch.add_blob_state(*blob_id, &blob_state)?;
                need_write = true;
            }
            latest_epoches.push(latest_epoch);
        }
        if need_write {
            self.write_batch(batch).await?;
        }
        Ok(latest_epoches)
    }

    async fn write_blob_state(
        &self,
        blob_id: BlobId,
        blob_state: &BlobState,
    ) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.add_blob_state(blob_id, blob_state)?;
        self.write_batch(batch).await?;
        Ok(())
    }

    async fn maybe_write_blobs(&self, blobs: &[Blob]) -> Result<Vec<bool>, ViewError> {
        if blobs.is_empty() {
            return Ok(Vec::new());
        }
        let blob_state_keys = blobs
            .iter()
            .map(|blob| bcs::to_bytes(&BaseKey::BlobState(blob.id())))
            .collect::<Result<_, _>>()?;
        let blob_states = self.store.contains_keys(blob_state_keys).await?;
        let mut batch = Batch::new();
        for (blob, has_state) in blobs.iter().zip(&blob_states) {
            if *has_state {
                batch.add_blob(blob)?;
            }
        }
        self.write_batch(batch).await?;
        Ok(blob_states)
    }

    async fn write_blobs(&self, blobs: &[Blob]) -> Result<(), ViewError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let mut batch = Batch::new();
        for blob in blobs {
            batch.add_blob(blob)?;
        }
        self.write_batch(batch).await
    }

    async fn write_blobs_and_certificate(
        &self,
        blobs: &[Blob],
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        for blob in blobs {
            batch.add_blob(blob)?;
        }
        batch.add_certificate(certificate)?;
        self.write_batch(batch).await
    }

    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        let keys = Self::get_keys_for_certificates(&[hash])?;
        let results = self.store.contains_keys(keys).await?;
        #[cfg(with_metrics)]
        CONTAINS_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        Ok(results[0] && results[1])
    }

    async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, ViewError> {
        let keys = Self::get_keys_for_certificates(&[hash])?;
        let values = self.store.read_multi_values_bytes(keys).await;
        if values.is_ok() {
            #[cfg(with_metrics)]
            READ_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        }
        let values = values?;
        Self::deserialize_certificate(&values, hash)
    }

    async fn read_certificates<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<ConfirmedBlockCertificate>, ViewError> {
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let keys = Self::get_keys_for_certificates(&hashes)?;
        let values = self.store.read_multi_values_bytes(keys).await;
        if values.is_ok() {
            #[cfg(with_metrics)]
            READ_CERTIFICATES_COUNTER.with_label_values(&[]).inc();
        }
        let values = values?;
        let mut certificates = Vec::new();
        for (pair, hash) in values.chunks_exact(2).zip(hashes) {
            let certificate = Self::deserialize_certificate(pair, hash)?;
            certificates.push(certificate);
        }
        Ok(certificates)
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.wasm_runtime
    }
}

impl<Store, C> DbStorage<Store, C>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock,
    Store::Error: Send + Sync,
{
    fn get_keys_for_certificates(hashes: &[CryptoHash]) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(hashes
            .iter()
            .flat_map(|hash| {
                let cert_key = bcs::to_bytes(&BaseKey::Certificate(*hash));
                let value_key = bcs::to_bytes(&BaseKey::ConfirmedBlock(*hash));
                vec![cert_key, value_key]
            })
            .collect::<Result<_, _>>()?)
    }

    fn deserialize_certificate(
        pair: &[Option<Vec<u8>>],
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, ViewError> {
        let cert_bytes = pair[0]
            .as_ref()
            .ok_or_else(|| ViewError::not_found("certificate bytes for hash", hash))?;
        let value_bytes = pair[1]
            .as_ref()
            .ok_or_else(|| ViewError::not_found("value bytes for hash", hash))?;
        let cert = bcs::from_bytes::<LiteCertificate>(cert_bytes)?;
        let value = bcs::from_bytes::<ConfirmedBlock>(value_bytes)?;
        let certificate = cert
            .with_value(value.with_hash_unchecked(hash))
            .ok_or(ViewError::InconsistentEntries)?;
        Ok(certificate)
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.store.write_batch(batch).await?;
        Ok(())
    }

    fn create(store: Store, wasm_runtime: Option<WasmRuntime>, clock: C) -> Self {
        Self {
            store: Arc::new(store),
            clock,
            wasm_runtime,
            user_contracts: Arc::new(DashMap::new()),
            user_services: Arc::new(DashMap::new()),
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        }
    }
}

impl<Store> DbStorage<Store, WallClock>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Send + Sync,
{
    pub async fn initialize(
        config: Store::Config,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, Store::Error> {
        let store = Store::maybe_create_and_connect(&config, namespace, root_key).await?;
        Ok(Self::create(store, wasm_runtime, WallClock))
    }

    pub async fn new(
        config: Store::Config,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, Store::Error> {
        let store = Store::connect(&config, namespace, root_key).await?;
        Ok(Self::create(store, wasm_runtime, WallClock))
    }
}

#[cfg(with_testing)]
impl<Store> DbStorage<Store, TestClock>
where
    Store: TestKeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Send + Sync,
{
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let config = Store::new_test_config().await.unwrap();
        let namespace = generate_test_namespace();
        let root_key = &[];
        DbStorage::<Store, TestClock>::new_for_testing(
            config,
            &namespace,
            root_key,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .unwrap()
    }

    pub async fn new_for_testing(
        config: Store::Config,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, Store::Error> {
        let store = Store::recreate_and_connect(&config, namespace, root_key).await?;
        Ok(Self::create(store, wasm_runtime, clock))
    }
}
