// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    crypto::CryptoHash,
    data_types::{TimeDelta, Timestamp},
    identifiers::ChainId,
};
use linera_chain::{
    data_types::{Certificate, CertificateValue, HashedValue, LiteCertificate},
    ChainStateView,
};
use linera_execution::{
    ExecutionRuntimeConfig, UserApplicationId, UserContractCode, UserServiceCode, WasmRuntime,
};
use linera_views::{
    batch::Batch,
    common::{AdminKeyValueStore, ContextFromStore, KeyValueStore},
    value_splitting::DatabaseConsistencyError,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
#[cfg(with_testing)]
use {
    futures::channel::oneshot::{self, Receiver},
    std::{cmp::Reverse, collections::BTreeMap},
};
#[cfg(with_metrics)]
use {
    linera_base::{
        prometheus_util::{self, MeasureLatency},
        sync::Lazy,
    },
    prometheus::{HistogramVec, IntCounterVec},
};

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, Storage};

/// The metric counting how often a value is read from storage.
#[cfg(with_metrics)]
static CONTAINS_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "contains_value",
        "The metric counting how often a value is tested for existence from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a value is read from storage.
#[cfg(with_metrics)]
static CONTAINS_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "contains_certificate",
        "The metric counting how often a certificate is tested for existence from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a value is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "read_value",
        "The metric counting how often a value is read from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a value is written to storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static WRITE_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "write_value",
        "The metric counting how often a value is written to storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a certificate is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static READ_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "read_certificate",
        "The metric counting how often a certificate is read from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a certificate is written to storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static WRITE_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "write_certificate",
        "The metric counting how often a certificate is written to storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The latency to load a chain state.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static LOAD_CHAIN_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "load_chain_latency",
        "The latency to load a chain state",
        &[],
        Some(vec![
            0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]),
    )
    .expect("Histogram creation should not fail")
});

/// A storage implemented from a [`KeyValueStore`]
pub struct DbStorageInner<Client> {
    client: Client,
    pub(crate) guards: ChainGuards,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    wasm_runtime: Option<WasmRuntime>,
}

impl<Client> DbStorageInner<Client>
where
    Client: KeyValueStore
        + AdminKeyValueStore<Error = <Client as KeyValueStore>::Error>
        + Clone
        + Send
        + Sync
        + 'static,
    ViewError: From<<Client as KeyValueStore>::Error>,
    <Client as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    pub(crate) fn new(client: Client, wasm_runtime: Option<WasmRuntime>) -> Self {
        Self {
            client,
            guards: ChainGuards::default(),
            user_contracts: Arc::new(DashMap::new()),
            user_services: Arc::new(DashMap::new()),
            wasm_runtime,
        }
    }

    #[cfg(with_testing)]
    pub async fn new_for_testing(
        store_config: Client::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, <Client as KeyValueStore>::Error> {
        let client = Client::recreate_and_connect(&store_config, namespace).await?;
        let storage = Self::new(client, wasm_runtime);
        Ok(storage)
    }

    pub async fn initialize(
        store_config: Client::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, <Client as KeyValueStore>::Error> {
        let store = Client::maybe_create_and_connect(&store_config, namespace).await?;
        let storage = Self::new(store, wasm_runtime);
        Ok(storage)
    }

    pub async fn make(
        store_config: Client::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, <Client as KeyValueStore>::Error> {
        let client = Client::connect(&store_config, namespace).await?;
        let storage = Self::new(client, wasm_runtime);
        Ok(storage)
    }
}

/// A DbStorage wrapping with Arc
#[derive(Clone)]
pub struct DbStorage<Client, Clock> {
    pub(crate) client: Arc<DbStorageInner<Client>>,
    pub clock: Clock,
    pub execution_runtime_config: ExecutionRuntimeConfig,
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(CryptoHash),
    Value(CryptoHash),
}

/// A clock that can be used to get the current `Timestamp`.
#[async_trait]
pub trait Clock {
    fn current_time(&self) -> Timestamp;

    async fn sleep(&self, delta: TimeDelta);

    async fn sleep_until(&self, timestamp: Timestamp);
}

/// A `Clock` implementation using the system clock.
#[derive(Clone)]
pub struct WallClock;

#[async_trait]
impl Clock for WallClock {
    fn current_time(&self) -> Timestamp {
        Timestamp::now()
    }

    async fn sleep(&self, delta: TimeDelta) {
        tokio::time::sleep(delta.as_duration()).await
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
#[async_trait]
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

    fn lock(&self) -> std::sync::MutexGuard<TestClockInner> {
        self.0.lock().expect("poisoned TestClock mutex")
    }
}

#[async_trait]
impl<Client, C> Storage for DbStorage<Client, C>
where
    Client: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
    ViewError: From<<Client as KeyValueStore>::Error>,
    <Client as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    type Context = ContextFromStore<ChainRuntimeContext<Self>, Client>;
    type ContextError = <Client as KeyValueStore>::Error;

    fn clock(&self) -> &dyn Clock {
        &self.clock
    }

    async fn load_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, ViewError> {
        #[cfg(with_metrics)]
        let _metric = LOAD_CHAIN_LATENCY.measure_latency();
        tracing::trace!("Acquiring lock on {:?}", chain_id);
        let guard = self.client.guards.guard(chain_id).await;
        let runtime_context = ChainRuntimeContext {
            storage: self.clone(),
            chain_id,
            execution_runtime_config: self.execution_runtime_config,
            user_contracts: self.client.user_contracts.clone(),
            user_services: self.client.user_services.clone(),
            _chain_guard: Arc::new(guard),
        };
        let client = self.client.client.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(chain_id))?;
        let context = ContextFromStore::create(client, base_key, runtime_context).await?;
        ChainStateView::load(context).await
    }

    async fn contains_value(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let test = self.client.client.contains_key(&value_key).await?;
        #[cfg(with_metrics)]
        CONTAINS_VALUE_COUNTER.with_label_values(&[]).inc();
        Ok(test)
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_value = self
            .client
            .client
            .read_value::<CertificateValue>(&value_key)
            .await?;
        #[cfg(with_metrics)]
        READ_VALUE_COUNTER.with_label_values(&[]).inc();
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
    }

    async fn read_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedValue>, ViewError> {
        let mut hash = Some(from);
        let mut values = Vec::new();
        for _ in 0..limit {
            let Some(next_hash) = hash else {
                break;
            };
            let value = self.read_value(next_hash).await?;
            let Some(executed_block) = value.inner().executed_block() else {
                break;
            };
            hash = executed_block.block.previous_block_hash;
            values.push(value);
        }
        Ok(values)
    }

    async fn write_value(&self, value: &HashedValue) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        self.add_value_to_batch(value, &mut batch)?;
        self.write_batch(batch).await
    }

    async fn write_values(&self, values: &[HashedValue]) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        for value in values {
            self.add_value_to_batch(value, &mut batch)?;
        }
        self.write_batch(batch).await
    }

    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let (cert_test, value_test) = tokio::join!(
            self.client.client.contains_key(&cert_key),
            self.client.client.contains_key(&value_key)
        );
        #[cfg(with_metrics)]
        CONTAINS_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        Ok(cert_test? && value_test?)
    }

    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError> {
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let (cert_result, value_result) = tokio::join!(
            self.client.client.read_value::<LiteCertificate>(&cert_key),
            self.client
                .client
                .read_value::<CertificateValue>(&value_key)
        );
        if value_result.is_ok() {
            #[cfg(with_metrics)]
            READ_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        }
        let value: CertificateValue =
            value_result?.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        let cert: LiteCertificate =
            cert_result?.ok_or_else(|| ViewError::not_found("certificate for hash", hash))?;
        Ok(cert
            .with_value(value.with_hash_unchecked(hash))
            .ok_or(ViewError::InconsistentEntries)?)
    }

    async fn write_certificate(&self, certificate: &Certificate) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        self.add_certificate_to_batch(certificate, &mut batch)?;
        self.write_batch(batch).await
    }

    async fn write_certificates(&self, certificates: &[Certificate]) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        for certificate in certificates {
            self.add_certificate_to_batch(certificate, &mut batch)?;
        }
        self.write_batch(batch).await
    }

    fn wasm_runtime(&self) -> Option<WasmRuntime> {
        self.client.wasm_runtime
    }
}

impl<Client, C> DbStorage<Client, C>
where
    Client: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock,
    ViewError: From<<Client as KeyValueStore>::Error>,
    <Client as KeyValueStore>::Error: From<bcs::Error> + Send + Sync + serde::ser::StdError,
{
    fn add_value_to_batch(&self, value: &HashedValue, batch: &mut Batch) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        WRITE_VALUE_COUNTER.with_label_values(&[]).inc();
        let value_key = bcs::to_bytes(&BaseKey::Value(value.hash()))?;
        batch.put_key_value(value_key.to_vec(), value)?;
        Ok(())
    }

    fn add_certificate_to_batch(
        &self,
        certificate: &Certificate,
        batch: &mut Batch,
    ) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        WRITE_CERTIFICATE_COUNTER.with_label_values(&[]).inc();
        let hash = certificate.hash();
        let cert_key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        batch.put_key_value(cert_key.to_vec(), &certificate.lite_certificate())?;
        batch.put_key_value(value_key.to_vec(), &certificate.value)?;
        Ok(())
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.client.client.write_batch(batch, &[]).await?;
        Ok(())
    }

    pub fn create(storage: DbStorageInner<Client>, clock: C) -> Self {
        Self {
            client: Arc::new(storage),
            clock,
            execution_runtime_config: ExecutionRuntimeConfig::default(),
        }
    }
}

impl<Client> DbStorage<Client, WallClock>
where
    Client: KeyValueStore
        + AdminKeyValueStore<Error = <Client as KeyValueStore>::Error>
        + Clone
        + Send
        + Sync
        + 'static,
    ViewError: From<<Client as KeyValueStore>::Error>,
    <Client as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    pub async fn initialize(
        store_config: Client::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, <Client as KeyValueStore>::Error> {
        let storage =
            DbStorageInner::<Client>::initialize(store_config, namespace, wasm_runtime).await?;
        Ok(Self::create(storage, WallClock))
    }

    pub async fn new(
        store_config: Client::Config,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
    ) -> Result<Self, <Client as KeyValueStore>::Error> {
        let storage = DbStorageInner::<Client>::make(store_config, namespace, wasm_runtime).await?;
        Ok(Self::create(storage, WallClock))
    }
}
