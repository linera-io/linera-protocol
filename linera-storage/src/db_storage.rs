// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{chain_guards::ChainGuards, ChainRuntimeContext, Storage};
use async_trait::async_trait;
use dashmap::DashMap;
use linera_base::{
    crypto::CryptoHash,
    data_types::Timestamp,
    identifiers::ChainId,
    prometheus_util::{self, MeasureLatency},
    sync::Lazy,
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
    common::{ContextFromStore, KeyValueStore},
    value_splitting::DatabaseConsistencyError,
    views::{View, ViewError},
};
use prometheus::{HistogramVec, IntCounterVec};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

/// The metric counting how often a value is read from storage.
static CONTAINS_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "contains_value",
        "The metric counting how often a value is tested for existence from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a value is read from storage.
static CONTAINS_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "contains_certificate",
        "The metric counting how often a certificate is tested for existence from storage",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// The metric counting how often a value is read from storage.
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

impl<Client> DbStorageInner<Client> {
    pub(crate) fn new(client: Client, wasm_runtime: Option<WasmRuntime>) -> Self {
        Self {
            client,
            guards: ChainGuards::default(),
            user_contracts: Arc::new(DashMap::new()),
            user_services: Arc::new(DashMap::new()),
            wasm_runtime,
        }
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
pub trait Clock {
    fn current_time(&self) -> Timestamp;
}

/// A `Clock` implementation using the system clock.
#[derive(Clone)]
pub struct WallClock;

impl Clock for WallClock {
    fn current_time(&self) -> Timestamp {
        Timestamp::now()
    }
}

/// A clock implementation that uses a stored number of microseconds and that can be updated
/// explicitly. All clones share the same time, and setting it in one clone updates all the others.
#[cfg(any(test, feature = "test"))]
#[derive(Clone, Default)]
pub struct TestClock(Arc<std::sync::atomic::AtomicU64>);

#[cfg(any(test, feature = "test"))]
impl Clock for TestClock {
    fn current_time(&self) -> Timestamp {
        Timestamp::from(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}

#[cfg(any(test, feature = "test"))]
impl TestClock {
    /// Creates a new clock with its time set to 0, i.e. the Unix epoch.
    pub fn new() -> Self {
        TestClock(Arc::new(0.into()))
    }

    /// Sets the current time.
    pub fn set(&self, timestamp: Timestamp) {
        self.0
            .store(timestamp.micros(), std::sync::atomic::Ordering::SeqCst);
    }

    /// Advances the current time by the specified number of microseconds.
    pub fn add_micros(&self, micros: u64) {
        self.0
            .fetch_add(micros, std::sync::atomic::Ordering::SeqCst);
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

    fn current_time(&self) -> Timestamp {
        self.clock.current_time()
    }

    async fn load_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, ViewError> {
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
}
