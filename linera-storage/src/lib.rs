// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

mod chain_guards;
#[cfg(feature = "aws")]
mod dynamo_db;
mod memory;
#[cfg(feature = "rocksdb")]
mod rocks_db;
#[cfg(feature = "scylladb")]
mod scylla_db;

#[cfg(feature = "aws")]
pub use crate::dynamo_db::DynamoDbStore;
pub use crate::memory::MemoryStoreClient;
#[cfg(feature = "rocksdb")]
pub use crate::rocks_db::RocksDbStore;
#[cfg(feature = "scylladb")]
pub use crate::scylla_db::ScyllaDbStore;

use crate::chain_guards::ChainGuards;
use async_trait::async_trait;
use chain_guards::ChainGuard;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::future;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_chain::{
    data_types::{Certificate, CertificateValue, HashedValue, LiteCertificate},
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    ChainOwnership, ExecutionError, ExecutionRuntimeContext, UserApplicationDescription,
    UserApplicationId, UserContractCode, UserServiceCode, WasmRuntime,
};
use linera_views::{
    batch::Batch,
    common::{Context, ContextFromDb, KeyValueStoreClient},
    value_splitting::DatabaseConsistencyError,
    views::{CryptoHashView, RootView, View, ViewError},
};
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

/// The metric counting how often a value is read from storage.
pub static READ_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "read_value",
        "The metric counting how often a value is read from storage",
        &[]
    )
    .expect("Counter creation should not fail")
});
/// The metric counting how often a value is written to storage.
pub static WRITE_VALUE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "write_value",
        "The metric counting how often a value is written to storage",
        &[]
    )
    .expect("Counter creation should not fail")
});
/// The metric counting how often a certificate is read from storage.
pub static READ_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "read_certificate",
        "The metric counting how often a certificate is read from storage",
        &[]
    )
    .expect("Counter creation should not fail")
});
/// The metric counting how often a certificate is written to storage.
pub static WRITE_CERTIFICATE_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "write_certificate",
        "The metric counting how often a certificate is written to storage",
        &[]
    )
    .expect("Counter creation should not fail")
});

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use linera_execution::{Operation, SystemOperation, WasmContract, WasmService};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Store: Sized {
    /// The low-level storage implementation in use.
    type Context: Context<Extra = ChainRuntimeContext<Self>, Error = Self::ContextError>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Alias to provide simpler trait bounds `ViewError: From<Self::ContextError>`
    type ContextError: std::error::Error + Debug + Sync + Send;

    /// Returns the current wall clock time.
    fn current_time(&self) -> Timestamp;

    /// Loads the view of a chain state.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Reads the value with the given hash.
    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError>;

    /// Reads the values in descending order from the given hash.
    async fn read_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedValue>, ViewError>;

    /// Writes the given value.
    async fn write_value(&self, value: &HashedValue) -> Result<(), ViewError>;

    /// Writes several values
    async fn write_values(&self, values: &[HashedValue]) -> Result<(), ViewError>;

    /// Reads the certificate with the given hash.
    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError>;

    /// Writes the given certificate.
    async fn write_certificate(&self, certificate: &Certificate) -> Result<(), ViewError>;

    /// Writes a vector of certificates.
    async fn write_certificates(&self, certificate: &[Certificate]) -> Result<(), ViewError>;

    /// Loads the view of a chain state and checks that it is active.
    async fn load_active_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, linera_chain::ChainError>
    where
        ChainRuntimeContext<Self>: ExecutionRuntimeContext,
        ViewError: From<Self::ContextError>,
    {
        let chain = self.load_chain(id).await?;
        chain.ensure_is_active()?;
        Ok(chain)
    }

    /// Reads a number of certificates in parallel.
    async fn read_certificates<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        keys: I,
    ) -> Result<Vec<Certificate>, ViewError>
    where
        Self: Clone + Send + 'static,
    {
        let mut tasks = Vec::new();
        for key in keys {
            // TODO: remove clone using scoped threads
            let client = self.clone();
            tasks.push(tokio::task::spawn(async move {
                client.read_certificate(key).await
            }));
        }
        let results = future::join_all(tasks).await;
        let mut certs = Vec::new();
        for result in results {
            certs.push(result.expect("storage access should not cancel or crash")?);
        }
        Ok(certs)
    }

    /// Initializes a chain in a simple way (used for testing and to create a genesis state).
    async fn create_chain(
        &self,
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        public_key: PublicKey,
        balance: Amount,
        timestamp: Timestamp,
    ) -> Result<(), ChainError>
    where
        ChainRuntimeContext<Self>: ExecutionRuntimeContext,
        ViewError: From<Self::ContextError>,
    {
        let id = description.into();
        let mut chain = self.load_chain(id).await?;
        assert!(!chain.is_active(), "Attempting to create a chain twice");
        let system_state = &mut chain.execution_state.system;
        system_state.description.set(Some(description));
        system_state.epoch.set(Some(Epoch::ZERO));
        system_state.admin_id.set(Some(admin_id));
        system_state
            .committees
            .get_mut()
            .insert(Epoch::ZERO, committee);
        system_state
            .ownership
            .set(ChainOwnership::single(public_key));
        system_state.balance.set(balance);
        system_state.timestamp.set(timestamp);
        let state_hash = chain.execution_state.crypto_hash().await?;
        chain.execution_state_hash.set(Some(state_hash));
        chain.manager.get_mut().reset(
            &ChainOwnership::single(public_key),
            BlockHeight(0),
            self.current_time(),
        )?;
        chain.save().await?;
        Ok(())
    }

    /// Selects the WebAssembly runtime to use for applications (if any).
    fn wasm_runtime(&self) -> Option<WasmRuntime>;

    /// Creates a [`UserContractCode`] instance using the bytecode in storage referenced
    /// by the `application_description`.
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    async fn load_contract(
        &self,
        application_description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError> {
        let Some(wasm_runtime) = self.wasm_runtime() else {
            panic!("A Wasm runtime is required to load user applications.");
        };
        let SystemOperation::PublishBytecode { contract, .. } =
            read_publish_bytecode_operation(self, application_description).await?
        else {
            unreachable!();
        };
        Ok(Arc::new(WasmContract::new(contract, wasm_runtime).await?))
    }

    #[cfg(not(any(feature = "wasmer", feature = "wasmtime")))]
    #[allow(clippy::diverging_sub_expression)]
    async fn load_contract(
        &self,
        _application_description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError> {
        panic!(
            "A Wasm runtime is required to load user applications. \
            Please enable the `wasmer` or the `wasmtime` feature flags \
            when compiling `linera-storage`."
        );
    }

    /// Creates a [`linera-sdk::UserContract`] instance using the bytecode in storage referenced
    /// by the `application_description`.
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    async fn load_service(
        &self,
        application_description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError> {
        let Some(wasm_runtime) = self.wasm_runtime() else {
            panic!("A Wasm runtime is required to load user applications.");
        };
        let SystemOperation::PublishBytecode { service, .. } =
            read_publish_bytecode_operation(self, application_description).await?
        else {
            unreachable!();
        };
        Ok(Arc::new(WasmService::new(service, wasm_runtime).await?))
    }

    #[cfg(not(any(feature = "wasmer", feature = "wasmtime")))]
    #[allow(clippy::diverging_sub_expression)]
    async fn load_service(
        &self,
        _application_description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError> {
        panic!(
            "A Wasm runtime is required to load user applications. \
            Please enable the `wasmer` or the `wasmtime` feature flags \
            when compiling `linera-storage`."
        );
    }
}

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
async fn read_publish_bytecode_operation(
    store: &impl Store,
    application_description: &UserApplicationDescription,
) -> Result<SystemOperation, ExecutionError> {
    let UserApplicationDescription {
        bytecode_id,
        bytecode_location,
        ..
    } = application_description;
    let value = store
        .read_value(bytecode_location.certificate_hash)
        .await
        .map_err(|error| match error {
            ViewError::NotFound(_) => ExecutionError::ApplicationBytecodeNotFound(Box::new(
                application_description.clone(),
            )),
            _ => error.into(),
        })?
        .into_inner();
    let operations = match value {
        CertificateValue::ConfirmedBlock { executed_block, .. } => executed_block.block.operations,
        _ => return Err(ExecutionError::InvalidBytecodeId(*bytecode_id)),
    };
    let index = usize::try_from(bytecode_location.operation_index)
        .map_err(|_| linera_base::data_types::ArithmeticError::Overflow)?;
    match operations.into_iter().nth(index) {
        Some(Operation::System(operation @ SystemOperation::PublishBytecode { .. })) => {
            Ok(operation)
        }
        _ => Err(ExecutionError::InvalidBytecodeId(*bytecode_id)),
    }
}

/// A store implemented from a [`KeyValueStoreClient`]
pub struct DbStoreInner<Client> {
    client: Client,
    guards: ChainGuards,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    wasm_runtime: Option<WasmRuntime>,
}

impl<Client> DbStoreInner<Client> {
    fn new(client: Client, wasm_runtime: Option<WasmRuntime>) -> Self {
        Self {
            client,
            guards: ChainGuards::default(),
            user_contracts: Arc::new(DashMap::new()),
            user_services: Arc::new(DashMap::new()),
            wasm_runtime,
        }
    }
}

#[derive(Clone)]
/// A DbStore wrapping with Arc
pub struct DbStore<Client, Clock> {
    client: Arc<DbStoreInner<Client>>,
    pub clock: Clock,
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
impl<Client, C> Store for DbStore<Client, C>
where
    Client: KeyValueStoreClient + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
    ViewError: From<<Client as KeyValueStoreClient>::Error>,
    <Client as KeyValueStoreClient>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    type Context = ContextFromDb<ChainRuntimeContext<Self>, Client>;
    type ContextError = <Client as KeyValueStoreClient>::Error;

    fn current_time(&self) -> Timestamp {
        self.clock.current_time()
    }

    async fn load_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, ViewError> {
        tracing::trace!("Acquiring lock on {:?}", chain_id);
        let guard = self.client.guards.guard(chain_id).await;
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id,
            user_contracts: self.client.user_contracts.clone(),
            user_services: self.client.user_services.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let client = self.client.client.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(chain_id))?;
        let context = ContextFromDb::create(client, base_key, runtime_context).await?;
        ChainStateView::load(context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_value: Option<CertificateValue> =
            self.client.client.read_value(&value_key).await?;
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
        let mut result = Vec::new();
        for _ in 0..limit {
            let Some(next_hash) = hash else {
                break;
            };
            let value = self.read_value(next_hash).await?;
            let Some(executed_block) = value.inner().executed_block() else {
                break;
            };
            hash = executed_block.block.previous_block_hash;
            result.push(value);
        }
        Ok(result)
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
        };
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

impl<Client, C> DbStore<Client, C>
where
    Client: KeyValueStoreClient + Clone + Send + Sync + 'static,
    C: Clock,
    ViewError: From<<Client as KeyValueStoreClient>::Error>,
    <Client as KeyValueStoreClient>::Error: From<bcs::Error> + Send + Sync + serde::ser::StdError,
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

#[derive(Clone)]
pub struct ChainRuntimeContext<StoreClient> {
    store: StoreClient,
    pub chain_id: ChainId,
    pub user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    pub user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    pub chain_guard: Option<Arc<ChainGuard>>,
}

#[async_trait]
impl<StoreClient> ExecutionRuntimeContext for ChainRuntimeContext<StoreClient>
where
    StoreClient: Store + Send + Sync,
{
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn user_contracts(&self) -> &Arc<DashMap<UserApplicationId, UserContractCode>> {
        &self.user_contracts
    }

    fn user_services(&self) -> &Arc<DashMap<UserApplicationId, UserServiceCode>> {
        &self.user_services
    }

    async fn get_user_contract(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserContractCode, ExecutionError> {
        match self.user_contracts.entry(description.into()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let contract = self.store.load_contract(description).await?;
                entry.insert(contract.clone());
                Ok(contract)
            }
        }
    }

    async fn get_user_service(
        &self,
        description: &UserApplicationDescription,
    ) -> Result<UserServiceCode, ExecutionError> {
        match self.user_services.entry(description.into()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let service = self.store.load_service(description).await?;
                entry.insert(service.clone());
                Ok(service)
            }
        }
    }
}
