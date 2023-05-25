// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

mod chain_guards;
#[cfg(feature = "aws")]
mod dynamo_db;
mod memory;
mod rocksdb;

#[cfg(feature = "aws")]
pub use crate::dynamo_db::DynamoDbStoreClient;
pub use crate::{memory::MemoryStoreClient, rocksdb::RocksdbStoreClient};

use crate::chain_guards::ChainGuards;
use async_trait::async_trait;
use chain_guards::ChainGuard;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::future;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainDescription, ChainId},
};
use linera_chain::{
    data_types::{Certificate, HashedValue, LiteCertificate, Value},
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    ApplicationDescription, ChainOwnership, ExecutionError, ExecutionRuntimeContext,
    UserApplicationCode, WasmRuntime,
};
use linera_views::{
    batch::Batch,
    common::{Context, ContextFromDb, KeyValueStoreClient},
    views::{CryptoHashView, RootView, View, ViewError},
};
use metrics::increment_counter;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

/// The metric counting how often a value is read from storage.
pub const READ_VALUE_COUNTER: &str = "read_value";
/// The metric counting how often a value is written to storage.
pub const WRITE_VALUE_COUNTER: &str = "write_value";
/// The metric counting how often a certificate is read from storage.
pub const READ_CERTIFICATE_COUNTER: &str = "read_certificate";
/// The metric counting how often a certificate is written to storage.
pub const WRITE_CERTIFICATE_COUNTER: &str = "write_certificate";

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use linera_execution::{Operation, SystemOperation, WasmApplication};

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

    /// Loads the view of a chain state.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Reads the value with the given hash.
    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError>;

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
        system_state.epoch.set(Some(Epoch::from(0)));
        system_state.admin_id.set(Some(admin_id));
        system_state
            .committees
            .get_mut()
            .insert(Epoch::from(0), committee);
        system_state
            .ownership
            .set(ChainOwnership::single(public_key));
        system_state.balance.set(balance);
        system_state.timestamp.set(timestamp);
        let state_hash = chain.execution_state.crypto_hash().await?;
        chain.execution_state_hash.set(Some(state_hash));
        chain
            .manager
            .get_mut()
            .reset(&ChainOwnership::single(public_key));
        chain.save().await?;
        Ok(())
    }

    /// Selects the WebAssembly runtime to use for applications (if any).
    fn wasm_runtime(&self) -> Option<WasmRuntime>;

    /// Creates a [`linera-sdk::UserApplication`] instance using the bytecode in storage referenced by the
    /// `application_description`.
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    async fn load_application(
        &self,
        application_description: &ApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        let Some(wasm_runtime) = self.wasm_runtime() else {
            panic!("A WASM runtime is required to load user applications.");
        };
        let ApplicationDescription {
            bytecode_id,
            bytecode_location,
            ..
        } = application_description;
        let value = self
            .read_value(bytecode_location.certificate_hash)
            .await
            .map_err(|error| match error {
                ViewError::NotFound(_) => ExecutionError::ApplicationBytecodeNotFound(Box::new(
                    application_description.clone(),
                )),
                _ => error.into(),
            })?;
        let operations = &value.block().operations;
        let index = usize::try_from(bytecode_location.operation_index)
            .map_err(|_| linera_base::data_types::ArithmeticError::Overflow)?;
        match operations.get(index) {
            Some(Operation::System(SystemOperation::PublishBytecode { contract, service })) => {
                Ok(Arc::new(WasmApplication::new(
                    contract.clone(),
                    service.clone(),
                    wasm_runtime,
                )?))
            }
            _ => Err(ExecutionError::InvalidBytecodeId(*bytecode_id)),
        }
    }

    #[cfg(not(any(feature = "wasmer", feature = "wasmtime")))]
    async fn load_application(
        &self,
        _application_description: &ApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        panic!(
            "A WASM runtime is required to load user applications. \
            Please enable the `wasmer` or the `wasmtime` feature flags \
            when compiling `linera-storage`."
        );
    }
}

/// A store implemented from a [`KeyValueStoreClient`]
pub struct DbStore<CL> {
    client: CL,
    guards: ChainGuards,
    user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
    wasm_runtime: Option<WasmRuntime>,
}

#[derive(Clone)]
/// A DbStoreClient wrapping with Arc
pub struct DbStoreClient<CL> {
    client: Arc<DbStore<CL>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(CryptoHash),
    Value(CryptoHash),
}

#[async_trait]
impl<CL> Store for DbStoreClient<CL>
where
    CL: KeyValueStoreClient + Clone + Send + Sync + 'static,
    ViewError: From<<CL as KeyValueStoreClient>::Error>,
    <CL as KeyValueStoreClient>::Error: From<bcs::Error> + Send + Sync + serde::ser::StdError,
{
    type Context = ContextFromDb<ChainRuntimeContext<Self>, CL>;
    type ContextError = <CL as KeyValueStoreClient>::Error;

    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError> {
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = self.client.guards.guard(id).await;
        let runtime_context = ChainRuntimeContext {
            store: self.clone(),
            chain_id: id,
            user_applications: self.client.user_applications.clone(),
            chain_guard: Some(Arc::new(guard)),
        };
        let client = self.client.client.clone();
        let base_key = bcs::to_bytes(&BaseKey::ChainState(id))?;
        let context = ContextFromDb::create(client, base_key, runtime_context).await?;
        ChainStateView::load(context).await
    }

    async fn read_value(&self, hash: CryptoHash) -> Result<HashedValue, ViewError> {
        let value_key = bcs::to_bytes(&BaseKey::Value(hash))?;
        let maybe_value: Option<Value> = self.client.client.read_key(&value_key).await?;
        let id = match &maybe_value {
            Some(value) => value.block.chain_id.to_string(),
            None => "not found".to_string(),
        };
        increment_counter!(READ_VALUE_COUNTER, &[("chain_id", id)]);
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value.with_hash_unchecked(hash))
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
            self.client.client.read_key::<LiteCertificate>(&cert_key),
            self.client.client.read_key::<Value>(&value_key)
        );
        if let Ok(maybe_value) = &value_result {
            let id = match maybe_value {
                Some(value) => value.block.chain_id.to_string(),
                None => "not found".to_string(),
            };
            increment_counter!(READ_CERTIFICATE_COUNTER, &[("chain_id", id)]);
        };
        let value: Value =
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

impl<CL> DbStoreClient<CL>
where
    CL: KeyValueStoreClient + Clone + Send + Sync + 'static,
    ViewError: From<<CL as KeyValueStoreClient>::Error>,
    <CL as KeyValueStoreClient>::Error: From<bcs::Error> + Send + Sync + serde::ser::StdError,
{
    fn add_value_to_batch(&self, value: &HashedValue, batch: &mut Batch) -> Result<(), ViewError> {
        let id = value.block().chain_id.to_string();
        increment_counter!(WRITE_VALUE_COUNTER, &[("chain_id", id)]);
        let value_key = bcs::to_bytes(&BaseKey::Value(value.hash()))?;
        batch.put_key_value(value_key.to_vec(), value)?;
        Ok(())
    }

    fn add_certificate_to_batch(
        &self,
        certificate: &Certificate,
        batch: &mut Batch,
    ) -> Result<(), ViewError> {
        let id = certificate.value.block().chain_id.to_string();
        increment_counter!(WRITE_CERTIFICATE_COUNTER, &[("chain_id", id)]);
        let hash = certificate.value.hash();
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
    pub user_applications: Arc<DashMap<ApplicationId, UserApplicationCode>>,
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

    fn user_applications(&self) -> &Arc<DashMap<ApplicationId, UserApplicationCode>> {
        &self.user_applications
    }

    async fn get_user_application(
        &self,
        description: &ApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        match self.user_applications.entry(description.into()) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let application = self.store.load_application(description).await?;
                entry.insert(application.clone());
                Ok(application)
            }
        }
    }
}
