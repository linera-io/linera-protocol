// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod chain_guards;
mod dynamo_db;
mod memory;
mod rocksdb;

pub use crate::{
    dynamo_db::DynamoDbStoreClient, memory::MemoryStoreClient, rocksdb::RocksdbStoreClient,
};

use async_trait::async_trait;
use chain_guards::ChainGuard;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::future;
use linera_base::{
    committee::Committee,
    crypto::HashValue,
    messages::{ChainDescription, ChainId, Epoch, Owner},
};
use linera_chain::{messages::Certificate, ChainError, ChainStateView};
use linera_execution::{
    system::Balance, ApplicationId, ChainOwnership, ExecutionError, ExecutionRuntimeContext,
    UserApplicationCode, UserApplicationDescription, UserApplicationId,
};
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use linera_execution::{Operation, SystemOperation, WasmApplication};
use linera_views::{common::Context, views::ViewError};
use std::{fmt::Debug, sync::Arc};

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

    /// Load the view of a chain state.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Read the certificate with the given hash.
    async fn read_certificate(&self, hash: HashValue) -> Result<Certificate, ViewError>;

    /// Write the given certificate.
    async fn write_certificate(&self, certificate: Certificate) -> Result<(), ViewError>;

    /// Load the view of a chain state and check that it is active.
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

    /// Read a number of certificates in parallel.
    async fn read_certificates<I: IntoIterator<Item = HashValue> + Send>(
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

    /// Initialize a chain in a simple way (used for testing and to create a genesis state).
    async fn create_chain(
        &self,
        committee: Committee,
        admin_id: ChainId,
        description: ChainDescription,
        owner: Owner,
        balance: Balance,
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
        system_state.ownership.set(ChainOwnership::single(owner));
        system_state.balance.set(balance);
        let state_hash = chain.execution_state.hash_value().await?;
        chain.execution_state_hash.set(Some(state_hash));
        chain
            .manager
            .get_mut()
            .reset(&ChainOwnership::single(owner));
        chain.save().await?;
        Ok(())
    }

    /// Create a [`UserApplication`] instance using the bytecode in storage referenced by the
    /// `application_description`.
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    async fn load_application(
        &self,
        application_description: &UserApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        let UserApplicationDescription {
            bytecode,
            bytecode_id,
            ..
        } = application_description;
        let invalid_bytecode_id_error = || ExecutionError::InvalidBytecodeId(*bytecode_id);

        let certificate = self.read_certificate(bytecode.certificate_hash).await?;
        let (operation_application_id, bytecode_operation) = certificate
            .value
            .block()
            .operations
            .get(bytecode.operation_index)
            .ok_or_else(invalid_bytecode_id_error)?;

        let ApplicationId::System = operation_application_id
            else { return Err(invalid_bytecode_id_error()); };
        let Operation::System(SystemOperation::PublishBytecode { contract, service }) =
            bytecode_operation.clone()
            else { return Err(invalid_bytecode_id_error()); };

        Ok(Arc::new(WasmApplication::new(contract, service)))
    }

    #[cfg(not(any(feature = "wasmer", feature = "wasmtime")))]
    async fn load_application(
        &self,
        _application_description: &UserApplicationDescription,
    ) -> Result<UserApplicationCode, ExecutionError> {
        panic!(
            "A WASM runtime is required to load user applications. \
            Please enable the `wasmer` or the `wasmtime` feature flags \
            when compiling `linera-storage`."
        );
    }
}

#[derive(Clone)]
pub struct ChainRuntimeContext<StoreClient> {
    store: StoreClient,
    pub chain_id: ChainId,
    pub user_applications: Arc<DashMap<UserApplicationId, UserApplicationCode>>,
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

    fn user_applications(&self) -> &Arc<DashMap<UserApplicationId, UserApplicationCode>> {
        &self.user_applications
    }

    async fn get_user_application(
        &self,
        description: &UserApplicationDescription,
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
