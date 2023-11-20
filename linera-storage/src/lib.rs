// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

mod chain_guards;
mod db_store;
#[cfg(feature = "aws")]
mod dynamo_db;
mod memory;
#[cfg(feature = "rocksdb")]
mod rocks_db;
#[cfg(feature = "scylladb")]
mod scylla_db;

#[cfg(feature = "aws")]
pub use crate::dynamo_db::DynamoDbStore;
#[cfg(feature = "rocksdb")]
pub use crate::rocks_db::RocksDbStore;
#[cfg(feature = "scylladb")]
pub use crate::scylla_db::ScyllaDbStore;
pub use crate::{
    db_store::{
        Clock, DbStore, TestClock, WallClock, READ_CERTIFICATE_COUNTER, READ_VALUE_COUNTER,
        WRITE_CERTIFICATE_COUNTER, WRITE_VALUE_COUNTER,
    },
    memory::MemoryStore,
};

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
    data_types::{Certificate, HashedValue},
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    ChainOwnership, ExecutionError, ExecutionRuntimeContext, UserApplicationDescription,
    UserApplicationId, UserContractCode, UserServiceCode, WasmRuntime,
};
use linera_views::{
    common::Context,
    views::{CryptoHashView, RootView, ViewError},
};
use std::{fmt::Debug, sync::Arc};

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
use {
    linera_chain::data_types::CertificateValue,
    linera_execution::{Operation, SystemOperation, WasmContract, WasmService},
};

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

#[derive(Clone)]
pub struct ChainRuntimeContext<S> {
    store: S,
    chain_id: ChainId,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
    _chain_guard: Arc<ChainGuard>,
}

#[async_trait]
impl<S> ExecutionRuntimeContext for ChainRuntimeContext<S>
where
    S: Store + Send + Sync,
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
