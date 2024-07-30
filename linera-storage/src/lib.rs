// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

#![deny(clippy::large_futures)]

mod chain_guards;
mod db_storage;
#[cfg(with_dynamodb)]
mod dynamo_db;
mod memory;
#[cfg(with_rocksdb)]
mod rocks_db;
#[cfg(with_scylladb)]
mod scylla_db;
#[cfg(not(target_arch = "wasm32"))]
mod service;

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::future;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, BlockHeight, HashedBlob, Timestamp},
    identifiers::{BlobId, ChainDescription, ChainId, GenericApplicationId},
    ownership::ChainOwnership,
};
use linera_chain::{
    data_types::{Certificate, ChannelFullName, HashedCertificateValue},
    ChainError, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::SystemChannel,
    BlobState, ChannelSubscription, ExecutionError, ExecutionRuntimeConfig,
    ExecutionRuntimeContext, UserApplicationDescription, UserApplicationId, UserContractCode,
    UserServiceCode, WasmRuntime,
};
use linera_views::{
    common::Context,
    views::{CryptoHashView, RootView, ViewError},
};
#[cfg(with_wasm_runtime)]
use {
    linera_chain::data_types::CertificateValue,
    linera_execution::{Operation, SystemOperation, WasmContractModule, WasmServiceModule},
};

#[cfg(with_testing)]
pub use crate::db_storage::TestClock;
#[cfg(with_metrics)]
pub use crate::db_storage::{
    READ_CERTIFICATE_COUNTER, READ_HASHED_CERTIFICATE_VALUE_COUNTER, WRITE_CERTIFICATE_COUNTER,
    WRITE_HASHED_CERTIFICATE_VALUE_COUNTER,
};
#[cfg(with_dynamodb)]
pub use crate::dynamo_db::DynamoDbStorage;
#[cfg(with_rocksdb)]
pub use crate::rocks_db::RocksDbStorage;
#[cfg(with_scylladb)]
pub use crate::scylla_db::ScyllaDbStorage;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::service::ServiceStorage;
pub use crate::{
    db_storage::{Clock, DbStorage, WallClock},
    memory::MemoryStorage,
};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Storage: Sized {
    /// The low-level storage implementation in use.
    type Context: Context<Extra = ChainRuntimeContext<Self>, Error = Self::StoreError>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Alias to provide simpler trait bounds `ViewError: From<Self::StoreError>`
    type StoreError: std::error::Error + Debug + Sync + Send;

    /// Returns the current wall clock time.
    fn clock(&self) -> &dyn Clock;

    /// Loads the view of a chain state.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>
    where
        ViewError: From<Self::StoreError>;

    /// Tests existence of a hashed certificate value with the given hash.
    async fn contains_hashed_certificate_value(&self, hash: CryptoHash) -> Result<bool, ViewError>;

    /// Tests existence of hashed certificate values with given hashes.
    async fn contains_hashed_certificate_values(
        &self,
        hash: Vec<CryptoHash>,
    ) -> Result<Vec<bool>, ViewError>;

    /// Tests existence of a blob with the given hash.
    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// List the missing blobs from the storage.
    async fn missing_blobs(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, ViewError>;

    /// Tests existence of a blob state with the given hash.
    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// Reads the hashed certificate value with the given hash.
    async fn read_hashed_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, ViewError>;

    /// Reads the blob with the given blob ID.
    async fn read_hashed_blob(&self, blob_id: BlobId) -> Result<HashedBlob, ViewError>;

    /// Reads the blobs with the given blob IDs.
    async fn read_hashed_blobs(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<Option<HashedBlob>>, ViewError>;

    /// Reads the blob state with the given blob ID.
    async fn read_blob_state(&self, blob_id: BlobId) -> Result<BlobState, ViewError>;

    /// Reads the hashed certificate values in descending order from the given hash.
    async fn read_hashed_certificate_values_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<HashedCertificateValue>, ViewError>;

    /// Writes the given hashed certificate value.
    async fn write_hashed_certificate_value(
        &self,
        value: &HashedCertificateValue,
    ) -> Result<(), ViewError>;

    /// Writes the given blob.
    async fn write_hashed_blob(&self, blob: &HashedBlob) -> Result<(), ViewError>;

    /// Writes hashed certificates, hashed blobs and certificate
    async fn write_hashed_certificate_values_hashed_blobs_certificate(
        &self,
        values: &[HashedCertificateValue],
        blobs: &[HashedBlob],
        certificate: &Certificate,
    ) -> Result<(), ViewError>;

    /// Writes the given blob state.
    async fn write_blob_state(
        &self,
        blob_id: BlobId,
        blob_state: &BlobState,
    ) -> Result<(), ViewError>;

    /// Attempts to write the given blob state. Returns the latest `Epoch` to have used this blob.
    async fn maybe_write_blob_state(
        &self,
        blob_id: BlobId,
        blob_state: BlobState,
    ) -> Result<Epoch, ViewError>;

    /// Writes several hashed certificate values.
    async fn write_hashed_certificate_values(
        &self,
        values: &[HashedCertificateValue],
    ) -> Result<(), ViewError>;

    /// Writes several blobs.
    async fn write_hashed_blobs(&self, blobs: &[HashedBlob]) -> Result<(), ViewError>;

    /// Tests existence of the certificate with the given hash.
    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError>;

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
        ViewError: From<Self::StoreError>,
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
        ViewError: From<Self::StoreError>,
    {
        let id = description.into();
        let mut chain = self.load_chain(id).await?;
        assert!(!chain.is_active(), "Attempting to create a chain twice");
        chain.manager.get_mut().reset(
            &ChainOwnership::single(public_key),
            BlockHeight(0),
            self.clock().current_time(),
            committee.keys_and_weights(),
        )?;
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

        if id != admin_id {
            // Add the new subscriber to the admin chain.
            system_state.subscriptions.insert(&ChannelSubscription {
                chain_id: admin_id,
                name: SystemChannel::Admin.name(),
            })?;
            let mut admin_chain = self.load_chain(admin_id).await?;
            let full_name = ChannelFullName {
                application_id: GenericApplicationId::System,
                name: SystemChannel::Admin.name(),
            };
            {
                let mut channel = admin_chain.channels.try_load_entry_mut(&full_name).await?;
                channel.subscribers.insert(&id)?;
            } // Make channel go out of scope, so we can call save.
            admin_chain.save().await?;
        }

        let state_hash = chain.execution_state.crypto_hash().await?;
        chain.execution_state_hash.set(Some(state_hash));
        chain.save().await?;
        Ok(())
    }

    /// Selects the WebAssembly runtime to use for applications (if any).
    fn wasm_runtime(&self) -> Option<WasmRuntime>;

    /// Creates a [`UserContractCode`] instance using the bytecode in storage referenced
    /// by the `application_description`.
    #[cfg(with_wasm_runtime)]
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
            unreachable!("unexpected bytecode operation");
        };
        Ok(Arc::new(
            WasmContractModule::new(contract, wasm_runtime).await?,
        ))
    }

    #[cfg(not(with_wasm_runtime))]
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
    #[cfg(with_wasm_runtime)]
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
            unreachable!("unexpected bytecode operation");
        };
        Ok(Arc::new(
            WasmServiceModule::new(service, wasm_runtime).await?,
        ))
    }

    #[cfg(not(with_wasm_runtime))]
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

#[cfg(with_wasm_runtime)]
async fn read_publish_bytecode_operation(
    storage: &impl Storage,
    application_description: &UserApplicationDescription,
) -> Result<SystemOperation, ExecutionError> {
    let UserApplicationDescription {
        bytecode_id,
        bytecode_location,
        ..
    } = application_description;
    let value = storage
        .read_hashed_certificate_value(bytecode_location.certificate_hash)
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
    storage: S,
    chain_id: ChainId,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<DashMap<UserApplicationId, UserContractCode>>,
    user_services: Arc<DashMap<UserApplicationId, UserServiceCode>>,
}

#[async_trait]
impl<S> ExecutionRuntimeContext for ChainRuntimeContext<S>
where
    S: Storage + Send + Sync,
{
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn execution_runtime_config(&self) -> linera_execution::ExecutionRuntimeConfig {
        self.execution_runtime_config
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
                let contract = self.storage.load_contract(description).await?;
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
                let service = self.storage.load_service(description).await?;
                entry.insert(service.clone());
                Ok(service)
            }
        }
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<HashedBlob, ExecutionError> {
        Ok(self.storage.read_hashed_blob(blob_id).await?)
    }
}
