// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

#![deny(clippy::large_futures)]

mod db_storage;

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, Blob, BlockHeight, TimeDelta, Timestamp, UserApplicationDescription},
    identifiers::{BlobId, ChainDescription, ChainId, GenericApplicationId, UserApplicationId},
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
    ExecutionRuntimeContext, UserContractCode, UserServiceCode, WasmRuntime,
};
use linera_views::{
    context::Context,
    views::{CryptoHashView, RootView, ViewError},
};
#[cfg(with_wasm_runtime)]
use {
    linera_base::{data_types::CompressedBytecode, identifiers::BlobType},
    linera_execution::{WasmContractModule, WasmServiceModule},
};

#[cfg(with_testing)]
pub use crate::db_storage::TestClock;
pub use crate::db_storage::{ChainStatesFirstAssignment, DbStorage, WallClock};
#[cfg(with_metrics)]
pub use crate::db_storage::{
    READ_CERTIFICATE_COUNTER, READ_HASHED_CERTIFICATE_VALUE_COUNTER, WRITE_CERTIFICATE_COUNTER,
    WRITE_HASHED_CERTIFICATE_VALUE_COUNTER,
};

/// Communicate with a persistent storage using the "views" abstraction.
#[async_trait]
pub trait Storage: Sized {
    /// The low-level storage implementation in use.
    type Context: Context<Extra = ChainRuntimeContext<Self>> + Clone + Send + Sync + 'static;

    /// The clock type being used.
    type Clock: Clock;

    /// Returns the current wall clock time.
    fn clock(&self) -> &Self::Clock;

    /// Loads the view of a chain state.
    ///
    /// # Notes
    ///
    /// Each time this method is called, a new [`ChainStateView`] is created. If there are multiple
    /// instances of the same chain active at any given moment, they will race to access persistent
    /// storage. This can lead to invalid states and data corruption.
    ///
    /// Other methods that also create [`ChainStateView`] instances that can cause conflicts are:
    /// [`load_active_chain`][`Self::load_active_chain`] and
    /// [`create_chain`][`Self::create_chain`].
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Tests existence of a hashed certificate value with the given hash.
    async fn contains_hashed_certificate_value(&self, hash: CryptoHash) -> Result<bool, ViewError>;

    /// Tests existence of hashed certificate values with given hashes.
    async fn contains_hashed_certificate_values(
        &self,
        hash: Vec<CryptoHash>,
    ) -> Result<Vec<bool>, ViewError>;

    /// Tests the existence of a blob with the given blob ID.
    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// Lists the missing blobs from storage.
    async fn missing_blobs(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, ViewError>;

    /// Tests existence of a blob state with the given blob ID.
    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// Reads the hashed certificate value with the given hash.
    async fn read_hashed_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, ViewError>;

    /// Reads the blob with the given blob ID.
    async fn read_blob(&self, blob_id: BlobId) -> Result<Blob, ViewError>;

    /// Reads the blobs with the given blob IDs.
    async fn read_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Option<Blob>>, ViewError>;

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
    async fn write_blob(&self, blob: &Blob) -> Result<(), ViewError>;

    /// Writes blobs and certificate
    async fn write_blobs_and_certificate(
        &self,
        blobs: &[Blob],
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
    async fn write_blobs(&self, blobs: &[Blob]) -> Result<(), ViewError>;

    /// Tests existence of the certificate with the given hash.
    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError>;

    /// Reads the certificate with the given hash.
    async fn read_certificate(&self, hash: CryptoHash) -> Result<Certificate, ViewError>;

    /// Reads a number of certificates
    async fn read_certificates<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Certificate>, ViewError>;

    /// Writes the given certificate.
    async fn write_certificate(&self, certificate: &Certificate) -> Result<(), ViewError>;

    /// Writes a vector of certificates.
    async fn write_certificates(&self, certificate: &[Certificate]) -> Result<(), ViewError>;

    /// Loads the view of a chain state and checks that it is active.
    ///
    /// # Notes
    ///
    /// Each time this method is called, a new [`ChainStateView`] is created. If there are multiple
    /// instances of the same chain active at any given moment, they will race to access persistent
    /// storage. This can lead to invalid states and data corruption.
    ///
    /// Other methods that also create [`ChainStateView`] instances that can cause conflicts are:
    /// [`load_chain`][`Self::load_chain`] and [`create_chain`][`Self::create_chain`].
    async fn load_active_chain(
        &self,
        id: ChainId,
    ) -> Result<ChainStateView<Self::Context>, linera_chain::ChainError>
    where
        ChainRuntimeContext<Self>: ExecutionRuntimeContext,
    {
        let chain = self.load_chain(id).await?;
        chain.ensure_is_active()?;
        Ok(chain)
    }

    /// Initializes a chain in a simple way (used for testing and to create a genesis state).
    ///
    /// # Notes
    ///
    /// This method creates a new [`ChainStateView`] instance. If there are multiple instances of
    /// the same chain active at any given moment, they will race to access persistent storage.
    /// This can lead to invalid states and data corruption.
    ///
    /// Other methods that also create [`ChainStateView`] instances that can cause conflicts are:
    /// [`load_chain`][`Self::load_chain`] and [`load_active_chain`][`Self::load_active_chain`].
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
        let contract_bytecode_blob_id = BlobId::new(
            application_description.bytecode_id.contract_blob_hash,
            BlobType::ContractBytecode,
        );
        let contract_blob = self.read_blob(contract_bytecode_blob_id).await?;
        let compressed_contract_bytecode = CompressedBytecode {
            compressed_bytes: contract_blob.inner_bytes(),
        };
        let contract_bytecode = linera_base::task::spawn_blocking(move || {
            compressed_contract_bytecode.decompress(u64::MAX)
        })
        .await??;
        Ok(WasmContractModule::new(contract_bytecode, wasm_runtime)
            .await?
            .into())
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
        let service_bytecode_blob_id = BlobId::new(
            application_description.bytecode_id.service_blob_hash,
            BlobType::ServiceBytecode,
        );
        let service_blob = self.read_blob(service_bytecode_blob_id).await?;
        let compressed_service_bytecode = CompressedBytecode {
            compressed_bytes: service_blob.inner_bytes(),
        };
        let service_bytecode = linera_base::task::spawn_blocking(move || {
            compressed_service_bytecode.decompress(u64::MAX)
        })
        .await??;
        Ok(WasmServiceModule::new(service_bytecode, wasm_runtime)
            .await?
            .into())
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

    async fn get_blob(&self, blob_id: BlobId) -> Result<Blob, ExecutionError> {
        Ok(self.storage.read_blob(blob_id).await?)
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        self.storage.contains_blob(blob_id).await
    }
}

/// A clock that can be used to get the current `Timestamp`.
#[async_trait]
pub trait Clock {
    fn current_time(&self) -> Timestamp;

    async fn sleep(&self, delta: TimeDelta);

    async fn sleep_until(&self, timestamp: Timestamp);
}
