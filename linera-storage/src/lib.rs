// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

mod db_storage;
mod migration;

use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        ApplicationDescription, Blob, BlockHeight, ChainDescription, CompressedBytecode,
        NetworkDescription, TimeDelta, Timestamp,
    },
    identifiers::{ApplicationId, BlobId, ChainId, EventId, IndexAndEvent, StreamId},
    vm::VmRuntime,
};
use linera_chain::{
    types::{ConfirmedBlock, ConfirmedBlockCertificate},
    ChainError, ChainStateView,
};
#[cfg(with_revm)]
use linera_execution::{
    evm::revm::{EvmContractModule, EvmServiceModule},
    EvmRuntime,
};
use linera_execution::{
    BlobState, ExecutionError, ExecutionRuntimeConfig, ExecutionRuntimeContext, TransactionTracker,
    UserContractCode, UserServiceCode, WasmRuntime,
};
#[cfg(with_wasm_runtime)]
use linera_execution::{WasmContractModule, WasmServiceModule};
use linera_views::{context::Context, views::RootView, ViewError};

#[cfg(with_metrics)]
pub use crate::db_storage::metrics;
#[cfg(with_testing)]
pub use crate::db_storage::TestClock;
pub use crate::db_storage::{ChainStatesFirstAssignment, DbStorage, WallClock};

/// The default namespace to be used when none is specified
pub const DEFAULT_NAMESPACE: &str = "table_linera";

/// Communicate with a persistent storage using the "views" abstraction.
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
pub trait Storage: linera_base::util::traits::AutoTraits + Sized {
    /// The low-level storage implementation in use by the core protocol (chain workers etc).
    type Context: Context<Extra = ChainRuntimeContext<Self>> + Clone + 'static;

    /// The clock type being used.
    type Clock: Clock;

    /// The low-level storage implementation in use by the block exporter.
    type BlockExporterContext: Context<Extra = u32> + Clone;

    /// Returns the current wall clock time.
    fn clock(&self) -> &Self::Clock;

    fn thread_pool(&self) -> &Arc<linera_execution::ThreadPool>;

    /// Loads the view of a chain state.
    ///
    /// # Notes
    ///
    /// Each time this method is called, a new [`ChainStateView`] is created. If there are multiple
    /// instances of the same chain active at any given moment, they will race to access persistent
    /// storage. This can lead to invalid states and data corruption.
    async fn load_chain(&self, id: ChainId) -> Result<ChainStateView<Self::Context>, ViewError>;

    /// Tests the existence of a blob with the given blob ID.
    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// Returns what blobs from the input are missing from storage.
    async fn missing_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<BlobId>, ViewError>;

    /// Tests existence of a blob state with the given blob ID.
    async fn contains_blob_state(&self, blob_id: BlobId) -> Result<bool, ViewError>;

    /// Reads the hashed certificate value with the given hash.
    async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlock>, ViewError>;

    /// Reads the blob with the given blob ID.
    async fn read_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError>;

    /// Reads the blobs with the given blob IDs.
    async fn read_blobs(&self, blob_ids: &[BlobId]) -> Result<Vec<Option<Blob>>, ViewError>;

    /// Reads the blob state with the given blob ID.
    async fn read_blob_state(&self, blob_id: BlobId) -> Result<Option<BlobState>, ViewError>;

    /// Reads the blob states with the given blob IDs.
    async fn read_blob_states(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<Option<BlobState>>, ViewError>;

    /// Writes the given blob.
    async fn write_blob(&self, blob: &Blob) -> Result<(), ViewError>;

    /// Writes blobs and certificate
    async fn write_blobs_and_certificate(
        &self,
        blobs: &[Blob],
        certificate: &ConfirmedBlockCertificate,
    ) -> Result<(), ViewError>;

    /// Writes the given blobs, but only if they already have a blob state. Returns `true` for the
    /// blobs that were written.
    async fn maybe_write_blobs(&self, blobs: &[Blob]) -> Result<Vec<bool>, ViewError>;

    /// Attempts to write the given blob state. Returns the latest `Epoch` to have used this blob.
    async fn maybe_write_blob_states(
        &self,
        blob_ids: &[BlobId],
        blob_state: BlobState,
    ) -> Result<(), ViewError>;

    /// Writes several blobs.
    async fn write_blobs(&self, blobs: &[Blob]) -> Result<(), ViewError>;

    /// Tests existence of the certificate with the given hash.
    async fn contains_certificate(&self, hash: CryptoHash) -> Result<bool, ViewError>;

    /// Reads the certificate with the given hash.
    async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<Option<ConfirmedBlockCertificate>, ViewError>;

    /// Reads a number of certificates
    async fn read_certificates<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Option<ConfirmedBlockCertificate>>, ViewError>;

    /// Reads certificates by hashes.
    ///
    /// Returns a vector of tuples where the first element is a lite certificate
    /// and the second element is confirmed block.
    ///
    /// It does not check if all hashes all returned.
    async fn read_certificates_raw<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError>;

    /// Reads certificates by heights for a given chain.
    /// Returns a vector where each element corresponds to the input height.
    /// Elements are `None` if no certificate exists at that height.
    async fn read_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<Option<ConfirmedBlockCertificate>>, ViewError>;

    /// Reads raw certificates by heights for a given chain.
    /// Returns a vector where each element corresponds to the input height.
    /// Elements are `None` if no certificate exists at that height.
    /// Each found certificate is returned as a tuple of (lite_certificate_bytes, confirmed_block_bytes).
    async fn read_certificates_by_heights_raw(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError>;

    /// Returns a vector of certificate hashes for the requested chain and heights.
    /// The resulting vector maintains the order of the input `heights` argument.
    /// Elements are `None` if no certificate exists at that height.
    async fn read_certificate_hashes_by_heights(
        &self,
        chain_id: ChainId,
        heights: &[BlockHeight],
    ) -> Result<Vec<Option<CryptoHash>>, ViewError>;

    /// Writes certificate height index entries for a given chain.
    /// This is used to populate the height->hash index when certificates are found
    /// via alternative methods (e.g., from chain state).
    async fn write_certificate_height_indices(
        &self,
        chain_id: ChainId,
        indices: &[(BlockHeight, CryptoHash)],
    ) -> Result<(), ViewError>;

    /// Reads the event with the given ID.
    async fn read_event(&self, id: EventId) -> Result<Option<Vec<u8>>, ViewError>;

    /// Tests existence of the event with the given ID.
    async fn contains_event(&self, id: EventId) -> Result<bool, ViewError>;

    /// Lists all the events from a starting index
    async fn read_events_from_index(
        &self,
        chain_id: &ChainId,
        stream_id: &StreamId,
        start_index: u32,
    ) -> Result<Vec<IndexAndEvent>, ViewError>;

    /// Writes a vector of events.
    async fn write_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError>;

    /// Reads the network description.
    async fn read_network_description(&self) -> Result<Option<NetworkDescription>, ViewError>;

    /// Writes the network description.
    async fn write_network_description(
        &self,
        information: &NetworkDescription,
    ) -> Result<(), ViewError>;

    /// Initializes a chain in a simple way (used for testing and to create a genesis state).
    ///
    /// # Notes
    ///
    /// This method creates a new [`ChainStateView`] instance. If there are multiple instances of
    /// the same chain active at any given moment, they will race to access persistent storage.
    /// This can lead to invalid states and data corruption.
    async fn create_chain(&self, description: ChainDescription) -> Result<(), ChainError>
    where
        ChainRuntimeContext<Self>: ExecutionRuntimeContext,
    {
        let id = description.id();
        // Store the description blob.
        self.write_blob(&Blob::new_chain_description(&description))
            .await?;
        let mut chain = self.load_chain(id).await?;
        assert!(!chain.is_active(), "Attempting to create a chain twice");
        let current_time = self.clock().current_time();
        chain.initialize_if_needed(current_time).await?;
        chain.save().await?;
        Ok(())
    }

    /// Selects the WebAssembly runtime to use for applications (if any).
    fn wasm_runtime(&self) -> Option<WasmRuntime>;

    /// Creates a [`UserContractCode`] instance using the bytecode in storage referenced
    /// by the `application_description`.
    async fn load_contract(
        &self,
        application_description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserContractCode, ExecutionError> {
        let contract_bytecode_blob_id = application_description.contract_bytecode_blob_id();
        let content = match txn_tracker.get_blob_content(&contract_bytecode_blob_id) {
            Some(content) => content.clone(),
            None => self
                .read_blob(contract_bytecode_blob_id)
                .await?
                .ok_or(ExecutionError::BlobsNotFound(vec![
                    contract_bytecode_blob_id,
                ]))?
                .into_content(),
        };
        let compressed_contract_bytecode = CompressedBytecode {
            compressed_bytes: content.into_arc_bytes(),
        };
        #[cfg_attr(not(any(with_wasm_runtime, with_revm)), allow(unused_variables))]
        let contract_bytecode = self
            .thread_pool()
            .run_send((), move |()| async move {
                compressed_contract_bytecode.decompress()
            })
            .await
            .await??;
        match application_description.module_id.vm_runtime {
            VmRuntime::Wasm => {
                cfg_if::cfg_if! {
                    if #[cfg(with_wasm_runtime)] {
                        let Some(wasm_runtime) = self.wasm_runtime() else {
                            panic!("A Wasm runtime is required to load user applications.");
                        };
                        Ok(WasmContractModule::new(contract_bytecode, wasm_runtime)
                           .await?
                           .into())
                    } else {
                        panic!(
                            "A Wasm runtime is required to load user applications. \
                             Please enable the `wasmer` or the `wasmtime` feature flags \
                             when compiling `linera-storage`."
                        );
                    }
                }
            }
            VmRuntime::Evm => {
                cfg_if::cfg_if! {
                    if #[cfg(with_revm)] {
                        let evm_runtime = EvmRuntime::Revm;
                        Ok(EvmContractModule::new(contract_bytecode, evm_runtime)?
                           .into())
                    } else {
                        panic!(
                            "An Evm runtime is required to load user applications. \
                             Please enable the `revm` feature flag \
                             when compiling `linera-storage`."
                        );
                    }
                }
            }
        }
    }

    /// Creates a [`linera-sdk::UserContract`] instance using the bytecode in storage referenced
    /// by the `application_description`.
    async fn load_service(
        &self,
        application_description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserServiceCode, ExecutionError> {
        let service_bytecode_blob_id = application_description.service_bytecode_blob_id();
        let content = match txn_tracker.get_blob_content(&service_bytecode_blob_id) {
            Some(content) => content.clone(),
            None => self
                .read_blob(service_bytecode_blob_id)
                .await?
                .ok_or(ExecutionError::BlobsNotFound(vec![
                    service_bytecode_blob_id,
                ]))?
                .into_content(),
        };
        let compressed_service_bytecode = CompressedBytecode {
            compressed_bytes: content.into_arc_bytes(),
        };
        #[cfg_attr(not(any(with_wasm_runtime, with_revm)), allow(unused_variables))]
        let service_bytecode = self
            .thread_pool()
            .run_send((), move |()| async move {
                compressed_service_bytecode.decompress()
            })
            .await
            .await??;
        match application_description.module_id.vm_runtime {
            VmRuntime::Wasm => {
                cfg_if::cfg_if! {
                    if #[cfg(with_wasm_runtime)] {
                        let Some(wasm_runtime) = self.wasm_runtime() else {
                            panic!("A Wasm runtime is required to load user applications.");
                        };
                        Ok(WasmServiceModule::new(service_bytecode, wasm_runtime)
                           .await?
                           .into())
                    } else {
                        panic!(
                            "A Wasm runtime is required to load user applications. \
                             Please enable the `wasmer` or the `wasmtime` feature flags \
                             when compiling `linera-storage`."
                        );
                    }
                }
            }
            VmRuntime::Evm => {
                cfg_if::cfg_if! {
                    if #[cfg(with_revm)] {
                        let evm_runtime = EvmRuntime::Revm;
                        Ok(EvmServiceModule::new(service_bytecode, evm_runtime)?
                           .into())
                    } else {
                        panic!(
                            "An Evm runtime is required to load user applications. \
                             Please enable the `revm` feature flag \
                             when compiling `linera-storage`."
                        );
                    }
                }
            }
        }
    }

    async fn block_exporter_context(
        &self,
        block_exporter_id: u32,
    ) -> Result<Self::BlockExporterContext, ViewError>;
}

/// The result of processing the obtained read certificates.
pub enum ResultReadCertificates {
    Certificates(Vec<ConfirmedBlockCertificate>),
    InvalidHashes(Vec<CryptoHash>),
}

impl ResultReadCertificates {
    /// Creating the processed read certificates.
    pub fn new(
        certificates: Vec<Option<ConfirmedBlockCertificate>>,
        hashes: Vec<CryptoHash>,
    ) -> Self {
        let (certificates, invalid_hashes) = certificates
            .into_iter()
            .zip(hashes)
            .partition_map::<Vec<_>, Vec<_>, _, _, _>(|(certificate, hash)| match certificate {
                Some(cert) => itertools::Either::Left(cert),
                None => itertools::Either::Right(hash),
            });
        if invalid_hashes.is_empty() {
            Self::Certificates(certificates)
        } else {
            Self::InvalidHashes(invalid_hashes)
        }
    }
}

/// An implementation of `ExecutionRuntimeContext` suitable for the core protocol.
#[derive(Clone)]
pub struct ChainRuntimeContext<S> {
    storage: S,
    chain_id: ChainId,
    thread_pool: Arc<linera_execution::ThreadPool>,
    execution_runtime_config: ExecutionRuntimeConfig,
    user_contracts: Arc<papaya::HashMap<ApplicationId, UserContractCode>>,
    user_services: Arc<papaya::HashMap<ApplicationId, UserServiceCode>>,
}

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl<S: Storage> ExecutionRuntimeContext for ChainRuntimeContext<S> {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn thread_pool(&self) -> &Arc<linera_execution::ThreadPool> {
        &self.thread_pool
    }

    fn execution_runtime_config(&self) -> linera_execution::ExecutionRuntimeConfig {
        self.execution_runtime_config
    }

    fn user_contracts(&self) -> &Arc<papaya::HashMap<ApplicationId, UserContractCode>> {
        &self.user_contracts
    }

    fn user_services(&self) -> &Arc<papaya::HashMap<ApplicationId, UserServiceCode>> {
        &self.user_services
    }

    async fn get_user_contract(
        &self,
        description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserContractCode, ExecutionError> {
        let application_id = description.into();
        let pinned = self.user_contracts.pin_owned();
        if let Some(contract) = pinned.get(&application_id) {
            return Ok(contract.clone());
        }
        let contract = self.storage.load_contract(description, txn_tracker).await?;
        pinned.insert(application_id, contract.clone());
        Ok(contract)
    }

    async fn get_user_service(
        &self,
        description: &ApplicationDescription,
        txn_tracker: &TransactionTracker,
    ) -> Result<UserServiceCode, ExecutionError> {
        let application_id = description.into();
        let pinned = self.user_services.pin_owned();
        if let Some(service) = pinned.get(&application_id) {
            return Ok(service.clone());
        }
        let service = self.storage.load_service(description, txn_tracker).await?;
        pinned.insert(application_id, service.clone());
        Ok(service)
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<Option<Blob>, ViewError> {
        self.storage.read_blob(blob_id).await
    }

    async fn get_event(&self, event_id: EventId) -> Result<Option<Vec<u8>>, ViewError> {
        self.storage.read_event(event_id).await
    }

    async fn get_network_description(&self) -> Result<Option<NetworkDescription>, ViewError> {
        self.storage.read_network_description().await
    }

    async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        self.storage.contains_blob(blob_id).await
    }

    async fn contains_event(&self, event_id: EventId) -> Result<bool, ViewError> {
        self.storage.contains_event(event_id).await
    }

    #[cfg(with_testing)]
    async fn add_blobs(
        &self,
        blobs: impl IntoIterator<Item = Blob> + Send,
    ) -> Result<(), ViewError> {
        let blobs = Vec::from_iter(blobs);
        self.storage.write_blobs(&blobs).await
    }

    #[cfg(with_testing)]
    async fn add_events(
        &self,
        events: impl IntoIterator<Item = (EventId, Vec<u8>)> + Send,
    ) -> Result<(), ViewError> {
        self.storage.write_events(events).await
    }
}

/// A clock that can be used to get the current `Timestamp`.
#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
pub trait Clock {
    fn current_time(&self) -> Timestamp;

    async fn sleep(&self, delta: TimeDelta);

    async fn sleep_until(&self, timestamp: Timestamp);
}
