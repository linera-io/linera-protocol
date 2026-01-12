// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the storage abstractions for individual chains and certificates.

mod db_storage;

use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        ApplicationDescription, Blob, ChainDescription, CompressedBytecode, NetworkDescription,
        TimeDelta, Timestamp,
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
pub const DEFAULT_NAMESPACE: &str = "default";

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

    /// Reads a number of confirmed blocks by their hashes.
    async fn read_confirmed_blocks<I: IntoIterator<Item = CryptoHash> + Send>(
        &self,
        hashes: I,
    ) -> Result<Vec<Option<ConfirmedBlock>>, ViewError>;

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

    /// Lists the blob IDs in storage.
    async fn list_blob_ids(&self) -> Result<Vec<BlobId>, ViewError>;

    /// Lists the chain IDs in storage.
    async fn list_chain_ids(&self) -> Result<Vec<ChainId>, ViewError>;

    /// Lists the event IDs in storage.
    async fn list_event_ids(&self) -> Result<Vec<EventId>, ViewError>;
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

/// The result of processing the obtained read confirmed blocks.
pub enum ResultReadConfirmedBlocks {
    Blocks(Vec<ConfirmedBlock>),
    InvalidHashes(Vec<CryptoHash>),
}

impl ResultReadConfirmedBlocks {
    /// Creating the processed read confirmed blocks.
    pub fn new(blocks: Vec<Option<ConfirmedBlock>>, hashes: Vec<CryptoHash>) -> Self {
        let (blocks, invalid_hashes) = blocks
            .into_iter()
            .zip(hashes)
            .partition_map::<Vec<_>, Vec<_>, _, _, _>(|(block, hash)| match block {
                Some(block) => itertools::Either::Left(block),
                None => itertools::Either::Right(hash),
            });
        if invalid_hashes.is_empty() {
            Self::Blocks(blocks)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use linera_base::{
        crypto::{AccountPublicKey, CryptoHash},
        data_types::{
            Amount, ApplicationPermissions, Blob, BlockHeight, ChainDescription, ChainOrigin,
            Epoch, InitialChainConfig, NetworkDescription, Round, Timestamp,
        },
        identifiers::{BlobId, BlobType, ChainId, EventId, StreamId},
        ownership::ChainOwnership,
    };
    use linera_chain::{
        block::{Block, ConfirmedBlock},
        data_types::{BlockExecutionOutcome, ProposedBlock},
    };
    use linera_execution::BlobState;
    #[cfg(feature = "dynamodb")]
    use linera_views::dynamo_db::DynamoDbDatabase;
    #[cfg(feature = "scylladb")]
    use linera_views::scylla_db::ScyllaDbDatabase;
    use linera_views::{memory::MemoryDatabase, ViewError};
    use test_case::test_case;

    use super::*;
    use crate::db_storage::DbStorage;

    /// Generic test function to test Storage trait features
    async fn test_storage_chain_exporter<S: Storage + Sync>(storage: &S) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        // Test clock functionality
        let _current_time = storage.clock().current_time();
        let test_chain_id = ChainId(CryptoHash::test_hash("test_chain"));

        // Test loading a chain (this creates a chain state view)
        let _chain_view = storage.load_chain(test_chain_id).await?;

        // Test block exporter context
        let _block_exporter_context = storage.block_exporter_context(0).await?;
        Ok(())
    }

    async fn test_storage_blob<S: Storage + Sync>(storage: &S) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        // Create test blobs
        let chain_description = ChainDescription::new(
            ChainOrigin::Root(0),
            InitialChainConfig {
                ownership: ChainOwnership::single(AccountPublicKey::test_key(0).into()),
                epoch: Epoch::ZERO,
                min_active_epoch: Epoch::ZERO,
                max_active_epoch: Epoch::ZERO,
                balance: Amount::ZERO,
                application_permissions: ApplicationPermissions::default(),
            },
            Timestamp::from(0),
        );

        let test_blob1 = Blob::new_chain_description(&chain_description);
        let test_blob2 = Blob::new_data(vec![10, 20, 30]);
        let test_blob3 = Blob::new_data(vec![40, 50, 60]);

        // Testing blobs existence
        let blob_id1 = test_blob1.id();
        let blob_id2 = test_blob2.id();
        let blob_id3 = test_blob3.id();

        // Test blob existence before writing
        assert!(!storage.contains_blob(blob_id1).await?);
        assert!(!storage.contains_blob(blob_id2).await?);
        assert!(!storage.contains_blob(blob_id3).await?);

        // Test single blob write
        storage.write_blob(&test_blob1).await?;
        assert!(storage.contains_blob(blob_id1).await?);

        // Test multiple blob write (write_blobs)
        storage
            .write_blobs(&[test_blob2.clone(), test_blob3.clone()])
            .await?;
        assert!(storage.contains_blob(blob_id2).await?);
        assert!(storage.contains_blob(blob_id3).await?);

        // Test single blob read
        let read_blob = storage.read_blob(blob_id1).await?;
        assert_eq!(read_blob, Some(test_blob1.clone()));

        // Test multiple blob read (read_blobs)
        let blob_ids = vec![blob_id1, blob_id2, blob_id3];
        let read_blobs = storage.read_blobs(&blob_ids).await?;
        assert_eq!(read_blobs.len(), 3);

        // Verify each blob was read correctly
        assert_eq!(read_blobs[0], Some(test_blob1.clone()));
        assert_eq!(read_blobs[1], Some(test_blob2));
        assert_eq!(read_blobs[2], Some(test_blob3));

        // Test missing blobs detection
        let missing_blob_id = BlobId::new(CryptoHash::test_hash("missing"), BlobType::Data);
        let missing_blobs = storage.missing_blobs(&[blob_id1, missing_blob_id]).await?;
        assert_eq!(missing_blobs, vec![missing_blob_id]);

        // Test maybe_write_blobs (should return false as blobs don't have blob states yet)
        let write_results = storage
            .maybe_write_blobs(std::slice::from_ref(&test_blob1))
            .await?;
        assert_eq!(write_results, vec![false]);

        // Test blob state operations
        let blob_state1 = BlobState {
            last_used_by: None,
            chain_id: ChainId(CryptoHash::test_hash("chain1")),
            block_height: BlockHeight(0),
            epoch: Some(Epoch::ZERO),
        };
        let blob_state2 = BlobState {
            last_used_by: Some(CryptoHash::test_hash("cert")),
            chain_id: ChainId(CryptoHash::test_hash("chain2")),
            block_height: BlockHeight(1),
            epoch: Some(Epoch::from(1)),
        };

        // Test blob state existence before writing
        assert!(!storage.contains_blob_state(blob_id1).await?);
        assert!(!storage.contains_blob_state(blob_id2).await?);

        // Test blob state writing
        storage
            .maybe_write_blob_states(&[blob_id1], blob_state1.clone())
            .await?;
        storage
            .maybe_write_blob_states(&[blob_id2], blob_state2.clone())
            .await?;

        // Test blob state existence after writing
        assert!(storage.contains_blob_state(blob_id1).await?);
        assert!(storage.contains_blob_state(blob_id2).await?);

        // Test single blob state read
        let read_blob_state = storage.read_blob_state(blob_id1).await?;
        assert_eq!(read_blob_state, Some(blob_state1.clone()));

        // Test multiple blob state read (read_blob_states)
        let read_blob_states = storage.read_blob_states(&[blob_id1, blob_id2]).await?;
        assert_eq!(read_blob_states.len(), 2);

        // Verify blob states
        assert_eq!(read_blob_states[0], Some(blob_state1));
        assert_eq!(read_blob_states[1], Some(blob_state2));

        // Test maybe_write_blobs now that blob states exist (should return true)
        let write_results = storage
            .maybe_write_blobs(std::slice::from_ref(&test_blob1))
            .await?;
        assert_eq!(write_results, vec![true]);

        Ok(())
    }

    async fn test_storage_certificate<S: Storage + Sync>(storage: &S) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        let cert_hash = CryptoHash::test_hash("certificate");

        // Test certificate existence (should be false initially)
        assert!(!storage.contains_certificate(cert_hash).await?);

        // Test reading non-existent certificate
        assert!(storage.read_certificate(cert_hash).await?.is_none());

        // Test reading multiple certificates
        let cert_hashes = vec![cert_hash, CryptoHash::test_hash("cert2")];
        let certs_result = storage.read_certificates(cert_hashes.clone()).await?;
        assert_eq!(certs_result.len(), 2);
        assert!(certs_result[0].is_none());
        assert!(certs_result[1].is_none());

        // Test raw certificate reading
        let raw_certs_result = storage.read_certificates_raw(cert_hashes).await?;
        assert!(raw_certs_result.is_empty()); // No certificates exist

        // Test confirmed block reading
        let block_hash = CryptoHash::test_hash("block");
        let block_result = storage.read_confirmed_block(block_hash).await?;
        assert!(block_result.is_none());

        // Test write_blobs_and_certificate functionality
        // Create test blobs
        let test_blob1 = Blob::new_data(vec![1, 2, 3]);
        let test_blob2 = Blob::new_data(vec![4, 5, 6]);
        let blobs = vec![test_blob1, test_blob2];

        // Create a test certificate using the working pattern from linera-indexer tests
        let chain_id = ChainId(CryptoHash::test_hash("test_chain_cert"));

        // Create a minimal proposed block (genesis block)
        let proposed_block = ProposedBlock {
            epoch: Epoch::ZERO,
            chain_id,
            transactions: vec![],
            previous_block_hash: None,
            height: BlockHeight::ZERO,
            authenticated_owner: None,
            timestamp: Timestamp::default(),
        };

        // Create a minimal block execution outcome with proper BTreeMap types
        let outcome = BlockExecutionOutcome {
            messages: vec![],
            state_hash: CryptoHash::default(),
            oracle_responses: vec![],
            events: vec![],
            blobs: vec![],
            operation_results: vec![],
            previous_event_blocks: BTreeMap::new(),
            previous_message_blocks: BTreeMap::new(),
        };

        let block = Block::new(proposed_block, outcome);
        let confirmed_block = ConfirmedBlock::new(block);
        let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);

        // Test writing blobs and certificate together
        storage
            .write_blobs_and_certificate(&blobs, &certificate)
            .await?;

        // Verify the certificate was written
        let cert_hash = certificate.hash();
        assert!(storage.contains_certificate(cert_hash).await?);

        // Verify the certificate can be read back
        let read_certificate = storage.read_certificate(cert_hash).await?;
        assert!(read_certificate.is_some());
        assert_eq!(read_certificate.unwrap().hash(), cert_hash);

        // Verify the blobs were written
        for blob in &blobs {
            assert!(storage.contains_blob(blob.id()).await?);
        }

        Ok(())
    }

    async fn test_storage_event<S: Storage + Sync>(storage: &S) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        let chain_id = ChainId(CryptoHash::test_hash("test_chain"));
        let stream_id = StreamId::system("test_stream");

        // Test multiple events
        let event_id1 = EventId {
            chain_id,
            stream_id: stream_id.clone(),
            index: 0,
        };
        let event_id2 = EventId {
            chain_id,
            stream_id: stream_id.clone(),
            index: 1,
        };
        let event_id3 = EventId {
            chain_id,
            stream_id: stream_id.clone(),
            index: 2,
        };

        let event_data1 = vec![1, 2, 3];
        let event_data2 = vec![4, 5, 6];
        let event_data3 = vec![7, 8, 9];

        // Test event existence before writing
        assert!(!storage.contains_event(event_id1.clone()).await?);
        assert!(!storage.contains_event(event_id2.clone()).await?);

        // Write multiple events
        storage
            .write_events([
                (event_id1.clone(), event_data1.clone()),
                (event_id2.clone(), event_data2.clone()),
                (event_id3.clone(), event_data3.clone()),
            ])
            .await?;

        // Test event existence after writing
        assert!(storage.contains_event(event_id1.clone()).await?);
        assert!(storage.contains_event(event_id2.clone()).await?);
        assert!(storage.contains_event(event_id3.clone()).await?);

        // Test individual event reading
        let read_event1 = storage.read_event(event_id1).await?;
        assert_eq!(read_event1, Some(event_data1));

        let read_event2 = storage.read_event(event_id2).await?;
        assert_eq!(read_event2, Some(event_data2));

        // Test reading events from index
        let events_from_index = storage
            .read_events_from_index(&chain_id, &stream_id, 1)
            .await?;
        assert!(events_from_index.len() >= 2); // Should contain events at index 1 and 2
        Ok(())
    }

    async fn test_storage_network_description<S: Storage + Sync>(
        storage: &S,
    ) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        let admin_chain_id = ChainId(CryptoHash::test_hash("test_chain_second"));

        let network_desc = NetworkDescription {
            name: "test_network".to_string(),
            genesis_config_hash: CryptoHash::test_hash("genesis_config"),
            genesis_timestamp: Timestamp::from(0),
            genesis_committee_blob_hash: CryptoHash::test_hash("committee"),
            admin_chain_id,
        };

        // Test reading non-existent network description
        assert!(storage.read_network_description().await?.is_none());

        // Write network description
        storage.write_network_description(&network_desc).await?;

        // Test reading existing network description
        let read_desc = storage.read_network_description().await?;
        assert_eq!(read_desc, Some(network_desc));

        Ok(())
    }

    /// Generic test function to test Storage trait features
    #[test_case(DbStorage::<MemoryDatabase, _>::make_test_storage(None).await; "memory")]
    #[cfg_attr(feature = "dynamodb", test_case(DbStorage::<DynamoDbDatabase, _>::make_test_storage(None).await; "dynamo_db"))]
    #[cfg_attr(feature = "scylladb", test_case(DbStorage::<ScyllaDbDatabase, _>::make_test_storage(None).await; "scylla_db"))]
    #[test_log::test(tokio::test)]
    async fn test_storage_features<S: Storage + Sync>(storage: S) -> Result<(), ViewError>
    where
        S::Context: Send + Sync,
    {
        test_storage_chain_exporter(&storage).await?;
        test_storage_blob(&storage).await?;
        test_storage_certificate(&storage).await?;
        test_storage_event(&storage).await?;
        test_storage_network_description(&storage).await?;
        Ok(())
    }
}
