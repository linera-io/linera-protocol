// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    slice::SliceIndex,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use futures::{lock::Mutex, Future};
use linera_base::{
    crypto::*,
    data_types::*,
    identifiers::{ChainDescription, ChainId},
};
use linera_chain::data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate};
use linera_execution::{
    committee::{Committee, ValidatorName},
    ResourceControlPolicy, WasmRuntime,
};
use linera_storage::{MemoryStorage, Storage, TestClock};
use linera_version::VersionInfo;
use linera_views::{memory::TEST_MEMORY_MAX_STREAM_QUERIES, views::ViewError};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
#[cfg(feature = "aws")]
use {
    linera_storage::DynamoDbStorage,
    linera_views::dynamo_db::DynamoDbStoreConfig,
    linera_views::dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
};
#[cfg(feature = "rocksdb")]
use {
    linera_storage::RocksDbStorage, linera_views::rocks_db::create_rocks_db_common_config,
    linera_views::rocks_db::RocksDbStoreConfig, tokio::sync::Semaphore,
};
#[cfg(feature = "scylladb")]
use {
    linera_storage::ScyllaDbStorage, linera_views::scylla_db::create_scylla_db_common_config,
    linera_views::scylla_db::ScyllaDbStoreConfig,
};
#[cfg(not(target_arch = "wasm32"))]
use {
    linera_storage::ServiceStorage,
    linera_storage_service::{
        child::{StorageService, StorageServiceGuard},
        client::service_config_from_endpoint,
        common::get_service_storage_binary,
    },
    linera_views::test_utils::generate_test_namespace,
};

use crate::{
    client::{ChainClient, ChainClientBuilder},
    data_types::*,
    node::{
        CrossChainMessageDelivery, LocalValidatorNodeProvider, NodeError, NotificationStream,
        ValidatorNode,
    },
    notifier::Notifier,
    worker::{Notification, ValidatorWorker, WorkerState},
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FaultType {
    Honest,
    Offline,
    OfflineWithInfo,
    Malicious,
    NoConfirm,
}

/// A validator used for testing. "Faulty" validators ignore block proposals (but not
/// certificates or info queries) and have the wrong initial balance for all chains.
///
/// All methods are executed in spawned Tokio tasks, so that canceling a client task doesn't cause
/// the validator's tasks to be canceled: In a real network, a validator also wouldn't cancel
/// tasks if the client stopped waiting for the response.
struct LocalValidator<S> {
    state: WorkerState<S>,
    fault_type: FaultType,
    notifier: Notifier<Notification>,
}

#[derive(Clone)]
pub struct LocalValidatorClient<S> {
    name: ValidatorName,
    client: Arc<Mutex<LocalValidator<S>>>,
}

impl<S> ValidatorNode for LocalValidatorClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    type NotificationStream = NotificationStream;

    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_block_proposal(proposal, sender)
        })
        .await
    }

    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let certificate = certificate.cloned();
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_lite_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_certificate(certificate, blobs, sender)
        })
        .await
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_chain_info_query(query, sender)
        })
        .await
    }

    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        self.spawn_and_receive(move |validator, sender| validator.do_subscribe(chains, sender))
            .await
    }

    async fn get_version_info(&mut self) -> Result<VersionInfo, NodeError> {
        Ok(Default::default())
    }
}

impl<S> LocalValidatorClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    fn new(name: ValidatorName, state: WorkerState<S>) -> Self {
        let client = LocalValidator {
            fault_type: FaultType::Honest,
            state,
            notifier: Notifier::default(),
        };
        Self {
            name,
            client: Arc::new(Mutex::new(client)),
        }
    }

    async fn set_fault_type(&self, fault_type: FaultType) {
        self.client.lock().await.fault_type = fault_type;
    }

    async fn fault_type(&self) -> FaultType {
        self.client.lock().await.fault_type
    }

    /// Executes the future produced by `f` in a new thread in a new Tokio runtime.
    /// Returns the value that the future puts into the sender.
    async fn spawn_and_receive<F, R, T>(&self, f: F) -> T
    where
        T: Send + 'static,
        R: Future<Output = Result<(), T>> + Send,
        F: FnOnce(Self, oneshot::Sender<T>) -> R + Send + 'static,
    {
        let validator = self.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            if f(validator, sender).await.is_err() {
                tracing::debug!("result could not be sent");
            }
        });
        receiver.await.unwrap()
    }

    async fn do_handle_block_proposal(
        self,
        proposal: BlockProposal,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.client.lock().await;
        let result = match validator.fault_type {
            FaultType::Offline | FaultType::OfflineWithInfo => Err(NodeError::ClientIoError {
                error: "offline".to_string(),
            }),
            FaultType::Malicious => Err(ArithmeticError::Overflow.into()),
            FaultType::Honest | FaultType::NoConfirm => validator
                .state
                .handle_block_proposal(proposal)
                .await
                .map_err(Into::into),
        };
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map(|(info, _actions)| info))
    }

    async fn do_handle_lite_certificate(
        self,
        certificate: LiteCertificate<'_>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.client.lock().await;
        let result = async move {
            let mut notifications = Vec::new();
            let cert = validator.state.full_certificate(certificate).await?;
            let result = match validator.fault_type {
                FaultType::NoConfirm if cert.value().is_validated() => {
                    Err(NodeError::ClientIoError {
                        error: "refusing to confirm".to_string(),
                    })
                }
                FaultType::Honest | FaultType::NoConfirm | FaultType::Malicious => validator
                    .state
                    .fully_handle_certificate_with_notifications(
                        cert,
                        vec![],
                        Some(&mut notifications),
                    )
                    .await
                    .map_err(Into::into),
                FaultType::Offline | FaultType::OfflineWithInfo => Err(NodeError::ClientIoError {
                    error: "offline".to_string(),
                }),
            };
            validator.notifier.handle_notifications(&notifications);
            result
        }
        .await;
        sender.send(result)
    }

    async fn do_handle_certificate(
        self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.client.lock().await;
        let mut notifications = Vec::new();
        let result = match validator.fault_type {
            FaultType::NoConfirm if certificate.value().is_validated() => {
                Err(NodeError::ClientIoError {
                    error: "refusing to confirm".to_string(),
                })
            }
            FaultType::Honest | FaultType::NoConfirm | FaultType::Malicious => validator
                .state
                .fully_handle_certificate_with_notifications(
                    certificate,
                    blobs,
                    Some(&mut notifications),
                )
                .await
                .map_err(Into::into),
            FaultType::Offline | FaultType::OfflineWithInfo => Err(NodeError::ClientIoError {
                error: "offline".to_string(),
            }),
        };
        validator.notifier.handle_notifications(&notifications);
        sender.send(result)
    }

    async fn do_handle_chain_info_query(
        self,
        query: ChainInfoQuery,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let validator = self.client.lock().await;
        let result = if validator.fault_type == FaultType::Offline {
            Err(NodeError::ClientIoError {
                error: "offline".to_string(),
            })
        } else {
            validator
                .state
                .handle_chain_info_query(query)
                .await
                .map_err(Into::into)
        };
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map(|(info, _actions)| info))
    }

    async fn do_subscribe(
        self,
        chains: Vec<ChainId>,
        sender: oneshot::Sender<Result<NotificationStream, NodeError>>,
    ) -> Result<(), Result<NotificationStream, NodeError>> {
        let validator = self.client.lock().await;
        let rx = validator.notifier.subscribe(chains);
        let stream: NotificationStream = Box::pin(UnboundedReceiverStream::new(rx));
        sender.send(Ok(stream))
    }
}

#[derive(Clone)]
pub struct NodeProvider<S>(BTreeMap<ValidatorName, Arc<Mutex<LocalValidator<S>>>>);

impl<S> LocalValidatorNodeProvider for NodeProvider<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    type Node = LocalValidatorClient<S>;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let name = ValidatorName::from_str(address).unwrap();
        let client = self
            .0
            .get(&name)
            .ok_or_else(|| NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            })?
            .clone();
        Ok(LocalValidatorClient { name, client })
    }
}

impl<S> FromIterator<LocalValidatorClient<S>> for NodeProvider<S> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = LocalValidatorClient<S>>,
    {
        let destructure = |validator: LocalValidatorClient<S>| (validator.name, validator.client);
        Self(iter.into_iter().map(destructure).collect())
    }
}

// NOTE:
// * To communicate with a quorum of validators, chain clients iterate over a copy of
// `validator_clients` to spawn I/O tasks.
// * When using `LocalValidatorClient`, clients communicate with an exact quorum then stop.
// * Most tests have 1 faulty validator out 4 so that there is exactly only 1 quorum to
// communicate with.
pub struct TestBuilder<B: StorageBuilder> {
    storage_builder: B,
    pub initial_committee: Committee,
    admin_id: ChainId,
    genesis_storage_builder: GenesisStorageBuilder,
    validator_clients: Vec<LocalValidatorClient<B::Storage>>,
    validator_storages: HashMap<ValidatorName, B::Storage>,
    chain_client_storages: Vec<B::Storage>,
}

#[async_trait]
pub trait StorageBuilder {
    type Storage: Storage + Clone + Send + Sync + 'static;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error>;

    fn clock(&self) -> &TestClock;
}

#[derive(Default)]
struct GenesisStorageBuilder {
    accounts: Vec<GenesisAccount>,
}

struct GenesisAccount {
    description: ChainDescription,
    public_key: PublicKey,
    balance: Amount,
}

impl GenesisStorageBuilder {
    fn add(&mut self, description: ChainDescription, public_key: PublicKey, balance: Amount) {
        self.accounts.push(GenesisAccount {
            description,
            public_key,
            balance,
        })
    }

    async fn build<S>(&self, storage: S, initial_committee: Committee, admin_id: ChainId) -> S
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for account in &self.accounts {
            storage
                .create_chain(
                    initial_committee.clone(),
                    admin_id,
                    account.description,
                    account.public_key,
                    account.balance,
                    Timestamp::from(0),
                )
                .await
                .unwrap();
        }
        storage
    }
}

impl<B> TestBuilder<B>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    pub async fn new(
        mut storage_builder: B,
        count: usize,
        with_faulty_validators: usize,
    ) -> Result<Self, anyhow::Error> {
        let mut key_pairs = Vec::new();
        let mut validators = Vec::new();
        for _ in 0..count {
            let key_pair = KeyPair::generate();
            let name = ValidatorName(key_pair.public());
            validators.push(name);
            key_pairs.push(key_pair);
        }
        let initial_committee = Committee::make_simple(validators);
        let mut validator_clients = Vec::new();
        let mut validator_storages = HashMap::new();
        let mut faulty_validators = HashSet::new();
        for (i, key_pair) in key_pairs.into_iter().enumerate() {
            let name = ValidatorName(key_pair.public());
            let storage = storage_builder.build().await?;
            let state = WorkerState::new(format!("Node {}", i), Some(key_pair), storage.clone())
                .with_allow_inactive_chains(false)
                .with_allow_messages_from_deprecated_epochs(false);
            let validator = LocalValidatorClient::new(name, state);
            if i < with_faulty_validators {
                faulty_validators.insert(name);
                validator.set_fault_type(FaultType::Malicious).await;
            }
            validator_clients.push(validator);
            validator_storages.insert(name, storage);
        }
        tracing::info!(
            "Test will use the following faulty validators: {:?}",
            faulty_validators
        );
        Ok(Self {
            storage_builder,
            initial_committee,
            admin_id: ChainId::root(0),
            genesis_storage_builder: GenesisStorageBuilder::default(),
            validator_clients,
            validator_storages,
            chain_client_storages: Vec::new(),
        })
    }

    pub fn with_policy(mut self, policy: ResourceControlPolicy) -> Self {
        let validators = self.initial_committee.validators().clone();
        self.initial_committee = Committee::new(validators, policy);
        self
    }

    pub async fn set_fault_type<I>(&mut self, range: I, fault_type: FaultType)
    where
        I: SliceIndex<
            [LocalValidatorClient<B::Storage>],
            Output = [LocalValidatorClient<B::Storage>],
        >,
    {
        let mut faulty_validators = vec![];
        for validator in &mut self.validator_clients[range] {
            validator.set_fault_type(fault_type).await;
            faulty_validators.push(validator.name);
        }
        tracing::info!(
            "Making the following validators {:?}: {:?}",
            fault_type,
            faulty_validators
        );
    }

    pub async fn add_initial_chain(
        &mut self,
        description: ChainDescription,
        balance: Amount,
    ) -> Result<ChainClient<NodeProvider<B::Storage>, B::Storage>, anyhow::Error> {
        let key_pair = KeyPair::generate();
        let public_key = key_pair.public();
        // Remember what's in the genesis store for future clients to join.
        self.genesis_storage_builder
            .add(description, public_key, balance);
        for validator in &self.validator_clients {
            let storage = self.validator_storages.get_mut(&validator.name).unwrap();
            if validator.fault_type().await == FaultType::Malicious {
                storage
                    .create_chain(
                        self.initial_committee.clone(),
                        self.admin_id,
                        description,
                        public_key,
                        Amount::ZERO,
                        Timestamp::from(0),
                    )
                    .await
                    .unwrap();
            } else {
                storage
                    .create_chain(
                        self.initial_committee.clone(),
                        self.admin_id,
                        description,
                        public_key,
                        balance,
                        Timestamp::from(0),
                    )
                    .await
                    .unwrap();
            }
        }
        for storage in self.chain_client_storages.iter_mut() {
            storage
                .create_chain(
                    self.initial_committee.clone(),
                    self.admin_id,
                    description,
                    public_key,
                    balance,
                    Timestamp::from(0),
                )
                .await
                .unwrap();
        }
        self.make_client(description.into(), key_pair, None, BlockHeight::ZERO)
            .await
    }

    pub async fn make_client(
        &mut self,
        chain_id: ChainId,
        key_pair: KeyPair,
        block_hash: Option<CryptoHash>,
        block_height: BlockHeight,
    ) -> Result<ChainClient<NodeProvider<B::Storage>, B::Storage>, anyhow::Error> {
        // Note that new clients are only given the genesis store: they must figure out
        // the rest by asking validators.
        let storage = self
            .genesis_storage_builder
            .build(
                self.storage_builder.build().await?,
                self.initial_committee.clone(),
                self.admin_id,
            )
            .await;
        self.chain_client_storages.push(storage.clone());
        let provider = self.validator_clients.iter().cloned().collect();
        let builder = ChainClientBuilder::new(
            provider,
            10,
            CrossChainMessageDelivery::NonBlocking,
            [chain_id],
        );
        Ok(builder.build(
            chain_id,
            vec![key_pair],
            storage,
            self.admin_id,
            block_hash,
            Timestamp::from(0),
            block_height,
            None,
        ))
    }

    /// Tries to find a (confirmation) certificate for the given chain_id and block height.
    pub async fn check_that_validators_have_certificate(
        &self,
        chain_id: ChainId,
        block_height: BlockHeight,
        target_count: usize,
    ) -> Option<Certificate> {
        let query =
            ChainInfoQuery::new(chain_id).with_sent_certificates_in_range(BlockHeightRange {
                start: block_height,
                limit: Some(1),
            });
        let mut count = 0;
        let mut certificate = None;
        for mut validator in self.validator_clients.clone() {
            if let Ok(response) = validator.handle_chain_info_query(query.clone()).await {
                if response.check(validator.name).is_ok() {
                    let ChainInfo {
                        mut requested_sent_certificates,
                        ..
                    } = *response.info;
                    if let Some(cert) = requested_sent_certificates.pop() {
                        if cert.value().is_confirmed()
                            && cert.value().chain_id() == chain_id
                            && cert.value().height() == block_height
                        {
                            cert.check(&self.initial_committee).unwrap();
                            count += 1;
                            certificate = Some(cert);
                        }
                    }
                }
            }
        }
        assert!(count >= target_count);
        certificate
    }

    /// Tries to find a (confirmation) certificate for the given chain_id and block height, and are
    /// in the expected round.
    pub async fn check_that_validators_are_in_round(
        &self,
        chain_id: ChainId,
        block_height: BlockHeight,
        round: Round,
        target_count: usize,
    ) {
        let query = ChainInfoQuery::new(chain_id);
        let mut count = 0;
        for mut validator in self.validator_clients.clone() {
            if let Ok(response) = validator.handle_chain_info_query(query.clone()).await {
                if response.info.manager.current_round == round
                    && response.info.next_block_height == block_height
                    && response.check(validator.name).is_ok()
                {
                    count += 1;
                }
            }
        }
        assert!(count >= target_count);
    }
}

#[cfg(feature = "rocksdb")]
/// Limit concurrency for rocksdb tests to avoid "too many open files" errors.
pub static ROCKS_DB_SEMAPHORE: Semaphore = Semaphore::const_new(5);

#[derive(Default)]
pub struct MemoryStorageBuilder {
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[async_trait]
impl StorageBuilder for MemoryStorageBuilder {
    type Storage = MemoryStorage<TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        Ok(MemoryStorage::new_for_testing(
            self.wasm_runtime,
            TEST_MEMORY_MAX_STREAM_QUERIES,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

impl MemoryStorageBuilder {
    /// Creates a [`MemoryStorageBuilder`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MemoryStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..MemoryStorageBuilder::default()
        }
    }
}

#[cfg(feature = "rocksdb")]
#[derive(Default)]
pub struct RocksDbStorageBuilder {
    temp_dirs: Vec<tempfile::TempDir>,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "rocksdb")]
impl RocksDbStorageBuilder {
    /// Creates a [`RocksDbStorageBuilder`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        RocksDbStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..RocksDbStorageBuilder::default()
        }
    }
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl StorageBuilder for RocksDbStorageBuilder {
    type Storage = RocksDbStorage<TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        let dir = tempfile::TempDir::new()?;
        let path_buf = dir.path().to_path_buf();
        self.temp_dirs.push(dir);
        let common_config = create_rocks_db_common_config();
        let store_config = RocksDbStoreConfig {
            path_buf,
            common_config,
        };
        let namespace = generate_test_namespace();
        let storage = RocksDbStorage::new_for_testing(
            store_config,
            &namespace,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?;
        Ok(storage)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct ServiceStorageBuilder {
    _guard: Option<StorageServiceGuard>,
    endpoint: String,
    namespace: String,
    use_child: bool,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for ServiceStorageBuilder {
    fn default() -> ServiceStorageBuilder {
        let _guard = None;
        let clock = TestClock::default();
        let endpoint = "127.0.0.1:8742".to_string();
        let namespace = generate_test_namespace();
        let use_child = true;
        Self {
            _guard,
            endpoint,
            namespace,
            use_child,
            instance_counter: 0,
            wasm_runtime: None,
            clock,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ServiceStorageBuilder {
    /// Creates a `ServiceStorage` from just an endpoint
    pub fn new(endpoint: &str) -> Self {
        Self::with_wasm_runtime(endpoint, None)
    }

    /// Creates a `ServiceStorage` from an endpoint and the wasm runtime.
    pub fn with_wasm_runtime(endpoint: &str, wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        let _guard = None;
        let endpoint = endpoint.to_string();
        let clock = TestClock::default();
        let namespace = generate_test_namespace();
        let use_child = true;
        Self {
            _guard,
            endpoint,
            namespace,
            use_child,
            instance_counter: 0,
            wasm_runtime: wasm_runtime.into(),
            clock,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl StorageBuilder for ServiceStorageBuilder {
    type Storage = ServiceStorage<TestClock>;

    async fn build(&mut self) -> anyhow::Result<Self::Storage> {
        if self._guard.is_none() && self.use_child {
            let binary = get_service_storage_binary().await?.display().to_string();
            let service = StorageService::new(&self.endpoint, binary);
            self._guard = Some(service.run().await.expect("child"));
        }
        let store_config = service_config_from_endpoint(&self.endpoint)?;
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        self.instance_counter += 1;
        Ok(ServiceStorage::new_for_testing(
            store_config,
            &namespace,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "aws")]
#[derive(Default)]
pub struct DynamoDbStorageBuilder {
    instance_counter: usize,
    localstack: Option<LocalStackTestContext>,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "aws")]
impl DynamoDbStorageBuilder {
    /// Creates a [`DynamoDbStorageBuilder`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        DynamoDbStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..DynamoDbStorageBuilder::default()
        }
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl StorageBuilder for DynamoDbStorageBuilder {
    type Storage = DynamoDbStorage<TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        if self.localstack.is_none() {
            self.localstack = Some(LocalStackTestContext::new().await?);
        }
        let config = self.localstack.as_ref().unwrap().dynamo_db_config();
        let namespace = generate_test_namespace();
        let namespace = format!("{}_{}", namespace, self.instance_counter);
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbStoreConfig {
            config,
            common_config,
        };
        self.instance_counter += 1;
        let storage = DynamoDbStorage::new_for_testing(
            store_config,
            &namespace,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?;
        Ok(storage)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "scylladb")]
pub struct ScyllaDbStorageBuilder {
    instance_counter: usize,
    uri: String,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "scylladb")]
impl Default for ScyllaDbStorageBuilder {
    fn default() -> Self {
        let instance_counter = 0;
        let uri = "localhost:9042".to_string();
        let wasm_runtime = None;
        let clock = TestClock::new();
        ScyllaDbStorageBuilder {
            instance_counter,
            uri,
            wasm_runtime,
            clock,
        }
    }
}

#[cfg(feature = "scylladb")]
impl ScyllaDbStorageBuilder {
    /// Creates a [`ScyllaDbStorageBuilder`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        ScyllaDbStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..ScyllaDbStorageBuilder::default()
        }
    }
}

#[cfg(feature = "scylladb")]
#[async_trait]
impl StorageBuilder for ScyllaDbStorageBuilder {
    type Storage = ScyllaDbStorage<TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        self.instance_counter += 1;
        let namespace = generate_test_namespace();
        let namespace = format!("{}_{}", namespace, self.instance_counter);
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbStoreConfig {
            uri: self.uri.clone(),
            common_config,
        };
        let storage = ScyllaDbStorage::new_for_testing(
            store_config,
            &namespace,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?;
        Ok(storage)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}
