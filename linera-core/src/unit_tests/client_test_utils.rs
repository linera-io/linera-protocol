// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    client::{ChainClient, ValidatorNodeProvider},
    data_types::*,
    node::{NodeError, NotificationStream, ValidatorNode},
    notifier::Notifier,
    worker::{Notification, ValidatorWorker, WorkerState},
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
    pricing::Pricing,
    WasmRuntime,
};
use linera_storage::{MemoryStoreClient, Store, TestClock};
use linera_views::{memory::TEST_MEMORY_MAX_STREAM_QUERIES, views::ViewError};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    slice::SliceIndex,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[cfg(feature = "rocksdb")]
use {
    linera_storage::RocksDbStore, linera_views::rocks_db::create_rocks_db_common_config,
    linera_views::rocks_db::RocksDbKvStoreConfig, tokio::sync::Semaphore,
};

#[cfg(feature = "aws")]
use {
    linera_storage::DynamoDbStore,
    linera_views::dynamo_db::DynamoDbKvStoreConfig,
    linera_views::dynamo_db::{create_dynamo_db_common_config, LocalStackTestContext},
};

#[cfg(feature = "scylladb")]
use {
    linera_storage::ScyllaDbStore, linera_views::scylla_db::create_scylla_db_common_config,
    linera_views::scylla_db::ScyllaDbKvStoreConfig,
};

#[cfg(any(feature = "aws", feature = "scylladb"))]
use linera_views::common::get_table_name;

use super::ChainClientBuilder;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FaultType {
    Honest,
    Offline,
    Malicious,
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

#[async_trait]
impl<S> ValidatorNode for LocalValidatorClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
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
}

impl<S> LocalValidatorClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
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
            FaultType::Offline => Err(NodeError::ClientIoError {
                error: "offline".to_string(),
            }),
            FaultType::Malicious => Err(ArithmeticError::Overflow.into()),
            FaultType::Honest => validator
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
            let result = validator
                .state
                .fully_handle_certificate_with_notifications(cert, vec![], Some(&mut notifications))
                .await;
            validator.notifier.handle_notifications(&notifications);
            result
        }
        .await;
        sender.send(result.map_err(Into::into))
    }

    async fn do_handle_certificate(
        self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.client.lock().await;
        let mut notifications = Vec::new();
        let result = validator
            .state
            .fully_handle_certificate_with_notifications(
                certificate,
                blobs,
                Some(&mut notifications),
            )
            .await;
        validator.notifier.handle_notifications(&notifications);
        sender.send(result.map_err(Into::into))
    }

    async fn do_handle_chain_info_query(
        self,
        query: ChainInfoQuery,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let validator = self.client.lock().await;
        let result = validator.state.handle_chain_info_query(query).await;
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map_err(Into::into).map(|(info, _actions)| info))
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

impl<S> ValidatorNodeProvider for NodeProvider<S>
where
    S: Store + Clone + Send + Sync + 'static,
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
pub struct TestBuilder<B: StoreBuilder> {
    store_builder: B,
    pub initial_committee: Committee,
    admin_id: ChainId,
    genesis_store_builder: GenesisStoreBuilder,
    validator_clients: Vec<LocalValidatorClient<B::Store>>,
    validator_stores: HashMap<ValidatorName, B::Store>,
    chain_client_stores: Vec<B::Store>,
}

#[async_trait]
pub trait StoreBuilder {
    type Store: Store + Clone + Send + Sync + 'static;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error>;

    fn clock(&self) -> &TestClock;
}

#[derive(Default)]
struct GenesisStoreBuilder {
    accounts: Vec<GenesisAccount>,
}

struct GenesisAccount {
    description: ChainDescription,
    public_key: PublicKey,
    balance: Amount,
}

impl GenesisStoreBuilder {
    fn add(&mut self, description: ChainDescription, public_key: PublicKey, balance: Amount) {
        self.accounts.push(GenesisAccount {
            description,
            public_key,
            balance,
        })
    }

    async fn build<S>(&self, store: S, initial_committee: Committee, admin_id: ChainId) -> S
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for account in &self.accounts {
            store
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
        store
    }
}

impl<B> TestBuilder<B>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    pub async fn new(
        mut store_builder: B,
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
        let mut validator_stores = HashMap::new();
        let mut faulty_validators = HashSet::new();
        for (i, key_pair) in key_pairs.into_iter().enumerate() {
            let name = ValidatorName(key_pair.public());
            let store = store_builder.build().await?;
            let state = WorkerState::new(format!("Node {}", i), Some(key_pair), store.clone())
                .with_allow_inactive_chains(false)
                .with_allow_messages_from_deprecated_epochs(false);
            let validator = LocalValidatorClient::new(name, state);
            if i < with_faulty_validators {
                faulty_validators.insert(name);
                validator.set_fault_type(FaultType::Malicious).await;
            }
            validator_clients.push(validator);
            validator_stores.insert(name, store);
        }
        tracing::info!(
            "Test will use the following faulty validators: {:?}",
            faulty_validators
        );
        Ok(Self {
            store_builder,
            initial_committee,
            admin_id: ChainId::root(0),
            genesis_store_builder: GenesisStoreBuilder::default(),
            validator_clients,
            validator_stores,
            chain_client_stores: Vec::new(),
        })
    }

    pub fn with_pricing(mut self, pricing: Pricing) -> Self {
        let validators = self.initial_committee.validators().clone();
        self.initial_committee = Committee::new(validators, pricing);
        self
    }

    pub async fn set_fault_type<I>(&mut self, range: I, fault_type: FaultType)
    where
        I: SliceIndex<[LocalValidatorClient<B::Store>], Output = [LocalValidatorClient<B::Store>]>,
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
    ) -> Result<ChainClient<NodeProvider<B::Store>, B::Store>, anyhow::Error> {
        let key_pair = KeyPair::generate();
        let public_key = key_pair.public();
        // Remember what's in the genesis store for future clients to join.
        self.genesis_store_builder
            .add(description, public_key, balance);
        for validator in &self.validator_clients {
            let store = self.validator_stores.get_mut(&validator.name).unwrap();
            if validator.fault_type().await == FaultType::Malicious {
                store
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
                store
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
        for store in self.chain_client_stores.iter_mut() {
            store
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
    ) -> Result<ChainClient<NodeProvider<B::Store>, B::Store>, anyhow::Error> {
        // Note that new clients are only given the genesis store: they must figure out
        // the rest by asking validators.
        let store = self
            .genesis_store_builder
            .build(
                self.store_builder.build().await?,
                self.initial_committee.clone(),
                self.admin_id,
            )
            .await;
        self.chain_client_stores.push(store.clone());
        let provider = self.validator_clients.iter().cloned().collect();
        let cross_chain_delay = std::time::Duration::from_millis(500);
        let builder = ChainClientBuilder::new(provider, 10, cross_chain_delay, 10);
        Ok(builder.build(
            chain_id,
            vec![key_pair],
            store,
            self.admin_id,
            block_hash,
            Timestamp::from(0),
            block_height,
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
                    } = response.info;
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
        round: RoundNumber,
        target_count: usize,
    ) {
        let query = ChainInfoQuery::new(chain_id);
        let mut count = 0;
        for mut validator in self.validator_clients.clone() {
            if let Ok(response) = validator.handle_chain_info_query(query.clone()).await {
                if response.info.manager.current_round() == round
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
pub struct MakeMemoryStoreClient {
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[async_trait]
impl StoreBuilder for MakeMemoryStoreClient {
    type Store = MemoryStoreClient<TestClock>;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        Ok(MemoryStoreClient::new(
            self.wasm_runtime,
            TEST_MEMORY_MAX_STREAM_QUERIES,
            self.clock.clone(),
        ))
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

impl MakeMemoryStoreClient {
    /// Creates a [`MakeMemoryStoreClient`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeMemoryStoreClient {
            wasm_runtime: wasm_runtime.into(),
            ..MakeMemoryStoreClient::default()
        }
    }
}

#[cfg(feature = "rocksdb")]
#[derive(Default)]
pub struct MakeRocksDbStore {
    temp_dirs: Vec<tempfile::TempDir>,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "rocksdb")]
impl MakeRocksDbStore {
    /// Creates a [`MakeRocksDbStore`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeRocksDbStore {
            wasm_runtime: wasm_runtime.into(),
            ..MakeRocksDbStore::default()
        }
    }
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl StoreBuilder for MakeRocksDbStore {
    type Store = RocksDbStore<TestClock>;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        let dir = tempfile::TempDir::new()?;
        let path_buf = dir.path().to_path_buf();
        self.temp_dirs.push(dir);
        let common_config = create_rocks_db_common_config();
        let store_config = RocksDbKvStoreConfig {
            path_buf,
            common_config,
        };
        let (store_client, _) =
            RocksDbStore::new_for_testing(store_config, self.wasm_runtime, self.clock.clone())
                .await?;
        Ok(store_client)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "aws")]
#[derive(Default)]
pub struct MakeDynamoDbStore {
    instance_counter: usize,
    localstack: Option<LocalStackTestContext>,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "aws")]
impl MakeDynamoDbStore {
    /// Creates a [`MakeDynamoDbStore`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeDynamoDbStore {
            wasm_runtime: wasm_runtime.into(),
            ..MakeDynamoDbStore::default()
        }
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl StoreBuilder for MakeDynamoDbStore {
    type Store = DynamoDbStore<TestClock>;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        if self.localstack.is_none() {
            self.localstack = Some(LocalStackTestContext::new().await?);
        }
        let config = self.localstack.as_ref().unwrap().dynamo_db_config();
        let table = get_table_name();
        let table = format!("{}_{}", table, self.instance_counter);
        let table_name = table.parse()?;
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbKvStoreConfig {
            config,
            table_name,
            common_config,
        };
        self.instance_counter += 1;
        let (store_client, _) =
            DynamoDbStore::new_for_testing(store_config, self.wasm_runtime, self.clock.clone())
                .await?;
        Ok(store_client)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "scylladb")]
pub struct MakeScyllaDbStore {
    instance_counter: usize,
    uri: String,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "scylladb")]
impl Default for MakeScyllaDbStore {
    fn default() -> Self {
        let instance_counter = 0;
        let uri = "localhost:9042".to_string();
        let wasm_runtime = None;
        let clock = TestClock::new();
        MakeScyllaDbStore {
            instance_counter,
            uri,
            wasm_runtime,
            clock,
        }
    }
}

#[cfg(feature = "scylladb")]
impl MakeScyllaDbStore {
    /// Creates a [`MakeScyllaDbStore`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeScyllaDbStore {
            wasm_runtime: wasm_runtime.into(),
            ..MakeScyllaDbStore::default()
        }
    }
}

#[cfg(feature = "scylladb")]
#[async_trait]
impl StoreBuilder for MakeScyllaDbStore {
    type Store = ScyllaDbStore<TestClock>;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        self.instance_counter += 1;
        let table_name = get_table_name();
        let table_name = format!("{}_{}", table_name, self.instance_counter);
        let common_config = create_scylla_db_common_config();
        let store_config = ScyllaDbKvStoreConfig {
            uri: self.uri.clone(),
            table_name,
            common_config,
        };
        let (store_client, _) =
            ScyllaDbStore::new_for_testing(store_config, self.wasm_runtime, self.clock.clone())
                .await?;
        Ok(store_client)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}
