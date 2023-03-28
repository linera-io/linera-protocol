// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    client::{ChainClient, ValidatorNodeProvider},
    data_types::*,
    node::{NodeError, NotificationStream, ValidatorNode},
    worker::{ValidatorWorker, WorkerError, WorkerState},
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
    WasmRuntime,
};
use linera_storage::{MemoryStoreClient, RocksdbStoreClient, Store};
use linera_views::views::ViewError;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{oneshot, Semaphore};

#[cfg(feature = "aws")]
use {linera_storage::DynamoDbStoreClient, linera_views::test_utils::LocalStackTestContext};

/// An validator used for testing. "Faulty" validators ignore block proposals (but not
/// certificates or info queries) and have the wrong initial balance for all chains.
///
/// All methods are executed in spawned Tokio tasks, so that canceling a client task doesn't cause
/// the validator's tasks to be canceled: In a real network, a validator also wouldn't cancel
/// tasks if the client stopped waiting for the response.
struct LocalValidator<S> {
    is_faulty: bool,
    state: WorkerState<S>,
}

#[derive(Clone)]
pub struct LocalValidatorClient<S>(Arc<Mutex<LocalValidator<S>>>);

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
        certificate: LiteCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
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

    async fn subscribe(&mut self, _chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        Err(NodeError::SubscriptionError {
            transport: "local".to_string(),
        })
    }
}

impl<S> LocalValidatorClient<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    fn new(is_faulty: bool, state: WorkerState<S>) -> Self {
        let validator = LocalValidator { is_faulty, state };
        Self(Arc::new(Mutex::new(validator)))
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
        let mut validator = self.0.lock().await;
        let result = if validator.is_faulty {
            Err(ArithmeticError::Overflow.into())
        } else {
            validator.state.handle_block_proposal(proposal).await
        };
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map_err(Into::into).map(|(info, _actions)| info))
    }

    async fn do_handle_lite_certificate(
        self,
        certificate: LiteCertificate,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.0.lock().await;
        let result = match validator.state.recent_value(&certificate.value.value_hash) {
            None => Err(NodeError::MissingCertificateValue),
            Some(value) => match certificate.with_value(value.clone()) {
                None => Err(WorkerError::InvalidLiteCertificate.into()),
                Some(full_cert) => validator
                    .state
                    .fully_handle_certificate(full_cert, vec![])
                    .await
                    .map_err(NodeError::from),
            },
        };
        sender.send(result)
    }

    async fn do_handle_certificate(
        self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.0.lock().await;
        let result = validator
            .state
            .fully_handle_certificate(certificate, blobs)
            .await;
        sender.send(result.map_err(Into::into))
    }

    async fn do_handle_chain_info_query(
        self,
        query: ChainInfoQuery,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.0.lock().await;
        let result = validator.state.handle_chain_info_query(query).await;
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map_err(Into::into).map(|(info, _actions)| info))
    }
}

pub struct NodeProvider<S>(BTreeMap<ValidatorName, LocalValidatorClient<S>>);

#[async_trait]
impl<S> ValidatorNodeProvider for NodeProvider<S>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    type Node = LocalValidatorClient<S>;

    async fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        let name = ValidatorName::from_str(address).unwrap();
        let node = self
            .0
            .get(&name)
            .ok_or_else(|| NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            })?;
        Ok(node.clone())
    }
}

// NOTE:
// * To communicate with a quorum of validators, chain clients iterate over a copy of
// `validator_clients` to spawn I/O tasks.
// * When using `LocalValidatorClient`, clients communicate with an exact quorum then stops.
// * Most tests have 1 faulty validator out 4 so that there is exactly only 1 quorum to
// communicate with.
pub struct TestBuilder<B: StoreBuilder> {
    store_builder: B,
    pub initial_committee: Committee,
    admin_id: ChainId,
    genesis_store_builder: GenesisStoreBuilder,
    faulty_validators: HashSet<ValidatorName>,
    validator_clients: Vec<(ValidatorName, LocalValidatorClient<B::Store>)>,
    validator_stores: HashMap<ValidatorName, B::Store>,
    chain_client_stores: Vec<B::Store>,
}

#[async_trait]
pub trait StoreBuilder {
    type Store: Store + Clone + Send + Sync + 'static;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error>;
}

#[derive(Default)]
struct GenesisStoreBuilder {
    accounts: Vec<GenesisAccount>,
}

struct GenesisAccount {
    description: ChainDescription,
    public_key: PublicKey,
    balance: Balance,
}

impl GenesisStoreBuilder {
    fn add(&mut self, description: ChainDescription, public_key: PublicKey, balance: Balance) {
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
            let validator = if i < with_faulty_validators {
                faulty_validators.insert(name);
                LocalValidatorClient::new(true, state)
            } else {
                LocalValidatorClient::new(false, state)
            };
            validator_clients.push((name, validator));
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
            faulty_validators,
            validator_clients,
            validator_stores,
            chain_client_stores: Vec::new(),
        })
    }

    pub async fn add_initial_chain(
        &mut self,
        description: ChainDescription,
        balance: Balance,
    ) -> Result<ChainClient<NodeProvider<B::Store>, B::Store>, anyhow::Error> {
        let key_pair = KeyPair::generate();
        let public_key = key_pair.public();
        // Remember what's in the genesis store for future clients to join.
        self.genesis_store_builder
            .add(description, public_key, balance);
        for (name, store) in self.validator_stores.iter_mut() {
            if self.faulty_validators.contains(name) {
                store
                    .create_chain(
                        self.initial_committee.clone(),
                        self.admin_id,
                        description,
                        public_key,
                        Balance::from(0),
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
        self.make_client(description.into(), key_pair, None, BlockHeight::from(0))
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
        let provider = NodeProvider(self.validator_clients.iter().cloned().collect());
        Ok(ChainClient::new(
            chain_id,
            vec![key_pair],
            provider,
            store,
            self.admin_id,
            10,
            block_hash,
            Timestamp::from(0),
            block_height,
            std::time::Duration::from_millis(500),
            10,
        ))
    }

    /// Try to find a (confirmation) certificate for the given chain_id and block height.
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
        for (name, mut client) in self.validator_clients.clone() {
            if let Ok(response) = client.handle_chain_info_query(query.clone()).await {
                if response.check(name).is_ok() {
                    let ChainInfo {
                        mut requested_sent_certificates,
                        ..
                    } = response.info;
                    if let Some(cert) = requested_sent_certificates.pop() {
                        if cert.value.is_confirmed()
                            && cert.value.block().chain_id == chain_id
                            && cert.value.block().height == block_height
                        {
                            cert.check(&self.initial_committee).unwrap();
                            count += 1;
                            certificate = Some(cert);
                        }
                    }
                }
            }
        }
        assert_eq!(count, target_count);
        certificate
    }
}

/// Limit concurrency for rocksdb tests to avoid "too many open files" errors.
pub static ROCKSDB_SEMAPHORE: Semaphore = Semaphore::const_new(20);

#[derive(Default)]
pub struct MakeMemoryStoreClient {
    wasm_runtime: Option<WasmRuntime>,
}

#[async_trait]
impl StoreBuilder for MakeMemoryStoreClient {
    type Store = MemoryStoreClient;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        Ok(MemoryStoreClient::new(self.wasm_runtime))
    }
}

impl MakeMemoryStoreClient {
    /// Creates a [`MakeMemoryStoreClient`] that uses the specified [`WasmRuntime`] to run WASM
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeMemoryStoreClient {
            wasm_runtime: wasm_runtime.into(),
        }
    }
}

#[derive(Default)]
pub struct MakeRocksdbStoreClient {
    temp_dirs: Vec<tempfile::TempDir>,
    wasm_runtime: Option<WasmRuntime>,
}

impl MakeRocksdbStoreClient {
    /// Creates a [`MakeRocksdbStoreClient`] that uses the specified [`WasmRuntime`] to run WASM
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeRocksdbStoreClient {
            wasm_runtime: wasm_runtime.into(),
            ..MakeRocksdbStoreClient::default()
        }
    }
}

#[async_trait]
impl StoreBuilder for MakeRocksdbStoreClient {
    type Store = RocksdbStoreClient;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_path_buf();
        self.temp_dirs.push(dir);
        Ok(RocksdbStoreClient::new(path, self.wasm_runtime))
    }
}

#[cfg(feature = "aws")]
#[derive(Default)]
pub struct MakeDynamoDbStoreClient {
    instance_counter: usize,
    localstack: Option<LocalStackTestContext>,
    wasm_runtime: Option<WasmRuntime>,
}

#[cfg(feature = "aws")]
impl MakeDynamoDbStoreClient {
    /// Creates a [`MakeDynamoDbStoreClient`] that uses the specified [`WasmRuntime`] to run WASM
    /// applications.
    #[allow(dead_code)]
    pub fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        MakeDynamoDbStoreClient {
            wasm_runtime: wasm_runtime.into(),
            ..MakeDynamoDbStoreClient::default()
        }
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl StoreBuilder for MakeDynamoDbStoreClient {
    type Store = DynamoDbStoreClient;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        if self.localstack.is_none() {
            self.localstack = Some(LocalStackTestContext::new().await?);
        }
        let config = self.localstack.as_ref().unwrap().dynamo_db_config();
        let table = format!("linera{}", self.instance_counter).parse()?;
        self.instance_counter += 1;
        let (store, _) = DynamoDbStoreClient::from_config(config, table, self.wasm_runtime).await?;
        Ok(store)
    }
}
