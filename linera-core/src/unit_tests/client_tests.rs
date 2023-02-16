// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_client_tests.rs"]
mod wasm;

use crate::{
    client::{ChainClient, CommunicateAction, ValidatorNodeProvider},
    data_types::*,
    node::{NodeError, NotificationStream, ValidatorNode},
    worker::{ValidatorWorker, WorkerError, WorkerState},
};
use async_trait::async_trait;
use futures::{lock::Mutex, Future};
use linera_base::{committee::Committee, crypto::*, data_types::*};
use linera_chain::data_types::{Block, BlockProposal, Certificate, LiteCertificate, Value};
use linera_execution::{
    system::{Amount, Balance, SystemOperation, UserData},
    ApplicationId, Operation, Query, Response, SystemQuery, SystemResponse, WasmRuntime,
};
use linera_storage::{MemoryStoreClient, RocksdbStoreClient, Store};
use linera_views::views::ViewError;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use test_log::test;
use tokio::sync::oneshot;

#[cfg(feature = "aws")]
use {linera_storage::DynamoDbStoreClient, linera_views::test_utils::LocalStackTestContext};

/// An validator used for testing. "Faulty" validators ignore block proposals (but not
/// certificates or info queries) and have the wrong initial balance for all chains.
///
/// All methods are executed in a separate Tokio runtime, so that canceling a client task
/// doesn't cause the validator's tasks to be canceled: In a real network, a validator also
/// wouldn't cancel tasks if the client stopped waiting for the response.
struct LocalValidator<S> {
    is_faulty: bool,
    state: WorkerState<S>,
}

#[derive(Clone)]
struct LocalValidatorClient<S>(Arc<Mutex<LocalValidator<S>>>);

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
        self.run_in_new_runtime(move |validator, sender| {
            validator.do_handle_block_proposal(proposal, sender)
        })
        .await
    }

    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.run_in_new_runtime(move |validator, sender| {
            validator.do_handle_lite_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blob_certificates: Vec<Certificate>,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.run_in_new_runtime(move |validator, sender| {
            validator.do_handle_certificate(certificate, blob_certificates, sender)
        })
        .await
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.run_in_new_runtime(move |validator, sender| {
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
    async fn run_in_new_runtime<F, R, T>(&self, f: F) -> T
    where
        T: Send + 'static,
        R: Future<Output = Result<(), T>>,
        F: FnOnce(Self, oneshot::Sender<T>) -> R + Send + 'static,
    {
        let validator = self.clone();
        let (sender, receiver) = oneshot::channel();
        let _join_handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            if runtime.block_on(f(validator, sender)).is_err() {
                log::debug!("result could not be sent");
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
            Err(ArithmeticError::SequenceOverflow.into())
        } else {
            validator.state.handle_block_proposal(proposal).await
        };
        sender.send(result.map_err(Into::into))
    }

    async fn do_handle_lite_certificate(
        self,
        certificate: LiteCertificate,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.0.lock().await;
        let result = async move {
            let value = validator
                .state
                .recent_value(&certificate.value.value_hash)
                .ok_or(NodeError::MissingCertificateValue)?
                .clone();
            let full_cert = certificate
                .with_value(value)
                .ok_or(WorkerError::InvalidLiteCertificate)?;
            let response = validator
                .state
                .fully_handle_certificate(full_cert, vec![])
                .await?;
            Ok(response)
        }
        .await;
        sender.send(result)
    }

    async fn do_handle_certificate(
        self,
        certificate: Certificate,
        blob_certificates: Vec<Certificate>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.0.lock().await;
        let result = validator
            .state
            .fully_handle_certificate(certificate, blob_certificates)
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
        sender.send(result.map_err(Into::into))
    }
}

struct NodeProvider<S>(BTreeMap<ValidatorName, LocalValidatorClient<S>>);

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
struct TestBuilder<B: StoreBuilder> {
    store_builder: B,
    initial_committee: Committee,
    admin_id: ChainId,
    genesis_store_builder: GenesisStoreBuilder,
    faulty_validators: HashSet<ValidatorName>,
    validator_clients: Vec<(ValidatorName, LocalValidatorClient<B::Store>)>,
    validator_stores: HashMap<ValidatorName, B::Store>,
    chain_client_stores: Vec<B::Store>,
}

#[async_trait]
trait StoreBuilder {
    type Store: Store + Clone + Send + Sync + 'static;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error>;
}

#[derive(Default)]
struct GenesisStoreBuilder {
    accounts: Vec<GenesisAccount>,
}

struct GenesisAccount {
    description: ChainDescription,
    owner: Owner,
    balance: Balance,
}

impl GenesisStoreBuilder {
    fn add(&mut self, description: ChainDescription, owner: Owner, balance: Balance) {
        self.accounts.push(GenesisAccount {
            description,
            owner,
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
                    account.owner,
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
    async fn new(
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
        log::info!(
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

    async fn add_initial_chain(
        &mut self,
        description: ChainDescription,
        balance: Balance,
    ) -> Result<ChainClient<NodeProvider<B::Store>, B::Store>, anyhow::Error> {
        let key_pair = KeyPair::generate();
        let owner = Owner(key_pair.public());
        // Remember what's in the genesis store for future clients to join.
        self.genesis_store_builder.add(description, owner, balance);
        for (name, store) in self.validator_stores.iter_mut() {
            if self.faulty_validators.contains(name) {
                store
                    .create_chain(
                        self.initial_committee.clone(),
                        self.admin_id,
                        description,
                        owner,
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
                        owner,
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
                    owner,
                    balance,
                    Timestamp::from(0),
                )
                .await
                .unwrap();
        }
        self.make_client(description.into(), key_pair, None, BlockHeight::from(0))
            .await
    }

    async fn make_client(
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
    async fn check_that_validators_have_certificate(
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
                        if let Value::ConfirmedBlock { block, .. } = &cert.value {
                            if block.chain_id == chain_id && block.height == block_height {
                                cert.check(&self.initial_committee).unwrap();
                                count += 1;
                                certificate = Some(cert);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(count, target_count);
        certificate
    }
}

/// Need a guard to avoid "too many open files" error
static GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct MakeMemoryStoreClient;

#[async_trait]
impl StoreBuilder for MakeMemoryStoreClient {
    type Store = MemoryStoreClient;

    async fn build(&mut self) -> Result<Self::Store, anyhow::Error> {
        Ok(MemoryStoreClient::default())
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

#[test(tokio::test)]
async fn test_memory_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_initiating_valid_transfer(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_initiating_valid_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let certificate = sender
        .transfer_to_chain(
            Amount::from(3),
            ChainId::root(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_rotate_key_pair() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_rotate_key_pair(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_rotate_key_pair() -> Result<(), anyhow::Error> {
    run_test_rotate_key_pair(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_rotate_key_pair<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.identity().await.unwrap(), new_owner);
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the chain.
    sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_transfer_ownership() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_transfer_ownership(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_ownership() -> Result<(), anyhow::Error> {
    run_test_transfer_ownership(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_transfer_ownership<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;

    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    let certificate = sender.transfer_ownership(new_owner).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(4)
    );
    // Cannot use the chain any more.
    assert!(sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .is_err());
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_share_ownership() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_share_ownership(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_share_ownership() -> Result<(), anyhow::Error> {
    run_test_share_ownership(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_share_ownership<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    let certificate = sender.share_ownership(new_owner).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the chain with the old client.
    sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    // Make a client to try the new key.
    let mut client = builder
        .make_client(
            sender.chain_id,
            new_key_pair,
            sender.block_hash,
            BlockHeight::from(2),
        )
        .await?;
    // Local balance fails because the client has block height 2 but we haven't downloaded
    // the blocks yet.
    assert!(client.local_balance().await.is_err());
    assert_eq!(
        client.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(1)
    );
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(1));
    client
        .transfer_to_chain(Amount::from(1), ChainId::root(3), UserData::default())
        .await
        .unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_open_chain_then_close_it(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_close_it() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_close_it(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_open_chain_then_close_it<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(0))
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    // Open the new chain.
    let (new_id, certificate) = sender.open_chain(new_owner).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(0)
    );
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
    client.close_chain().await.unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_transfer_then_open_chain(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfer_then_open_chain() -> Result<(), anyhow::Error> {
    run_test_transfer_then_open_chain(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_transfer_then_open_chain<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(0))
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    let new_id = ChainId::child(EffectId {
        chain_id: ChainId::root(1),
        height: BlockHeight::from(1),
        index: 0,
    });
    // Transfer before creating the chain.
    sender
        .transfer_to_chain(Amount::from(3), new_id, UserData::default())
        .await
        .unwrap();
    // Open the new chain.
    let (new_id2, certificate) = sender.open_chain(new_owner).await.unwrap();
    assert_eq!(new_id, new_id2);
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(1), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert!(matches!(&certificate.value, Value::ConfirmedBlock{
        block: Block {
            operations,
            ..
        }, ..} if matches!(&operations[..], &[(_, Operation::System(SystemOperation::OpenChain { id, .. }))] if new_id == id)
    ));
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await?;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(3));
    client
        .transfer_to_chain(Amount::from(3), ChainId::root(3), UserData::default())
        .await
        .unwrap();
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_open_chain_then_transfer(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_open_chain_then_transfer() -> Result<(), anyhow::Error> {
    run_test_open_chain_then_transfer(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_open_chain_then_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    // New chains use the admin chain to verify their creation certificate.
    builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(0))
        .await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let new_key_pair = KeyPair::generate();
    let new_owner = Owner(new_key_pair.public());
    // Open the new chain.
    let (new_id, creation_certificate) = sender.open_chain(new_owner).await.unwrap();
    // Transfer after creating the chain.
    let transfer_certificate = sender
        .transfer_to_chain(Amount::from(3), new_id, UserData::default())
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await?;
    // Must process the creation certificate before using the new chain.
    client
        .receive_certificate(creation_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
    client
        .receive_certificate(transfer_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(3));
    client
        .transfer_to_chain(Amount::from(3), ChainId::root(3), UserData::default())
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_close_chain() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_close_chain(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_close_chain() -> Result<(), anyhow::Error> {
    run_test_close_chain(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_close_chain<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    let certificate = sender.close_chain().await.unwrap();
    assert!(matches!(
        &certificate.value,
        Value::ConfirmedBlock {
            block: Block {
                operations,
                ..
            },
            ..
        } if matches!(&operations[..], &[(_, Operation::System(SystemOperation::CloseChain))])
    ));
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Cannot use the chain any more.
    assert!(sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .is_err());
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_initiating_valid_transfer_too_many_faults(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_initiating_valid_transfer_too_many_faults() -> Result<(), anyhow::Error> {
    run_test_initiating_valid_transfer_too_many_faults(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_initiating_valid_transfer_too_many_faults<B>(
    store_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 2).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await?;
    assert!(sender
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(3),
            ChainId::root(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .is_err());
    assert_eq!(sender.next_block_height, BlockHeight::from(0));
    assert!(sender.pending_block.is_some());
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_bidirectional_transfer() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_bidirectional_transfer(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_bidirectional_transfer() -> Result<(), anyhow::Error> {
    run_test_bidirectional_transfer(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_bidirectional_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await?;
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(3));
    assert_eq!(
        client1
            .query_application(ApplicationId::System, &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Balance::from(3),
        })
    );

    let certificate = client1
        .transfer_to_chain(Amount::from(3), client2.chain_id, UserData::default())
        .await
        .unwrap();

    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(0));
    assert_eq!(
        client1
            .query_application(ApplicationId::System, &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Balance::from(0),
        })
    );

    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Local balance is lagging.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Force synchronization of local balance.
    assert_eq!(
        client2.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(3)
    );
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(3));
    // The local balance from the client is reflecting incoming messages but the
    // SystemResponse only reads the ChainState.
    assert_eq!(
        client2
            .query_application(ApplicationId::System, &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Balance::from(0),
        })
    );

    // Send back some money.
    assert_eq!(client2.next_block_height, BlockHeight::from(0));
    client2
        .transfer_to_chain(Amount::from(1), client1.chain_id, UserData::default())
        .await
        .unwrap();
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(2));
    assert_eq!(
        client1.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(1)
    );
    // Local balance from client2 is now consolidated.
    assert_eq!(
        client2
            .query_application(ApplicationId::System, &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Balance::from(2),
        })
    );
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_receiving_unconfirmed_transfer(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer() -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_receiving_unconfirmed_transfer<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await?;
    let certificate = client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(2),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    // Transfer was executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(2));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(MakeMemoryStoreClient)
        .await
}

#[test(tokio::test)]
async fn test_rocksdb_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeRocksdbStoreClient::default(),
    )
    .await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_receiving_unconfirmed_transfer_with_lagging_sender_balances(
) -> Result<(), anyhow::Error> {
    run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances(
        MakeDynamoDbStoreClient::default(),
    )
    .await
}

async fn run_test_receiving_unconfirmed_transfer_with_lagging_sender_balances<B>(
    store_builder: B,
) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await?;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await?;
    let mut client3 = builder
        .add_initial_chain(ChainDescription::Root(3), Balance::from(0))
        .await?;

    // Transferring funds from client1 to client2.
    // Confirming to a quorum of nodes only at the end.
    client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(1),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(1),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .communicate_chain_updates(
            &builder.initial_committee,
            client1.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight(client1.next_block_height),
        )
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Sending money from client2 fails, as a consequence.
    assert!(client2
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(2),
            client3.chain_id,
            UserData::default(),
        )
        .await
        .is_err());
    // Retrying the same block doesn't work.
    assert!(client2.retry_pending_block().await.is_err());
    client2.clear_pending_block().await;
    // Retrying the whole command works after synchronization.
    assert_eq!(
        client2.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(2)
    );
    let certificate = client2
        .transfer_to_chain(Amount::from(2), client3.chain_id, UserData::default())
        .await
        .unwrap();
    // Blocks were executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(client1.next_block_height, BlockHeight::from(2));
    assert!(client1.pending_block.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Let the receiver confirm in last resort.
    client3.receive_certificate(certificate).await.unwrap();
    assert_eq!(client3.local_balance().await.unwrap(), Balance::from(2));
    Ok(())
}

#[test(tokio::test)]
async fn test_memory_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_change_voting_rights() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_change_voting_rights(MakeRocksdbStoreClient::default()).await
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_change_voting_rights() -> Result<(), anyhow::Error> {
    run_test_change_voting_rights(MakeDynamoDbStoreClient::default()).await
}

async fn run_test_change_voting_rights<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut admin = builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(3))
        .await?;
    let mut user = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(0))
        .await?;

    // Create a new committee.
    let validators = builder.initial_committee.validators;
    admin.stage_new_committee(validators).await.unwrap();
    assert_eq!(admin.next_block_height, BlockHeight::from(1));
    assert!(admin.pending_block.is_none());
    assert!(admin.key_pair().await.is_ok());
    assert_eq!(admin.epoch().await.unwrap(), Epoch::from(1));

    // Sending money from the admin chain is supported.
    let cert = admin
        .transfer_to_chain(Amount::from(2), ChainId::root(1), UserData(None))
        .await
        .unwrap();
    admin
        .transfer_to_chain(Amount::from(1), ChainId::root(1), UserData(None))
        .await
        .unwrap();

    // User is still at the initial epoch, but we can receive transfers from future
    // epochs AFTER synchronizing the client with the admin chain.
    assert!(user.receive_certificate(cert).await.is_err());
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(0));
    assert_eq!(
        user.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(3)
    );

    // User is a genesis chain so the migration message is not even in the inbox yet.
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(0));

    // Now subscribe explicitly to migrations.
    let cert = user.subscribe_to_new_committees().await.unwrap();
    admin.receive_certificate(cert).await.unwrap();
    admin.process_inbox().await.unwrap();

    // Have the admin chain deprecate the previous epoch.
    admin.finalize_committee().await.unwrap();

    // Try to make a transfer back to the admin chain.
    let cert = user
        .transfer_to_chain(Amount::from(2), ChainId::root(0), UserData(None))
        .await
        .unwrap();
    assert!(admin.receive_certificate(cert).await.is_err());
    // Transfer is blocked because the epoch #0 has been retired by admin.
    assert_eq!(
        admin.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(0)
    );

    // Have the user receive the notification to migrate to epoch #1.
    user.synchronize_and_recompute_balance().await.unwrap();
    user.process_inbox().await.unwrap();
    assert_eq!(user.epoch().await.unwrap(), Epoch::from(1));

    // Try again to make a transfer back to the admin chain.
    let cert = user
        .transfer_to_chain(Amount::from(1), ChainId::root(0), UserData(None))
        .await
        .unwrap();
    admin.receive_certificate(cert).await.unwrap();
    // Transfer goes through and the previous one as well thanks to block chaining.
    assert_eq!(
        admin.synchronize_and_recompute_balance().await.unwrap(),
        Balance::from(3)
    );
    Ok(())
}
