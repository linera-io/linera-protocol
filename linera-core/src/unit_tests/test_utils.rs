// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    vec,
};

use async_trait::async_trait;
use futures::{
    future::Either,
    lock::{Mutex, MutexGuard},
    Future,
};
use linera_base::{
    crypto::*,
    data_types::*,
    identifiers::{BlobId, ChainDescription, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        CertificateKind, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, ValidatedBlock,
    },
};
use linera_execution::{
    committee::{Committee, ValidatorName},
    ResourceControlPolicy, WasmRuntime,
};
use linera_storage::{DbStorage, Storage, TestClock};
#[cfg(all(not(target_arch = "wasm32"), feature = "storage-service"))]
use linera_storage_service::client::ServiceStoreClient;
use linera_version::VersionInfo;
#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{
    memory::MemoryStore, random::generate_test_namespace, store::TestKeyValueStore as _,
};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
#[cfg(feature = "rocksdb")]
use {
    linera_views::rocks_db::RocksDbStore,
    tokio::sync::{Semaphore, SemaphorePermit},
};

use crate::{
    client::{ChainClient, Client},
    data_types::*,
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider,
    },
    notifier::ChannelNotifier,
    updater::DEFAULT_GRACE_PERIOD,
    worker::{NetworkActions, Notification, ProcessableCertificate, WorkerState},
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FaultType {
    Honest,
    Offline,
    OfflineWithInfo,
    Malicious,
    DontSendConfirmVote,
    DontProcessValidated,
    DontSendValidateVote,
}

/// A validator used for testing. "Faulty" validators ignore block proposals (but not
/// certificates or info queries) and have the wrong initial balance for all chains.
///
/// All methods are executed in spawned Tokio tasks, so that canceling a client task doesn't cause
/// the validator's tasks to be canceled: In a real network, a validator also wouldn't cancel
/// tasks if the client stopped waiting for the response.
struct LocalValidator<S>
where
    S: Storage,
{
    state: WorkerState<S>,
    fault_type: FaultType,
    notifier: Arc<ChannelNotifier<Notification>>,
}

#[derive(Clone)]
pub struct LocalValidatorClient<S>
where
    S: Storage,
{
    name: ValidatorName,
    client: Arc<Mutex<LocalValidator<S>>>,
}

impl<S> ValidatorNode for LocalValidatorClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    type NotificationStream = NotificationStream;

    async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_block_proposal(proposal, sender)
        })
        .await
    }

    async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let certificate = certificate.cloned();
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_lite_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_timeout_certificate(
        &self,
        certificate: GenericCertificate<Timeout>,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_validated_certificate(
        &self,
        certificate: GenericCertificate<ValidatedBlock>,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_confirmed_certificate(
        &self,
        certificate: GenericCertificate<ConfirmedBlock>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_certificate(certificate, sender)
        })
        .await
    }

    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_chain_info_query(query, sender)
        })
        .await
    }

    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        self.spawn_and_receive(move |validator, sender| validator.do_subscribe(chains, sender))
            .await
    }

    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        Ok(Default::default())
    }

    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        Ok(CryptoHash::test_hash("genesis config"))
    }

    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError> {
        self.spawn_and_receive(move |validator, sender| validator.do_upload_blob(content, sender))
            .await
    }

    async fn download_blob(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        self.spawn_and_receive(move |validator, sender| validator.do_download_blob(blob_id, sender))
            .await
    }

    async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_download_pending_blob(chain_id, blob_id, sender)
        })
        .await
    }

    async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_handle_pending_blob(chain_id, blob, sender)
        })
        .await
    }

    async fn download_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_download_certificate(hash, sender)
        })
        .await
        .map(Into::into)
    }

    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_download_certificates(hashes, sender)
        })
        .await
    }

    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_blob_last_used_by(blob_id, sender)
        })
        .await
    }

    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        self.spawn_and_receive(move |validator, sender| {
            validator.do_missing_blob_ids(blob_ids, sender)
        })
        .await
    }
}

impl<S> LocalValidatorClient<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(name: ValidatorName, state: WorkerState<S>) -> Self {
        let client = LocalValidator {
            fault_type: FaultType::Honest,
            state,
            notifier: Arc::new(ChannelNotifier::default()),
        };
        Self {
            name,
            client: Arc::new(Mutex::new(client)),
        }
    }

    pub fn name(&self) -> ValidatorName {
        self.name
    }

    async fn set_fault_type(&self, fault_type: FaultType) {
        self.client.lock().await.fault_type = fault_type;
    }

    async fn fault_type(&self) -> FaultType {
        self.client.lock().await.fault_type
    }

    /// Obtains the basic `ChainInfo` data for the local validator chain, with chain manager values.
    pub async fn chain_info_with_manager_values(
        &mut self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, NodeError> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let response = self.handle_chain_info_query(query).await?;
        Ok(response.info)
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
        let handle_block_proposal_result =
            Self::handle_block_proposal(proposal, &mut validator).await;
        let result = match handle_block_proposal_result {
            Some(Err(NodeError::BlobsNotFound(_))) => {
                handle_block_proposal_result.expect("handle_block_proposal_result should be Some")
            }
            _ => match validator.fault_type {
                FaultType::Offline | FaultType::OfflineWithInfo => Err(NodeError::ClientIoError {
                    error: "offline".to_string(),
                }),
                FaultType::Malicious => Err(ArithmeticError::Overflow.into()),
                FaultType::DontSendValidateVote => Err(NodeError::ClientIoError {
                    error: "refusing to validate".to_string(),
                }),
                FaultType::Honest
                | FaultType::DontSendConfirmVote
                | FaultType::DontProcessValidated => handle_block_proposal_result
                    .expect("handle_block_proposal_result should be Some"),
            },
        };
        // In a local node cross-chain messages can't get lost, so we can ignore the actions here.
        sender.send(result.map(|(info, _actions)| info))
    }

    async fn handle_block_proposal(
        proposal: BlockProposal,
        validator: &mut MutexGuard<'_, LocalValidator<S>>,
    ) -> Option<Result<(ChainInfoResponse, NetworkActions), NodeError>> {
        match validator.fault_type {
            FaultType::Offline | FaultType::OfflineWithInfo | FaultType::Malicious => None,
            FaultType::Honest
            | FaultType::DontSendConfirmVote
            | FaultType::DontProcessValidated
            | FaultType::DontSendValidateVote => Some(
                validator
                    .state
                    .handle_block_proposal(proposal)
                    .await
                    .map_err(Into::into),
            ),
        }
    }

    async fn handle_certificate<T: ProcessableCertificate>(
        certificate: GenericCertificate<T>,
        validator: &mut MutexGuard<'_, LocalValidator<S>>,
    ) -> Option<Result<ChainInfoResponse, NodeError>> {
        match validator.fault_type {
            FaultType::DontProcessValidated if T::KIND == CertificateKind::Validated => None,
            FaultType::Honest
            | FaultType::DontSendConfirmVote
            | FaultType::Malicious
            | FaultType::DontProcessValidated
            | FaultType::DontSendValidateVote => Some(
                validator
                    .state
                    .fully_handle_certificate_with_notifications(certificate, &validator.notifier)
                    .await
                    .map_err(Into::into),
            ),
            FaultType::Offline | FaultType::OfflineWithInfo => None,
        }
    }

    async fn do_handle_lite_certificate(
        self,
        certificate: LiteCertificate<'_>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let client = self.client.clone();
        let mut validator = client.lock().await;
        let result = async move {
            match validator.state.full_certificate(certificate).await? {
                Either::Left(confirmed) => {
                    self.do_handle_certificate_internal(confirmed, &mut validator)
                        .await
                }
                Either::Right(validated) => {
                    self.do_handle_certificate_internal(validated, &mut validator)
                        .await
                }
            }
        }
        .await;
        sender.send(result)
    }

    async fn do_handle_certificate_internal<T: ProcessableCertificate>(
        &self,
        certificate: GenericCertificate<T>,
        validator: &mut MutexGuard<'_, LocalValidator<S>>,
    ) -> Result<ChainInfoResponse, NodeError> {
        let handle_certificate_result = Self::handle_certificate(certificate, validator).await;
        match handle_certificate_result {
            Some(Err(NodeError::BlobsNotFound(_))) => {
                handle_certificate_result.expect("handle_certificate_result should be Some")
            }
            _ => match validator.fault_type {
                FaultType::DontSendConfirmVote | FaultType::DontProcessValidated
                    if T::KIND == CertificateKind::Validated =>
                {
                    Err(NodeError::ClientIoError {
                        error: "refusing to confirm".to_string(),
                    })
                }
                FaultType::Honest
                | FaultType::DontSendConfirmVote
                | FaultType::DontProcessValidated
                | FaultType::Malicious
                | FaultType::DontSendValidateVote => {
                    handle_certificate_result.expect("handle_certificate_result should be Some")
                }
                FaultType::Offline | FaultType::OfflineWithInfo => Err(NodeError::ClientIoError {
                    error: "offline".to_string(),
                }),
            },
        }
    }

    async fn do_handle_certificate<T: ProcessableCertificate>(
        self,
        certificate: GenericCertificate<T>,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let mut validator = self.client.lock().await;
        let result = self
            .do_handle_certificate_internal(certificate, &mut validator)
            .await;
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

    async fn do_upload_blob(
        self,
        content: BlobContent,
        sender: oneshot::Sender<Result<BlobId, NodeError>>,
    ) -> Result<(), Result<BlobId, NodeError>> {
        let validator = self.client.lock().await;
        let blob = Blob::new(content);
        let id = blob.id();
        let storage = validator.state.storage_client();
        let result = match storage.maybe_write_blobs(&[blob]).await {
            Ok(has_state) if has_state.first() == Some(&true) => Ok(id),
            Ok(_) => Err(NodeError::BlobsNotFound(vec![id])),
            Err(error) => Err(error.into()),
        };
        sender.send(result)
    }

    async fn do_download_blob(
        self,
        blob_id: BlobId,
        sender: oneshot::Sender<Result<BlobContent, NodeError>>,
    ) -> Result<(), Result<BlobContent, NodeError>> {
        let validator = self.client.lock().await;
        let blob = validator
            .state
            .storage_client()
            .read_blob(blob_id)
            .await
            .map_err(Into::into);
        sender.send(blob.map(|blob| blob.into_content()))
    }

    async fn do_download_pending_blob(
        self,
        chain_id: ChainId,
        blob_id: BlobId,
        sender: oneshot::Sender<Result<BlobContent, NodeError>>,
    ) -> Result<(), Result<BlobContent, NodeError>> {
        let validator = self.client.lock().await;
        let result = validator
            .state
            .download_pending_blob(chain_id, blob_id)
            .await
            .map_err(Into::into);
        sender.send(result.map(|blob| blob.into_content()))
    }

    async fn do_handle_pending_blob(
        self,
        chain_id: ChainId,
        blob: BlobContent,
        sender: oneshot::Sender<Result<ChainInfoResponse, NodeError>>,
    ) -> Result<(), Result<ChainInfoResponse, NodeError>> {
        let validator = self.client.lock().await;
        let result = validator
            .state
            .handle_pending_blob(chain_id, Blob::new(blob))
            .await
            .map_err(Into::into);
        sender.send(result)
    }

    async fn do_download_certificate(
        self,
        hash: CryptoHash,
        sender: oneshot::Sender<Result<ConfirmedBlockCertificate, NodeError>>,
    ) -> Result<(), Result<ConfirmedBlockCertificate, NodeError>> {
        let validator = self.client.lock().await;
        let certificate = validator
            .state
            .storage_client()
            .read_certificate(hash)
            .await
            .map_err(Into::into);

        sender.send(certificate)
    }

    async fn do_download_certificates(
        self,
        hashes: Vec<CryptoHash>,
        sender: oneshot::Sender<Result<Vec<ConfirmedBlockCertificate>, NodeError>>,
    ) -> Result<(), Result<Vec<ConfirmedBlockCertificate>, NodeError>> {
        let validator = self.client.lock().await;
        let certificates = validator
            .state
            .storage_client()
            .read_certificates(hashes)
            .await
            .map_err(Into::into);

        sender.send(certificates)
    }

    async fn do_blob_last_used_by(
        self,
        blob_id: BlobId,
        sender: oneshot::Sender<Result<CryptoHash, NodeError>>,
    ) -> Result<(), Result<CryptoHash, NodeError>> {
        let validator = self.client.lock().await;
        let certificate_hash = validator
            .state
            .storage_client()
            .read_blob_state(blob_id)
            .await
            .map(|blob_state| blob_state.last_used_by)
            .map_err(Into::into);

        sender.send(certificate_hash)
    }

    async fn do_missing_blob_ids(
        self,
        blob_ids: Vec<BlobId>,
        sender: oneshot::Sender<Result<Vec<BlobId>, NodeError>>,
    ) -> Result<(), Result<Vec<BlobId>, NodeError>> {
        let validator = self.client.lock().await;
        let missing_blob_ids = validator
            .state
            .storage_client()
            .missing_blobs(&blob_ids)
            .await
            .map_err(Into::into);
        sender.send(missing_blob_ids)
    }
}

#[derive(Clone)]
pub struct NodeProvider<S>(BTreeMap<ValidatorName, Arc<Mutex<LocalValidator<S>>>>)
where
    S: Storage;

impl<S> ValidatorNodeProvider for NodeProvider<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    type Node = LocalValidatorClient<S>;

    fn make_node(&self, _name: &str) -> Result<Self::Node, NodeError> {
        unimplemented!()
    }

    fn make_nodes_from_list<A>(
        &self,
        validators: impl IntoIterator<Item = (ValidatorName, A)>,
    ) -> Result<impl Iterator<Item = (ValidatorName, Self::Node)>, NodeError>
    where
        A: AsRef<str>,
    {
        Ok(validators
            .into_iter()
            .map(|(name, address)| {
                self.0
                    .get(&name)
                    .ok_or_else(|| NodeError::CannotResolveValidatorAddress {
                        address: address.as_ref().to_string(),
                    })
                    .cloned()
                    .map(|client| (name, LocalValidatorClient { name, client }))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }
}

impl<S> FromIterator<LocalValidatorClient<S>> for NodeProvider<S>
where
    S: Storage,
{
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
    {
        for account in &self.accounts {
            storage
                .create_chain(
                    initial_committee.clone(),
                    admin_id,
                    account.description,
                    account.public_key.into(),
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
            let state = WorkerState::new(
                format!("Node {}", i),
                Some(key_pair),
                storage.clone(),
                NonZeroUsize::new(100).expect("Chain worker limit should not be zero"),
            )
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

    pub async fn set_fault_type(&mut self, indexes: impl AsRef<[usize]>, fault_type: FaultType) {
        let mut faulty_validators = vec![];
        for index in indexes.as_ref() {
            let validator = &mut self.validator_clients[*index];
            validator.set_fault_type(fault_type).await;
            faulty_validators.push(validator.name);
        }
        tracing::info!(
            "Making the following validators {:?}: {:?}",
            fault_type,
            faulty_validators
        );
    }

    /// Creates the root chain with the given `index`, and returns a client for it.
    pub async fn add_root_chain(
        &mut self,
        index: u32,
        balance: Amount,
    ) -> Result<ChainClient<NodeProvider<B::Storage>, B::Storage>, anyhow::Error> {
        let description = ChainDescription::Root(index);
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
                        public_key.into(),
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
                        public_key.into(),
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
                    public_key.into(),
                    balance,
                    Timestamp::from(0),
                )
                .await
                .unwrap();
        }
        self.make_client(description.into(), key_pair, None, BlockHeight::ZERO)
            .await
    }

    pub fn genesis_chains(&self) -> Vec<(PublicKey, Amount)> {
        let mut result = Vec::new();
        for (i, genesis_account) in self.genesis_storage_builder.accounts.iter().enumerate() {
            assert_eq!(
                genesis_account.description,
                ChainDescription::Root(i as u32)
            );
            result.push((genesis_account.public_key, genesis_account.balance));
        }
        result
    }

    pub fn admin_id(&self) -> ChainId {
        self.admin_id
    }

    pub fn make_node_provider(&self) -> NodeProvider<B::Storage> {
        self.validator_clients.iter().cloned().collect()
    }

    pub fn node(&mut self, index: usize) -> &mut LocalValidatorClient<B::Storage> {
        &mut self.validator_clients[index]
    }

    pub async fn make_storage(&mut self) -> anyhow::Result<B::Storage> {
        Ok(self
            .genesis_storage_builder
            .build(
                self.storage_builder.build().await?,
                self.initial_committee.clone(),
                self.admin_id,
            )
            .await)
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
        let storage = self.make_storage().await?;
        self.chain_client_storages.push(storage.clone());
        let provider = self.make_node_provider();
        let builder = Arc::new(Client::new(
            provider,
            storage,
            10,
            CrossChainMessageDelivery::NonBlocking,
            false,
            [chain_id],
            format!("Client node for {:.8}", chain_id),
            NonZeroUsize::new(20).expect("Chain worker limit should not be zero"),
            DEFAULT_GRACE_PERIOD,
        ));
        Ok(builder.create_chain_client(
            chain_id,
            vec![key_pair],
            self.admin_id,
            block_hash,
            Timestamp::from(0),
            block_height,
            None,
            BTreeMap::new(),
        ))
    }

    /// Tries to find a (confirmation) certificate for the given chain_id and block height.
    pub async fn check_that_validators_have_certificate(
        &self,
        chain_id: ChainId,
        block_height: BlockHeight,
        target_count: usize,
    ) -> Option<ConfirmedBlockCertificate> {
        let query =
            ChainInfoQuery::new(chain_id).with_sent_certificate_hashes_in_range(BlockHeightRange {
                start: block_height,
                limit: Some(1),
            });
        let mut count = 0;
        let mut certificate = None;
        for validator in self.validator_clients.clone() {
            if let Ok(response) = validator.handle_chain_info_query(query.clone()).await {
                if response.check(&validator.name).is_ok() {
                    let ChainInfo {
                        mut requested_sent_certificate_hashes,
                        ..
                    } = *response.info;
                    debug_assert!(requested_sent_certificate_hashes.len() <= 1);
                    if let Some(cert_hash) = requested_sent_certificate_hashes.pop() {
                        if let Ok(cert) = validator.download_certificate(cert_hash).await {
                            if cert.inner().block().header.chain_id == chain_id
                                && cert.inner().block().header.height == block_height
                            {
                                cert.check(&self.initial_committee).unwrap();
                                count += 1;
                                certificate = Some(cert);
                            }
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
        for validator in self.validator_clients.clone() {
            if let Ok(response) = validator.handle_chain_info_query(query.clone()).await {
                if response.info.manager.current_round == round
                    && response.info.next_block_height == block_height
                    && response.check(&validator.name).is_ok()
                {
                    count += 1;
                }
            }
        }
        assert!(count >= target_count);
    }

    /// Panics if any validator has a nonempty outbox for the given chain.
    pub async fn check_that_validators_have_empty_outboxes(&self, chain_id: ChainId) {
        for validator in &self.validator_clients {
            let guard = validator.client.lock().await;
            let chain = guard.state.chain_state_view(chain_id).await.unwrap();
            assert_eq!(chain.outboxes.indices().await.unwrap(), []);
        }
    }
}

#[cfg(feature = "rocksdb")]
/// Limit concurrency for RocksDB tests to avoid "too many open files" errors.
static ROCKS_DB_SEMAPHORE: Semaphore = Semaphore::const_new(5);

#[derive(Default)]
pub struct MemoryStorageBuilder {
    namespace: String,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[async_trait]
impl StorageBuilder for MemoryStorageBuilder {
    type Storage = DbStorage<MemoryStore, TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        self.instance_counter += 1;
        let config = MemoryStore::new_test_config().await?;
        if self.namespace.is_empty() {
            self.namespace = generate_test_namespace();
        }
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        let root_key = &[];
        Ok(DbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            self.wasm_runtime,
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
pub struct RocksDbStorageBuilder {
    namespace: String,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
    _permit: SemaphorePermit<'static>,
}

#[cfg(feature = "rocksdb")]
impl RocksDbStorageBuilder {
    pub async fn new() -> Self {
        RocksDbStorageBuilder {
            namespace: String::new(),
            instance_counter: 0,
            wasm_runtime: None,
            clock: TestClock::default(),
            _permit: ROCKS_DB_SEMAPHORE.acquire().await.unwrap(),
        }
    }

    /// Creates a [`RocksDbStorageBuilder`] that uses the specified [`WasmRuntime`] to run Wasm
    /// applications.
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    pub async fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        RocksDbStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..RocksDbStorageBuilder::new().await
        }
    }
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl StorageBuilder for RocksDbStorageBuilder {
    type Storage = DbStorage<RocksDbStore, TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        self.instance_counter += 1;
        let config = RocksDbStore::new_test_config().await?;
        if self.namespace.is_empty() {
            self.namespace = generate_test_namespace();
        }
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        let root_key = &[];
        Ok(DbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "storage-service"))]
#[derive(Default)]
pub struct ServiceStorageBuilder {
    namespace: String,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "storage-service"))]
impl ServiceStorageBuilder {
    /// Creates a `ServiceStorage`.
    pub async fn new() -> Self {
        Self::with_wasm_runtime(None).await
    }

    /// Creates a `ServiceStorage` with the given Wasm runtime.
    pub async fn with_wasm_runtime(wasm_runtime: impl Into<Option<WasmRuntime>>) -> Self {
        ServiceStorageBuilder {
            wasm_runtime: wasm_runtime.into(),
            ..ServiceStorageBuilder::default()
        }
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "storage-service"))]
#[async_trait]
impl StorageBuilder for ServiceStorageBuilder {
    type Storage = DbStorage<ServiceStoreClient, TestClock>;

    async fn build(&mut self) -> anyhow::Result<Self::Storage> {
        self.instance_counter += 1;
        let config = ServiceStoreClient::new_test_config().await?;
        if self.namespace.is_empty() {
            self.namespace = generate_test_namespace();
        }
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        let root_key = &[];
        Ok(DbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "dynamodb")]
#[derive(Default)]
pub struct DynamoDbStorageBuilder {
    namespace: String,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
}

#[cfg(feature = "dynamodb")]
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

#[cfg(feature = "dynamodb")]
#[async_trait]
impl StorageBuilder for DynamoDbStorageBuilder {
    type Storage = DbStorage<DynamoDbStore, TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        self.instance_counter += 1;
        let config = DynamoDbStore::new_test_config().await?;
        if self.namespace.is_empty() {
            self.namespace = generate_test_namespace();
        }
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        let root_key = &[];
        Ok(DbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}

#[cfg(feature = "scylladb")]
#[derive(Default)]
pub struct ScyllaDbStorageBuilder {
    namespace: String,
    instance_counter: usize,
    wasm_runtime: Option<WasmRuntime>,
    clock: TestClock,
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
    type Storage = DbStorage<ScyllaDbStore, TestClock>;

    async fn build(&mut self) -> Result<Self::Storage, anyhow::Error> {
        self.instance_counter += 1;
        let config = ScyllaDbStore::new_test_config().await?;
        if self.namespace.is_empty() {
            self.namespace = generate_test_namespace();
        }
        let namespace = format!("{}_{}", self.namespace, self.instance_counter);
        let root_key = &[];
        Ok(DbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            self.wasm_runtime,
            self.clock.clone(),
        )
        .await?)
    }

    fn clock(&self) -> &TestClock {
        &self.clock
    }
}
