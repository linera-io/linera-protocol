// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use crate::{
    data_types::*,
    worker::{
        CrossChainUpdateHelper, Notification, Reason,
        Reason::{NewBlock, NewIncomingMessage},
        ValidatorWorker, WorkerError, WorkerState,
    },
};
use linera_base::{
    crypto::{BcsSignable, CryptoHash, *},
    data_types::*,
    identifiers::{ChainDescription, ChainId, ChannelName, Destination, MessageId, Owner},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, CertificateValue, ChainAndHeight, ChannelFullName,
        Event, ExecutedBlock, HashedValue, IncomingMessage, LiteVote, Medium, Origin,
        OutgoingMessage, SignatureAggregator,
    },
    test::{make_child_block, make_first_block, BlockTestExt},
    ChainError, ChainManager, ChainManagerInfo, MultiOwnerManagerInfo,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{Account, AdminOperation, Recipient, SystemChannel, SystemMessage, SystemOperation},
    ChainOwnership, ChannelSubscription, ExecutionStateView, GenericApplicationId, Message, Query,
    Response, SystemExecutionState, SystemQuery, SystemResponse,
};
use linera_storage::{DbStoreClient, MemoryStoreClient, Store, TestClock};
use linera_views::{
    common::KeyValueStoreClient,
    memory::TEST_MEMORY_MAX_STREAM_QUERIES,
    value_splitting::DatabaseConsistencyError,
    views::{CryptoHashView, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, iter};
use test_log::test;

#[cfg(feature = "rocksdb")]
use {
    linera_core::client::client_test_utils::ROCKS_DB_SEMAPHORE, linera_storage::RocksDbStoreClient,
};

#[cfg(feature = "aws")]
use linera_storage::DynamoDbStoreClient;

#[cfg(feature = "scylladb")]
use linera_storage::ScyllaDbStoreClient;

#[derive(Serialize, Deserialize)]
struct Dummy;

impl BcsSignable for Dummy {}

async fn make_state_hash(state: SystemExecutionState) -> CryptoHash {
    ExecutionStateView::from_system_state(state)
        .await
        .crypto_hash()
        .await
        .expect("hashing from memory should not fail")
}

fn make_state(
    epoch: Epoch,
    description: ChainDescription,
    admin_id: impl Into<ChainId>,
) -> SystemExecutionState {
    SystemExecutionState {
        epoch: Some(epoch),
        description: Some(description),
        admin_id: Some(admin_id.into()),
        ..SystemExecutionState::default()
    }
}

/// The test worker accepts blocks with a timestamp this far in the future.
const TEST_GRACE_PERIOD_MICROS: u64 = 500_000;

/// Instantiates the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker<S>(client: S, is_client: bool) -> (Committee, WorkerState<S>)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
    let worker = WorkerState::new("Single validator node".to_string(), Some(key_pair), client)
        .with_allow_inactive_chains(is_client)
        .with_allow_messages_from_deprecated_epochs(is_client)
        .with_grace_period_micros(TEST_GRACE_PERIOD_MICROS);
    (committee, worker)
}

/// Same as `init_worker` but also instantiates some initial chains.
async fn init_worker_with_chains<S, I>(client: S, balances: I) -> (Committee, WorkerState<S>)
where
    I: IntoIterator<Item = (ChainDescription, PublicKey, Amount)>,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let (committee, worker) = init_worker(client, /* is_client */ false);
    for (description, pubk, balance) in balances {
        worker
            .storage
            .create_chain(
                committee.clone(),
                ChainId::root(0),
                description,
                pubk,
                balance,
                Timestamp::from(0),
            )
            .await
            .unwrap();
    }
    (committee, worker)
}

/// Same as `init_worker` but also instantiate a single initial chain.
async fn init_worker_with_chain<S>(
    client: S,
    description: ChainDescription,
    owner: PublicKey,
    balance: Amount,
) -> (Committee, WorkerState<S>)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    init_worker_with_chains(client, [(description, owner, balance)]).await
}

fn make_certificate<S>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: HashedValue,
) -> Certificate {
    let vote = LiteVote::new(
        value.lite(),
        RoundNumber(0),
        worker.key_pair.as_ref().unwrap(),
    );
    let mut builder = SignatureAggregator::new(value, RoundNumber(0), committee);
    builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn make_transfer_certificate<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    recipient: Recipient,
    amount: Amount,
    incoming_messages: Vec<IncomingMessage>,
    committee: &Committee,
    balance: Amount,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    make_transfer_certificate_for_epoch(
        chain_description,
        key_pair,
        recipient,
        amount,
        incoming_messages,
        Epoch::ZERO,
        committee,
        balance,
        worker,
        previous_confirmed_block,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn make_transfer_certificate_for_epoch<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    recipient: Recipient,
    amount: Amount,
    incoming_messages: Vec<IncomingMessage>,
    epoch: Epoch,
    committee: &Committee,
    balance: Amount,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    let chain_id = chain_description.into();
    let system_state = SystemExecutionState {
        committees: [(epoch, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(key_pair.public()),
        balance,
        ..make_state(epoch, chain_description, ChainId::root(0))
    };
    let block_template = match &previous_confirmed_block {
        None => make_first_block(chain_id),
        Some(cert) => make_child_block(&cert.value),
    };
    let block = Block {
        epoch,
        incoming_messages,
        ..block_template
    }
    .with_simple_transfer(recipient, amount);
    let messages = match recipient {
        Recipient::Account(account) => vec![direct_outgoing_message(
            account.chain_id,
            SystemMessage::Credit { account, amount },
        )],
        Recipient::Burn => Vec::new(),
    };
    let state_hash = make_state_hash(system_state).await;
    let value = HashedValue::new_confirmed(ExecutedBlock {
        block,
        messages,
        state_hash,
    });
    make_certificate(committee, worker, value)
}

fn direct_outgoing_message(recipient: ChainId, message: SystemMessage) -> OutgoingMessage {
    OutgoingMessage {
        destination: Destination::Recipient(recipient),
        authenticated_signer: None,
        is_skippable: false,
        message: Message::System(message),
    }
}

fn channel_outgoing_message(name: ChannelName, message: SystemMessage) -> OutgoingMessage {
    OutgoingMessage {
        destination: Destination::Subscribers(name),
        authenticated_signer: None,
        is_skippable: false,
        message: Message::System(message),
    }
}

fn direct_credit_message(recipient: ChainId, amount: Amount) -> OutgoingMessage {
    let account = Account::chain(recipient);
    let message = SystemMessage::Credit { account, amount };
    direct_outgoing_message(recipient, message)
}

/// Creates `count` key pairs and returns them, sorted by the `Owner` created from their public key.
fn generate_key_pairs(count: usize) -> Vec<KeyPair> {
    let mut key_pairs: Vec<_> = iter::repeat_with(KeyPair::generate).take(count).collect();
    key_pairs.sort_by_key(|key_pair| Owner::from(key_pair.public()));
    key_pairs
}

/// Returns the `MultiOwnerManagerInfo`; panics if there is a different kind of chain manager.
fn multi_manager(info: &ChainInfo) -> &MultiOwnerManagerInfo {
    match &info.manager {
        ChainManagerInfo::Multi(multi) => multi,
        _ => panic!("Expected multi-owner chain manager."),
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_bad_signature() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_bad_signature(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_bad_signature() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_bad_signature(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_bad_signature() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_bad_signature(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_bad_signature() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_bad_signature(client).await;
}

async fn run_test_handle_block_proposal_bad_signature<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(5))
        .into_simple_proposal(&sender_key_pair, 0);
    let unknown_key_pair = KeyPair::generate();
    let mut bad_signature_block_proposal = block_proposal.clone();
    bad_signature_block_proposal.signature =
        Signature::new(&block_proposal.content, &unknown_key_pair);
    assert!(worker
        .handle_block_proposal(bad_signature_block_proposal)
        .await
        .is_err());
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_none());
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_zero_amount() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_zero_amount(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_zero_amount() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_zero_amount(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_zero_amount() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_zero_amount(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_zero_amount() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_zero_amount(client).await;
}

async fn run_test_handle_block_proposal_zero_amount<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    // test block non-positive amount
    let zero_amount_block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::ZERO)
        .into_simple_proposal(&sender_key_pair, 0);
    assert!(worker
        .handle_block_proposal(zero_amount_block_proposal)
        .await
        .is_err());
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_none());
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_ticks() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_ticks(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_ticks() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_ticks(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_ticks() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_ticks(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_ticks() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_ticks(client).await;
}

async fn run_test_handle_block_proposal_ticks<C>(client: DbStoreClient<C, TestClock>)
where
    C: KeyValueStoreClient + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStoreClient>::Error>,
    <C as KeyValueStoreClient>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let key_pair = KeyPair::generate();
    let balance: Amount = Amount::from_tokens(5);
    let balances = vec![(ChainDescription::Root(1), key_pair.public(), balance)];
    let epoch = Epoch::ZERO;
    let clock = client.clock.clone();
    let (committee, mut worker) = init_worker_with_chains(client, balances).await;

    {
        let block_proposal = make_first_block(ChainId::root(1))
            .with_timestamp(Timestamp::from(TEST_GRACE_PERIOD_MICROS + 1_000_000))
            .into_simple_proposal(&key_pair, 0);
        // Timestamp too far in the future
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }

    let block_0_time = Timestamp::from(TEST_GRACE_PERIOD_MICROS);
    let certificate = {
        let block = make_first_block(ChainId::root(1)).with_timestamp(block_0_time);
        let block_proposal = block.clone().into_simple_proposal(&key_pair, 0);
        assert!(worker.handle_block_proposal(block_proposal).await.is_ok());

        let system_state = SystemExecutionState {
            committees: [(epoch, committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(key_pair.public()),
            balance,
            timestamp: block_0_time,
            ..make_state(epoch, ChainDescription::Root(1), ChainId::root(0))
        };
        let state_hash = make_state_hash(system_state).await;
        let value = HashedValue::new_confirmed(ExecutedBlock {
            block,
            messages: vec![],
            state_hash,
        });
        make_certificate(&committee, &worker, value)
    };
    let future = worker.fully_handle_certificate(certificate.clone(), vec![]);
    clock.set(block_0_time);
    future.await.expect("handle certificate with valid tick");

    {
        let block_proposal = make_child_block(&certificate.value)
            .with_timestamp(block_0_time.saturating_sub_micros(1))
            .into_simple_proposal(&key_pair, 0);
        // Timestamp older than previous one
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_unknown_sender() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_unknown_sender(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_unknown_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_unknown_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_unknown_sender() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_unknown_sender(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_unknown_sender() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_unknown_sender(client).await;
}

async fn run_test_handle_block_proposal_unknown_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(5))
        .into_simple_proposal(&sender_key_pair, 0);
    let unknown_key = KeyPair::generate();

    let unknown_sender_block_proposal =
        BlockProposal::new(block_proposal.content, &unknown_key, vec![], None);
    assert!(worker
        .handle_block_proposal(unknown_sender_block_proposal)
        .await
        .is_err());
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_none());
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_with_chaining() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_chaining(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_with_chaining() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_chaining(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_chaining() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_chaining(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_with_chaining() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_chaining(client).await;
}

async fn run_test_handle_block_proposal_with_chaining<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::root(2);
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let block_proposal0 = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::ONE)
        .into_simple_proposal(&sender_key_pair, 0);
    let certificate0 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        recipient,
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::from_tokens(4),
        &worker,
        None,
    )
    .await;
    let block_proposal1 = make_child_block(&certificate0.value)
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(2))
        .into_simple_proposal(&sender_key_pair, 0);

    assert!(worker
        .handle_block_proposal(block_proposal1.clone())
        .await
        .is_err());
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_none());

    worker
        .handle_block_proposal(block_proposal0.clone())
        .await
        .unwrap();
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_some());
    worker
        .handle_certificate(certificate0, vec![], None)
        .await
        .unwrap();
    worker.handle_block_proposal(block_proposal1).await.unwrap();
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_some());
    assert!(worker.handle_block_proposal(block_proposal0).await.is_err());
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_with_incoming_messages() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_with_incoming_messages() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_incoming_messages() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_with_incoming_messages() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

async fn run_test_handle_block_proposal_with_incoming_messages<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let recipient = Recipient::root(2);
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(6),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public(),
                Amount::ZERO,
            ),
        ],
    )
    .await;

    let epoch = Epoch::ZERO;
    let certificate0 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(ChainId::root(1))
                .with_simple_transfer(recipient, Amount::ONE)
                .with_simple_transfer(recipient, Amount::from_tokens(2)),
            messages: vec![
                direct_credit_message(ChainId::root(2), Amount::ONE),
                direct_credit_message(ChainId::root(2), Amount::from_tokens(2)),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public()),
                balance: Amount::from_tokens(3),
                ..make_state(epoch, ChainDescription::Root(1), ChainId::root(0))
            })
            .await,
        }),
    );

    let certificate1 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_child_block(&certificate0.value)
                .with_simple_transfer(recipient, Amount::from_tokens(3)),
            messages: vec![direct_credit_message(
                ChainId::root(2),
                Amount::from_tokens(3),
            )],
            state_hash: make_state_hash(SystemExecutionState {
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public()),
                ..make_state(epoch, ChainDescription::Root(1), ChainId::root(0))
            })
            .await,
        }),
    );
    // Missing earlier blocks
    assert!(worker
        .handle_certificate(certificate1.clone(), vec![], None)
        .await
        .is_err());

    // Run transfers
    let mut notifications = Vec::new();
    worker
        .fully_handle_certificate_with_notifications(
            certificate0.clone(),
            vec![],
            Some(&mut notifications),
        )
        .await
        .unwrap();
    worker
        .fully_handle_certificate_with_notifications(
            certificate1.clone(),
            vec![],
            Some(&mut notifications),
        )
        .await
        .unwrap();
    assert_eq!(
        notifications,
        vec![
            Notification {
                chain_id: ChainId::root(1),
                reason: NewBlock {
                    height: BlockHeight(0),
                    hash: certificate0.hash(),
                }
            },
            Notification {
                chain_id: ChainId::root(2),
                reason: NewIncomingMessage {
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight(0)
                }
            },
            Notification {
                chain_id: ChainId::root(1),
                reason: NewBlock {
                    height: BlockHeight(1),
                    hash: certificate1.hash(),
                }
            },
            Notification {
                chain_id: ChainId::root(2),
                reason: NewIncomingMessage {
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight(1)
                }
            }
        ]
    );
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(Recipient::root(3), Amount::from_tokens(6))
            .into_simple_proposal(&recipient_key_pair, 0);
        // Insufficient funding
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(Recipient::root(3), Amount::from_tokens(5))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::ONE,
                    }),
                },
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(2),
                    }),
                },
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(2), // wrong
                    }),
                },
            })
            .into_simple_proposal(&recipient_key_pair, 0);
        // Inconsistent received messages.
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnexpectedMessage { .. })
        ));
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(Recipient::root(3), Amount::from_tokens(6))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(2),
                    }),
                },
            })
            .into_simple_proposal(&recipient_key_pair, 0);
        // Skipped message.
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnskippableMessage { .. })
        ));
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(Recipient::root(3), Amount::from_tokens(6))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(3),
                    }),
                },
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::ONE,
                    }),
                },
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(2),
                    }),
                },
            })
            .into_simple_proposal(&recipient_key_pair, 0);
        // Inconsistent order in received messages (heights).
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnskippableMessage { .. })
        ));
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(Recipient::root(3), Amount::ONE)
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::ONE,
                    }),
                },
            })
            .into_simple_proposal(&recipient_key_pair, 0);
        // Taking the first message only is ok.
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
        let certificate = make_certificate(
            &committee,
            &worker,
            HashedValue::new_confirmed(ExecutedBlock {
                block: block_proposal.content.block,
                messages: vec![direct_credit_message(ChainId::root(3), Amount::ONE)],
                state_hash: make_state_hash(SystemExecutionState {
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    ownership: ChainOwnership::single(recipient_key_pair.public()),
                    ..make_state(epoch, ChainDescription::Root(2), ChainId::root(0))
                })
                .await,
            }),
        );
        worker
            .handle_certificate(certificate.clone(), vec![], None)
            .await
            .unwrap();

        // Then receive the next two messages.
        let block_proposal = make_child_block(&certificate.value)
            .with_simple_transfer(Recipient::root(3), Amount::from_tokens(3))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::from(0),
                    index: 1,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(2),
                    }),
                },
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    is_skippable: false,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from_tokens(3),
                    }),
                },
            })
            .into_simple_proposal(&recipient_key_pair, 0);
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_exceed_balance() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_exceed_balance(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_exceed_balance() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_exceed_balance(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_exceed_balance() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_exceed_balance(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_exceed_balance() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_exceed_balance(client).await;
}

async fn run_test_handle_block_proposal_exceed_balance<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(1000))
        .into_simple_proposal(&sender_key_pair, 0);
    assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .is_none());
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal(client).await;
}

async fn run_test_handle_block_proposal<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(5))
        .into_simple_proposal(&sender_key_pair, 0);

    let (chain_info_response, _actions) =
        worker.handle_block_proposal(block_proposal).await.unwrap();
    chain_info_response
        .check(ValidatorName(worker.key_pair.unwrap().public()))
        .unwrap();
    let pending_value = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .manager
        .get()
        .pending()
        .unwrap()
        .lite();
    assert_eq!(
        *chain_info_response.info.manager.pending().unwrap(),
        pending_value
    );
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_replay() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_replay(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_replay() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_replay(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_replay() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_replay(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_replay() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_block_proposal_replay(client).await;
}

async fn run_test_handle_block_proposal_replay<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(Recipient::root(2), Amount::from_tokens(5))
        .into_simple_proposal(&sender_key_pair, 0);

    let (response, _actions) = worker
        .handle_block_proposal(block_proposal.clone())
        .await
        .unwrap();
    response
        .check(ValidatorName(worker.key_pair.as_ref().unwrap().public()))
        .as_ref()
        .unwrap();
    let (replay_response, _actions) = worker.handle_block_proposal(block_proposal).await.unwrap();
    // Workaround lack of equality.
    assert_eq!(
        CryptoHash::new(&response.info),
        CryptoHash::new(&replay_response.info)
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_unknown_sender() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_unknown_sender(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_unknown_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_unknown_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_unknown_sender() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_unknown_sender(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_unknown_sender() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_unknown_sender(client).await;
}

async fn run_test_handle_certificate_unknown_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO)],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    assert!(worker
        .fully_handle_certificate(certificate, vec![])
        .await
        .is_err());
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_bad_block_height() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_bad_block_height(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_bad_block_height() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_bad_block_height(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_bad_block_height() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_bad_block_height(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_bad_block_height() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_bad_block_height(client).await;
}

async fn run_test_handle_certificate_bad_block_height<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    // Replays are ignored.
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap();
    worker
        .fully_handle_certificate(certificate, vec![])
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_with_anticipated_incoming_message() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_with_anticipated_incoming_message() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_with_anticipated_incoming_message() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_with_anticipated_incoming_message() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

async fn run_test_handle_certificate_with_anticipated_incoming_message<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                key_pair.public(),
                Amount::from_tokens(5),
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::ZERO),
        ],
    )
    .await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        Recipient::root(2),
        Amount::from_tokens(1000),
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(3)),
            event: Event {
                certificate_hash: CryptoHash::new(&Dummy),
                height: BlockHeight::ZERO,
                index: 0,
                authenticated_signer: None,
                is_skippable: false,
                timestamp: Timestamp::from(0),
                message: Message::System(SystemMessage::Credit {
                    account: Account::chain(ChainId::root(1)),
                    amount: Amount::from_tokens(995),
                }),
            },
        }],
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap();
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get(),);
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    let inbox = chain
        .inboxes
        .try_load_entry_mut(&Origin::chain(ChainId::root(3)))
        .await
        .unwrap();
    assert_eq!(
        BlockHeight::ZERO,
        inbox.next_block_height_to_receive().unwrap()
    );
    assert_eq!(inbox.added_events.count(), 0);
    assert!(matches!(
        inbox
            .removed_events
            .front()
            .await
            .unwrap()
            .unwrap(),
        Event {
            certificate_hash,
            height,
            index: 0,
            authenticated_signer: None,
            is_skippable: false,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. }),
        } if certificate_hash == CryptoHash::new(&Dummy)
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::from_tokens(995),
    ));
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
    worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_receiver_balance_overflow() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_receiver_balance_overflow() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_balance_overflow() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_receiver_balance_overflow() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

async fn run_test_handle_certificate_receiver_balance_overflow<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::ONE,
            ),
            (ChainDescription::Root(2), PublicKey::debug(2), Amount::MAX),
        ],
    )
    .await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap();
    let new_sender_chain = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(
        Amount::ZERO,
        *new_sender_chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(1),
        new_sender_chain.tip_state.get().next_block_height
    );
    assert_eq!(new_sender_chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash()),
        new_sender_chain.tip_state.get().block_hash
    );
    let new_recipient_chain = worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(
        Amount::MAX,
        *new_recipient_chain.execution_state.system.balance.get()
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_receiver_equal_sender() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_receiver_equal_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_equal_sender() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_receiver_equal_sender() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

async fn run_test_handle_certificate_receiver_equal_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut worker) =
        init_worker_with_chain(client, ChainDescription::Root(1), name, Amount::ONE).await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        Recipient::root(1),
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap();
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get());
    let inbox = chain
        .inboxes
        .try_load_entry_mut(&Origin::chain(ChainId::root(1)))
        .await
        .unwrap();
    assert_eq!(
        BlockHeight::from(1),
        inbox.next_block_height_to_receive().unwrap()
    );
    assert!(matches!(
        inbox
            .added_events
            .front()
            .await
            .unwrap()
            .unwrap(),
        Event {
            certificate_hash,
            height,
            index: 0,
            authenticated_signer: None,
            is_skippable: false,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. })
        } if certificate_hash == certificate.hash()
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::ONE,
    ));
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request(client).await;
}

async fn run_test_handle_cross_chain_request<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(ChainDescription::Root(2), PublicKey::debug(2), Amount::ONE)],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(10),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(Medium::Direct, vec![certificate.value().height()])],
            sender: ChainId::root(1),
            recipient: ChainId::root(2),
            certificates: vec![certificate.clone()],
        })
        .await
        .unwrap();
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(Amount::ONE, *chain.execution_state.system.balance.get());
    assert_eq!(BlockHeight::ZERO, chain.tip_state.get().next_block_height);
    let inbox = chain
        .inboxes
        .try_load_entry_mut(&Origin::chain(ChainId::root(1)))
        .await
        .unwrap();
    assert_eq!(
        BlockHeight::from(1),
        inbox.next_block_height_to_receive().unwrap()
    );
    assert!(matches!(
        inbox
            .added_events
            .front()
            .await
            .unwrap()
            .unwrap(),
        Event {
            certificate_hash,
            height,
            index: 0,
            authenticated_signer: None,
            is_skippable: false,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. })
        } if certificate_hash == certificate.hash()
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::from_tokens(10),
    ));
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.tip_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request_no_recipient_chain() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request_no_recipient_chain() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request_no_recipient_chain() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

async fn run_test_handle_cross_chain_request_no_recipient_chain<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(client, /* is_client */ false);
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(10),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    assert!(worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(Medium::Direct, vec![certificate.value().height()],)],
            sender: ChainId::root(1),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap()
        .cross_chain_requests
        .is_empty());
    let chain = worker.storage.load_chain(ChainId::root(2)).await.unwrap();
    // The target chain did not receive the message
    assert!(chain.inboxes.indices().await.unwrap().is_empty());
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request_no_recipient_chain_on_client() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

async fn run_test_handle_cross_chain_request_no_recipient_chain_on_client<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(client, /* is_client */ true);
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(10),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    // An inactive target chain is created and it acknowledges the message.
    let actions = worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(Medium::Direct, vec![certificate.value().height()])],
            sender: ChainId::root(1),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap();
    assert!(matches!(
        actions.cross_chain_requests.as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    ));
    assert_eq!(
        actions.notifications,
        vec![Notification {
            chain_id: ChainId::root(2),
            reason: Reason::NewIncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                height: BlockHeight::ZERO,
            }
        }]
    );
    let chain = worker.storage.load_chain(ChainId::root(2)).await.unwrap();
    assert!(!chain.inboxes.indices().await.unwrap().is_empty());
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_to_active_recipient() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_active_recipient(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_to_active_recipient() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_active_recipient(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_active_recipient() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_active_recipient(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_to_active_recipient() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_active_recipient(client).await;
}

async fn run_test_handle_certificate_to_active_recipient<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    assert_eq!(
        worker
            .query_application(ChainId::root(1), &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::from_tokens(5),
        })
    );
    assert_eq!(
        worker
            .query_application(ChainId::root(2), &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::ZERO,
        })
    );

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;

    let info = worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        worker
            .query_application(ChainId::root(1), &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::ZERO,
        })
    );

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        Recipient::root(3),
        Amount::ONE,
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(1)),
            event: Event {
                certificate_hash: certificate.hash(),
                height: BlockHeight::ZERO,
                index: 0,
                authenticated_signer: None,
                is_skippable: false,
                timestamp: Timestamp::from(0),
                message: Message::System(SystemMessage::Credit {
                    account: Account::chain(ChainId::root(2)),
                    amount: Amount::from_tokens(5),
                }),
            },
        }],
        &committee,
        Amount::from_tokens(4),
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap();

    assert_eq!(
        worker
            .query_application(ChainId::root(2), &Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::from_tokens(4),
        })
    );

    {
        let recipient_chain = worker
            .storage
            .load_active_chain(ChainId::root(2))
            .await
            .unwrap();
        assert_eq!(
            *recipient_chain.execution_state.system.balance.get(),
            Amount::from_tokens(4)
        );
        assert!(matches!(
            recipient_chain.manager.get(),
            ChainManager::Single(single) if single.owner == recipient_key_pair.public().into()
        ));
        assert_eq!(recipient_chain.confirmed_log.count(), 1);
        assert_eq!(
            recipient_chain.tip_state.get().block_hash,
            Some(certificate.hash())
        );
        assert_eq!(recipient_chain.received_log.count(), 1);
    }
    let query = ChainInfoQuery::new(ChainId::root(2)).with_received_log_excluding_first_nth(0);
    let (response, _actions) = worker.handle_chain_info_query(query).await.unwrap();
    assert_eq!(response.info.requested_received_log.len(), 1);
    assert_eq!(
        response.info.requested_received_log[0],
        ChainAndHeight {
            chain_id: ChainId::root(1),
            height: BlockHeight::ZERO
        }
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_to_inactive_recipient() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_to_inactive_recipient() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_inactive_recipient() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_to_inactive_recipient() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

async fn run_test_handle_certificate_to_inactive_recipient<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::root(2), // the recipient chain does not exist
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;

    let info = worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());
}

#[test(tokio::test)]
async fn test_memory_chain_creation_with_committee_creation() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_chain_creation_with_committee_creation(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_chain_creation_with_committee_creation() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_chain_creation_with_committee_creation(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_chain_creation_with_committee_creation() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_chain_creation_with_committee_creation(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_chain_creation_with_committee_creation() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_chain_creation_with_committee_creation(client).await;
}

async fn run_test_chain_creation_with_committee_creation<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(0),
            key_pair.public(),
            Amount::from_tokens(2),
        )],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = ChainId::root(0);
    let admin_channel_subscription = ChannelSubscription {
        chain_id: admin_id,
        name: SystemChannel::Admin.name(),
    };
    let admin_channel_full_name = ChannelFullName {
        application_id: GenericApplicationId::System,
        name: SystemChannel::Admin.name(),
    };
    let admin_channel_origin = Origin::channel(admin_id, admin_channel_full_name.clone());
    // Have the admin chain create a user chain.
    let user_id = ChainId::child(MessageId {
        chain_id: admin_id,
        height: BlockHeight::ZERO,
        index: 0,
    });
    let user_description = ChainDescription::Child(MessageId {
        chain_id: admin_id,
        height: BlockHeight::ZERO,
        index: 0,
    });
    let certificate0 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(admin_id).with_operation(SystemOperation::OpenChain {
                ownership: ChainOwnership::single(key_pair.public()),
                epoch: Epoch::ZERO,
                committees: committees.clone(),
                admin_id,
            }),
            messages: vec![
                direct_outgoing_message(
                    user_id,
                    SystemMessage::OpenChain {
                        ownership: ChainOwnership::single(key_pair.public()),
                        epoch: Epoch::ZERO,
                        committees: committees.clone(),
                        admin_id,
                    },
                ),
                direct_outgoing_message(
                    admin_id,
                    SystemMessage::Subscribe {
                        id: user_id,
                        subscription: admin_channel_subscription.clone(),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                balance: Amount::from_tokens(2),
                ..make_state(Epoch::ZERO, ChainDescription::Root(0), admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate0.clone(), vec![])
        .await
        .unwrap();
    {
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
        assert_eq!(
            BlockHeight::from(1),
            admin_chain.tip_state.get().next_block_height
        );
        assert!(admin_chain.outboxes.indices().await.unwrap().is_empty());
        assert_eq!(
            *admin_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        // The root chain has no subscribers yet.
        assert!(!admin_chain
            .channels
            .indices()
            .await
            .unwrap()
            .contains(&admin_channel_full_name));
    }

    // Create a new committee and transfer money before accepting the subscription.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_child_block(&certificate0.value)
                .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    committee: committee.clone(),
                }))
                .with_simple_transfer(Recipient::chain(user_id), Amount::from_tokens(2)),
            messages: vec![
                channel_outgoing_message(
                    SystemChannel::Admin.name(),
                    SystemMessage::SetCommittees {
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    },
                ),
                direct_credit_message(user_id, Amount::from_tokens(2)),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                ..SystemExecutionState::default()
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), vec![])
        .await
        .unwrap();

    // Have the admin chain accept the subscription now.
    let certificate2 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_child_block(&certificate1.value)
                .with_epoch(1)
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate0.value.hash(),
                        height: BlockHeight::ZERO,
                        index: 1,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Subscribe {
                            id: user_id,
                            subscription: admin_channel_subscription.clone(),
                        }),
                    },
                }),
            messages: vec![direct_outgoing_message(
                user_id,
                SystemMessage::Notify { id: user_id },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                ..make_state(Epoch::from(1), ChainDescription::Root(0), admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate2.clone(), vec![])
        .await
        .unwrap();
    {
        // The root chain has 1 subscribers.
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
        assert_eq!(
            admin_chain
                .channels
                .try_load_entry_mut(&admin_channel_full_name)
                .await
                .unwrap()
                .subscribers
                .indices()
                .await
                .unwrap()
                .len(),
            1
        );
    }
    {
        // The child is active and has not migrated yet.
        let mut user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
        assert_eq!(
            BlockHeight::ZERO,
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        assert_eq!(
            user_chain
                .execution_state
                .system
                .subscriptions
                .indices()
                .await
                .unwrap()
                .len(),
            1
        );
        user_chain.validate_incoming_messages().await.unwrap();
        matches!(
            user_chain
                .inboxes
                .try_load_entry_mut(&Origin::chain(admin_id))
                .await
                .unwrap()
                .added_events
                .read_front(10)
                .await
                .unwrap()[..],
            [
                Event {
                    message: Message::System(SystemMessage::OpenChain { .. }),
                    ..
                },
                Event {
                    message: Message::System(SystemMessage::Credit { .. }),
                    ..
                },
                Event {
                    message: Message::System(SystemMessage::Notify { .. }),
                    ..
                }
            ]
        );
        let channel_inbox = user_chain
            .inboxes
            .try_load_entry_mut(&admin_channel_origin)
            .await
            .unwrap();
        matches!(
            channel_inbox.added_events.read_front(10).await.unwrap()[..],
            [Event {
                message: Message::System(SystemMessage::SetCommittees { .. }),
                ..
            }]
        );
        assert_eq!(channel_inbox.removed_events.count(), 0);
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 1);
    }
    // Make the child receive the pending messages.
    let certificate3 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(user_id)
                .with_incoming_message(IncomingMessage {
                    origin: admin_channel_origin.clone(),
                    event: Event {
                        certificate_hash: certificate1.value.hash(),
                        height: BlockHeight::from(1),
                        index: 0,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::SetCommittees {
                            epoch: Epoch::from(1),
                            committees: committees2.clone(),
                        }),
                    },
                })
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate0.value.hash(),
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::OpenChain {
                            ownership: ChainOwnership::single(key_pair.public()),
                            epoch: Epoch::from(0),
                            committees: committees.clone(),
                            admin_id,
                        }),
                    },
                })
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate1.value.hash(),
                        height: BlockHeight::from(1),
                        index: 1,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Credit {
                            account: Account::chain(user_id),
                            amount: Amount::from_tokens(2),
                        }),
                    },
                })
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate2.value.hash(),
                        height: BlockHeight::from(2),
                        index: 0,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Notify { id: user_id }),
                    },
                }),
            messages: Vec::new(),
            state_hash: make_state_hash(SystemExecutionState {
                subscriptions: [ChannelSubscription {
                    chain_id: admin_id,
                    name: SystemChannel::Admin.name(),
                }]
                .into_iter()
                .collect(),
                // Finally the child knows about both committees and has the money.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                balance: Amount::from_tokens(2),
                ..make_state(Epoch::from(1), user_description, admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate3, vec![])
        .await
        .unwrap();
    {
        let mut user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
        assert_eq!(
            BlockHeight::from(1),
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        assert_eq!(
            user_chain
                .execution_state
                .system
                .subscriptions
                .indices()
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 2);
        user_chain.validate_incoming_messages().await.unwrap();
        {
            let inbox = user_chain
                .inboxes
                .try_load_entry_mut(&Origin::chain(admin_id))
                .await
                .unwrap();
            assert_eq!(
                inbox.next_block_height_to_receive().unwrap(),
                BlockHeight(3)
            );
            assert_eq!(inbox.added_events.count(), 0);
            assert_eq!(inbox.removed_events.count(), 0);
        }
        {
            let inbox = user_chain
                .inboxes
                .try_load_entry_mut(&admin_channel_origin)
                .await
                .unwrap();
            assert_eq!(
                inbox.next_block_height_to_receive().unwrap(),
                BlockHeight(2)
            );
            assert_eq!(inbox.added_events.count(), 0);
            assert_eq!(inbox.removed_events.count(), 0);
        }
    }
}

#[test(tokio::test)]
async fn test_memory_transfers_and_committee_creation() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_creation(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfers_and_committee_creation() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_creation(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_creation() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_creation(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfers_and_committee_creation() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_creation(client).await;
}

async fn run_test_transfers_and_committee_creation<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (ChainDescription::Root(0), key_pair0.public(), Amount::ZERO),
            (
                ChainDescription::Root(1),
                key_pair1.public(),
                Amount::from_tokens(3),
            ),
        ],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = ChainId::root(0);
    let user_id = ChainId::root(1);

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(user_id)
                .with_simple_transfer(Recipient::chain(admin_id), Amount::ONE),
            messages: vec![direct_credit_message(admin_id, Amount::ONE)],
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public()),
                balance: Amount::from_tokens(2),
                ..make_state(Epoch::ZERO, ChainDescription::Root(1), admin_id)
            })
            .await,
        }),
    );
    // Have the admin chain create a new epoch without retiring the old one.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(admin_id).with_operation(SystemOperation::Admin(
                AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    committee: committee.clone(),
                },
            )),
            messages: vec![channel_outgoing_message(
                SystemChannel::Admin.name(),
                SystemMessage::SetCommittees {
                    epoch: Epoch::from(1),
                    committees: committees2.clone(),
                },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                ..make_state(Epoch::from(1), ChainDescription::Root(0), admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), vec![])
        .await
        .unwrap();

    // Try to execute the transfer.
    worker
        .fully_handle_certificate(certificate0.clone(), vec![])
        .await
        .unwrap();

    // The transfer was started..
    let user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
    assert_eq!(
        BlockHeight::from(1),
        user_chain.tip_state.get().next_block_height
    );
    assert_eq!(
        *user_chain.execution_state.system.balance.get(),
        Amount::from_tokens(2)
    );
    assert_eq!(
        *user_chain.execution_state.system.epoch.get(),
        Some(Epoch::ZERO)
    );

    // .. and the message has gone through.
    let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
    assert_eq!(admin_chain.inboxes.indices().await.unwrap().len(), 1);
    matches!(
        admin_chain
            .inboxes
            .try_load_entry_mut(&Origin::chain(user_id))
            .await
            .unwrap()
            .added_events
            .read_front(10)
            .await
            .unwrap()[..],
        [Event {
            message: Message::System(SystemMessage::Credit { .. }),
            ..
        }]
    );
}

#[test(tokio::test)]
async fn test_memory_transfers_and_committee_removal() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_removal(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfers_and_committee_removal() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_removal(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_removal() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_removal(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfers_and_committee_removal() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_transfers_and_committee_removal(client).await;
}

async fn run_test_transfers_and_committee_removal<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (ChainDescription::Root(0), key_pair0.public(), Amount::ZERO),
            (
                ChainDescription::Root(1),
                key_pair1.public(),
                Amount::from_tokens(3),
            ),
        ],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = ChainId::root(0);
    let user_id = ChainId::root(1);

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(user_id)
                .with_simple_transfer(Recipient::chain(admin_id), Amount::ONE),
            messages: vec![direct_credit_message(admin_id, Amount::ONE)],
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public()),
                balance: Amount::from_tokens(2),
                ..make_state(Epoch::ZERO, ChainDescription::Root(1), admin_id)
            })
            .await,
        }),
    );
    // Have the admin chain create a new epoch and retire the old one immediately.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let committees3 = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(admin_id)
                .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    committee: committee.clone(),
                }))
                .with_operation(SystemOperation::Admin(AdminOperation::RemoveCommittee {
                    epoch: Epoch::ZERO,
                })),
            messages: vec![
                channel_outgoing_message(
                    SystemChannel::Admin.name(),
                    SystemMessage::SetCommittees {
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    },
                ),
                channel_outgoing_message(
                    SystemChannel::Admin.name(),
                    SystemMessage::SetCommittees {
                        epoch: Epoch::from(1),
                        committees: committees3.clone(),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                ..make_state(Epoch::from(1), ChainDescription::Root(0), admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), vec![])
        .await
        .unwrap();

    // Try to execute the transfer from the user chain to the admin chain.
    worker
        .fully_handle_certificate(certificate0.clone(), vec![])
        .await
        .unwrap();

    {
        // The transfer was started..
        let user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
        assert_eq!(
            BlockHeight::from(1),
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.balance.get(),
            Amount::from_tokens(2)
        );
        assert_eq!(
            *user_chain.execution_state.system.epoch.get(),
            Some(Epoch::ZERO)
        );

        // .. but the message hasn't gone through.
        let admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        assert!(admin_chain.inboxes.indices().await.unwrap().is_empty());
    }

    // Force the admin chain to receive the money nonetheless by anticipation.
    let certificate2 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_child_block(&certificate1.value)
                .with_epoch(1)
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(user_id),
                    event: Event {
                        certificate_hash: certificate0.value.hash(),
                        height: BlockHeight::ZERO,
                        index: 0,
                        authenticated_signer: None,
                        is_skippable: false,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Credit {
                            account: Account::chain(admin_id),
                            amount: Amount::ONE,
                        }),
                    },
                }),
            messages: Vec::new(),
            state_hash: make_state_hash(SystemExecutionState {
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                balance: Amount::ONE,
                ..make_state(Epoch::from(1), ChainDescription::Root(0), admin_id)
            })
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate2.clone(), vec![])
        .await
        .unwrap();

    {
        // The admin chain has an anticipated message.
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        assert!(admin_chain.validate_incoming_messages().await.is_err());
    }

    // Try again to execute the transfer from the user chain to the admin chain.
    // This time, the epoch verification should be overruled.
    worker
        .fully_handle_certificate(certificate0.clone(), vec![])
        .await
        .unwrap();

    {
        // The admin chain has no more anticipated messages.
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
    }
}

#[test(tokio::test)]
async fn test_cross_chain_helper() {
    // Make a committee and worker (only used for signing certificates)
    let (committee, worker) = init_worker(
        MemoryStoreClient::new(None, TEST_MEMORY_MAX_STREAM_QUERIES, TestClock::new()),
        true,
    );
    let committees = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);

    let key_pair0 = KeyPair::generate();
    let id0 = ChainId::root(0);
    let id1 = ChainId::root(1);

    let certificate0 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        &worker,
        None,
    )
    .await;
    let certificate1 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        &worker,
        Some(&certificate0),
    )
    .await;
    let certificate2 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::from(1),
        &committee,
        Amount::ONE,
        &worker,
        Some(&certificate1),
    )
    .await;
    // Weird case: epoch going backward.
    let certificate3 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        &worker,
        Some(&certificate2),
    )
    .await;

    let helper = CrossChainUpdateHelper {
        nickname: "test",
        allow_messages_from_deprecated_epochs: true,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is not tested when `allow_messages_from_deprecated_epochs` is true.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![certificate0.clone(), certificate1.clone()]
    );
    // Received heights is removing prefixes.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                None,
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![certificate1.clone()]
    );
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(2),
                None,
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![]
    );
    // Order of certificates is checked.
    assert!(helper
        .select_certificates(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            vec![certificate1.clone(), certificate0.clone()]
        )
        .is_err());
    // Sender is checked.
    assert!(helper
        .select_certificates(
            &Origin::chain(id1),
            id0,
            BlockHeight::ZERO,
            None,
            vec![certificate0.clone()]
        )
        .is_err());

    let helper = CrossChainUpdateHelper {
        nickname: "test",
        allow_messages_from_deprecated_epochs: false,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is tested when `allow_messages_from_deprecated_epochs` is false.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![]
    );
    // A certificate with a recent epoch certifies all the previous blocks.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![
                    certificate0.clone(),
                    certificate1.clone(),
                    certificate2.clone(),
                    certificate3
                ]
            )
            .unwrap(),
        vec![
            certificate0.clone(),
            certificate1.clone(),
            certificate2.clone()
        ]
    );
    // Received heights is still removing prefixes.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                None,
                vec![
                    certificate0.clone(),
                    certificate1.clone(),
                    certificate2.clone()
                ]
            )
            .unwrap(),
        vec![certificate1.clone(), certificate2.clone()]
    );
    // Anticipated messages re-certify blocks up to the given height.
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                Some(BlockHeight::from(1)),
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![certificate1.clone()]
    );
    assert_eq!(
        helper
            .select_certificates(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                Some(BlockHeight::from(1)),
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![certificate0.clone(), certificate1.clone()]
    );
}

#[test(tokio::test)]
async fn test_memory_leader_timeouts() {
    let client = MemoryStoreClient::make_test_client(None).await;
    run_test_leader_timeouts(client).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_leader_timeouts() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let client = RocksDbStoreClient::make_test_client(None).await;
    run_test_leader_timeouts(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_leader_timeouts() {
    let client = DynamoDbStoreClient::make_test_client(None).await;
    run_test_leader_timeouts(client).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_leader_timeouts() {
    let client = ScyllaDbStoreClient::make_test_client(None).await;
    run_test_leader_timeouts(client).await;
}

async fn run_test_leader_timeouts<C>(client: DbStoreClient<C, TestClock>)
where
    C: KeyValueStoreClient + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStoreClient>::Error>,
    <C as KeyValueStoreClient>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let chain_desc = ChainDescription::Root(0);
    let chain_id = ChainId::from(chain_desc);
    let key_pairs = generate_key_pairs(2);
    let pub_key0 = key_pairs[0].public();
    let pub_key1 = key_pairs[1].public();
    let clock = client.clock.clone();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(chain_desc, key_pairs[0].public(), Amount::from_tokens(2))],
    )
    .await;

    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeMultipleOwners {
        new_public_keys: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: RoundNumber::from(0),
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await.unwrap();
    let value0 = HashedValue::new_confirmed(executed_block0);
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate(certificate0, vec![])
        .await
        .unwrap();
    let leader = multi_manager(&response.info).leader.unwrap();
    assert_eq!(leader, Owner::from(pub_key1));

    let proposal1 = make_child_block(&value0).into_simple_proposal(&key_pairs[0], 1);
    assert!(worker
        .handle_block_proposal(proposal1.clone())
        .await
        .is_err());

    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    assert!(multi_manager(&response.info).timeout_vote.is_none());

    clock.add_micros(11_000_000);

    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    let vote = multi_manager(&response.info).timeout_vote.clone().unwrap();
    let value_timeout = HashedValue::from(CertificateValue::LeaderTimeout {
        chain_id,
        height: BlockHeight::from(1),
        epoch: Epoch::from(0),
    });
    let mut builder = SignatureAggregator::new(value_timeout, RoundNumber(0), &committee);
    let certificate_timeout = builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap();
    let (response, _) = worker
        .handle_certificate(certificate_timeout, vec![], None)
        .await
        .unwrap();

    let leader = multi_manager(&response.info).leader.unwrap();
    assert_eq!(leader, Owner::from(pub_key0));

    worker
        .handle_block_proposal(proposal1.clone())
        .await
        .unwrap();
}
