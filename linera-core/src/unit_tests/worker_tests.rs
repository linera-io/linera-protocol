// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    time::Duration,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{CryptoHash, *},
    data_types::*,
    identifiers::{
        Account, ChainDescription, ChainId, ChannelName, Destination, GenericApplicationId,
        MessageId, Owner,
    },
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        Block, BlockProposal, Certificate, ChainAndHeight, ChannelFullName, Event, ExecutedBlock,
        HashedValue, IncomingMessage, LiteVote, Medium, MessageAction, Origin, OutgoingMessage,
        SignatureAggregator,
    },
    test::{make_child_block, make_first_block, BlockTestExt, VoteTestExt},
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemMessage, SystemOperation,
    },
    test_utils::SystemExecutionState,
    ChannelSubscription, ExecutionError, Message, MessageKind, Query, Response,
    SystemExecutionError, SystemQuery, SystemResponse,
};
#[cfg(feature = "aws")]
use linera_storage::DynamoDbStorage;
#[cfg(feature = "scylladb")]
use linera_storage::ScyllaDbStorage;
use linera_storage::{DbStorage, MemoryStorage, Storage, TestClock};
use linera_views::{
    common::KeyValueStore, memory::TEST_MEMORY_MAX_STREAM_QUERIES,
    value_splitting::DatabaseConsistencyError, views::ViewError,
};
use test_log::test;
#[cfg(feature = "rocksdb")]
use {linera_core::client::client_test_utils::ROCKS_DB_SEMAPHORE, linera_storage::RocksDbStorage};

use crate::{
    data_types::*,
    worker::{
        CrossChainUpdateHelper, Notification, Reason,
        Reason::{NewBlock, NewIncomingMessage},
        ValidatorWorker, WorkerError, WorkerState,
    },
};

/// The test worker accepts blocks with a timestamp this far in the future.
const TEST_GRACE_PERIOD_MICROS: u64 = 500_000;

/// Instantiates the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker<S>(storage: S, is_client: bool) -> (Committee, WorkerState<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
    let worker = WorkerState::new("Single validator node".to_string(), Some(key_pair), storage)
        .with_allow_inactive_chains(is_client)
        .with_allow_messages_from_deprecated_epochs(is_client)
        .with_grace_period(Duration::from_micros(TEST_GRACE_PERIOD_MICROS));
    (committee, worker)
}

/// Same as `init_worker` but also instantiates some initial chains.
async fn init_worker_with_chains<S, I>(storage: S, balances: I) -> (Committee, WorkerState<S>)
where
    I: IntoIterator<Item = (ChainDescription, PublicKey, Amount)>,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let (committee, worker) = init_worker(storage, /* is_client */ false);
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
    storage: S,
    description: ChainDescription,
    owner: PublicKey,
    balance: Amount,
) -> (Committee, WorkerState<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    init_worker_with_chains(storage, [(description, owner, balance)]).await
}

fn make_certificate<S>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: HashedValue,
) -> Certificate {
    make_certificate_with_round(committee, worker, value, Round::Fast)
}

fn make_certificate_with_round<S>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: HashedValue,
    round: Round,
) -> Certificate {
    let vote = LiteVote::new(value.lite(), round, worker.key_pair.as_ref().unwrap());
    let mut builder = SignatureAggregator::new(value, round, committee);
    builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn make_simple_transfer_certificate<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    target_id: ChainId,
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
        None,
        Recipient::chain(target_id),
        amount,
        incoming_messages,
        Epoch::ZERO,
        committee,
        balance,
        BTreeMap::new(),
        worker,
        previous_confirmed_block,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn make_transfer_certificate<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    source: Option<Owner>,
    recipient: Recipient,
    amount: Amount,
    incoming_messages: Vec<IncomingMessage>,
    committee: &Committee,
    balance: Amount,
    balances: BTreeMap<Owner, Amount>,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    make_transfer_certificate_for_epoch(
        chain_description,
        key_pair,
        source,
        recipient,
        amount,
        incoming_messages,
        Epoch::ZERO,
        committee,
        balance,
        balances,
        worker,
        previous_confirmed_block,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn make_transfer_certificate_for_epoch<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    source: Option<Owner>,
    recipient: Recipient,
    amount: Amount,
    incoming_messages: Vec<IncomingMessage>,
    epoch: Epoch,
    committee: &Committee,
    balance: Amount,
    balances: BTreeMap<Owner, Amount>,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    let chain_id = chain_description.into();
    let system_state = SystemExecutionState {
        committees: [(epoch, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(key_pair.public()),
        balance,
        balances,
        ..SystemExecutionState::new(epoch, chain_description, ChainId::root(0))
    };
    let block_template = match &previous_confirmed_block {
        None => make_first_block(chain_id),
        Some(cert) => make_child_block(&cert.value),
    };

    let mut message_counts = Vec::new();
    let mut message_count = 0;
    let mut messages = Vec::new();
    for incoming_message in &incoming_messages {
        if matches!(incoming_message.action, MessageAction::Reject)
            && matches!(incoming_message.event.kind, MessageKind::Tracked)
        {
            messages.push(OutgoingMessage {
                authenticated_signer: incoming_message.event.authenticated_signer,
                destination: Destination::Recipient(incoming_message.origin.sender),
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Bouncing,
                message: incoming_message.event.message.clone(),
            });
            message_count += 1;
        }
        message_counts.push(message_count);
    }

    let block = Block {
        epoch,
        incoming_messages,
        authenticated_signer: source,
        ..block_template
    }
    .with_transfer(source, recipient, amount);
    match recipient {
        Recipient::Account(account) => {
            messages.push(direct_outgoing_message(
                account.chain_id,
                MessageKind::Tracked,
                SystemMessage::Credit {
                    source,
                    target: account.owner,
                    amount,
                },
            ));
            message_count += 1;
        }
        Recipient::Burn => (),
    }
    message_counts.push(message_count);
    let state_hash = system_state.into_hash().await;
    let value = HashedValue::new_confirmed(ExecutedBlock {
        block,
        messages,
        message_counts,
        state_hash,
    });
    make_certificate(committee, worker, value)
}

fn direct_outgoing_message(
    recipient: ChainId,
    kind: MessageKind,
    message: SystemMessage,
) -> OutgoingMessage {
    OutgoingMessage {
        destination: Destination::Recipient(recipient),
        authenticated_signer: None,
        grant: Amount::ZERO,
        refund_grant_to: None,
        kind,
        message: Message::System(message),
    }
}

fn channel_outgoing_message(
    name: ChannelName,
    kind: MessageKind,
    message: SystemMessage,
) -> OutgoingMessage {
    OutgoingMessage {
        destination: Destination::Subscribers(name),
        authenticated_signer: None,
        grant: Amount::ZERO,
        refund_grant_to: None,
        kind,
        message: Message::System(message),
    }
}

fn channel_admin_message(message: SystemMessage) -> OutgoingMessage {
    channel_outgoing_message(SystemChannel::Admin.name(), MessageKind::Protected, message)
}

fn system_credit_message(amount: Amount) -> Message {
    Message::System(SystemMessage::Credit {
        source: None,
        target: None,
        amount,
    })
}

fn direct_credit_message(recipient: ChainId, amount: Amount) -> OutgoingMessage {
    let message = SystemMessage::Credit {
        source: None,
        target: None,
        amount,
    };
    direct_outgoing_message(recipient, MessageKind::Tracked, message)
}

/// Creates `count` key pairs and returns them, sorted by the `Owner` created from their public key.
fn generate_key_pairs(count: usize) -> Vec<KeyPair> {
    let mut key_pairs = iter::repeat_with(KeyPair::generate)
        .take(count)
        .collect::<Vec<_>>();
    key_pairs.sort_by_key(|key_pair| Owner::from(key_pair.public()));
    key_pairs
}

/// Creates a `CrossChainRequest` with the messages sent by the certificate to the recipient.
fn update_recipient_direct(recipient: ChainId, certificate: &Certificate) -> CrossChainRequest {
    let sender = certificate.value().chain_id();
    let bundle = certificate
        .message_bundle_for(&Medium::Direct, recipient)
        .unwrap();
    CrossChainRequest::UpdateRecipient {
        sender,
        recipient,
        bundle_vecs: vec![(Medium::Direct, vec![bundle])],
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_bad_signature() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_bad_signature(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_bad_signature() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_bad_signature(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_bad_signature() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_bad_signature(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_bad_signature() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_bad_signature(storage).await;
}

async fn run_test_handle_block_proposal_bad_signature<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_fast_proposal(&sender_key_pair);
    let unknown_key_pair = KeyPair::generate();
    let mut bad_signature_block_proposal = block_proposal.clone();
    bad_signature_block_proposal.signature =
        Signature::new(&block_proposal.content, &unknown_key_pair);
    assert_matches!(
        worker
            .handle_block_proposal(bad_signature_block_proposal)
            .await,
            Err(WorkerError::CryptoError(error)) if matches!(error, CryptoError::InvalidSignature {..})
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_zero_amount(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_zero_amount() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_zero_amount(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_zero_amount() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_zero_amount(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_zero_amount() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_zero_amount(storage).await;
}

async fn run_test_handle_block_proposal_zero_amount<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    // test block non-positive amount
    let zero_amount_block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::ZERO)
        .into_fast_proposal(&sender_key_pair);
    assert_matches!(
    worker
        .handle_block_proposal(zero_amount_block_proposal)
        .await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(
            *error,
            ChainError::ExecutionError(
                ExecutionError::SystemError(SystemExecutionError::IncorrectTransferAmount),
                ChainExecutionContext::Operation(_)
            )
        )
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_ticks(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_ticks() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_ticks(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_ticks() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_ticks(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_ticks() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_ticks(storage).await;
}

async fn run_test_handle_block_proposal_ticks<C>(storage: DbStorage<C, TestClock>)
where
    C: KeyValueStore + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStore>::Error>,
    <C as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let key_pair = KeyPair::generate();
    let balance: Amount = Amount::from_tokens(5);
    let balances = vec![(ChainDescription::Root(1), key_pair.public(), balance)];
    let epoch = Epoch::ZERO;
    let clock = storage.clock.clone();
    let (committee, mut worker) = init_worker_with_chains(storage, balances).await;

    {
        let block_proposal = make_first_block(ChainId::root(1))
            .with_timestamp(Timestamp::from(TEST_GRACE_PERIOD_MICROS + 1_000_000))
            .into_fast_proposal(&key_pair);
        // Timestamp too far in the future
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::InvalidTimestamp)
        );
    }

    let block_0_time = Timestamp::from(TEST_GRACE_PERIOD_MICROS);
    let certificate = {
        let block = make_first_block(ChainId::root(1)).with_timestamp(block_0_time);
        let block_proposal = block.clone().into_fast_proposal(&key_pair);
        assert!(worker.handle_block_proposal(block_proposal).await.is_ok());

        let system_state = SystemExecutionState {
            committees: [(epoch, committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(key_pair.public()),
            balance,
            timestamp: block_0_time,
            ..SystemExecutionState::new(epoch, ChainDescription::Root(1), ChainId::root(0))
        };
        let state_hash = system_state.into_hash().await;
        let value = HashedValue::new_confirmed(ExecutedBlock {
            block,
            messages: vec![],
            message_counts: vec![],
            state_hash,
        });
        make_certificate(&committee, &worker, value)
    };
    let future = worker.fully_handle_certificate(certificate.clone(), &[]);
    clock.set(block_0_time);
    future.await.expect("handle certificate with valid tick");

    {
        let block_proposal = make_child_block(&certificate.value)
            .with_timestamp(block_0_time.saturating_sub_micros(1))
            .into_fast_proposal(&key_pair);
        // Timestamp older than previous one
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::InvalidBlockTimestamp {..})
        );
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_unknown_sender() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_unknown_sender(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_unknown_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_unknown_sender(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_unknown_sender() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_unknown_sender(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_unknown_sender() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_unknown_sender(storage).await;
}

async fn run_test_handle_block_proposal_unknown_sender<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_fast_proposal(&sender_key_pair);
    let unknown_key = KeyPair::generate();

    let unknown_sender_block_proposal =
        BlockProposal::new(block_proposal.content, &unknown_key, vec![], None);
    assert_matches!(
        worker
            .handle_block_proposal(unknown_sender_block_proposal)
            .await,
        Err(WorkerError::InvalidOwner)
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_chaining(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_with_chaining() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_chaining(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_chaining() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_chaining(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_with_chaining() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_chaining(storage).await;
}

async fn run_test_handle_block_proposal_with_chaining<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let block_proposal0 = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::ONE)
        .into_fast_proposal(&sender_key_pair);
    let certificate0 = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::from_tokens(4),
        &worker,
        None,
    )
    .await;
    let block_proposal1 = make_child_block(&certificate0.value)
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(2))
        .into_fast_proposal(&sender_key_pair);

    assert_matches!(
        worker.handle_block_proposal(block_proposal1.clone()).await,
        Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::UnexpectedBlockHeight {..})
    );
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
        .handle_certificate(certificate0, &[], None)
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
    assert_matches!(
        worker.handle_block_proposal(block_proposal0.clone()).await,
        Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::UnexpectedBlockHeight {..})
    );
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_with_incoming_messages() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_incoming_messages(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_with_incoming_messages() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_incoming_messages(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_incoming_messages() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_incoming_messages(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_with_incoming_messages() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_with_incoming_messages(storage).await;
}

async fn run_test_handle_block_proposal_with_incoming_messages<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
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
    let admin_id = ChainId::root(0);
    let certificate0 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_first_block(ChainId::root(1))
                .with_simple_transfer(ChainId::root(2), Amount::ONE)
                .with_simple_transfer(ChainId::root(2), Amount::from_tokens(2)),
            messages: vec![
                direct_credit_message(ChainId::root(2), Amount::ONE),
                direct_credit_message(ChainId::root(2), Amount::from_tokens(2)),
            ],
            message_counts: vec![1, 2],
            state_hash: SystemExecutionState {
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public()),
                balance: Amount::from_tokens(3),
                ..SystemExecutionState::new(epoch, ChainDescription::Root(1), admin_id)
            }
            .into_hash()
            .await,
        }),
    );

    let certificate1 = make_certificate(
        &committee,
        &worker,
        HashedValue::new_confirmed(ExecutedBlock {
            block: make_child_block(&certificate0.value)
                .with_simple_transfer(ChainId::root(2), Amount::from_tokens(3)),
            messages: vec![direct_credit_message(
                ChainId::root(2),
                Amount::from_tokens(3),
            )],
            message_counts: vec![1],
            state_hash: SystemExecutionState {
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public()),
                ..SystemExecutionState::new(epoch, ChainDescription::Root(1), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    // Missing earlier blocks
    assert_matches!(
        worker
            .handle_certificate(certificate1.clone(), &[], None)
            .await,
        Err(WorkerError::MissingEarlierBlocks { .. })
    );

    // Run transfers
    let mut notifications = Vec::new();
    worker
        .fully_handle_certificate_with_notifications(
            certificate0.clone(),
            &[],
            Some(&mut notifications),
        )
        .await
        .unwrap();
    worker
        .fully_handle_certificate_with_notifications(
            certificate1.clone(),
            &[],
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
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(6))
            .into_fast_proposal(&recipient_key_pair);
        // Insufficient funding
        assert_matches!(
                worker.handle_block_proposal(block_proposal).await,
                Err(
                    WorkerError::ChainError(error)
                ) if matches!(
                    *error,
                    ChainError::ExecutionError(
                        ExecutionError::SystemError(
                            SystemExecutionError::InsufficientFunding { .. }
                        ),
                        ChainExecutionContext::Operation(_)
                    )
                )
        );
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(5))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::ONE),
                },
                action: MessageAction::Accept,
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(2)),
                },
                action: MessageAction::Accept,
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(2)), // wrong amount
                },
                action: MessageAction::Accept,
            })
            .into_fast_proposal(&recipient_key_pair);
        // Inconsistent received messages.
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnexpectedMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(6))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(2)),
                },
                action: MessageAction::Accept,
            })
            .into_fast_proposal(&recipient_key_pair);
        // Skipped message.
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::CannotSkipMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(6))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(3)),
                },
                action: MessageAction::Accept,
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::ONE),
                },
                action: MessageAction::Accept,
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 1,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(2)),
                },
                action: MessageAction::Accept,
            })
            .into_fast_proposal(&recipient_key_pair);
        // Inconsistent order in received messages (heights).
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::CannotSkipMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::ONE)
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::ZERO,
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::ONE),
                },
                action: MessageAction::Accept,
            })
            .into_fast_proposal(&recipient_key_pair);
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
                message_counts: vec![0, 1],
                state_hash: SystemExecutionState {
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    ownership: ChainOwnership::single(recipient_key_pair.public()),
                    ..SystemExecutionState::new(epoch, ChainDescription::Root(2), admin_id)
                }
                .into_hash()
                .await,
            }),
        );
        worker
            .handle_certificate(certificate.clone(), &[], None)
            .await
            .unwrap();

        // Then receive the next two messages.
        let block_proposal = make_child_block(&certificate.value)
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(3))
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.value.hash(),
                    height: BlockHeight::from(0),
                    index: 1,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(2)),
                },
                action: MessageAction::Accept,
            })
            .with_incoming_message(IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.value.hash(),
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: system_credit_message(Amount::from_tokens(3)),
                },
                action: MessageAction::Accept,
            })
            .into_fast_proposal(&recipient_key_pair);
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_exceed_balance() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_exceed_balance(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_exceed_balance() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_exceed_balance(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_exceed_balance() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_exceed_balance(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_exceed_balance() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_exceed_balance(storage).await;
}

async fn run_test_handle_block_proposal_exceed_balance<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(1000))
        .into_fast_proposal(&sender_key_pair);
    assert_matches!(
        worker.handle_block_proposal(block_proposal).await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(
            *error,
            ChainError::ExecutionError(
                ExecutionError::SystemError(SystemExecutionError::InsufficientFunding { .. }),
                ChainExecutionContext::Operation(_)
            )
        )
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal(storage).await;
}

async fn run_test_handle_block_proposal<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_fast_proposal(&sender_key_pair);

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
        chain_info_response.info.manager.pending.unwrap(),
        pending_value
    );
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_replay() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_replay(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_block_proposal_replay() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_replay(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_replay() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_replay(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_block_proposal_replay() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_block_proposal_replay(storage).await;
}

async fn run_test_handle_block_proposal_replay<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (_, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_fast_proposal(&sender_key_pair);

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
        CryptoHash::new(&*response.info),
        CryptoHash::new(&*replay_response.info)
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_unknown_sender() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_unknown_sender(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_unknown_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_unknown_sender(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_unknown_sender() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_unknown_sender(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_unknown_sender() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_unknown_sender(storage).await;
}

async fn run_test_handle_certificate_unknown_sender<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2),
            Amount::ZERO,
        )],
    )
    .await;
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    assert_matches!(
        worker
            .fully_handle_certificate(certificate, &[])
            .await,
        Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::InactiveChain {..})
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_with_open_chain() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_open_chain(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_with_open_chain() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_open_chain(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_with_open_chain() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_open_chain(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_with_open_chain() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_open_chain(storage).await;
}

async fn run_test_handle_certificate_with_open_chain<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2),
            Amount::ZERO,
        )],
    )
    .await;
    let admin_id = ChainId::root(0);
    let epoch = Epoch::ZERO;
    let description = ChainDescription::Child(MessageId {
        chain_id: ChainId::root(3),
        height: BlockHeight::ZERO,
        index: 0,
    });
    let chain_id = ChainId::from(description);
    let ownership = ChainOwnership::single(sender_key_pair.public());
    let committees = BTreeMap::from_iter([(epoch, committee.clone())]);
    let subscriptions = BTreeSet::from_iter([ChannelSubscription {
        chain_id: admin_id,
        name: SystemChannel::Admin.name(),
    }]);
    let balance = Amount::from_tokens(42);
    let state = SystemExecutionState {
        committees: committees.clone(),
        ownership: ownership.clone(),
        balance,
        subscriptions,
        ..SystemExecutionState::new(epoch, description, admin_id)
    };
    let open_chain_message = IncomingMessage {
        origin: Origin::chain(ChainId::root(3)),
        event: Event {
            certificate_hash: CryptoHash::test_hash("certificate"),
            height: BlockHeight::ZERO,
            index: 0,
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Protected,
            timestamp: Timestamp::from(0),
            message: Message::System(SystemMessage::OpenChain(OpenChainConfig {
                ownership,
                admin_id,
                epoch,
                committees,
                balance,
                application_permissions: Default::default(),
            })),
        },
        action: MessageAction::Accept,
    };
    let value = HashedValue::new_confirmed(ExecutedBlock {
        block: make_first_block(chain_id).with_incoming_message(open_chain_message),
        messages: vec![],
        message_counts: vec![0],
        state_hash: state.into_hash().await,
    });
    let certificate = make_certificate(&committee, &worker, value);
    let info = worker
        .fully_handle_certificate(certificate, &[])
        .await
        .unwrap()
        .info;
    assert_eq!(info.next_block_height, BlockHeight::from(1));
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_wrong_owner() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_wrong_owner(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_wrong_owner() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_wrong_owner(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_wrong_owner() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_wrong_owner(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_wrong_owner() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_wrong_owner(storage).await;
}

async fn run_test_handle_certificate_wrong_owner<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(2),
        &sender_key_pair,
        ChainId::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    // This fails because `make_simple_transfer_certificate` uses `sender_key_pair.public()` to
    // compute the hash of the execution state.
    assert_matches!(
        worker.fully_handle_certificate(certificate, &[]).await,
        Err(WorkerError::IncorrectStateHash)
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_bad_block_height() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_bad_block_height(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_bad_block_height() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_bad_block_height(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_bad_block_height() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_bad_block_height(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_bad_block_height() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_bad_block_height(storage).await;
}

async fn run_test_handle_certificate_bad_block_height<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
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
        .fully_handle_certificate(certificate.clone(), &[])
        .await
        .unwrap();
    worker
        .fully_handle_certificate(certificate, &[])
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_with_anticipated_incoming_message() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_with_anticipated_incoming_message() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_with_anticipated_incoming_message() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_with_anticipated_incoming_message() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_anticipated_incoming_message(storage).await;
}

async fn run_test_handle_certificate_with_anticipated_incoming_message<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                key_pair.public(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::ZERO,
            ),
        ],
    )
    .await;

    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        ChainId::root(2),
        Amount::from_tokens(1000),
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(3)),
            event: Event {
                certificate_hash: CryptoHash::test_hash("certificate"),
                height: BlockHeight::ZERO,
                index: 0,
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Tracked,
                timestamp: Timestamp::from(0),
                message: system_credit_message(Amount::from_tokens(995)),
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), &[])
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
    assert_matches!(
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
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. }),
        } if certificate_hash == CryptoHash::test_hash("certificate")
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::from_tokens(995),
        "Unexpected event",
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_balance_overflow(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_receiver_balance_overflow() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_balance_overflow(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_balance_overflow() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_balance_overflow(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_receiver_balance_overflow() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_balance_overflow(storage).await;
}

async fn run_test_handle_certificate_receiver_balance_overflow<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Amount::ONE,
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2),
                Amount::MAX,
            ),
        ],
    )
    .await;

    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), &[])
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_equal_sender(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_receiver_equal_sender() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_equal_sender(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_equal_sender() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_equal_sender(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_receiver_equal_sender() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_receiver_equal_sender(storage).await;
}

async fn run_test_handle_certificate_receiver_equal_sender<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut worker) =
        init_worker_with_chain(storage, ChainDescription::Root(1), name, Amount::ONE).await;

    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        ChainId::root(1),
        Amount::ONE,
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), &[])
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
    assert_matches!(
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
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. })
        } if certificate_hash == certificate.hash()
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::ONE,
        "Unexpected event",
    );
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request(storage).await;
}

async fn run_test_handle_cross_chain_request<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2),
            Amount::ONE,
        )],
    )
    .await;
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::from_tokens(10),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    worker
        .handle_cross_chain_request(update_recipient_direct(ChainId::root(2), &certificate))
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
    assert_matches!(
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
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            timestamp,
            message: Message::System(SystemMessage::Credit { amount, .. })
        } if certificate_hash == certificate.hash()
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && amount == Amount::from_tokens(10),
        "Unexpected event",
    );
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.tip_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request_no_recipient_chain() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request_no_recipient_chain() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request_no_recipient_chain() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain(storage).await;
}

async fn run_test_handle_cross_chain_request_no_recipient_chain<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(storage, /* is_client */ false);
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::from_tokens(10),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;
    assert!(worker
        .handle_cross_chain_request(update_recipient_direct(ChainId::root(2), &certificate))
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_cross_chain_request_no_recipient_chain_on_client() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(storage).await;
}

async fn run_test_handle_cross_chain_request_no_recipient_chain_on_client<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(storage, /* is_client */ true);
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
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
        .handle_cross_chain_request(update_recipient_direct(ChainId::root(2), &certificate))
        .await
        .unwrap();
    assert_matches!(
        actions.cross_chain_requests.as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_active_recipient(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_to_active_recipient() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_active_recipient(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_active_recipient() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_active_recipient(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_to_active_recipient() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_active_recipient(storage).await;
}

async fn run_test_handle_certificate_to_active_recipient<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
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
            .query_application(ChainId::root(1), Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::from_tokens(5),
        })
    );
    assert_eq!(
        worker
            .query_application(ChainId::root(2), Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::ZERO,
        })
    );

    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;

    let info = worker
        .fully_handle_certificate(certificate.clone(), &[])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    assert_eq!(
        worker
            .query_application(ChainId::root(1), Query::System(SystemQuery))
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::ZERO,
        })
    );

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        ChainId::root(3),
        Amount::ONE,
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(1)),
            event: Event {
                certificate_hash: certificate.hash(),
                height: BlockHeight::ZERO,
                index: 0,
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Tracked,
                timestamp: Timestamp::from(0),
                message: system_credit_message(Amount::from_tokens(5)),
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::from_tokens(4),
        &worker,
        None,
    )
    .await;
    worker
        .fully_handle_certificate(certificate.clone(), &[])
        .await
        .unwrap();

    assert_eq!(
        worker
            .query_application(ChainId::root(2), Query::System(SystemQuery))
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
        let ownership = &recipient_chain.manager.get().ownership;
        assert!(
            ownership
                .super_owners
                .contains_key(&recipient_key_pair.public().into())
                && ownership.super_owners.len() == 1
                && ownership.owners.is_empty()
        );
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_inactive_recipient(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_to_inactive_recipient() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_inactive_recipient(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_inactive_recipient() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_inactive_recipient(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_to_inactive_recipient() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_to_inactive_recipient(storage).await;
}

async fn run_test_handle_certificate_to_inactive_recipient<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let certificate = make_simple_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        ChainId::root(2), // the recipient chain does not exist
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ZERO,
        &worker,
        None,
    )
    .await;

    let info = worker
        .fully_handle_certificate(certificate.clone(), &[])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_with_rejected_transfer() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_rejected_transfer(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_handle_certificate_with_rejected_transfer() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_rejected_transfer(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_with_rejected_transfer() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_rejected_transfer(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_handle_certificate_with_rejected_transfer() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_handle_certificate_with_rejected_transfer(storage).await;
}

async fn run_test_handle_certificate_with_rejected_transfer<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let sender = Owner::from(sender_key_pair.public());
    let sender_account = Account {
        chain_id: ChainId::root(1),
        owner: Some(sender),
    };

    let recipient_key_pair = KeyPair::generate();
    let recipient = Owner::from(sender_key_pair.public());
    let recipient_account = Account {
        chain_id: ChainId::root(2),
        owner: Some(recipient),
    };

    let (committee, mut worker) = init_worker_with_chains(
        storage,
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

    // First move the money from the public balance to the sender's account.
    // This takes two certificates (sending, receiving) sadly.
    let certificate00 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        None,
        Recipient::Account(sender_account),
        Amount::from_tokens(5),
        Vec::new(),
        &committee,
        Amount::ONE,
        BTreeMap::new(),
        &worker,
        None,
    )
    .await;

    worker
        .fully_handle_certificate(certificate00.clone(), &[])
        .await
        .unwrap();

    let certificate01 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        None,
        Recipient::Burn,
        Amount::ONE,
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(1)),
            event: Event {
                certificate_hash: certificate00.hash(),
                height: BlockHeight::from(0),
                index: 0,
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Tracked,
                timestamp: Timestamp::from(0),
                message: Message::System(SystemMessage::Credit {
                    source: None,
                    target: Some(sender),
                    amount: Amount::from_tokens(5),
                }),
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(sender, Amount::from_tokens(5))]),
        &worker,
        Some(&certificate00),
    )
    .await;

    worker
        .fully_handle_certificate(certificate01.clone(), &[])
        .await
        .unwrap();

    {
        let chain = worker
            .storage
            .load_active_chain(ChainId::root(1))
            .await
            .unwrap();
        chain.validate_incoming_messages().await.unwrap();
    }

    // Then, make two transfers to the recipient.
    let certificate1 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Some(sender),
        Recipient::Account(recipient_account),
        Amount::from_tokens(3),
        Vec::new(),
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(sender, Amount::from_tokens(2))]),
        &worker,
        Some(&certificate01),
    )
    .await;

    worker
        .fully_handle_certificate(certificate1.clone(), &[])
        .await
        .unwrap();

    let certificate2 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Some(sender),
        Recipient::Account(recipient_account),
        Amount::from_tokens(2),
        Vec::new(),
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(sender, Amount::from_tokens(0))]),
        &worker,
        Some(&certificate1),
    )
    .await;

    worker
        .fully_handle_certificate(certificate2.clone(), &[])
        .await
        .unwrap();

    // Reject the first transfer and try to use the money of the second one.
    let certificate = make_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        Some(recipient),
        Recipient::Burn,
        Amount::ONE,
        vec![
            IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(2),
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        source: Some(sender),
                        target: Some(recipient),
                        amount: Amount::from_tokens(3),
                    }),
                },
                action: MessageAction::Reject,
            },
            IncomingMessage {
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate2.hash(),
                    height: BlockHeight::from(3),
                    index: 0,
                    authenticated_signer: None,
                    grant: Amount::ZERO,
                    refund_grant_to: None,
                    kind: MessageKind::Tracked,
                    timestamp: Timestamp::from(0),
                    message: Message::System(SystemMessage::Credit {
                        source: Some(sender),
                        target: Some(recipient),
                        amount: Amount::from_tokens(2),
                    }),
                },
                action: MessageAction::Accept,
            },
        ],
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(recipient, Amount::from_tokens(1))]),
        &worker,
        None,
    )
    .await;

    worker
        .fully_handle_certificate(certificate.clone(), &[])
        .await
        .unwrap();

    {
        let chain = worker
            .storage
            .load_active_chain(ChainId::root(2))
            .await
            .unwrap();
        chain.validate_incoming_messages().await.unwrap();
    }

    // Process the bounced message and try to use the refund.
    let certificate3 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Some(sender),
        Recipient::Burn,
        Amount::from_tokens(3),
        vec![IncomingMessage {
            origin: Origin::chain(ChainId::root(2)),
            event: Event {
                certificate_hash: certificate.hash(),
                height: BlockHeight::from(0),
                index: 0,
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Bouncing,
                timestamp: Timestamp::from(0),
                message: Message::System(SystemMessage::Credit {
                    source: Some(sender),
                    target: Some(recipient),
                    amount: Amount::from_tokens(3),
                }),
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(sender, Amount::from_tokens(0))]),
        &worker,
        Some(&certificate2),
    )
    .await;

    worker
        .fully_handle_certificate(certificate3.clone(), &[])
        .await
        .unwrap();

    {
        let chain = worker
            .storage
            .load_active_chain(ChainId::root(1))
            .await
            .unwrap();
        chain.validate_incoming_messages().await.unwrap();
    }
}

#[test(tokio::test)]
async fn test_memory_chain_creation_with_committee_creation() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_chain_creation_with_committee_creation(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_chain_creation_with_committee_creation() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_chain_creation_with_committee_creation(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_chain_creation_with_committee_creation() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_chain_creation_with_committee_creation(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_chain_creation_with_committee_creation() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_chain_creation_with_committee_creation(storage).await;
}

async fn run_test_chain_creation_with_committee_creation<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
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
            block: make_first_block(admin_id).with_operation(SystemOperation::OpenChain(
                OpenChainConfig {
                    ownership: ChainOwnership::single(key_pair.public()),
                    epoch: Epoch::ZERO,
                    committees: committees.clone(),
                    admin_id,
                    balance: Amount::ZERO,
                    application_permissions: Default::default(),
                },
            )),
            messages: vec![
                direct_outgoing_message(
                    user_id,
                    MessageKind::Protected,
                    SystemMessage::OpenChain(OpenChainConfig {
                        ownership: ChainOwnership::single(key_pair.public()),
                        epoch: Epoch::ZERO,
                        committees: committees.clone(),
                        admin_id,
                        balance: Amount::ZERO,
                        application_permissions: Default::default(),
                    }),
                ),
                direct_outgoing_message(
                    admin_id,
                    MessageKind::Protected,
                    SystemMessage::Subscribe {
                        id: user_id,
                        subscription: admin_channel_subscription.clone(),
                    },
                ),
            ],
            message_counts: vec![2],
            state_hash: SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                balance: Amount::from_tokens(2),
                ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate0.clone(), &[])
        .await
        .unwrap();
    {
        let admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
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
                .with_simple_transfer(user_id, Amount::from_tokens(2)),
            messages: vec![
                channel_admin_message(SystemMessage::SetCommittees {
                    epoch: Epoch::from(1),
                    committees: committees2.clone(),
                }),
                direct_credit_message(user_id, Amount::from_tokens(2)),
            ],
            message_counts: vec![1, 2],
            state_hash: SystemExecutionState {
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), &[])
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
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Protected,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Subscribe {
                            id: user_id,
                            subscription: admin_channel_subscription.clone(),
                        }),
                    },
                    action: MessageAction::Accept,
                }),
            messages: vec![direct_outgoing_message(
                user_id,
                MessageKind::Protected,
                SystemMessage::Notify { id: user_id },
            )],
            message_counts: vec![1],
            state_hash: SystemExecutionState {
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public()),
                ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate2.clone(), &[])
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
                    message: Message::System(SystemMessage::OpenChain(_)),
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
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate0.value.hash(),
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Protected,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::OpenChain(OpenChainConfig {
                            ownership: ChainOwnership::single(key_pair.public()),
                            epoch: Epoch::from(0),
                            committees: committees.clone(),
                            admin_id,
                            balance: Amount::ZERO,
                            application_permissions: Default::default(),
                        })),
                    },
                    action: MessageAction::Accept,
                })
                .with_incoming_message(IncomingMessage {
                    origin: admin_channel_origin.clone(),
                    event: Event {
                        certificate_hash: certificate1.value.hash(),
                        height: BlockHeight::from(1),
                        index: 0,
                        authenticated_signer: None,
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Protected,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::SetCommittees {
                            epoch: Epoch::from(1),
                            committees: committees2.clone(),
                        }),
                    },
                    action: MessageAction::Accept,
                })
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate1.value.hash(),
                        height: BlockHeight::from(1),
                        index: 1,
                        authenticated_signer: None,
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Tracked,
                        timestamp: Timestamp::from(0),
                        message: system_credit_message(Amount::from_tokens(2)),
                    },
                    action: MessageAction::Accept,
                })
                .with_incoming_message(IncomingMessage {
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate2.value.hash(),
                        height: BlockHeight::from(2),
                        index: 0,
                        authenticated_signer: None,
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Protected,
                        timestamp: Timestamp::from(0),
                        message: Message::System(SystemMessage::Notify { id: user_id }),
                    },
                    action: MessageAction::Accept,
                }),
            messages: Vec::new(),
            message_counts: vec![0, 0, 0, 0],
            state_hash: SystemExecutionState {
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
                ..SystemExecutionState::new(Epoch::from(1), user_description, admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate3, &[])
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_creation(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfers_and_committee_creation() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_creation(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_creation() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_creation(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfers_and_committee_creation() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_creation(storage).await;
}

async fn run_test_transfers_and_committee_creation<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
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
            block: make_first_block(user_id).with_simple_transfer(admin_id, Amount::ONE),
            messages: vec![direct_credit_message(admin_id, Amount::ONE)],
            message_counts: vec![1],
            state_hash: SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public()),
                balance: Amount::from_tokens(2),
                ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(1), admin_id)
            }
            .into_hash()
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
            messages: vec![channel_admin_message(SystemMessage::SetCommittees {
                epoch: Epoch::from(1),
                committees: committees2.clone(),
            })],
            message_counts: vec![1],
            state_hash: SystemExecutionState {
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), &[])
        .await
        .unwrap();

    // Try to execute the transfer.
    worker
        .fully_handle_certificate(certificate0.clone(), &[])
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
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_removal(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_transfers_and_committee_removal() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_removal(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_removal() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_removal(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_transfers_and_committee_removal() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_transfers_and_committee_removal(storage).await;
}

async fn run_test_transfers_and_committee_removal<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        storage,
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
            block: make_first_block(user_id).with_simple_transfer(admin_id, Amount::ONE),
            messages: vec![direct_credit_message(admin_id, Amount::ONE)],
            message_counts: vec![1],
            state_hash: SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public()),
                balance: Amount::from_tokens(2),
                ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(1), admin_id)
            }
            .into_hash()
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
                channel_admin_message(SystemMessage::SetCommittees {
                    epoch: Epoch::from(1),
                    committees: committees2.clone(),
                }),
                channel_admin_message(SystemMessage::SetCommittees {
                    epoch: Epoch::from(1),
                    committees: committees3.clone(),
                }),
            ],
            message_counts: vec![1, 2],
            state_hash: SystemExecutionState {
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate1.clone(), &[])
        .await
        .unwrap();

    // Try to execute the transfer from the user chain to the admin chain.
    worker
        .fully_handle_certificate(certificate0.clone(), &[])
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
                        grant: Amount::ZERO,
                        refund_grant_to: None,
                        kind: MessageKind::Tracked,
                        timestamp: Timestamp::from(0),
                        message: system_credit_message(Amount::ONE),
                    },
                    action: MessageAction::Accept,
                }),
            messages: Vec::new(),
            message_counts: vec![0],
            state_hash: SystemExecutionState {
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public()),
                balance: Amount::ONE,
                ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
            }
            .into_hash()
            .await,
        }),
    );
    worker
        .fully_handle_certificate(certificate2.clone(), &[])
        .await
        .unwrap();

    {
        // The admin chain has an anticipated message.
        let admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        assert_matches!(
            admin_chain.validate_incoming_messages().await,
            Err(ChainError::MissingCrossChainUpdate { .. })
        );
    }

    // Try again to execute the transfer from the user chain to the admin chain.
    // This time, the epoch verification should be overruled.
    worker
        .fully_handle_certificate(certificate0.clone(), &[])
        .await
        .unwrap();

    {
        // The admin chain has no more anticipated messages.
        let admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
    }
}

#[test(tokio::test)]
async fn test_cross_chain_helper() {
    // Make a committee and worker (only used for signing certificates)
    let store =
        MemoryStorage::new_for_testing(None, TEST_MEMORY_MAX_STREAM_QUERIES, TestClock::new())
            .await
            .expect("store");
    let (committee, worker) = init_worker(store, true);
    let committees = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);

    let key_pair0 = KeyPair::generate();
    let id0 = ChainId::root(0);
    let id1 = ChainId::root(1);

    let certificate0 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        None,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        BTreeMap::new(),
        &worker,
        None,
    )
    .await;
    let certificate1 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        None,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        BTreeMap::new(),
        &worker,
        Some(&certificate0),
    )
    .await;
    let certificate2 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        None,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::from(1),
        &committee,
        Amount::ONE,
        BTreeMap::new(),
        &worker,
        Some(&certificate1),
    )
    .await;
    // Weird case: epoch going backward.
    let certificate3 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        None,
        Recipient::chain(id1),
        Amount::ONE,
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ONE,
        BTreeMap::new(),
        &worker,
        Some(&certificate2),
    )
    .await;
    let bundle0 = certificate0
        .message_bundle_for(&Medium::Direct, id1)
        .unwrap();
    let bundle1 = certificate1
        .message_bundle_for(&Medium::Direct, id1)
        .unwrap();
    let bundle2 = certificate2
        .message_bundle_for(&Medium::Direct, id1)
        .unwrap();
    let bundle3 = certificate3
        .message_bundle_for(&Medium::Direct, id1)
        .unwrap();

    let helper = CrossChainUpdateHelper {
        nickname: "test",
        allow_messages_from_deprecated_epochs: true,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is not tested when `allow_messages_from_deprecated_epochs` is true.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![bundle0.clone(), bundle1.clone()]
    );
    // Received heights is removing prefixes.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                None,
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![bundle1.clone()]
    );
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(2),
                None,
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![]
    );
    // Order of certificates is checked.
    assert_matches!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            vec![bundle1.clone(), bundle0.clone()]
        ),
        Err(WorkerError::InvalidCrossChainRequest)
    );

    let helper = CrossChainUpdateHelper {
        nickname: "test",
        allow_messages_from_deprecated_epochs: false,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is tested when `allow_messages_from_deprecated_epochs` is false.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![]
    );
    // A certificate with a recent epoch certifies all the previous blocks.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                None,
                vec![bundle0.clone(), bundle1.clone(), bundle2.clone(), bundle3]
            )
            .unwrap(),
        vec![bundle0.clone(), bundle1.clone(), bundle2.clone()]
    );
    // Received heights is still removing prefixes.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                None,
                vec![bundle0.clone(), bundle1.clone(), bundle2.clone()]
            )
            .unwrap(),
        vec![bundle1.clone(), bundle2.clone()]
    );
    // Anticipated messages re-certify blocks up to the given height.
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::from(1),
                Some(BlockHeight::from(1)),
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![bundle1.clone()]
    );
    assert_eq!(
        helper
            .select_message_bundles(
                &Origin::chain(id0),
                id1,
                BlockHeight::ZERO,
                Some(BlockHeight::from(1)),
                vec![bundle0.clone(), bundle1.clone()]
            )
            .unwrap(),
        vec![bundle0.clone(), bundle1.clone()]
    );
}

#[test(tokio::test)]
async fn test_memory_leader_timeouts() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_leader_timeouts(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_leader_timeouts() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_leader_timeouts(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_leader_timeouts() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_leader_timeouts(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_leader_timeouts() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_leader_timeouts(storage).await;
}

async fn run_test_leader_timeouts<C>(storage: DbStorage<C, TestClock>)
where
    C: KeyValueStore + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStore>::Error>,
    <C as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let (pub_key0, pub_key1) = (key_pairs[0].public(), key_pairs[1].public());
    let clock = storage.clock.clone();
    let balances = vec![(ChainDescription::Root(0), pub_key0, Amount::from_tokens(2))];
    let (committee, mut worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and use the leader-based protocol in all rounds.
    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
        super_owners: Vec::new(),
        owners: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: 0,
        timeout_config: TimeoutConfig::default(),
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await.unwrap();
    let value0 = HashedValue::new_confirmed(executed_block0);
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate(certificate0, &[])
        .await
        .unwrap();

    // The leader sequence is pseudorandom but deterministic. The first leader is owner 1.
    assert_eq!(response.info.manager.leader, Some(Owner::from(pub_key1)));

    // So owner 0 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal =
        make_child_block(&value0).into_proposal_with_round(&key_pairs[0], Round::SingleLeader(0));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal =
        make_child_block(&value0).into_proposal_with_round(&key_pairs[0], Round::SingleLeader(1));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::SingleLeader(0)))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout =
        HashedValue::new_leader_timeout(chain_id, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts, where owner
    // 0 happens to be the leader.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.leader, Some(Owner::from(pub_key0)));

    // Now owner 0 can propose a block, but owner 1 can't.
    let block1 = make_child_block(&value0);
    let (executed_block1, _) = worker.stage_block_execution(block1.clone()).await.unwrap();
    let proposal1_wrong_owner = block1
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::SingleLeader(1));
    let result = worker.handle_block_proposal(proposal1_wrong_owner).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal1 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[0], Round::SingleLeader(1));
    let (response, _) = worker.handle_block_proposal(proposal1).await.unwrap();
    let value1 = HashedValue::new_validated(executed_block1.clone());

    // If we send the validated block certificate to the worker, it votes to confirm.
    let vote = response.info.manager.pending.clone().unwrap();
    let certificate1 = vote.with_value(value1.clone()).unwrap().into_certificate();
    let (response, _) = worker
        .handle_certificate(certificate1.clone(), &[], None)
        .await
        .unwrap();
    let vote = response.info.manager.pending.as_ref().unwrap();
    let value = HashedValue::new_confirmed(executed_block1.clone());
    assert_eq!(vote.value, value.lite());

    // Instead of submitting the confirmed block certificate, let rounds 2 to 4 time out, too.
    let certificate_timeout = make_certificate_with_round(
        &committee,
        &worker,
        value_timeout.clone(),
        Round::SingleLeader(4),
    );
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.leader, Some(Owner::from(pub_key1)));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(5));

    // Create block2, also at height 1, but different from block 1.
    let amount = Amount::from_tokens(1);
    let block2 = make_child_block(&value0).with_simple_transfer(ChainId::root(1), amount);
    let (executed_block2, _) = worker.stage_block_execution(block2.clone()).await.unwrap();

    // Since round 3 is already over, a validated block from round 3 won't update the validator's
    // locked block; certificate1 (with block1) remains locked.
    let value2 = HashedValue::new_validated(executed_block2.clone());
    let certificate =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::SingleLeader(2));
    worker
        .handle_certificate(certificate, &[], None)
        .await
        .unwrap();
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker
        .handle_chain_info_query(query_values.clone())
        .await
        .unwrap();
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(certificate1))
    );

    // Proposing block2 now would fail.
    let proposal = block2
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::SingleLeader(5));
    let result = worker.handle_block_proposal(proposal.clone()).await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasLockedBlock(_, _))
    );

    // But with the validated block certificate for block2, it is allowed.
    let mut proposal = block2.into_proposal_with_round(&key_pairs[1], Round::SingleLeader(5));
    let lite_value2 = value2.lite();
    let certificate2 =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::SingleLeader(4));
    proposal.validated = Some(certificate2.clone());
    let (_, _) = worker.handle_block_proposal(proposal).await.unwrap();
    let (response, _) = worker
        .handle_chain_info_query(query_values.clone())
        .await
        .unwrap();
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(certificate2))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::SingleLeader(5));

    // Let round 5 time out, too.
    let certificate_timeout = make_certificate_with_round(
        &committee,
        &worker,
        value_timeout.clone(),
        Round::SingleLeader(5),
    );
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.leader, Some(Owner::from(pub_key0)));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(6));

    // Since the validator now voted for block2, it can't vote for block1 anymore.
    let proposal = block1.into_proposal_with_round(&key_pairs[0], Round::SingleLeader(6));
    let result = worker.handle_block_proposal(proposal.clone()).await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasLockedBlock(_, _))
    );

    // Let rounds 6 and 7 time out.
    let certificate_timeout =
        make_certificate_with_round(&committee, &worker, value_timeout, Round::SingleLeader(7));
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(8));

    // If the worker does not belong to a validator, it does update its locked block even if it's
    // from a past round.
    let certificate =
        make_certificate_with_round(&committee, &worker, value1, Round::SingleLeader(7));
    let mut worker = worker.with_key_pair(None); // Forget validator keys.
    worker
        .handle_certificate(certificate.clone(), &[], None)
        .await
        .unwrap();
    let (response, _) = worker.handle_chain_info_query(query_values).await.unwrap();
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(certificate))
    );
}

#[test(tokio::test)]
async fn test_memory_round_types() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_round_types(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_round_types() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_round_types(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_round_types() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_round_types(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_round_types() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_round_types(storage).await;
}

async fn run_test_round_types<C>(storage: DbStorage<C, TestClock>)
where
    C: KeyValueStore + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStore>::Error>,
    <C as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let (pub_key0, pub_key1) = (key_pairs[0].public(), key_pairs[1].public());
    let clock = storage.clock.clone();
    let balances = vec![(ChainDescription::Root(0), pub_key0, Amount::from_tokens(2))];
    let (committee, mut worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and configure two multi-leader rounds.
    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
        super_owners: vec![pub_key0],
        owners: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: 2,
        timeout_config: TimeoutConfig {
            fast_round_duration: Some(Duration::from_secs(5)),
            ..TimeoutConfig::default()
        },
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await.unwrap();
    let value0 = HashedValue::new_confirmed(executed_block0);
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate(certificate0, &[])
        .await
        .unwrap();

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // So owner 1 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0).into_proposal_with_round(&key_pairs[1], Round::Fast);
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal =
        make_child_block(&value0).into_proposal_with_round(&key_pairs[1], Round::MultiLeader(0));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::Fast))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_id).with_leader_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await.unwrap();
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout =
        HashedValue::new_leader_timeout(chain_id, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. And multi-leader rounds can be skipped without timeout.
    let block1 = make_child_block(&value0);
    let proposal1 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(1));
    let _ = worker.handle_block_proposal(proposal1).await.unwrap();
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values).await.unwrap();
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(1));
}

#[test(tokio::test)]
async fn test_memory_fast_proposal_is_locked() {
    let storage = MemoryStorage::make_test_storage(None).await;
    run_test_fast_proposal_is_locked(storage).await;
}

#[cfg(feature = "rocksdb")]
#[test(tokio::test)]
async fn test_rocks_db_fast_proposal_is_locked() {
    let _lock = ROCKS_DB_SEMAPHORE.acquire().await;
    let (storage, _dir) = RocksDbStorage::make_test_storage(None).await;
    run_test_fast_proposal_is_locked(storage).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_fast_proposal_is_locked() {
    let storage = DynamoDbStorage::make_test_storage(None).await;
    run_test_fast_proposal_is_locked(storage).await;
}

#[cfg(feature = "scylladb")]
#[test(tokio::test)]
async fn test_scylla_db_fast_proposal_is_locked() {
    let storage = ScyllaDbStorage::make_test_storage(None).await;
    run_test_fast_proposal_is_locked(storage).await;
}

async fn run_test_fast_proposal_is_locked<C>(storage: DbStorage<C, TestClock>)
where
    C: KeyValueStore + Clone + Send + Sync + 'static,
    ViewError: From<<C as KeyValueStore>::Error>,
    <C as KeyValueStore>::Error:
        From<bcs::Error> + From<DatabaseConsistencyError> + Send + Sync + serde::ser::StdError,
{
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let (pub_key0, pub_key1) = (key_pairs[0].public(), key_pairs[1].public());
    let clock = storage.clock.clone();
    let balances = vec![(ChainDescription::Root(0), pub_key0, Amount::from_tokens(2))];
    let (committee, mut worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and configure two multi-leader rounds.
    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
        super_owners: vec![pub_key0],
        owners: vec![(pub_key0, 100), (pub_key1, 100)],
        multi_leader_rounds: 3,
        timeout_config: TimeoutConfig {
            fast_round_duration: Some(Duration::from_millis(5)),
            ..TimeoutConfig::default()
        },
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await.unwrap();
    let value0 = HashedValue::new_confirmed(executed_block0);
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate(certificate0, &[])
        .await
        .unwrap();

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // Owner 0 proposes another block. The validator votes to confirm.
    let block1 = make_child_block(&value0);
    let proposal1 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[0], Round::Fast);
    let (executed_block1, _) = worker.stage_block_execution(block1.clone()).await.unwrap();
    let value1 = HashedValue::new_confirmed(executed_block1);
    let (response, _) = worker.handle_block_proposal(proposal1).await.unwrap();
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value.value_hash, value1.hash());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Once we provide the validator with a timeout certificate, the next round starts.
    let value_timeout =
        HashedValue::new_leader_timeout(chain_id, BlockHeight::from(1), Epoch::from(0));
    let certificate_timeout =
        make_certificate_with_round(&committee, &worker, value_timeout.clone(), Round::Fast);
    let (response, _) = worker
        .handle_certificate(certificate_timeout, &[], None)
        .await
        .unwrap();
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. But block1 is locked.
    let block2 = make_child_block(&value0).with_simple_transfer(ChainId::root(1), Amount::ONE);
    let proposal2 = block2
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(0));
    let result = worker.handle_block_proposal(proposal2).await;
    assert_matches!(result, Err(WorkerError::ChainError(err))
        if matches!(*err, ChainError::HasLockedBlock(_, Round::Fast))
    );
    let proposal3 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(0));
    assert!(worker.handle_block_proposal(proposal3).await.is_ok());

    // A validated block certificate from a later round can override the locked fast block.
    let (executed_block2, _) = worker.stage_block_execution(block2.clone()).await.unwrap();
    let value2 = HashedValue::new_validated(executed_block2.clone());
    let mut proposal = block2
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(1));
    let lite_value2 = value2.lite();
    let certificate2 =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::MultiLeader(0));
    proposal.validated = Some(certificate2.clone());
    let (_, _) = worker.handle_block_proposal(proposal).await.unwrap();
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values).await.unwrap();
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(certificate2))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::MultiLeader(1));

    // Re-proposing the locked block also works.
    let proposal = block2.into_proposal_with_round(&key_pairs[1], Round::MultiLeader(2));
    let (_, _) = worker.handle_block_proposal(proposal).await.unwrap();
    let certificate3 =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::MultiLeader(2));
    worker
        .handle_certificate(certificate3.clone(), &[], None)
        .await
        .unwrap();
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values).await.unwrap();
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(certificate3))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, value2.validated_to_confirmed().unwrap().lite());
    assert_eq!(vote.round, Round::MultiLeader(2));
}
