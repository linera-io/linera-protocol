// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{CryptoHash, *},
    data_types::*,
    hashed::Hashed,
    identifiers::{
        Account, AccountOwner, ChainDescription, ChainId, ChannelName, Destination,
        GenericApplicationId, MessageId, Owner,
    },
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ChainAndHeight, ChannelFullName, ExecutedBlock,
        IncomingBundle, LiteValue, LiteVote, Medium, MessageAction, MessageBundle, Origin,
        OutgoingMessage, PostedMessage, ProposedBlock, SignatureAggregator,
    },
    manager::LockedBlock,
    test::{make_child_block, make_first_block, BlockTestExt, MessageTestExt, VoteTestExt},
    types::{
        CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate, Timeout,
        ValidatedBlock,
    },
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemChannel, SystemMessage, SystemOperation,
    },
    test_utils::{ExpectedCall, RegisterMockApplication, SystemExecutionState},
    ChannelSubscription, ExecutionError, Message, MessageKind, Query, QueryContext, Response,
    SystemExecutionError, SystemQuery, SystemResponse,
};
use linera_storage::{DbStorage, Storage, TestClock};
use linera_views::{
    memory::MemoryStore,
    random::generate_test_namespace,
    store::TestKeyValueStore as _,
    views::{CryptoHashView, RootView},
};
use test_case::test_case;
use test_log::test;

#[cfg(feature = "dynamodb")]
use crate::test_utils::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
use crate::{
    chain_worker::CrossChainUpdateHelper,
    data_types::*,
    test_utils::{MemoryStorageBuilder, StorageBuilder},
    worker::{
        Notification,
        Reason::{self, NewBlock, NewIncomingBundle},
        WorkerError, WorkerState,
    },
};

/// The test worker accepts blocks with a timestamp this far in the future.
const TEST_GRACE_PERIOD_MICROS: u64 = 500_000;

/// Instantiates the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker<S>(
    storage: S,
    is_client: bool,
    has_long_lived_services: bool,
) -> (Committee, WorkerState<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let key_pair = KeyPair::generate();
    let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
    let worker = WorkerState::new(
        "Single validator node".to_string(),
        Some(key_pair),
        storage,
        NonZeroUsize::new(10).expect("Chain worker limit should not be zero"),
    )
    .with_allow_inactive_chains(is_client)
    .with_allow_messages_from_deprecated_epochs(is_client)
    .with_long_lived_services(has_long_lived_services)
    .with_grace_period(Duration::from_micros(TEST_GRACE_PERIOD_MICROS));
    (committee, worker)
}

/// Same as `init_worker` but also instantiates some initial chains.
async fn init_worker_with_chains<S, I>(storage: S, balances: I) -> (Committee, WorkerState<S>)
where
    I: IntoIterator<Item = (ChainDescription, Owner, Amount)>,
    S: Storage + Clone + Send + Sync + 'static,
{
    let (committee, worker) = init_worker(
        storage, /* is_client */ false, /* has_long_lived_services */ false,
    );
    for (description, owner, balance) in balances {
        worker
            .storage
            .create_chain(
                committee.clone(),
                ChainId::root(0),
                description,
                owner,
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
    owner: Owner,
    balance: Amount,
) -> (Committee, WorkerState<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    init_worker_with_chains(storage, [(description, owner, balance)]).await
}

fn make_certificate<S, T>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: Hashed<T>,
) -> GenericCertificate<T>
where
    S: Storage,
    T: CertificateValue,
{
    make_certificate_with_round(committee, worker, value, Round::MultiLeader(0))
}

fn make_certificate_with_round<S, T>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: Hashed<T>,
    round: Round,
) -> GenericCertificate<T>
where
    S: Storage,
    T: CertificateValue,
{
    let vote = LiteVote::new(
        LiteValue::new(&value),
        round,
        worker.chain_worker_config.key_pair().unwrap(),
    );
    let mut builder = SignatureAggregator::new(value, round, committee);
    builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap()
}

#[expect(clippy::too_many_arguments)]
async fn make_simple_transfer_certificate<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    target_id: ChainId,
    amount: Amount,
    incoming_bundles: Vec<IncomingBundle>,
    committee: &Committee,
    balance: Amount,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
) -> ConfirmedBlockCertificate
where
    S: Storage,
{
    make_transfer_certificate_for_epoch(
        chain_description,
        key_pair,
        Some(key_pair.public().into()),
        None,
        Recipient::chain(target_id),
        amount,
        incoming_bundles,
        Epoch::ZERO,
        committee,
        balance,
        BTreeMap::new(),
        worker,
        previous_confirmed_block,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn make_transfer_certificate<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    source: Option<Owner>,
    recipient: Recipient,
    amount: Amount,
    incoming_bundles: Vec<IncomingBundle>,
    committee: &Committee,
    balance: Amount,
    balances: BTreeMap<AccountOwner, Amount>,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
) -> ConfirmedBlockCertificate
where
    S: Storage,
{
    make_transfer_certificate_for_epoch(
        chain_description,
        key_pair,
        source.or_else(|| Some(key_pair.public().into())),
        source,
        recipient,
        amount,
        incoming_bundles,
        Epoch::ZERO,
        committee,
        balance,
        balances,
        worker,
        previous_confirmed_block,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn make_transfer_certificate_for_epoch<S>(
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    authenticated_signer: Option<Owner>,
    source: Option<Owner>,
    recipient: Recipient,
    amount: Amount,
    incoming_bundles: Vec<IncomingBundle>,
    epoch: Epoch,
    committee: &Committee,
    balance: Amount,
    balances: BTreeMap<AccountOwner, Amount>,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
) -> ConfirmedBlockCertificate
where
    S: Storage,
{
    let chain_id = chain_description.into();
    let system_state = SystemExecutionState {
        committees: [(epoch, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(key_pair.public().into()),
        balance,
        balances,
        ..SystemExecutionState::new(epoch, chain_description, ChainId::root(0))
    };
    let block_template = match &previous_confirmed_block {
        None => make_first_block(chain_id),
        Some(cert) => make_child_block(cert.value()),
    };

    let mut messages = incoming_bundles
        .iter()
        .flat_map(|incoming_bundle| {
            incoming_bundle
                .bundle
                .messages
                .iter()
                .map(|posted_message| {
                    if matches!(incoming_bundle.action, MessageAction::Reject)
                        && matches!(posted_message.kind, MessageKind::Tracked)
                    {
                        vec![OutgoingMessage {
                            authenticated_signer: posted_message.authenticated_signer,
                            destination: Destination::Recipient(incoming_bundle.origin.sender),
                            grant: Amount::ZERO,
                            refund_grant_to: None,
                            kind: MessageKind::Bouncing,
                            message: posted_message.message.clone(),
                        }]
                    } else {
                        Vec::new()
                    }
                })
        })
        .collect::<Vec<_>>();

    let block = ProposedBlock {
        epoch,
        incoming_bundles,
        authenticated_signer,
        ..block_template
    }
    .with_transfer(source, recipient, amount);
    match recipient {
        Recipient::Account(account) => {
            messages.push(vec![direct_outgoing_message(
                account.chain_id,
                MessageKind::Tracked,
                SystemMessage::Credit {
                    source: source.map(AccountOwner::User),
                    target: account.owner,
                    amount,
                },
            )]);
        }
        Recipient::Burn => messages.push(Vec::new()),
    }
    let tx_count = block.operations.len() + block.incoming_bundles.len();
    let oracle_responses = iter::repeat_with(Vec::new).take(tx_count).collect();
    let events = iter::repeat_with(Vec::new).take(tx_count).collect();
    let state_hash = system_state.into_hash().await;
    let value = Hashed::new(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages,
            events,
            state_hash,
            oracle_responses,
        }
        .with(block),
    ));
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
fn update_recipient_direct(
    recipient: ChainId,
    certificate: &ConfirmedBlockCertificate,
) -> CrossChainRequest {
    let sender = certificate.inner().block().header.chain_id;
    let bundles = certificate.message_bundles_for(&Medium::Direct, recipient);
    CrossChainRequest::UpdateRecipient {
        sender,
        recipient,
        bundle_vecs: vec![(Medium::Direct, bundles.collect())],
    }
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_bad_signature<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (_, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_first_proposal(&sender_key_pair);
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
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_zero_amount<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (_, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    // test block non-positive amount
    let zero_amount_block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::ZERO)
        .with_authenticated_signer(Some(sender_key_pair.public().into()))
        .into_first_proposal(&sender_key_pair);
    assert_matches!(
    worker
        .handle_block_proposal(zero_amount_block_proposal)
        .await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
            execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::SystemError(
            SystemExecutionError::IncorrectTransferAmount
        )))
    );
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_ticks<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let key_pair = KeyPair::generate();
    let balance = Amount::from_tokens(5);
    let balances = vec![(ChainDescription::Root(1), key_pair.public().into(), balance)];
    let epoch = Epoch::ZERO;
    let (committee, worker) = init_worker_with_chains(storage, balances).await;

    {
        let block_proposal = make_first_block(ChainId::root(1))
            .with_timestamp(Timestamp::from(TEST_GRACE_PERIOD_MICROS + 1_000_000))
            .into_first_proposal(&key_pair);
        // Timestamp too far in the future
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::InvalidTimestamp)
        );
    }

    let block_0_time = Timestamp::from(TEST_GRACE_PERIOD_MICROS);
    let certificate = {
        let block = make_first_block(ChainId::root(1)).with_timestamp(block_0_time);
        let block_proposal = block.clone().into_first_proposal(&key_pair);
        let future = worker.handle_block_proposal(block_proposal);
        clock.set(block_0_time);
        future.await?;

        let system_state = SystemExecutionState {
            committees: [(epoch, committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(key_pair.public().into()),
            balance,
            timestamp: block_0_time,
            ..SystemExecutionState::new(epoch, ChainDescription::Root(1), ChainId::root(0))
        };
        let state_hash = system_state.into_hash().await;
        let value = Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                state_hash,
                ..BlockExecutionOutcome::default()
            }
            .with(block),
        ));
        make_certificate(&committee, &worker, value)
    };
    worker
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    {
        let block_proposal = make_child_block(&certificate.into_value())
            .with_timestamp(block_0_time.saturating_sub_micros(1))
            .into_first_proposal(&key_pair);
        // Timestamp older than previous one
        assert_matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::InvalidBlockTimestamp {..})
        );
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_unknown_sender<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (_, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let unknown_key = KeyPair::generate();
    let unknown_sender_block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .into_first_proposal(&unknown_key);
    assert_matches!(
        worker
            .handle_block_proposal(unknown_sender_block_proposal)
            .await,
        Err(WorkerError::InvalidOwner)
    );
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_with_chaining<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chain(
        storage_builder.build().await?,
        ChainDescription::Root(1),
        sender_key_pair.public().into(),
        Amount::from_tokens(5),
    )
    .await;
    let block_proposal0 = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::ONE)
        .with_authenticated_signer(Some(sender_key_pair.public().into()))
        .into_first_proposal(&sender_key_pair);
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
    let block_proposal1 = make_child_block(certificate0.value())
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(2))
        .into_first_proposal(&sender_key_pair);

    assert_matches!(
        worker.handle_block_proposal(block_proposal1.clone()).await,
        Err(WorkerError::ChainError(error)) if matches!(
            *error,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: BlockHeight(0),
                found_block_height: BlockHeight(1)
            })
    );
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());

    drop(chain);
    worker
        .handle_block_proposal(block_proposal0.clone())
        .await?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    let executed_block: ExecutedBlock = chain
        .manager
        .validated_vote()
        .unwrap()
        .value()
        .inner()
        .block()
        .clone()
        .into();
    // Multi-leader round - it's not confirmed yet.
    assert_eq!(&executed_block.block, &block_proposal0.content.block);
    assert!(chain.manager.confirmed_vote().is_none());
    let block_certificate0 = make_certificate(
        &committee,
        &worker,
        chain.manager.validated_vote().unwrap().value().clone(),
    );
    drop(chain);

    worker
        .handle_validated_certificate(block_certificate0)
        .await?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    let executed_block: ExecutedBlock = chain
        .manager
        .confirmed_vote()
        .unwrap()
        .value()
        .inner()
        .block()
        .clone()
        .into();

    // Should be confirmed after handling the certificate.
    assert_eq!(&executed_block.block, &block_proposal0.content.block);
    assert!(chain.manager.validated_vote().is_none());
    drop(chain);

    worker
        .handle_confirmed_certificate(certificate0, None)
        .await?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    drop(chain);

    worker
        .handle_block_proposal(block_proposal1.clone())
        .await?;

    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    let executed_block: ExecutedBlock = chain
        .manager
        .validated_vote()
        .unwrap()
        .value()
        .inner()
        .block()
        .clone()
        .into();
    assert_eq!(&executed_block.block, &block_proposal1.content.block);
    assert!(chain.manager.confirmed_vote().is_none());
    drop(chain);
    assert_matches!(
        worker.handle_block_proposal(block_proposal0).await,
        Err(WorkerError::ChainError(error)) if matches!(
            *error,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: BlockHeight(1),
                found_block_height: BlockHeight(0),
            })
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_with_incoming_bundles<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(6),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public().into(),
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
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![
                    vec![direct_credit_message(ChainId::root(2), Amount::ONE)],
                    vec![direct_credit_message(
                        ChainId::root(2),
                        Amount::from_tokens(2),
                    )],
                ],
                events: vec![Vec::new(); 2],
                state_hash: SystemExecutionState {
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    ownership: ChainOwnership::single(sender_key_pair.public().into()),
                    balance: Amount::from_tokens(3),
                    ..SystemExecutionState::new(epoch, ChainDescription::Root(1), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new(); 2],
            }
            .with(
                make_first_block(ChainId::root(1))
                    .with_simple_transfer(ChainId::root(2), Amount::ONE)
                    .with_simple_transfer(ChainId::root(2), Amount::from_tokens(2))
                    .with_authenticated_signer(Some(sender_key_pair.public().into())),
            ),
        )),
    );

    let certificate1 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![vec![direct_credit_message(
                    ChainId::root(2),
                    Amount::from_tokens(3),
                )]],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    ownership: ChainOwnership::single(sender_key_pair.public().into()),
                    ..SystemExecutionState::new(epoch, ChainDescription::Root(1), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_child_block(&certificate0.clone().into_value())
                    .with_simple_transfer(ChainId::root(2), Amount::from_tokens(3))
                    .with_authenticated_signer(Some(sender_key_pair.public().into())),
            ),
        )),
    );
    // Missing earlier blocks
    assert_matches!(
        worker
            .handle_confirmed_certificate(certificate1.clone(), None)
            .await,
        Err(WorkerError::MissingEarlierBlocks { .. })
    );

    // Run transfers
    let notifications = Arc::new(Mutex::new(Vec::new()));
    worker
        .fully_handle_certificate_with_notifications(certificate0.clone(), &notifications)
        .await?;
    worker
        .fully_handle_certificate_with_notifications(certificate1.clone(), &notifications)
        .await?;
    assert_eq!(
        *notifications.lock().unwrap(),
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
                reason: NewIncomingBundle {
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
                reason: NewIncomingBundle {
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight(1)
                }
            }
        ]
    );
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(6))
            .with_authenticated_signer(Some(recipient_key_pair.public().into()))
            .into_first_proposal(&recipient_key_pair);
        // Insufficient funding
        assert_matches!(
                worker.handle_block_proposal(block_proposal).await,
                Err(
                    WorkerError::ChainError(error)
                ) if matches!(&*error, ChainError::ExecutionError(
                    execution_error, ChainExecutionContext::Operation(_)
                ) if matches!(**execution_error, ExecutionError::SystemError(
                    SystemExecutionError::InsufficientFunding { .. }
                )))
        );
    }
    {
        let block_proposal = make_first_block(ChainId::root(2))
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(5))
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::from_tokens(2)) // wrong amount
                            .to_posted(0, MessageKind::Tracked),
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_key_pair.public().into()))
            .into_first_proposal(&recipient_key_pair);
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
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_key_pair.public().into()))
            .into_first_proposal(&recipient_key_pair);
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
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(3))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_key_pair.public().into()))
            .into_first_proposal(&recipient_key_pair);
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
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_key_pair.public().into()))
            .into_first_proposal(&recipient_key_pair);
        // Taking the first message only is ok.
        worker.handle_block_proposal(block_proposal.clone()).await?;
        let certificate: ConfirmedBlockCertificate = make_certificate(
            &committee,
            &worker,
            Hashed::new(ConfirmedBlock::new(
                BlockExecutionOutcome {
                    messages: vec![
                        Vec::new(),
                        vec![direct_credit_message(ChainId::root(3), Amount::ONE)],
                    ],
                    events: vec![Vec::new(); 2],
                    state_hash: SystemExecutionState {
                        committees: [(epoch, committee.clone())].into_iter().collect(),
                        ownership: ChainOwnership::single(recipient_key_pair.public().into()),
                        ..SystemExecutionState::new(epoch, ChainDescription::Root(2), admin_id)
                    }
                    .into_hash()
                    .await,
                    oracle_responses: vec![Vec::new(); 2],
                }
                .with(block_proposal.content.block),
            )),
        );
        worker
            .handle_confirmed_certificate(certificate.clone(), None)
            .await?;

        // Then receive the next two messages.
        let block_proposal = make_child_block(&certificate.into_value())
            .with_simple_transfer(ChainId::root(3), Amount::from_tokens(3))
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::from(0),
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(3))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .into_first_proposal(&recipient_key_pair);
        worker.handle_block_proposal(block_proposal.clone()).await?;
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_exceed_balance<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (_, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(1000))
        .with_authenticated_signer(Some(sender_key_pair.public().into()))
        .into_first_proposal(&sender_key_pair);
    assert_matches!(
        worker.handle_block_proposal(block_proposal).await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
                execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::SystemError(
            SystemExecutionError::InsufficientFunding { .. }
        )))
    );
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public().into(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .with_authenticated_signer(Some(sender_key_pair.public().into()))
        .into_first_proposal(&sender_key_pair);

    let (chain_info_response, _actions) = worker.handle_block_proposal(block_proposal).await?;
    chain_info_response.check(&ValidatorName(worker.public_key()))?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none()); // It was a multi-leader
                                                       // round.
    let validated_certificate = make_certificate(
        &committee,
        &worker,
        chain.manager.validated_vote().unwrap().value().clone(),
    );
    drop(chain);

    let (chain_info_response, _actions) = worker
        .handle_validated_certificate(validated_certificate)
        .await?;
    chain_info_response.check(&ValidatorName(worker.public_key()))?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert!(chain.manager.validated_vote().is_none()); // Should be confirmed by now.
    let pending_vote = chain.manager.confirmed_vote().unwrap().lite();
    assert_eq!(
        chain_info_response.info.manager.pending.unwrap(),
        pending_vote
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_replay<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (_, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    let block_proposal = make_first_block(ChainId::root(1))
        .with_simple_transfer(ChainId::root(2), Amount::from_tokens(5))
        .with_authenticated_signer(Some(sender_key_pair.public().into()))
        .into_first_proposal(&sender_key_pair);

    let (response, _actions) = worker.handle_block_proposal(block_proposal.clone()).await?;
    response.check(&ValidatorName(worker.public_key()))?;
    let (replay_response, _actions) = worker.handle_block_proposal(block_proposal).await?;
    // Workaround lack of equality.
    assert_eq!(
        CryptoHash::new(&*response.info),
        CryptoHash::new(&*replay_response.info)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_unknown_sender<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2).into(),
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
            .fully_handle_certificate_with_notifications(certificate, &())
            .await,
        Err(WorkerError::ChainError(error)) if matches!(*error, ChainError::InactiveChain {..})
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_open_chain<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![(
            ChainDescription::Root(2),
            PublicKey::test_key(2).into(),
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
    let ownership = ChainOwnership::single(sender_key_pair.public().into());
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
    let open_chain_message = IncomingBundle {
        origin: Origin::chain(ChainId::root(3)),
        bundle: MessageBundle {
            certificate_hash: CryptoHash::test_hash("certificate"),
            height: BlockHeight::ZERO,
            timestamp: Timestamp::from(0),
            transaction_index: 0,
            messages: vec![Message::System(SystemMessage::OpenChain(OpenChainConfig {
                ownership,
                admin_id,
                epoch,
                committees,
                balance,
                application_permissions: Default::default(),
            }))
            .to_posted(0, MessageKind::Protected)],
        },
        action: MessageAction::Accept,
    };
    let value = Hashed::new(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![Vec::new()],
            events: vec![Vec::new()],
            state_hash: state.into_hash().await,
            oracle_responses: vec![Vec::new()],
        }
        .with(make_first_block(chain_id).with_incoming_bundle(open_chain_message)),
    ));
    let certificate = make_certificate(&committee, &worker, value);
    let info = worker
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?
        .info;
    assert_eq!(info.next_block_height, BlockHeight::from(1));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_wrong_owner<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let chain_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![(
            ChainDescription::Root(2),
            chain_key_pair.public().into(),
            Amount::from_tokens(5),
        )],
    )
    .await;
    let certificate = make_transfer_certificate_for_epoch(
        ChainDescription::Root(2),
        &sender_key_pair,
        Some(chain_key_pair.public().into()),
        None,
        Recipient::chain(ChainId::root(2)),
        Amount::from_tokens(5),
        Vec::new(),
        Epoch::ZERO,
        &committee,
        Amount::ZERO,
        BTreeMap::new(),
        &worker,
        None,
    )
    .await;
    // This fails because `make_simple_transfer_certificate` uses `sender_key_pair.public()` to
    // compute the hash of the execution state.
    assert_matches!(
        worker
            .fully_handle_certificate_with_notifications(certificate, &())
            .await,
        Err(WorkerError::IncorrectOutcome { .. })
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_bad_block_height<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    worker
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?;
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_anticipated_incoming_bundle<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
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
        vec![IncomingBundle {
            origin: Origin::chain(ChainId::root(3)),
            bundle: MessageBundle {
                certificate_hash: CryptoHash::test_hash("certificate"),
                height: BlockHeight::ZERO,
                timestamp: Timestamp::from(0),
                transaction_index: 0,
                messages: vec![system_credit_message(Amount::from_tokens(995))
                    .to_posted(0, MessageKind::Tracked)],
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get());
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    let inbox = chain
        .inboxes
        .try_load_entry(&Origin::chain(ChainId::root(3)))
        .await?
        .expect("Missing inbox for `ChainId::root(3)` in `ChainId::root(1)`");
    assert_eq!(BlockHeight::ZERO, inbox.next_block_height_to_receive()?);
    assert_eq!(inbox.added_bundles.count(), 0);
    assert_matches!(
        inbox
            .removed_bundles
            .front()
            .await?
            .unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == CryptoHash::test_hash("certificate")
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && matches!(messages[..], [PostedMessage {
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Tracked,
                index: 0,
                message: Message::System(SystemMessage::Credit { amount, .. }),
            }] if amount == Amount::from_tokens(995)),
        "Unexpected bundle",
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
    let chain = worker.chain_state_view(ChainId::root(2)).await?;
    assert!(chain.is_active());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_receiver_balance_overflow<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::ONE,
            ),
            (
                ChainDescription::Root(2),
                PublicKey::test_key(2).into(),
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let new_sender_chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(new_sender_chain.is_active());
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
    let new_recipient_chain = worker.chain_state_view(ChainId::root(2)).await?;
    assert!(new_recipient_chain.is_active());
    assert_eq!(
        Amount::MAX,
        *new_recipient_chain.execution_state.system.balance.get()
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_receiver_equal_sender<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let key_pair = KeyPair::generate();
    let owner = key_pair.public().into();
    let (committee, worker) =
        init_worker_with_chain(storage, ChainDescription::Root(1), owner, Amount::ONE).await;

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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let chain = worker.chain_state_view(ChainId::root(1)).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get());
    let inbox = chain
        .inboxes
        .try_load_entry(&Origin::chain(ChainId::root(1)))
        .await?
        .expect("Missing inbox for `ChainId::root(1)` in `ChainId::root(1)`");
    assert_eq!(BlockHeight::from(1), inbox.next_block_height_to_receive()?);
    assert_matches!(
        inbox.added_bundles.front().await?.unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == certificate.hash()
        && height == BlockHeight::ZERO
        && timestamp == Timestamp::from(0)
        && matches!(messages[..], [PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            index: 0,
            message: Message::System(SystemMessage::Credit { amount, .. })
        }] if amount == Amount::ONE),
        "Unexpected bundle",
    );
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chain(
        storage_builder.build().await?,
        ChainDescription::Root(2),
        PublicKey::test_key(2).into(),
        Amount::ONE,
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
        .handle_cross_chain_request(update_recipient_direct(
            ChainId::root(2),
            &certificate.clone(),
        ))
        .await?;
    let chain = worker.chain_state_view(ChainId::root(2)).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ONE, *chain.execution_state.system.balance.get());
    assert_eq!(BlockHeight::ZERO, chain.tip_state.get().next_block_height);
    let inbox = chain
        .inboxes
        .try_load_entry(&Origin::chain(ChainId::root(1)))
        .await?
        .expect("Missing inbox for `ChainId::root(1)` in `ChainId::root(2)`");
    assert_eq!(BlockHeight::from(1), inbox.next_block_height_to_receive()?);
    assert_matches!(
        inbox
            .added_bundles
            .front()
            .await?
            .unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == certificate.hash()
        && height == BlockHeight::ZERO
        && timestamp == Timestamp::from(0)
        && matches!(messages[..], [PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            index: 0,
            message: Message::System(SystemMessage::Credit { amount, .. })
        }] if amount == Amount::from_tokens(10)),
        "Unexpected bundle",
    );
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.tip_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker(
        storage, /* is_client */ false, /* has_long_lived_services */ false,
    );
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
        .await?
        .cross_chain_requests
        .is_empty());
    let chain = worker.chain_state_view(ChainId::root(2)).await?;
    // The target chain did not receive the message
    assert!(chain.inboxes.indices().await?.is_empty());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain_on_client<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker(
        storage, /* is_client */ true, /* has_long_lived_services */ false,
    );
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
        .await?;
    assert_matches!(
        actions.cross_chain_requests.as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    );
    assert_eq!(
        actions.notifications,
        vec![Notification {
            chain_id: ChainId::root(2),
            reason: Reason::NewIncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                height: BlockHeight::ZERO,
            }
        }]
    );
    let chain = worker.chain_state_view(ChainId::root(2)).await?;
    assert!(!chain.inboxes.indices().await?.is_empty());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_to_active_recipient<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(5),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public().into(),
                Amount::ZERO,
            ),
        ],
    )
    .await;
    assert_eq!(
        worker
            .query_application(ChainId::root(1), Query::System(SystemQuery))
            .await?,
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Amount::from_tokens(5),
        })
    );
    assert_eq!(
        worker
            .query_application(ChainId::root(2), Query::System(SystemQuery))
            .await?,
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    assert_eq!(
        worker
            .query_application(ChainId::root(1), Query::System(SystemQuery))
            .await?,
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
        vec![IncomingBundle {
            origin: Origin::chain(ChainId::root(1)),
            bundle: MessageBundle {
                certificate_hash: certificate.hash(),
                height: BlockHeight::ZERO,
                timestamp: Timestamp::from(0),
                transaction_index: 0,
                messages: vec![system_credit_message(Amount::from_tokens(5))
                    .to_posted(0, MessageKind::Tracked)],
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    assert_eq!(
        worker
            .query_application(ChainId::root(2), Query::System(SystemQuery))
            .await?,
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Amount::from_tokens(4),
        })
    );

    {
        let recipient_chain = worker.chain_state_view(ChainId::root(2)).await?;
        assert!(recipient_chain.is_active());
        assert_eq!(
            *recipient_chain.execution_state.system.balance.get(),
            Amount::from_tokens(4)
        );
        let ownership = &recipient_chain.manager.ownership.get();
        assert!(
            ownership
                .owners
                .contains_key(&recipient_key_pair.public().into())
                && ownership.super_owners.is_empty()
                && ownership.owners.len() == 1
        );
        assert_eq!(recipient_chain.confirmed_log.count(), 1);
        assert_eq!(
            recipient_chain.tip_state.get().block_hash,
            Some(certificate.hash())
        );
        assert_eq!(recipient_chain.received_log.count(), 1);
    }
    let query = ChainInfoQuery::new(ChainId::root(2)).with_received_log_excluding_first_n(0);
    let (response, _actions) = worker.handle_chain_info_query(query).await?;
    assert_eq!(response.info.requested_received_log.len(), 1);
    assert_eq!(
        response.info.requested_received_log[0],
        ChainAndHeight {
            chain_id: ChainId::root(1),
            height: BlockHeight::ZERO
        }
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_to_inactive_recipient<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chain(
        storage_builder.build().await?,
        ChainDescription::Root(1),
        sender_key_pair.public().into(),
        Amount::from_tokens(5),
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
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_rejected_transfer<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = KeyPair::generate();
    let sender = Owner::from(sender_key_pair.public());
    let sender_account = Account {
        chain_id: ChainId::root(1),
        owner: Some(AccountOwner::User(sender)),
    };

    let recipient_key_pair = KeyPair::generate();
    let recipient = Owner::from(sender_key_pair.public());
    let recipient_account = Account {
        chain_id: ChainId::root(2),
        owner: Some(AccountOwner::User(recipient)),
    };

    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public().into(),
                Amount::from_tokens(6),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public().into(),
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
        .fully_handle_certificate_with_notifications(certificate00.clone(), &())
        .await?;

    let certificate01 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        None,
        Recipient::Burn,
        Amount::ONE,
        vec![IncomingBundle {
            origin: Origin::chain(ChainId::root(1)),
            bundle: MessageBundle {
                certificate_hash: certificate00.hash(),
                height: BlockHeight::from(0),
                timestamp: Timestamp::from(0),
                transaction_index: 0,
                messages: vec![Message::System(SystemMessage::Credit {
                    source: None,
                    target: Some(AccountOwner::User(sender)),
                    amount: Amount::from_tokens(5),
                })
                .to_posted(0, MessageKind::Tracked)],
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(sender.into(), Amount::from_tokens(5))]),
        &worker,
        Some(&certificate00),
    )
    .await;

    worker
        .fully_handle_certificate_with_notifications(certificate01.clone(), &())
        .await?;

    {
        let chain = worker.chain_state_view(ChainId::root(1)).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
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
        BTreeMap::from_iter([(sender.into(), Amount::from_tokens(2))]),
        &worker,
        Some(&certificate01),
    )
    .await;

    worker
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    let certificate2 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Some(sender),
        Recipient::Account(recipient_account),
        Amount::from_tokens(2),
        Vec::new(),
        &committee,
        Amount::ZERO,
        BTreeMap::new(),
        &worker,
        Some(&certificate1),
    )
    .await;

    worker
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    // Reject the first transfer and try to use the money of the second one.
    let certificate = make_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        Some(recipient),
        Recipient::Burn,
        Amount::ONE,
        vec![
            IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(2),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![Message::System(SystemMessage::Credit {
                        source: Some(AccountOwner::User(sender)),
                        target: Some(AccountOwner::User(recipient)),
                        amount: Amount::from_tokens(3),
                    })
                    .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Reject,
            },
            IncomingBundle {
                origin: Origin::chain(ChainId::root(1)),
                bundle: MessageBundle {
                    certificate_hash: certificate2.hash(),
                    height: BlockHeight::from(3),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![Message::System(SystemMessage::Credit {
                        source: Some(AccountOwner::User(sender)),
                        target: Some(AccountOwner::User(recipient)),
                        amount: Amount::from_tokens(2),
                    })
                    .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            },
        ],
        &committee,
        Amount::ZERO,
        BTreeMap::from_iter([(recipient.into(), Amount::from_tokens(1))]),
        &worker,
        None,
    )
    .await;

    worker
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    {
        let chain = worker.chain_state_view(ChainId::root(2)).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
    }

    // Process the bounced message and try to use the refund.
    let certificate3 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Some(sender),
        Recipient::Burn,
        Amount::from_tokens(3),
        vec![IncomingBundle {
            origin: Origin::chain(ChainId::root(2)),
            bundle: MessageBundle {
                certificate_hash: certificate.hash(),
                height: BlockHeight::from(0),
                timestamp: Timestamp::from(0),
                transaction_index: 0,
                messages: vec![Message::System(SystemMessage::Credit {
                    source: Some(AccountOwner::User(sender)),
                    target: Some(AccountOwner::User(recipient)),
                    amount: Amount::from_tokens(3),
                })
                .to_posted(0, MessageKind::Bouncing)],
            },
            action: MessageAction::Accept,
        }],
        &committee,
        Amount::ZERO,
        BTreeMap::new(),
        &worker,
        Some(&certificate2),
    )
    .await;

    worker
        .fully_handle_certificate_with_notifications(certificate3.clone(), &())
        .await?;

    {
        let chain = worker.chain_state_view(ChainId::root(1)).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn run_test_chain_creation_with_committee_creation<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let key_pair = KeyPair::generate();
    let (committee, worker) = init_worker_with_chain(
        storage_builder.build().await?,
        ChainDescription::Root(0),
        key_pair.public().into(),
        Amount::from_tokens(2),
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
    let user_description = ChainDescription::Child(MessageId {
        chain_id: admin_id,
        height: BlockHeight::ZERO,
        index: 0,
    });
    let user_id = ChainId::from(user_description);
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![vec![
                    direct_outgoing_message(
                        user_id,
                        MessageKind::Protected,
                        SystemMessage::OpenChain(OpenChainConfig {
                            ownership: ChainOwnership::single(key_pair.public().into()),
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
                ]],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: committees.clone(),
                    ownership: ChainOwnership::single(key_pair.public().into()),
                    balance: Amount::from_tokens(2),
                    ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(0), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_first_block(admin_id)
                    .with_operation(SystemOperation::OpenChain(OpenChainConfig {
                        ownership: ChainOwnership::single(key_pair.public().into()),
                        epoch: Epoch::ZERO,
                        committees: committees.clone(),
                        admin_id,
                        balance: Amount::ZERO,
                        application_permissions: Default::default(),
                    }))
                    .with_authenticated_signer(Some(key_pair.public().into())),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;
    {
        let admin_chain = worker.chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        admin_chain.validate_incoming_bundles().await?;
        assert_eq!(
            BlockHeight::from(1),
            admin_chain.tip_state.get().next_block_height
        );
        assert!(admin_chain.outboxes.indices().await?.is_empty());
        assert_eq!(
            *admin_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        // The new chain is subscribed to the admin chain.
        assert!(admin_chain
            .channels
            .indices()
            .await?
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
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![
                    vec![channel_admin_message(SystemMessage::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    })],
                    vec![direct_credit_message(user_id, Amount::from_tokens(2))],
                ],
                events: vec![Vec::new(); 2],
                state_hash: SystemExecutionState {
                    // The root chain knows both committees at the end.
                    committees: committees2.clone(),
                    ownership: ChainOwnership::single(key_pair.public().into()),
                    ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new(); 2],
            }
            .with(
                make_child_block(&certificate0.clone().into_value())
                    .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    }))
                    .with_simple_transfer(user_id, Amount::from_tokens(2)),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    {
        // The root chain has 1 subscriber.
        let admin_chain = worker.chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        admin_chain.validate_incoming_bundles().await?;
        assert_eq!(
            admin_chain
                .channels
                .try_load_entry(&admin_channel_full_name)
                .await?
                .expect("Missing channel for admin channel in `ChainId::root(0)`")
                .subscribers
                .indices()
                .await?
                .len(),
            1
        );
    }
    {
        // The child is active and has not migrated yet.
        let user_chain = worker.chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
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
                .await?
                .len(),
            1
        );
        user_chain.validate_incoming_bundles().await?;
        matches!(
            &user_chain
                .inboxes
                .try_load_entry(&Origin::chain(admin_id))
                .await?
                .expect("Missing inbox for admin chain in user chain")
                .added_bundles
                .read_front(10)
                .await?[..],
            [bundle1, bundle2]
            if matches!(bundle1.messages[..], [PostedMessage {
                 message: Message::System(SystemMessage::OpenChain(_)), ..
            }]) && matches!(bundle2.messages[..], [PostedMessage {
                message: Message::System(SystemMessage::Credit { .. }), ..
            }])
        );
        let channel_inbox = user_chain
            .inboxes
            .try_load_entry(&admin_channel_origin)
            .await?
            .expect("Missing inbox for admin channel in user chain");
        matches!(&channel_inbox.added_bundles.read_front(10).await?[..], [bundle]
            if matches!(bundle.messages[..], [PostedMessage {
                message: Message::System(SystemMessage::CreateCommittee { .. }), ..
            }])
        );
        assert_eq!(channel_inbox.removed_bundles.count(), 0);
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 1);
    }
    // Make the child receive the pending messages.
    let certificate3 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![Vec::new(); 3],
                events: vec![Vec::new(); 3],
                state_hash: SystemExecutionState {
                    subscriptions: [ChannelSubscription {
                        chain_id: admin_id,
                        name: SystemChannel::Admin.name(),
                    }]
                    .into_iter()
                    .collect(),
                    // Finally the child knows about both committees and has the money.
                    committees: committees2.clone(),
                    ownership: ChainOwnership::single(key_pair.public().into()),
                    balance: Amount::from_tokens(2),
                    ..SystemExecutionState::new(Epoch::from(1), user_description, admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new(); 3],
            }
            .with(
                make_first_block(user_id)
                    .with_incoming_bundle(IncomingBundle {
                        origin: Origin::chain(admin_id),
                        bundle: MessageBundle {
                            certificate_hash: certificate0.hash(),
                            height: BlockHeight::from(0),
                            timestamp: Timestamp::from(0),
                            transaction_index: 0,
                            messages: vec![Message::System(SystemMessage::OpenChain(
                                OpenChainConfig {
                                    ownership: ChainOwnership::single(key_pair.public().into()),
                                    epoch: Epoch::from(0),
                                    committees: committees.clone(),
                                    admin_id,
                                    balance: Amount::ZERO,
                                    application_permissions: Default::default(),
                                },
                            ))
                            .to_posted(0, MessageKind::Protected)],
                        },
                        action: MessageAction::Accept,
                    })
                    .with_incoming_bundle(IncomingBundle {
                        origin: admin_channel_origin.clone(),
                        bundle: MessageBundle {
                            certificate_hash: certificate1.hash(),
                            height: BlockHeight::from(1),
                            timestamp: Timestamp::from(0),
                            transaction_index: 0,
                            messages: vec![Message::System(SystemMessage::CreateCommittee {
                                epoch: Epoch::from(1),
                                committee: committee.clone(),
                            })
                            .to_posted(0, MessageKind::Protected)],
                        },
                        action: MessageAction::Accept,
                    })
                    .with_incoming_bundle(IncomingBundle {
                        origin: Origin::chain(admin_id),
                        bundle: MessageBundle {
                            certificate_hash: certificate1.hash(),
                            height: BlockHeight::from(1),
                            timestamp: Timestamp::from(0),
                            transaction_index: 1,
                            messages: vec![system_credit_message(Amount::from_tokens(2))
                                .to_posted(1, MessageKind::Tracked)],
                        },
                        action: MessageAction::Accept,
                    }),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate3, &())
        .await?;
    {
        let user_chain = worker.chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
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
                .await?
                .len(),
            1
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 2);
        user_chain.validate_incoming_bundles().await?;
        {
            let inbox = user_chain
                .inboxes
                .try_load_entry(&Origin::chain(admin_id))
                .await?
                .expect("Missing inbox for admin chain in user chain");
            assert_eq!(inbox.next_block_height_to_receive()?, BlockHeight(2));
            assert_eq!(inbox.added_bundles.count(), 0);
            assert_eq!(inbox.removed_bundles.count(), 0);
        }
        {
            let inbox = user_chain
                .inboxes
                .try_load_entry(&admin_channel_origin)
                .await?
                .expect("Missing inbox for admin channel in user chain");
            assert_eq!(inbox.next_block_height_to_receive()?, BlockHeight(2));
            assert_eq!(inbox.added_bundles.count(), 0);
            assert_eq!(inbox.removed_bundles.count(), 0);
        }
        Ok(())
    }
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfers_and_committee_creation<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let owner0 = KeyPair::generate().public().into();
    let owner1 = KeyPair::generate().public().into();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (ChainDescription::Root(0), owner0, Amount::ZERO),
            (ChainDescription::Root(1), owner1, Amount::from_tokens(3)),
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
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![vec![direct_credit_message(admin_id, Amount::ONE)]],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: committees.clone(),
                    ownership: ChainOwnership::single(owner1),
                    balance: Amount::from_tokens(2),
                    ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(1), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_first_block(user_id)
                    .with_simple_transfer(admin_id, Amount::ONE)
                    .with_authenticated_signer(Some(owner1)),
            ),
        )),
    );
    // Have the admin chain create a new epoch without retiring the old one.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![vec![channel_admin_message(
                    SystemMessage::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    },
                )]],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: committees2.clone(),
                    ownership: ChainOwnership::single(owner0),
                    ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_first_block(admin_id).with_operation(SystemOperation::Admin(
                    AdminOperation::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    },
                )),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    // Try to execute the transfer.
    worker
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    // The transfer was started..
    let user_chain = worker.chain_state_view(user_id).await?;
    assert!(user_chain.is_active());
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
    let admin_chain = worker.chain_state_view(admin_id).await?;
    assert!(admin_chain.is_active());
    assert_eq!(admin_chain.inboxes.indices().await?.len(), 1);
    matches!(
        &admin_chain
            .inboxes
            .try_load_entry(&Origin::chain(user_id))
            .await?
            .expect("Missing inbox for user chain in admin chain")
            .added_bundles
            .read_front(10)
            .await?[..],
        [bundle] if matches!(bundle.messages[..], [PostedMessage {
            message: Message::System(SystemMessage::Credit { .. }),
            ..
        }])
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfers_and_committee_removal<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let owner0 = KeyPair::generate().public().into();
    let owner1 = KeyPair::generate().public().into();
    let (committee, worker) = init_worker_with_chains(
        storage_builder.build().await?,
        vec![
            (ChainDescription::Root(0), owner0, Amount::ZERO),
            (ChainDescription::Root(1), owner1, Amount::from_tokens(3)),
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
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![vec![direct_credit_message(admin_id, Amount::ONE)]],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: committees.clone(),
                    ownership: ChainOwnership::single(owner1),
                    balance: Amount::from_tokens(2),
                    ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(1), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_first_block(user_id)
                    .with_simple_transfer(admin_id, Amount::ONE)
                    .with_authenticated_signer(Some(owner1)),
            ),
        )),
    );
    // Have the admin chain create a new epoch and retire the old one immediately.
    let committees3 = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![
                    vec![channel_admin_message(SystemMessage::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    })],
                    vec![channel_admin_message(SystemMessage::RemoveCommittee {
                        epoch: Epoch::from(0),
                    })],
                ],
                events: vec![Vec::new(); 2],
                state_hash: SystemExecutionState {
                    committees: committees3.clone(),
                    ownership: ChainOwnership::single(owner0),
                    ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new(); 2],
            }
            .with(
                make_first_block(admin_id)
                    .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    }))
                    .with_operation(SystemOperation::Admin(AdminOperation::RemoveCommittee {
                        epoch: Epoch::ZERO,
                    })),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    // Try to execute the transfer from the user chain to the admin chain.
    worker
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    {
        // The transfer was started..
        let user_chain = worker.chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
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
        let admin_chain = worker.chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        assert!(admin_chain.inboxes.indices().await?.is_empty());
    }

    // Force the admin chain to receive the money nonetheless by anticipation.
    let certificate2 = make_certificate(
        &committee,
        &worker,
        Hashed::new(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![Vec::new()],
                events: vec![Vec::new()],
                state_hash: SystemExecutionState {
                    committees: committees3.clone(),
                    ownership: ChainOwnership::single(owner0),
                    balance: Amount::ONE,
                    ..SystemExecutionState::new(Epoch::from(1), ChainDescription::Root(0), admin_id)
                }
                .into_hash()
                .await,
                oracle_responses: vec![Vec::new()],
            }
            .with(
                make_child_block(&certificate1.into_value())
                    .with_epoch(1)
                    .with_incoming_bundle(IncomingBundle {
                        origin: Origin::chain(user_id),
                        bundle: MessageBundle {
                            certificate_hash: certificate0.hash(),
                            height: BlockHeight::ZERO,
                            timestamp: Timestamp::from(0),
                            transaction_index: 0,
                            messages: vec![system_credit_message(Amount::ONE)
                                .to_posted(0, MessageKind::Tracked)],
                        },
                        action: MessageAction::Accept,
                    }),
            ),
        )),
    );
    worker
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    {
        // The admin chain has an anticipated message.
        let admin_chain = worker.chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        assert_matches!(
            admin_chain.validate_incoming_bundles().await,
            Err(ChainError::MissingCrossChainUpdate { .. })
        );
    }

    // Try again to execute the transfer from the user chain to the admin chain.
    // This time, the epoch verification should be overruled.
    worker
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    {
        // The admin chain has no more anticipated messages.
        let admin_chain = worker.chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        admin_chain.validate_incoming_bundles().await?;
    }
    Ok(())
}

#[test(tokio::test)]
async fn test_cross_chain_helper() -> anyhow::Result<()> {
    // Make a committee and worker (only used for signing certificates)
    let store_config = MemoryStore::new_test_config().await?;
    let namespace = generate_test_namespace();
    let root_key = &[];
    let store = DbStorage::<MemoryStore, _>::new_for_testing(
        store_config,
        &namespace,
        root_key,
        None,
        TestClock::new(),
    )
    .await?;
    let (committee, worker) = init_worker(store, true, false);
    let committees = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);

    let key_pair0 = KeyPair::generate();
    let id0 = ChainId::root(0);
    let id1 = ChainId::root(1);

    let certificate0 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Some(key_pair0.public().into()),
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
        Some(key_pair0.public().into()),
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
        Some(key_pair0.public().into()),
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
        Some(key_pair0.public().into()),
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
    let bundles0 = certificate0
        .message_bundles_for(&Medium::Direct, id1)
        .collect::<Vec<_>>();
    let bundles1 = certificate1
        .message_bundles_for(&Medium::Direct, id1)
        .collect::<Vec<_>>();
    let bundles2 = certificate2
        .message_bundles_for(&Medium::Direct, id1)
        .collect::<Vec<_>>();
    let bundles3 = certificate3
        .message_bundles_for(&Medium::Direct, id1)
        .collect::<Vec<_>>();
    let bundles01 = Vec::from_iter(bundles0.iter().cloned().chain(bundles1.iter().cloned()));
    let bundles012 = Vec::from_iter(bundles01.iter().cloned().chain(bundles2.iter().cloned()));
    let bundles0123 = Vec::from_iter(bundles012.iter().cloned().chain(bundles3.iter().cloned()));

    fn without_epochs<'a>(
        bundles: impl IntoIterator<Item = &'a (Epoch, MessageBundle)>,
    ) -> Vec<MessageBundle> {
        bundles
            .into_iter()
            .map(|(_, bundle)| bundle.clone())
            .collect()
    }

    let helper = CrossChainUpdateHelper {
        allow_messages_from_deprecated_epochs: true,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is not tested when `allow_messages_from_deprecated_epochs` is true.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            bundles01.clone()
        )?,
        without_epochs(&bundles01)
    );
    // Received heights is removing prefixes.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::from(1),
            None,
            bundles01.clone()
        )?,
        without_epochs(&bundles1)
    );
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::from(2),
            None,
            bundles01.clone()
        )?,
        vec![]
    );
    // Order of certificates is checked.
    assert_matches!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            Vec::from_iter(bundles1.iter().cloned().chain(bundles0.iter().cloned()))
        ),
        Err(WorkerError::InvalidCrossChainRequest)
    );

    let helper = CrossChainUpdateHelper {
        allow_messages_from_deprecated_epochs: false,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is tested when `allow_messages_from_deprecated_epochs` is false.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            bundles01.clone()
        )?,
        vec![]
    );
    // A certificate with a recent epoch certifies all the previous blocks.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            None,
            bundles0123.clone()
        )?,
        without_epochs(&bundles012)
    );
    // Received heights is still removing prefixes.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::from(1),
            None,
            bundles012.clone()
        )?,
        without_epochs(bundles1.iter().chain(&bundles2))
    );
    // Anticipated messages re-certify blocks up to the given height.
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::from(1),
            Some(BlockHeight::from(1)),
            bundles01.clone()
        )?,
        without_epochs(&bundles1)
    );
    assert_eq!(
        helper.select_message_bundles(
            &Origin::chain(id0),
            id1,
            BlockHeight::ZERO,
            Some(BlockHeight::from(1)),
            bundles01.clone()
        )?,
        without_epochs(&bundles01)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_timeouts<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let owner0 = Owner::from(key_pairs[0].public());
    let owner1 = Owner::from(key_pairs[1].public());
    let balances = vec![(ChainDescription::Root(0), owner0, Amount::from_tokens(2))];
    let (committee, worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and use the leader-based protocol in all rounds.
    let block0 = make_first_block(chain_id)
        .with_operation(SystemOperation::ChangeOwnership {
            super_owners: Vec::new(),
            owners: vec![(owner0, 100), (owner1, 100)],
            multi_leader_rounds: 0,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        })
        .with_authenticated_signer(Some(owner0));
    let (executed_block0, _) = worker.stage_block_execution(block0).await?;
    let value0 = Hashed::new(ConfirmedBlock::new(executed_block0));
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The leader sequence is pseudorandom but deterministic. The first leader is owner 1.
    assert_eq!(response.info.manager.leader, Some(owner1));

    // So owner 0 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0.clone())
        .into_proposal_with_round(&key_pairs[0], Round::SingleLeader(0));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal = make_child_block(&value0.clone())
        .into_proposal_with_round(&key_pairs[0], Round::SingleLeader(1));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::SingleLeader(0)))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await?;
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Hashed::new(Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0)));

    // Once we provide the validator with a timeout certificate, the next round starts, where owner
    // 0 happens to be the leader.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = worker
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));

    // Now owner 0 can propose a block, but owner 1 can't.
    let block1 = make_child_block(&value0.clone());
    let (executed_block1, _) = worker.stage_block_execution(block1.clone()).await?;
    let proposal1_wrong_owner = block1
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(&key_pairs[1], Round::SingleLeader(1));
    let result = worker.handle_block_proposal(proposal1_wrong_owner).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal1 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[0], Round::SingleLeader(1));
    let (response, _) = worker.handle_block_proposal(proposal1).await?;
    let value1 = Hashed::new(ValidatedBlock::new(executed_block1.clone()));

    // If we send the validated block certificate to the worker, it votes to confirm.
    let vote = response.info.manager.pending.clone().unwrap();
    let certificate1 = vote.with_value(value1.clone()).unwrap().into_certificate();
    let (response, _) = worker
        .handle_validated_certificate(certificate1.clone())
        .await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    let value = Hashed::new(ConfirmedBlock::new(executed_block1.clone()));
    assert_eq!(vote.value, LiteValue::new(&value));

    // Instead of submitting the confirmed block certificate, let rounds 2 to 4 time out, too.
    let certificate_timeout = make_certificate_with_round(
        &committee,
        &worker,
        value_timeout.clone(),
        Round::SingleLeader(4),
    );
    let (response, _) = worker
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner1));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(5));

    // Create block2, also at height 1, but different from block 1.
    let amount = Amount::from_tokens(1);
    let block2 = make_child_block(&value0.clone()).with_simple_transfer(ChainId::root(1), amount);
    let (executed_block2, _) = worker.stage_block_execution(block2.clone()).await?;

    // Since round 3 is already over, a validated block from round 3 won't update the validator's
    // locked block; certificate1 (with block1) remains locked.
    let value2 = Hashed::new(ValidatedBlock::new(executed_block2.clone()));
    let certificate =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::SingleLeader(2));
    worker.handle_validated_certificate(certificate).await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values.clone()).await?;
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(LockedBlock::Regular(certificate1)))
    );

    // Proposing block2 now would fail.
    let proposal = block2
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(&key_pairs[1], Round::SingleLeader(5));
    let result = worker.handle_block_proposal(proposal.clone()).await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasLockedBlock(_, _))
    );

    // But with the validated block certificate for block2, it is allowed.
    let certificate2 =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::SingleLeader(4));
    let proposal = BlockProposal::new_retry(
        Round::SingleLeader(5),
        certificate2.clone(),
        &key_pairs[1],
        Vec::new(),
    );
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = worker.handle_block_proposal(proposal).await?;
    let (response, _) = worker.handle_chain_info_query(query_values.clone()).await?;
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(LockedBlock::Regular(certificate2)))
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
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));
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
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(8));

    // If the worker does not belong to a validator, it does update its locked block even if it's
    // from a past round.
    let certificate =
        make_certificate_with_round(&committee, &worker, value1, Round::SingleLeader(7));
    let worker = worker.with_key_pair(None).await; // Forget validator keys.
    worker
        .handle_validated_certificate(certificate.clone())
        .await?;
    let (response, _) = worker.handle_chain_info_query(query_values).await?;
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(LockedBlock::Regular(certificate)))
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_round_types<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let owner0 = Owner::from(key_pairs[0].public());
    let owner1 = Owner::from(key_pairs[1].public());
    let balances = vec![(ChainDescription::Root(0), owner0, Amount::from_tokens(2))];
    let (committee, worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and configure two multi-leader rounds.
    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
        super_owners: vec![owner0],
        owners: vec![(owner0, 100), (owner1, 100)],
        multi_leader_rounds: 2,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig {
            fast_round_duration: Some(TimeDelta::from_secs(5)),
            ..TimeoutConfig::default()
        },
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await?;
    let value0 = Hashed::new(ConfirmedBlock::new(executed_block0));
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

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
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await?;
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = worker.handle_chain_info_query(query).await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Hashed::new(Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0)));

    // Once we provide the validator with a timeout certificate, the next round starts.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = worker
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. And multi-leader rounds can be skipped without timeout.
    let block1 = make_child_block(&value0);
    let proposal1 = block1
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(1));
    let _ = worker.handle_block_proposal(proposal1).await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values).await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(1));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_multi_leader_rounds<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let chain_id = ChainId::root(0);
    let key_pair = KeyPair::generate();
    let owner = key_pair.public().into();
    let description = ChainDescription::Root(0);
    let (committee, worker) =
        init_worker_with_chain(storage, description, owner, Amount::from_tokens(2)).await;

    // Configure open multi-leader rounds.
    let change_ownership_block =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![],
            owners: vec![(owner, 100)],
            multi_leader_rounds: 2,
            open_multi_leader_rounds: true,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                ..TimeoutConfig::default()
            },
        });
    let (change_ownership_executed_block, _) =
        worker.stage_block_execution(change_ownership_block).await?;
    let change_ownership_value = Hashed::new(ConfirmedBlock::new(change_ownership_executed_block));
    let change_ownership_certificate =
        make_certificate(&committee, &worker, change_ownership_value.clone());
    worker
        .fully_handle_certificate_with_notifications(change_ownership_certificate, &())
        .await?;

    // The first round is the multi-leader round 0. Anyone is allowed to propose.
    // But non-owners are not allowed to transfer the chain's funds.
    let proposal = make_child_block(&change_ownership_value)
        .with_transfer(None, Recipient::Burn, Amount::from_tokens(1))
        .into_proposal_with_round(&KeyPair::generate(), Round::MultiLeader(0));
    let result = worker.handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(error)) if matches!(&*error,
        ChainError::ExecutionError(error, _) if matches!(&**error,
        ExecutionError::SystemError(SystemExecutionError::UnauthenticatedTransferOwner
    ))));

    // Without the transfer, a random key pair can propose a block.
    let proposal = make_child_block(&change_ownership_value)
        .into_proposal_with_round(&KeyPair::generate(), Round::MultiLeader(0));
    let (executed_block, _) = worker
        .stage_block_execution(proposal.content.block.clone())
        .await?;
    let value = Hashed::new(ConfirmedBlock::new(executed_block));
    let (response, _) = worker.handle_block_proposal(proposal).await?;
    let vote = response.info.manager.pending.unwrap();
    assert_eq!(vote.round, Round::MultiLeader(0));
    assert_eq!(vote.value.value_hash, value.hash());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_fast_proposal_is_locked<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_id = ChainId::root(0);
    let key_pairs = generate_key_pairs(2);
    let owner0 = Owner::from(key_pairs[0].public());
    let owner1 = Owner::from(key_pairs[1].public());
    let balances = vec![(ChainDescription::Root(0), owner0, Amount::from_tokens(2))];
    let (committee, worker) = init_worker_with_chains(storage, balances).await;

    // Add another owner and configure two multi-leader rounds.
    let block0 = make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
        super_owners: vec![owner0],
        owners: vec![(owner0, 100), (owner1, 100)],
        multi_leader_rounds: 3,
        open_multi_leader_rounds: false,
        timeout_config: TimeoutConfig {
            fast_round_duration: Some(TimeDelta::from_millis(5)),
            ..TimeoutConfig::default()
        },
    });
    let (executed_block0, _) = worker.stage_block_execution(block0).await?;
    let value0 = Hashed::new(ConfirmedBlock::new(executed_block0));
    let certificate0 = make_certificate(&committee, &worker, value0.clone());
    let response = worker
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // Owner 0 proposes another block. The validator votes to confirm.
    let block1 = make_child_block(&value0.clone());
    let proposal1 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[0], Round::Fast);
    let (executed_block1, _) = worker.stage_block_execution(block1.clone()).await?;
    let value1 = Hashed::new(ConfirmedBlock::new(executed_block1));
    let (response, _) = worker.handle_block_proposal(proposal1).await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::Fast);
    assert_eq!(vote.value.value_hash, value1.hash());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Once we provide the validator with a timeout certificate, the next round starts.
    let value_timeout = Hashed::new(Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0)));
    let certificate_timeout =
        make_certificate_with_round(&committee, &worker, value_timeout.clone(), Round::Fast);
    let (response, _) = worker
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. But block1 is locked. Re-proposing it is allowed.
    let proposal1b = block1
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(0));
    let (response, _) = worker.handle_block_proposal(proposal1b).await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::MultiLeader(0));
    assert_eq!(vote.value.value_hash, value1.hash());

    // Proposing a different block is not.
    let block2 = make_child_block(&value0)
        .with_simple_transfer(ChainId::root(1), Amount::ONE)
        .with_authenticated_signer(Some(owner1));
    let proposal2 = block2
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(1));
    let result = worker.handle_block_proposal(proposal2).await;
    assert_matches!(result, Err(WorkerError::ChainError(err))
        if matches!(*err, ChainError::HasLockedBlock(_, Round::Fast))
    );
    let proposal3 = block1
        .clone()
        .into_proposal_with_round(&key_pairs[1], Round::MultiLeader(2));
    worker.handle_block_proposal(proposal3).await?;

    // A validated block certificate from a later round can override the locked fast block.
    let (executed_block2, _) = worker.stage_block_execution(block2.clone()).await?;
    let value2 = Hashed::new(ValidatedBlock::new(executed_block2.clone()));
    let certificate2 =
        make_certificate_with_round(&committee, &worker, value2.clone(), Round::MultiLeader(0));
    let proposal = BlockProposal::new_retry(
        Round::MultiLeader(3),
        certificate2.clone(),
        &key_pairs[1],
        Vec::new(),
    );
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = worker.handle_block_proposal(proposal).await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = worker.handle_chain_info_query(query_values).await?;
    assert_eq!(
        response.info.manager.requested_locked,
        Some(Box::new(LockedBlock::Regular(certificate2)))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::MultiLeader(3));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_fallback<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_id = ChainId::root(1);
    let key_pair = KeyPair::generate();
    let balance = Amount::from_tokens(5);
    let balances = vec![(ChainDescription::Root(1), key_pair.public().into(), balance)];
    let (committee, worker) = init_worker_with_chains(storage, balances).await;

    // At time 0 we don't vote for fallback mode.
    let query = ChainInfoQuery::new(chain_id).with_fallback();
    let (response, _) = worker.handle_chain_info_query(query.clone()).await?;
    let manager = response.info.manager;
    assert!(manager.fallback_vote.is_none());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    assert!(manager.leader.is_none());
    let fallback_duration = manager.ownership.timeout_config.fallback_duration;

    // Even if a long time passes: Without an incoming message there's no fallback mode.
    clock.add(fallback_duration);
    let (response, _) = worker.handle_chain_info_query(query.clone()).await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // Make a tracked message to ourselves. It's in the inbox now.
    let block = make_first_block(chain_id)
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_authenticated_signer(Some(key_pair.public().into()));
    let (executed_block, _) = worker.stage_block_execution(block).await?;
    let value = Hashed::new(ConfirmedBlock::new(executed_block));
    let certificate = make_certificate(&committee, &worker, value);
    worker
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?;

    // The message only just arrived: No fallback mode.
    let (response, _) = worker.handle_chain_info_query(query.clone()).await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // If for a long time the message isn't handled, we vote for fallback mode.
    clock.add(fallback_duration);
    let (response, _) = worker.handle_chain_info_query(query.clone()).await?;
    let vote = response.info.manager.fallback_vote.unwrap();
    let value = Hashed::new(Timeout::new(chain_id, BlockHeight(1), Epoch::ZERO));
    let round = Round::SingleLeader(u32::MAX);
    assert_eq!(vote.value.value_hash, value.hash());
    assert_eq!(vote.round, round);
    let certificate = make_certificate_with_round(&committee, &worker, value, round);
    worker.handle_timeout_certificate(certificate).await?;

    // Now we are in fallback mode, and the validator is the leader.
    let (response, _) = worker.handle_chain_info_query(query.clone()).await?;
    let manager = response.info.manager;
    let validator_key = worker.public_key();
    assert_eq!(manager.current_round, Round::Validator(0));
    assert_eq!(manager.leader, Some(Owner::from(validator_key)));
    Ok(())
}

/// Tests if a service is able to handle more than one query without restarting.
///
/// If the service is restarted, a new [`MockApplicationInstance`] is created with an empty list of
/// expected calls, and the test fails because the first [`MockApplicationInstance`] still expects
/// some calls.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_long_lived_service<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    const NUM_QUERIES: usize = 5;

    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_description = ChainDescription::Root(1);
    let chain_id = ChainId::from(chain_description);
    let key_pair = KeyPair::generate();
    let balance = Amount::ZERO;

    let (committee, worker) = init_worker(
        storage.clone(),
        /* is_client */ false,
        /* has_long_lived_services */ true,
    );
    worker
        .storage
        .create_chain(
            committee,
            ChainId::root(0),
            chain_description,
            key_pair.public().into(),
            balance,
            Timestamp::from(0),
        )
        .await
        .unwrap();

    let (application_id, application);
    {
        let mut chain = storage.load_chain(chain_id).await?;
        (application_id, application) = chain.execution_state.register_mock_application().await?;
        chain.save().await?;
    }

    let query_times = (0..NUM_QUERIES as u64).map(Timestamp::from);
    let query_contexts = query_times.clone().map(|local_time| QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time,
    });

    for query_context in query_contexts {
        application.expect_call(ExpectedCall::handle_query(
            move |_runtime, context, query| {
                assert_eq!(context, query_context);
                assert!(query.is_empty());
                Ok(vec![])
            },
        ));
    }

    let query = Query::User {
        application_id,
        bytes: vec![],
    };
    for query_time in query_times {
        clock.set(query_time);

        assert_eq!(
            worker.query_application(chain_id, query.clone()).await?,
            Response::User(vec![])
        );
    }

    drop(worker);
    linera_base::time::timer::sleep(Duration::from_millis(10)).await;
    application.assert_no_more_expected_calls();
    application.assert_no_active_instances();

    Ok(())
}

/// Tests if a service is restarted when a block is added to the chain.
///
/// A new block must force the service to restart, because the context will have changed and the
/// application state may have changed.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_new_block_causes_service_restart<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    const NUM_QUERIES: usize = 2;
    const BLOCK_TIMESTAMP: u64 = 10;

    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let chain_description = ChainDescription::Root(1);
    let chain_id = ChainId::from(chain_description);
    let key_pair = KeyPair::generate();
    let balance = Amount::ZERO;

    let (committee, worker) = init_worker(
        storage.clone(),
        /* is_client */ false,
        /* has_long_lived_services */ true,
    );
    worker
        .storage
        .create_chain(
            committee.clone(),
            ChainId::root(0),
            chain_description,
            key_pair.public().into(),
            balance,
            Timestamp::from(0),
        )
        .await
        .unwrap();

    let (application_id, application);
    {
        let mut chain = storage.load_chain(chain_id).await?;
        (application_id, application) = chain.execution_state.register_mock_application().await?;
        chain.save().await?;
    }

    let queries_before_proposal = (0..NUM_QUERIES as u64).map(Timestamp::from);
    let queries_before_confirmation =
        (0..NUM_QUERIES as u64).map(|delta| Timestamp::from(NUM_QUERIES as u64 + delta));

    let queries_before_new_block = queries_before_proposal
        .clone()
        .chain(queries_before_confirmation.clone());
    let queries_after_new_block =
        (1..=NUM_QUERIES as u64).map(|delta| Timestamp::from(BLOCK_TIMESTAMP + delta));

    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    let query_contexts_before_new_block =
        queries_before_new_block
            .clone()
            .map(|local_time| QueryContext {
                chain_id,
                next_block_height: BlockHeight(0),
                local_time,
            });
    let query_contexts_after_new_block =
        queries_after_new_block
            .clone()
            .map(|local_time| QueryContext {
                chain_id,
                next_block_height: BlockHeight(1),
                local_time,
            });

    for query_context in query_contexts_before_new_block {
        application.expect_call(ExpectedCall::handle_query(
            move |_runtime, context, query| {
                assert_eq!(context, query_context);
                assert!(query.is_empty());
                Ok(vec![])
            },
        ));
    }

    for local_time in queries_before_proposal {
        clock.set(local_time);

        assert_eq!(
            worker.query_application(chain_id, query.clone()).await?,
            Response::User(vec![])
        );
    }

    clock.set(Timestamp::from(BLOCK_TIMESTAMP));
    let block = make_first_block(chain_id).with_timestamp(Timestamp::from(BLOCK_TIMESTAMP));

    let block_proposal = block.clone().into_first_proposal(&key_pair);
    let _ = worker.handle_block_proposal(block_proposal).await?;

    for local_time in queries_before_confirmation {
        clock.set(local_time);

        assert_eq!(
            worker.query_application(chain_id, query.clone()).await?,
            Response::User(vec![])
        );
    }

    let epoch = Epoch::ZERO;
    let admin_id = ChainId::root(0);
    let mut state = SystemExecutionState {
        committees: BTreeMap::from_iter([(epoch, committee.clone())]),
        ownership: ChainOwnership::single(key_pair.public().into()),
        balance,
        timestamp: Timestamp::from(BLOCK_TIMESTAMP),
        ..SystemExecutionState::new(epoch, chain_description, admin_id)
    }
    .into_view()
    .await;
    let _ = state.register_mock_application().await?;

    let value = Hashed::new(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![],
            events: vec![],
            state_hash: state.crypto_hash_mut().await?,
            oracle_responses: vec![],
        }
        .with(block),
    ));
    let certificate = make_certificate(&committee, &worker, value);
    worker
        .handle_confirmed_certificate(certificate, None)
        .await?;

    for query_context in query_contexts_after_new_block.clone() {
        application.expect_call(ExpectedCall::handle_query(
            move |_runtime, context, query| {
                assert_eq!(context, query_context);
                assert!(query.is_empty());
                Ok(vec![])
            },
        ));
    }

    for local_time in queries_after_new_block {
        clock.set(local_time);

        assert_eq!(
            worker.query_application(chain_id, query.clone()).await?,
            Response::User(vec![])
        );
    }

    drop(worker);
    linera_base::time::timer::sleep(Duration::from_millis(10)).await;
    application.assert_no_more_expected_calls();
    application.assert_no_active_instances();

    Ok(())
}
