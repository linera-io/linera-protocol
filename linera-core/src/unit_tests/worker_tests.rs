// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use crate::{
    data_types::*,
    worker::{
        ApplicationId::System, CrossChainUpdateHelper, Notification, Reason, Reason::NewMessage,
        ValidatorWorker, WorkerError, WorkerState,
    },
};
use linera_base::{
    committee::Committee,
    crypto::{BcsSignable, CryptoHash, *},
    data_types::*,
};
use linera_chain::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, Event, LiteVote, Medium, Message, Origin,
        OutgoingEffect, SignatureAggregator, Value,
    },
    ChainError,
};
use linera_execution::{
    system::{
        Account, Amount, Balance, Recipient, SystemChannel, SystemEffect, SystemOperation, UserData,
    },
    ApplicationId, ApplicationRegistry, ChainOwnership, ChannelId, ChannelName, Destination,
    Effect, ExecutionStateView, Operation, Query, Response, SystemExecutionState, SystemQuery,
    SystemResponse, UserApplicationId,
};
use linera_storage::{MemoryStoreClient, RocksdbStoreClient, Store};
use linera_views::views::{HashableRootView, ViewError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use test_log::test;

#[cfg(feature = "aws")]
use {linera_storage::DynamoDbStoreClient, linera_views::test_utils::LocalStackTestContext};

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

/// The test worker accepts blocks with a timestamp this far in the future.
const TEST_GRACE_PERIOD_MICROS: u64 = 500_000;

/// Instantiate the protocol with a single validator. Returns the corresponding committee
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

/// Same as `init_worker` but also instantiate some initial chains.
async fn init_worker_with_chains<S, I>(client: S, balances: I) -> (Committee, WorkerState<S>)
where
    I: IntoIterator<Item = (ChainDescription, PublicKey, Balance)>,
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
                pubk.into(),
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
    balance: Balance,
) -> (Committee, WorkerState<S>)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    init_worker_with_chains(client, [(description, owner, balance)]).await
}

fn make_block(
    epoch: Epoch,
    chain_id: ChainId,
    operations: Vec<impl IntoApplicationIdAndOperation>,
    incoming_messages: Vec<Message>,
    previous_confirmed_block: Option<&Certificate>,
    authenticated_signer: Option<Owner>,
    timestamp: Timestamp,
) -> Block {
    let previous_block_hash = previous_confirmed_block.as_ref().map(|cert| cert.hash);
    let height = match &previous_confirmed_block {
        None => BlockHeight::default(),
        Some(cert) => cert
            .value
            .confirmed_block()
            .unwrap()
            .height
            .try_add_one()
            .unwrap(),
    };
    Block {
        epoch,
        chain_id,
        incoming_messages,
        operations: operations
            .into_iter()
            .map(IntoApplicationIdAndOperation::into_application_id_and_operation)
            .collect(),
        previous_block_hash,
        height,
        authenticated_signer,
        timestamp,
    }
}

fn make_transfer_block_proposal(
    chain_id: ChainId,
    key_pair: &KeyPair,
    recipient: Recipient,
    amount: Amount,
    incoming_messages: Vec<Message>,
    previous_confirmed_block: Option<&Certificate>,
) -> BlockProposal {
    let block = make_block(
        Epoch::from(0),
        chain_id,
        vec![SystemOperation::Transfer {
            owner: None,
            recipient,
            amount,
            user_data: UserData::default(),
        }],
        incoming_messages,
        previous_confirmed_block,
        None,
        Timestamp::from(0),
    );
    BlockProposal::new(
        BlockAndRound {
            block,
            round: RoundNumber::default(),
        },
        key_pair,
    )
}

fn make_certificate<S>(
    committee: &Committee,
    worker: &WorkerState<S>,
    value: Value,
) -> Certificate {
    let vote = LiteVote::new(value.lite(), worker.key_pair.as_ref().unwrap());
    let mut builder = SignatureAggregator::new(value, committee);
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
    incoming_messages: Vec<Message>,
    committee: &Committee,
    balance: Balance,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    make_transfer_certificate_for_epoch(
        chain_description,
        key_pair,
        recipient,
        amount,
        incoming_messages,
        Epoch::from(0),
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
    incoming_messages: Vec<Message>,
    epoch: Epoch,
    committee: &Committee,
    balance: Balance,
    worker: &WorkerState<S>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    let chain_id = chain_description.into();
    let system_state = SystemExecutionState {
        epoch: Some(epoch),
        description: Some(chain_description),
        admin_id: Some(ChainId::root(0)),
        subscriptions: BTreeSet::new(),
        committees: [(epoch, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(key_pair.public().into()),
        balance,
        balances: BTreeMap::new(),
        timestamp: Timestamp::from(0),
        registry: ApplicationRegistry::default(),
    };
    let block = make_block(
        epoch,
        chain_id,
        vec![SystemOperation::Transfer {
            owner: None,
            recipient,
            amount,
            user_data: UserData::default(),
        }],
        incoming_messages,
        previous_confirmed_block,
        None,
        Timestamp::from(0),
    );
    let effects = match recipient {
        Recipient::Account(account) => vec![direct_outgoing_effect(
            account.chain_id,
            SystemEffect::Credit { account, amount },
        )],
        Recipient::Burn => Vec::new(),
    };
    let state_hash = make_state_hash(system_state).await;
    let value = Value::ConfirmedBlock {
        block,
        effects,
        state_hash,
    };
    make_certificate(committee, worker, value)
}

fn direct_outgoing_effect(recipient: ChainId, effect: SystemEffect) -> OutgoingEffect {
    OutgoingEffect {
        application_id: ApplicationId::System,
        destination: Destination::Recipient(recipient),
        authenticated_signer: None,
        effect: Effect::System(effect),
    }
}

fn channel_outgoing_effect(name: ChannelName, effect: SystemEffect) -> OutgoingEffect {
    OutgoingEffect {
        application_id: ApplicationId::System,
        destination: Destination::Subscribers(name),
        authenticated_signer: None,
        effect: Effect::System(effect),
    }
}

trait IntoApplicationIdAndOperation {
    fn into_application_id_and_operation(self) -> (ApplicationId, Operation);
}

impl IntoApplicationIdAndOperation for SystemOperation {
    fn into_application_id_and_operation(self) -> (ApplicationId, Operation) {
        (ApplicationId::System, Operation::System(self))
    }
}

impl IntoApplicationIdAndOperation for (UserApplicationId, Vec<u8>) {
    fn into_application_id_and_operation(self) -> (ApplicationId, Operation) {
        (ApplicationId::User(self.0), Operation::User(self.1))
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_bad_signature() {
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_bad_signature(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_bad_signature() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_bad_signature(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_bad_signature() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_bad_signature(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_bad_signature<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    let block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
        None,
    );
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_zero_amount(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_zero_amount() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_zero_amount(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_zero_amount() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_zero_amount(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_zero_amount<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    // test block non-positive amount
    let zero_amount_block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::zero(),
        Vec::new(),
        None,
    );
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_ticks(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_ticks() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_ticks(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_ticks() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_ticks(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_ticks<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let balance = Balance::from(5);
    let balances = vec![(ChainDescription::Root(1), key_pair.public(), balance)];
    let epoch = Epoch::from(0);
    let (committee, mut worker) = init_worker_with_chains(client, balances).await;

    let make_tick_block = |micros: u64, parent: Option<&Certificate>| {
        make_block(
            epoch,
            ChainId::root(1),
            Vec::<SystemOperation>::new(),
            vec![],
            parent,
            None,
            Timestamp::from(micros),
        )
    };

    let make_proposal = |block: Block| {
        BlockProposal::new(
            BlockAndRound {
                block,
                round: RoundNumber::default(),
            },
            &key_pair,
        )
    };

    {
        let time = Timestamp::now().micros() + TEST_GRACE_PERIOD_MICROS + 1_000_000;
        let block_proposal = make_proposal(make_tick_block(time, None));
        // Timestamp too far in the future
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }

    let block_0_time = Timestamp::now().micros() + TEST_GRACE_PERIOD_MICROS;
    let certificate = {
        let block = make_tick_block(block_0_time, None);
        let block_proposal = make_proposal(block.clone());
        assert!(worker.handle_block_proposal(block_proposal).await.is_ok());

        let system_state = SystemExecutionState {
            epoch: Some(epoch),
            description: Some(ChainDescription::Root(1)),
            admin_id: Some(ChainId::root(0)),
            subscriptions: BTreeSet::new(),
            committees: [(epoch, committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(key_pair.public().into()),
            balance,
            balances: BTreeMap::new(),
            timestamp: Timestamp::from(block_0_time),
            registry: ApplicationRegistry::default(),
        };
        let state_hash = make_state_hash(system_state).await;
        let value = Value::ConfirmedBlock {
            block,
            effects: vec![],
            state_hash,
        };
        make_certificate(&committee, &worker, value)
    };
    worker
        .fully_handle_certificate(certificate.clone(), vec![])
        .await
        .expect("handle certificate with valid tick");
    assert!(Timestamp::now().micros() >= block_0_time);

    {
        let block_proposal = make_proposal(make_tick_block(block_0_time - 1, Some(&certificate)));
        // Timestamp older than previous one
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_unknown_sender() {
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_unknown_sender(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_unknown_sender() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_unknown_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_unknown_sender() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_unknown_sender(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_unknown_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    let block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
        None,
    );
    let unknown_key = KeyPair::generate();

    let unknown_sender_block_proposal = BlockProposal::new(block_proposal.content, &unknown_key);
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_with_chaining(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_with_chaining() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_with_chaining(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_chaining() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_with_chaining(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_with_chaining<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Balance::from(5),
        )],
    )
    .await;
    let block_proposal0 = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(1),
        Vec::new(),
        None,
    );
    let certificate0 = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        recipient,
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(4),
        &worker,
        None,
    )
    .await;
    let block_proposal1 = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(2),
        Vec::new(),
        Some(&certificate0),
    );

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
        .handle_certificate(certificate0, vec![])
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_with_incoming_messages() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_with_incoming_messages(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_with_incoming_messages() -> Result<(), anyhow::Error>
{
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_with_incoming_messages(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_with_incoming_messages<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(6),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public(),
                Balance::from(0),
            ),
        ],
    )
    .await;

    let epoch = Epoch::from(0);
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch,
                chain_id: ChainId::root(1),
                incoming_messages: Vec::new(),
                operations: vec![
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::Transfer {
                            owner: None,
                            recipient,
                            amount: Amount::from(1),
                            user_data: UserData::default(),
                        }),
                    ),
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::Transfer {
                            owner: None,
                            recipient,
                            amount: Amount::from(2),
                            user_data: UserData::default(),
                        }),
                    ),
                ],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![
                direct_outgoing_effect(
                    ChainId::root(2),
                    SystemEffect::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from(1),
                    },
                ),
                direct_outgoing_effect(
                    ChainId::root(2),
                    SystemEffect::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from(2),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(epoch),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(ChainId::root(0)),
                subscriptions: BTreeSet::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public().into()),
                balance: Balance::from(3),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
    );

    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch,
                chain_id: ChainId::root(1),
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::Transfer {
                        owner: None,
                        recipient,
                        amount: Amount::from(3),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![direct_outgoing_effect(
                ChainId::root(2),
                SystemEffect::Credit {
                    account: Account::chain(ChainId::root(2)),
                    amount: Amount::from(3),
                },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(epoch),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(ChainId::root(0)),
                subscriptions: BTreeSet::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                ownership: ChainOwnership::single(sender_key_pair.public().into()),
                balance: Balance::from(0),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
    );
    // Missing earlier blocks
    assert!(worker
        .handle_certificate(certificate1.clone(), vec![])
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
                chain_id: ChainId::root(2),
                reason: NewMessage {
                    application_id: System,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight(0)
                }
            },
            Notification {
                chain_id: ChainId::root(2),
                reason: NewMessage {
                    application_id: System,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight(1)
                }
            }
        ]
    );
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(6),
            Vec::new(),
            None,
        );
        // Insufficient funding
        assert!(worker.handle_block_proposal(block_proposal).await.is_err());
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(5),
            vec![
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(1),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 1,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(2),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate1.hash,
                        height: BlockHeight::from(1),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(2), // wrong
                        }),
                    },
                },
            ],
            None,
        );
        // Inconsistent received messages.
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnexpectedMessage { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(6),
            vec![
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 1,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(2),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(1),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(1),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(3),
                        }),
                    },
                },
            ],
            None,
        );
        // Inconsistent order in received messages (indices).
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::IncorrectMessageOrder { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(6),
            vec![
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate1.hash,
                        height: BlockHeight::from(1),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(3),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(1),
                        }),
                    },
                },
                Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(ChainId::root(1)),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 1,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(ChainId::root(2)),
                            amount: Amount::from(2),
                        }),
                    },
                },
            ],
            None,
        );
        // Inconsistent order in received messages (heights).
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::IncorrectMessageOrder { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(1),
            vec![Message {
                application_id: ApplicationId::System,
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate0.hash,
                    height: BlockHeight::from(0),
                    index: 0,
                    authenticated_signer: None,
                    timestamp: Timestamp::from(0),
                    effect: Effect::System(SystemEffect::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from(1),
                    }),
                },
            }],
            None,
        );
        // Taking the first message only is ok.
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
        let certificate = make_certificate(
            &committee,
            &worker,
            Value::ConfirmedBlock {
                block: block_proposal.content.block,
                effects: vec![direct_outgoing_effect(
                    ChainId::root(3),
                    SystemEffect::Credit {
                        account: Account::chain(ChainId::root(3)),
                        amount: Amount::from(1),
                    },
                )],
                state_hash: make_state_hash(SystemExecutionState {
                    epoch: Some(epoch),
                    description: Some(ChainDescription::Root(2)),
                    admin_id: Some(ChainId::root(0)),
                    subscriptions: BTreeSet::new(),
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    ownership: ChainOwnership::single(recipient_key_pair.public().into()),
                    balance: Balance::from(0),
                    balances: BTreeMap::new(),
                    timestamp: Timestamp::from(0),
                    registry: ApplicationRegistry::default(),
                })
                .await,
            },
        );
        worker
            .handle_certificate(certificate.clone(), vec![])
            .await
            .unwrap();
        // Then skip the second message and receive the last one.
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Recipient::Account(Account::chain(ChainId::root(3))),
            Amount::from(3),
            vec![Message {
                application_id: ApplicationId::System,
                origin: Origin::chain(ChainId::root(1)),
                event: Event {
                    certificate_hash: certificate1.hash,
                    height: BlockHeight::from(1),
                    index: 0,
                    authenticated_signer: None,
                    timestamp: Timestamp::from(0),
                    effect: Effect::System(SystemEffect::Credit {
                        account: Account::chain(ChainId::root(2)),
                        amount: Amount::from(3),
                    }),
                },
            }],
            Some(&certificate),
        );
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
    }
}

#[test(tokio::test)]
async fn test_memory_handle_block_proposal_exceed_balance() {
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_exceed_balance(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_exceed_balance() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_exceed_balance(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_exceed_balance() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_exceed_balance(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_exceed_balance<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    let block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(1000),
        Vec::new(),
        None,
    );
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(1),
            sender_key_pair.public(),
            Balance::from(5),
        )],
    )
    .await;
    let block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
        None,
    );

    let chain_info_response = worker.handle_block_proposal(block_proposal).await.unwrap();
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
    let client = MemoryStoreClient::default();
    run_test_handle_block_proposal_replay(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_block_proposal_replay() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_block_proposal_replay(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_block_proposal_replay() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_block_proposal_replay(client).await;
    Ok(())
}

async fn run_test_handle_block_proposal_replay<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let recipient = Recipient::Account(Account::chain(ChainId::root(2)));
    let (_, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                ChainDescription::Root(1),
                sender_key_pair.public(),
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    let block_proposal = make_transfer_block_proposal(
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
        None,
    );

    let response = worker
        .handle_block_proposal(block_proposal.clone())
        .await
        .unwrap();
    response
        .check(ValidatorName(worker.key_pair.as_ref().unwrap().public()))
        .as_ref()
        .unwrap();
    let replay_response = worker.handle_block_proposal(block_proposal).await.unwrap();
    // Workaround lack of equality.
    assert_eq!(
        CryptoHash::new(&response.info),
        CryptoHash::new(&replay_response.info)
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_unknown_sender() {
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_unknown_sender(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_unknown_sender() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_unknown_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_unknown_sender() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_unknown_sender(client).await;
    Ok(())
}

async fn run_test_handle_certificate_unknown_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(2),
            PublicKey::debug(2),
            Balance::from(0),
        )],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
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
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_bad_block_height(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_bad_block_height() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_bad_block_height(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_bad_block_height() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_bad_block_height(client).await;
    Ok(())
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
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
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
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_with_anticipated_incoming_message() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_with_anticipated_incoming_message(
) -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_with_anticipated_incoming_message(client).await;
    Ok(())
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
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::from(0),
            ),
        ],
    )
    .await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(1000),
        vec![Message {
            application_id: ApplicationId::System,
            origin: Origin::chain(ChainId::root(3)),
            event: Event {
                certificate_hash: CryptoHash::new(&Dummy),
                height: BlockHeight::from(0),
                index: 0,
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
                effect: Effect::System(SystemEffect::Credit {
                    account: Account::chain(ChainId::root(1)),
                    amount: Amount::from(995),
                }),
            },
        }],
        &committee,
        Balance::from(0),
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
    assert_eq!(
        Balance::from(0),
        *chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(
        BlockHeight::from(0),
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(3)))
            .await
            .unwrap()
            .next_block_height_to_receive()
            .unwrap()
    );
    assert_eq!(
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(3)))
            .await
            .unwrap()
            .added_events
            .count(),
        0
    );
    assert!(matches!(
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(3)))
            .await
            .unwrap()
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
            timestamp,
            effect: Effect::System(SystemEffect::Credit { amount, .. }),
        } if certificate_hash == CryptoHash::new(&Dummy)
            && height == BlockHeight::from(0)
            && timestamp == Timestamp::from(0)
            && amount == Amount::from(995),
    ));
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash), chain.tip_state.get().block_hash);
    worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_receiver_balance_overflow() {
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_receiver_balance_overflow() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_receiver_balance_overflow(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_balance_overflow() -> Result<(), anyhow::Error>
{
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_receiver_balance_overflow(client).await;
    Ok(())
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
                Balance::from(1),
            ),
            (
                ChainDescription::Root(2),
                PublicKey::debug(2),
                Balance::max(),
            ),
        ],
    )
    .await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(0),
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
        Balance::from(0),
        *new_sender_chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(1),
        new_sender_chain.tip_state.get().next_block_height
    );
    assert_eq!(new_sender_chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash),
        new_sender_chain.tip_state.get().block_hash
    );
    let new_recipient_chain = worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(
        Balance::max(),
        *new_recipient_chain.execution_state.system.balance.get()
    );
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_receiver_equal_sender() {
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_receiver_equal_sender() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_receiver_equal_sender(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_receiver_equal_sender() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_receiver_equal_sender(client).await;
    Ok(())
}

async fn run_test_handle_certificate_receiver_equal_sender<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut worker) =
        init_worker_with_chain(client, ChainDescription::Root(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &key_pair,
        Recipient::Account(Account::chain(ChainId::root(1))),
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(0),
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
    assert_eq!(
        Balance::from(0),
        *chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(1),
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
            .next_block_height_to_receive()
            .unwrap()
    );
    assert!(matches!(
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
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
            timestamp,
            effect: Effect::System(SystemEffect::Credit { amount, .. })
        } if certificate_hash == certificate.hash
            && height == BlockHeight::from(0)
            && timestamp == Timestamp::from(0)
            && amount == Amount::from(1),
    ));
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash), chain.tip_state.get().block_hash);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request() {
    let client = MemoryStoreClient::default();
    run_test_handle_cross_chain_request(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_cross_chain_request() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_cross_chain_request(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_cross_chain_request(client).await;
    Ok(())
}

async fn run_test_handle_cross_chain_request<S>(client: S)
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![(
            ChainDescription::Root(2),
            PublicKey::debug(2),
            Balance::from(1),
        )],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    )
    .await;
    worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(
                ApplicationId::System,
                Medium::Direct,
                vec![certificate.value.block().height],
            )],
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
    assert_eq!(
        Balance::from(1),
        *chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(0),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(
        BlockHeight::from(1),
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
            .next_block_height_to_receive()
            .unwrap()
    );
    assert!(matches!(
        chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
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
            timestamp,
            effect: Effect::System(SystemEffect::Credit { amount, .. })
        } if certificate_hash == certificate.hash
            && height == BlockHeight::from(0)
            && timestamp == Timestamp::from(0)
            && amount == Amount::from(10),
    ));
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.tip_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request_no_recipient_chain() {
    let client = MemoryStoreClient::default();
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_cross_chain_request_no_recipient_chain() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain() -> Result<(), anyhow::Error>
{
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_cross_chain_request_no_recipient_chain(client).await;
    Ok(())
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
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    )
    .await;
    assert!(worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(
                ApplicationId::System,
                Medium::Direct,
                vec![certificate.value.block().height],
            )],
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
    assert!(!chain
        .communication_states
        .indices()
        .await
        .unwrap()
        .contains(&ApplicationId::System));
}

#[test(tokio::test)]
async fn test_memory_handle_cross_chain_request_no_recipient_chain_on_client() {
    let client = MemoryStoreClient::default();
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_cross_chain_request_no_recipient_chain_on_client() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_cross_chain_request_no_recipient_chain_on_client(
) -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_cross_chain_request_no_recipient_chain_on_client(client).await;
    Ok(())
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
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    )
    .await;
    // An inactive target chain is created and it acknowledges the message.
    let actions = worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            height_map: vec![(
                ApplicationId::System,
                Medium::Direct,
                vec![certificate.value.block().height],
            )],
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
            reason: Reason::NewMessage {
                application_id: ApplicationId::System,
                origin: Origin::chain(ChainId::root(1)),
                height: BlockHeight::from(0),
            }
        }]
    );
    let mut chain = worker.storage.load_chain(ChainId::root(2)).await.unwrap();
    assert!(!chain
        .communication_states
        .load_entry_mut(&ApplicationId::System)
        .await
        .unwrap()
        .inboxes
        .indices()
        .await
        .unwrap()
        .is_empty());
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_to_active_recipient() {
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_to_active_recipient(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_to_active_recipient() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_to_active_recipient(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_active_recipient() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_to_active_recipient(client).await;
    Ok(())
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
                Balance::from(5),
            ),
            (
                ChainDescription::Root(2),
                recipient_key_pair.public(),
                Balance::from(0),
            ),
        ],
    )
    .await;
    assert_eq!(
        worker
            .query_application(
                ChainId::root(1),
                ApplicationId::System,
                &Query::System(SystemQuery)
            )
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Balance::from(5),
        })
    );
    assert_eq!(
        worker
            .query_application(
                ChainId::root(2),
                ApplicationId::System,
                &Query::System(SystemQuery)
            )
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Balance::from(0),
        })
    );

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
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
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        worker
            .query_application(
                ChainId::root(1),
                ApplicationId::System,
                &Query::System(SystemQuery)
            )
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(1),
            balance: Balance::from(0),
        })
    );

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        Recipient::Account(Account::chain(ChainId::root(3))),
        Amount::from(1),
        vec![Message {
            application_id: ApplicationId::System,
            origin: Origin::chain(ChainId::root(1)),
            event: Event {
                certificate_hash: certificate.hash,
                height: BlockHeight::from(0),
                index: 0,
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
                effect: Effect::System(SystemEffect::Credit {
                    account: Account::chain(ChainId::root(2)),
                    amount: Amount::from(5),
                }),
            },
        }],
        &committee,
        Balance::from(4),
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
            .query_application(
                ChainId::root(2),
                ApplicationId::System,
                &Query::System(SystemQuery)
            )
            .await
            .unwrap(),
        Response::System(SystemResponse {
            chain_id: ChainId::root(2),
            balance: Balance::from(4),
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
            Balance::from(4)
        );
        assert!(recipient_chain
            .manager
            .get()
            .has_owner(&recipient_key_pair.public().into()));
        assert_eq!(recipient_chain.confirmed_log.count(), 1);
        assert_eq!(
            recipient_chain.tip_state.get().block_hash,
            Some(certificate.hash)
        );
        assert_eq!(recipient_chain.received_log.count(), 1);
    }
    let query =
        ChainInfoQuery::new(ChainId::root(2)).with_received_certificates_excluding_first_nth(0);
    let response = worker.handle_chain_info_query(query).await.unwrap();
    assert_eq!(response.info.requested_received_certificates.len(), 1);
    assert!(matches!(response.info.requested_received_certificates[0]
            .value
            .confirmed_block()
            .unwrap()
            .operations[..], [(_, Operation::System(SystemOperation::Transfer { amount, .. }))] if amount == Amount::from(5)));
}

#[test(tokio::test)]
async fn test_memory_handle_certificate_to_inactive_recipient() {
    let client = MemoryStoreClient::default();
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_handle_certificate_to_inactive_recipient() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_handle_certificate_to_inactive_recipient(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_handle_certificate_to_inactive_recipient() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_handle_certificate_to_inactive_recipient(client).await;
    Ok(())
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
            Balance::from(5),
        )],
    )
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
        &sender_key_pair,
        Recipient::Account(Account::chain(ChainId::root(2))), // the recipient chain does not exist
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
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
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());
}

#[test(tokio::test)]
async fn test_memory_chain_creation_with_committee_creation() {
    let client = MemoryStoreClient::default();
    run_test_chain_creation_with_committee_creation(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_chain_creation_with_committee_creation() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_chain_creation_with_committee_creation(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_chain_creation_with_committee_creation() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_chain_creation_with_committee_creation(client).await;
    Ok(())
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
            Balance::from(2),
        )],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::from(0), committee.clone());
    let admin_id = ChainId::root(0);
    let admin_channel_id = ChannelId {
        chain_id: admin_id,
        name: SystemChannel::Admin.name(),
    };
    let admin_channel_origin = Origin::channel(admin_id, SystemChannel::Admin.name());
    // Have the admin chain create a user chain.
    let user_id = ChainId::child(EffectId {
        chain_id: admin_id,
        height: BlockHeight::from(0),
        index: 0,
    });
    let user_description = ChainDescription::Child(EffectId {
        chain_id: admin_id,
        height: BlockHeight::from(0),
        index: 0,
    });
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::OpenChain {
                        id: user_id,
                        owner: key_pair.public().into(),
                        epoch: Epoch::from(0),
                        committees: committees.clone(),
                        admin_id,
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![
                direct_outgoing_effect(
                    user_id,
                    SystemEffect::OpenChain {
                        id: user_id,
                        owner: key_pair.public().into(),
                        epoch: Epoch::from(0),
                        committees: committees.clone(),
                        admin_id,
                    },
                ),
                direct_outgoing_effect(
                    admin_id,
                    SystemEffect::Subscribe {
                        id: user_id,
                        channel_id: admin_channel_id.clone(),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair.public().into()),
                balance: Balance::from(2),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
        assert!(admin_chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .outboxes
            .indices()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            *admin_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        // The root chain has no subscribers yet.
        assert!(!admin_chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .channels
            .indices()
            .await
            .unwrap()
            .contains(&SystemChannel::Admin.name()));
    }

    // Create a new committee and transfer money before accepting the subscription.
    let committees2 = BTreeMap::from_iter([
        (Epoch::from(0), committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::CreateCommittee {
                            admin_id,
                            epoch: Epoch::from(1),
                            committee: committee.clone(),
                        }),
                    ),
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::Transfer {
                            owner: None,
                            recipient: Recipient::Account(Account::chain(user_id)),
                            amount: Amount::from(2),
                            user_data: UserData::default(),
                        }),
                    ),
                ],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![
                channel_outgoing_effect(
                    SystemChannel::Admin.name(),
                    SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    },
                ),
                direct_outgoing_effect(
                    user_id,
                    SystemEffect::Credit {
                        account: Account::chain(user_id),
                        amount: Amount::from(2),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public().into()),
                balance: Balance::from(0),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
    );
    worker
        .fully_handle_certificate(certificate1.clone(), vec![])
        .await
        .unwrap();

    // Have the admin chain accept the subscription now.
    let certificate2 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(1),
                chain_id: admin_id,
                incoming_messages: vec![Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(admin_id),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 1,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Subscribe {
                            id: user_id,
                            channel_id: admin_channel_id.clone(),
                        }),
                    },
                }],
                operations: Vec::new(),
                previous_block_hash: Some(certificate1.hash),
                height: BlockHeight::from(2),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![direct_outgoing_effect(
                user_id,
                SystemEffect::Notify { id: user_id },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public().into()),
                balance: Balance::from(0),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .channels
                .load_entry_mut(&SystemChannel::Admin.name())
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
            BlockHeight::from(0),
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
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .inboxes
                .load_entry_mut(&Origin::chain(admin_id))
                .await
                .unwrap()
                .added_events
                .read_front(10)
                .await
                .unwrap()[..],
            [
                Event {
                    effect: Effect::System(SystemEffect::OpenChain { .. }),
                    ..
                },
                Event {
                    effect: Effect::System(SystemEffect::Credit { .. }),
                    ..
                },
                Event {
                    effect: Effect::System(SystemEffect::Notify { .. }),
                    ..
                }
            ]
        );
        matches!(
            user_chain
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .inboxes
                .load_entry_mut(&admin_channel_origin.clone())
                .await
                .unwrap()
                .added_events
                .read_front(10)
                .await
                .unwrap()[..],
            [Event {
                effect: Effect::System(SystemEffect::SetCommittees { .. }),
                ..
            }]
        );
        assert_eq!(
            user_chain
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .inboxes
                .load_entry_mut(&admin_channel_origin)
                .await
                .unwrap()
                .removed_events
                .count(),
            0
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 1);
    }
    // Make the child receive the pending messages.
    let certificate3 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: user_id,
                incoming_messages: vec![
                    Message {
                        application_id: ApplicationId::System,
                        origin: admin_channel_origin.clone(),
                        event: Event {
                            certificate_hash: certificate1.hash,
                            height: BlockHeight::from(1),
                            index: 0,
                            authenticated_signer: None,
                            timestamp: Timestamp::from(0),
                            effect: Effect::System(SystemEffect::SetCommittees {
                                admin_id,
                                epoch: Epoch::from(1),
                                committees: committees2.clone(),
                            }),
                        },
                    },
                    Message {
                        application_id: ApplicationId::System,
                        origin: Origin::chain(admin_id),
                        event: Event {
                            certificate_hash: certificate1.hash,
                            height: BlockHeight::from(1),
                            index: 1,
                            authenticated_signer: None,
                            timestamp: Timestamp::from(0),
                            effect: Effect::System(SystemEffect::Credit {
                                account: Account::chain(user_id),
                                amount: Amount::from(2),
                            }),
                        },
                    },
                    Message {
                        application_id: ApplicationId::System,
                        origin: Origin::chain(admin_id),
                        event: Event {
                            certificate_hash: certificate2.hash,
                            height: BlockHeight::from(2),
                            index: 0,
                            authenticated_signer: None,
                            timestamp: Timestamp::from(0),
                            effect: Effect::System(SystemEffect::Notify { id: user_id }),
                        },
                    },
                ],
                operations: Vec::new(),
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: Vec::new(),
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(user_description),
                admin_id: Some(admin_id),
                subscriptions: [ChannelId {
                    chain_id: admin_id,
                    name: SystemChannel::Admin.name(),
                }]
                .into_iter()
                .collect(),
                // Finally the child knows about both committees and has the money.
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair.public().into()),
                balance: Balance::from(2),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .inboxes
                .load_entry_mut(&Origin::chain(admin_id))
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
                .communication_states
                .load_entry_mut(&ApplicationId::System)
                .await
                .unwrap()
                .inboxes
                .load_entry_mut(&admin_channel_origin)
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
    let client = MemoryStoreClient::default();
    run_test_transfers_and_committee_creation(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_transfers_and_committee_creation() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_transfers_and_committee_creation(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_creation() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_transfers_and_committee_creation(client).await;
    Ok(())
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
            (
                ChainDescription::Root(0),
                key_pair0.public(),
                Balance::from(0),
            ),
            (
                ChainDescription::Root(1),
                key_pair1.public(),
                Balance::from(3),
            ),
        ],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::from(0), committee.clone());
    let admin_id = ChainId::root(0);
    let user_id = ChainId::root(1);

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: user_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::Transfer {
                        owner: None,
                        recipient: Recipient::Account(Account::chain(admin_id)),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![direct_outgoing_effect(
                admin_id,
                SystemEffect::Credit {
                    account: Account::chain(admin_id),
                    amount: Amount::from(1),
                },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public().into()),
                balance: Balance::from(2),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
    );
    // Have the admin chain create a new epoch without retiring the old one.
    let committees2 = BTreeMap::from_iter([
        (Epoch::from(0), committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::CreateCommittee {
                        admin_id,
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![channel_outgoing_effect(
                SystemChannel::Admin.name(),
                SystemEffect::SetCommittees {
                    admin_id,
                    epoch: Epoch::from(1),
                    committees: committees2.clone(),
                },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees2.clone(),
                ownership: ChainOwnership::single(key_pair0.public().into()),
                balance: Balance::from(0),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
        Balance::from(2)
    );
    assert_eq!(
        *user_chain.execution_state.system.epoch.get(),
        Some(Epoch::from(0))
    );

    // .. and the message has gone through.
    let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
    assert_eq!(
        admin_chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .indices()
            .await
            .unwrap()
            .len(),
        1
    );
    matches!(
        admin_chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .load_entry_mut(&Origin::chain(user_id))
            .await
            .unwrap()
            .added_events
            .read_front(10)
            .await
            .unwrap()[..],
        [Event {
            effect: Effect::System(SystemEffect::Credit { .. }),
            ..
        }]
    );
}

#[test(tokio::test)]
async fn test_memory_transfers_and_committee_removal() {
    let client = MemoryStoreClient::default();
    run_test_transfers_and_committee_removal(client).await;
}

#[test(tokio::test)]
async fn test_rocksdb_transfers_and_committee_removal() {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), None);
    run_test_transfers_and_committee_removal(client).await;
}

#[cfg(feature = "aws")]
#[test(tokio::test)]
async fn test_dynamo_db_transfers_and_committee_removal() -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, None).await?;
    run_test_transfers_and_committee_removal(client).await;
    Ok(())
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
            (
                ChainDescription::Root(0),
                key_pair0.public(),
                Balance::from(0),
            ),
            (
                ChainDescription::Root(1),
                key_pair1.public(),
                Balance::from(3),
            ),
        ],
    )
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::from(0), committee.clone());
    let admin_id = ChainId::root(0);
    let user_id = ChainId::root(1);

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: user_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::Transfer {
                        owner: None,
                        recipient: Recipient::Account(Account::chain(admin_id)),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![direct_outgoing_effect(
                admin_id,
                SystemEffect::Credit {
                    account: Account::chain(admin_id),
                    amount: Amount::from(1),
                },
            )],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees.clone(),
                ownership: ChainOwnership::single(key_pair1.public().into()),
                balance: Balance::from(2),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
    );
    // Have the admin chain create a new epoch and retire the old one immediately.
    let committees2 = BTreeMap::from_iter([
        (Epoch::from(0), committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let committees3 = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::CreateCommittee {
                            admin_id,
                            epoch: Epoch::from(1),
                            committee: committee.clone(),
                        }),
                    ),
                    (
                        ApplicationId::System,
                        Operation::System(SystemOperation::RemoveCommittee {
                            admin_id,
                            epoch: Epoch::from(0),
                        }),
                    ),
                ],
                previous_block_hash: None,
                height: BlockHeight::from(0),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: vec![
                channel_outgoing_effect(
                    SystemChannel::Admin.name(),
                    SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    },
                ),
                channel_outgoing_effect(
                    SystemChannel::Admin.name(),
                    SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees3.clone(),
                    },
                ),
            ],
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public().into()),
                balance: Balance::from(0),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
            Balance::from(2)
        );
        assert_eq!(
            *user_chain.execution_state.system.epoch.get(),
            Some(Epoch::from(0))
        );

        // .. but the message hasn't gone through.
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        assert!(admin_chain
            .communication_states
            .load_entry_mut(&ApplicationId::System)
            .await
            .unwrap()
            .inboxes
            .indices()
            .await
            .unwrap()
            .is_empty());
    }

    // Force the admin chain to receive the money nonetheless by anticipation.
    let certificate2 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(1),
                chain_id: admin_id,
                incoming_messages: vec![Message {
                    application_id: ApplicationId::System,
                    origin: Origin::chain(user_id),
                    event: Event {
                        certificate_hash: certificate0.hash,
                        height: BlockHeight::from(0),
                        index: 0,
                        authenticated_signer: None,
                        timestamp: Timestamp::from(0),
                        effect: Effect::System(SystemEffect::Credit {
                            account: Account::chain(admin_id),
                            amount: Amount::from(1),
                        }),
                    },
                }],
                operations: Vec::new(),
                previous_block_hash: Some(certificate1.hash),
                height: BlockHeight::from(1),
                authenticated_signer: None,
                timestamp: Timestamp::from(0),
            },
            effects: Vec::new(),
            state_hash: make_state_hash(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeSet::new(),
                committees: committees3.clone(),
                ownership: ChainOwnership::single(key_pair0.public().into()),
                balance: Balance::from(1),
                balances: BTreeMap::new(),
                timestamp: Timestamp::from(0),
                registry: ApplicationRegistry::default(),
            })
            .await,
        },
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
    let (committee, worker) = init_worker(MemoryStoreClient::default(), true);
    let committees = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);

    let key_pair0 = KeyPair::generate();
    let id0 = ChainId::root(0);
    let id1 = ChainId::root(1);

    let certificate0 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::Account(Account::chain(id1)),
        Amount::from(1),
        Vec::new(),
        Epoch::from(0),
        &committee,
        Balance::from(1),
        &worker,
        None,
    )
    .await;
    let certificate1 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::Account(Account::chain(id1)),
        Amount::from(1),
        Vec::new(),
        Epoch::from(0),
        &committee,
        Balance::from(1),
        &worker,
        Some(&certificate0),
    )
    .await;
    let certificate2 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::Account(Account::chain(id1)),
        Amount::from(1),
        Vec::new(),
        Epoch::from(1),
        &committee,
        Balance::from(1),
        &worker,
        Some(&certificate1),
    )
    .await;
    // Weird case: epoch going backward.
    let certificate3 = make_transfer_certificate_for_epoch(
        ChainDescription::Root(0),
        &key_pair0,
        Recipient::Account(Account::chain(id1)),
        Amount::from(1),
        Vec::new(),
        Epoch::from(0),
        &committee,
        Balance::from(1),
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
                ApplicationId::System,
                &Origin::chain(id0),
                id1,
                BlockHeight::from(0),
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
                ApplicationId::System,
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
                ApplicationId::System,
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
            ApplicationId::System,
            &Origin::chain(id0),
            id1,
            BlockHeight::from(0),
            None,
            vec![certificate1.clone(), certificate0.clone()]
        )
        .is_err());
    // Sender is checked.
    assert!(helper
        .select_certificates(
            ApplicationId::System,
            &Origin::chain(id1),
            id0,
            BlockHeight::from(0),
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
                ApplicationId::System,
                &Origin::chain(id0),
                id1,
                BlockHeight::from(0),
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
                ApplicationId::System,
                &Origin::chain(id0),
                id1,
                BlockHeight::from(0),
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
                ApplicationId::System,
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
                ApplicationId::System,
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
                ApplicationId::System,
                &Origin::chain(id0),
                id1,
                BlockHeight::from(0),
                Some(BlockHeight::from(1)),
                vec![certificate0.clone(), certificate1.clone()]
            )
            .unwrap(),
        vec![certificate0.clone(), certificate1.clone()]
    );
}
