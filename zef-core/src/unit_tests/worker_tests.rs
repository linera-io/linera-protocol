// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use std::collections::BTreeMap;
use zef_base::{
    base_types::*, chain::ChainState, committee::Committee, manager::ChainManager, messages::*,
};
use zef_storage::{InMemoryStoreClient, Storage};

#[tokio::test]
async fn test_handle_block_proposal_bad_signature() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
    );
    let unknown_key_pair = KeyPair::generate();
    let mut bad_signature_block_proposal = block_proposal.clone();
    bad_signature_block_proposal.signature =
        Signature::new(&block_proposal.content, &unknown_key_pair);
    assert!(state
        .handle_block_proposal(bad_signature_block_proposal)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal_zero_amount() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    // test block non-positive amount
    let zero_amount_block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::zero(),
        Vec::new(),
    );
    assert!(state
        .handle_block_proposal(zero_amount_block_proposal)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
    );
    let unknown_key = KeyPair::generate();

    let unknown_sender_block_proposal = BlockProposal::new(block_proposal.content, &unknown_key);
    assert!(state
        .handle_block_proposal(unknown_sender_block_proposal)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal_bad_block_height() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
    );

    let mut sender_chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    sender_chain.next_block_height.try_add_assign_one().unwrap();
    state.storage.write_chain(sender_chain).await.unwrap();
    assert!(state.handle_block_proposal(block_proposal).await.is_err());
    assert!(state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(1000),
        Vec::new(),
    );
    assert!(state.handle_block_proposal(block_proposal).await.is_err());
    assert!(state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![(
        dbg_chain(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
    );

    let chain_info_response = state.handle_block_proposal(block_proposal).await.unwrap();
    chain_info_response
        .check(state.key_pair.unwrap().public())
        .unwrap();
    let pending = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .cloned()
        .unwrap();
    assert_eq!(
        chain_info_response.info.manager.pending().unwrap().value,
        pending.value
    );
}

#[tokio::test]
async fn test_handle_block_proposal_replay() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let block_proposal = make_transfer_block_proposal(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(5),
        Vec::new(),
    );

    let response = state
        .handle_block_proposal(block_proposal.clone())
        .await
        .unwrap();
    response
        .check(state.key_pair.as_ref().unwrap().public())
        .as_ref()
        .unwrap();
    let replay_response = state.handle_block_proposal(block_proposal).await.unwrap();
    // Workaround lack of equality.
    assert_eq!(
        HashValue::new(&response.info),
        HashValue::new(&replay_response.info)
    );
}

#[tokio::test]
async fn test_handle_certificate_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) =
        init_state_with_chains(vec![(dbg_chain(2), dbg_addr(2), Balance::from(0))]).await;
    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_err());
}

#[tokio::test]
async fn test_handle_certificate_bad_block_height() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        &state,
    );
    // Replays are ignored.
    state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    state.fully_handle_certificate(certificate).await.unwrap();
    // TODO: test the case of a block height in the future (aka lagging validator)
}

#[tokio::test]
async fn test_handle_certificate_with_early_incoming_message() {
    let key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(1000),
        vec![Message {
            sender_id: dbg_chain(3),
            height: BlockHeight::from(0),
            operation: Operation::Transfer {
                recipient: Address::Account(dbg_chain(1)),
                amount: Amount::from(995),
                user_data: UserData::default(),
            },
        }],
        &committee,
        &state,
    );
    state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.state.balance);
    assert_eq!(BlockHeight::from(1), chain.next_block_height);
    assert_eq!(
        BlockHeight::from(0),
        chain
            .inboxes
            .get(&dbg_chain(3))
            .unwrap()
            .next_height_to_receive
    );
    assert!(chain
        .inboxes
        .get(&dbg_chain(3))
        .unwrap()
        .received
        .is_empty(),);
    assert!(matches!(
        chain.inboxes.get(&dbg_chain(3)).unwrap().expected.front().unwrap(),
        (height, Operation::Transfer { amount, .. }) if *height == BlockHeight::from(0) && *amount == Amount::from(995),
    ));
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
    state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_handle_certificate_receiver_balance_overflow() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(1)),
        (dbg_chain(2), dbg_addr(2), Balance::max()),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(1),
        Vec::new(),
        &committee,
        &state,
    );
    state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let new_sender_chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), new_sender_chain.state.balance);
    assert_eq!(BlockHeight::from(1), new_sender_chain.next_block_height);
    assert_eq!(new_sender_chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), new_sender_chain.block_hash);
    let new_recipient_chain = state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
    assert_eq!(Balance::max(), new_recipient_chain.state.balance);
}

#[tokio::test]
async fn test_handle_certificate_receiver_equal_sender() {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut state) = init_state_with_chain(dbg_chain(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &key_pair,
        Address::Account(dbg_chain(1)),
        Amount::from(1),
        Vec::new(),
        &committee,
        &state,
    );
    state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.state.balance);
    assert_eq!(
        BlockHeight::from(1),
        chain
            .inboxes
            .get(&dbg_chain(1))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&dbg_chain(1)).unwrap().received.front().unwrap(),
        (height, Operation::Transfer { amount, .. }) if *height == BlockHeight::from(0) && *amount == Amount::from(1),
    ));
    assert_eq!(BlockHeight::from(1), chain.next_block_height);
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
}

#[tokio::test]
async fn test_update_recipient_chain() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) =
        init_state_with_chains(vec![(dbg_chain(2), dbg_addr(2), Balance::from(1))]).await;
    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(10),
        Vec::new(),
        &committee,
        &state,
    );
    let operation = certificate
        .value
        .confirmed_block()
        .unwrap()
        .operation
        .clone();
    state
        .update_recipient_chain(operation, certificate)
        .await
        .unwrap();
    let chain = state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
    assert_eq!(Balance::from(1), chain.state.balance);
    assert_eq!(BlockHeight::from(0), chain.next_block_height);
    assert_eq!(
        BlockHeight::from(1),
        chain
            .inboxes
            .get(&dbg_chain(1))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&dbg_chain(1)).unwrap().received.front().unwrap(),
        (height, Operation::Transfer { amount, .. }) if *height == BlockHeight::from(0) && *amount == Amount::from(10),
    ));
    assert_eq!(chain.confirmed_log.len(), 0);
    assert_eq!(None, chain.block_hash);
    assert_eq!(chain.received_log.len(), 1);
}

#[tokio::test]
async fn test_handle_certificate_to_active_recipient() {
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), recipient_key_pair.public(), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        &state,
    );

    let info = state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(dbg_chain(1), info.chain_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_transfer_certificate(
        dbg_chain(2),
        &recipient_key_pair,
        Address::Account(dbg_chain(3)),
        Amount::from(1),
        vec![Message {
            sender_id: dbg_chain(1),
            height: BlockHeight::from(0),
            operation: Operation::Transfer {
                recipient: Address::Account(dbg_chain(2)),
                amount: Amount::from(5),
                user_data: UserData::default(),
            },
        }],
        &committee,
        &state,
    );
    state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();

    let recipient_chain = state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
    assert_eq!(recipient_chain.state.balance, Balance::from(4));
    assert!(recipient_chain
        .state
        .manager
        .has_owner(&recipient_key_pair.public()));
    assert_eq!(recipient_chain.confirmed_log.len(), 1);
    assert_eq!(recipient_chain.block_hash, Some(certificate.hash));
    assert_eq!(recipient_chain.received_log.len(), 1);

    let info_query = ChainInfoQuery {
        chain_id: dbg_chain(2),
        check_next_block_height: None,
        query_committee: false,
        query_pending_messages: false,
        query_sent_certificates_in_range: None,
        query_received_certificates_excluding_first_nth: Some(0),
    };
    let response = state.handle_chain_info_query(info_query).await.unwrap();
    assert_eq!(response.info.queried_received_certificates.len(), 1);
    assert!(matches!(response.info.queried_received_certificates[0]
            .value
            .confirmed_block()
            .unwrap()
            .operation, Operation::Transfer { amount, .. } if amount == Amount::from(5)));
}

#[tokio::test]
async fn test_handle_certificate_to_inactive_recipient() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_chains(vec![(
        dbg_chain(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let certificate = make_transfer_certificate(
        dbg_chain(1),
        &sender_key_pair,
        Address::Account(dbg_chain(2)), // the recipient chain does not exist
        Amount::from(5),
        Vec::new(),
        &committee,
        &state,
    );

    let info = state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(dbg_chain(1), info.chain_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());
}

#[tokio::test]
async fn test_read_chain_state() {
    let sender = dbg_chain(1);
    let (_, mut state) = init_state_with_chain(sender.clone(), dbg_addr(1), Balance::from(5)).await;
    state.storage.read_active_chain(&sender).await.unwrap();
}

#[tokio::test]
async fn test_read_chain_state_unknown_chain() {
    let sender = dbg_chain(1);
    let unknown_chain_id = dbg_chain(99);
    let (committee, mut state) = init_state_with_chain(sender, dbg_addr(1), Balance::from(5)).await;
    assert!(state
        .storage
        .read_active_chain(&unknown_chain_id)
        .await
        .is_err());
    let mut chain = state
        .storage
        .read_chain_or_default(&unknown_chain_id)
        .await
        .unwrap();
    chain.state.committee = Some(committee);
    chain.state.manager = ChainManager::single(dbg_addr(4));
    state.storage.write_chain(chain).await.unwrap();
    state
        .storage
        .read_active_chain(&unknown_chain_id)
        .await
        .unwrap();
}

// helpers

fn init_state() -> (Committee, WorkerState<InMemoryStoreClient>) {
    let key_pair = KeyPair::generate();
    let mut validators = BTreeMap::new();
    validators.insert(key_pair.public(), /* voting right */ 1);
    let committee = Committee::new(validators);
    let client = InMemoryStoreClient::default();
    let state = WorkerState::new(Some(key_pair), client);
    (committee, state)
}

async fn init_state_with_chains<I: IntoIterator<Item = (ChainId, Owner, Balance)>>(
    balances: I,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let (committee, mut state) = init_state();
    for (id, owner, balance) in balances {
        let chain = ChainState::create(committee.clone(), id, owner, balance);
        state.storage.write_chain(chain).await.unwrap();
    }
    (committee, state)
}

async fn init_state_with_chain(
    id: ChainId,
    owner: Owner,
    balance: Balance,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    init_state_with_chains(std::iter::once((id, owner, balance))).await
}

fn make_transfer_block_proposal(
    chain_id: ChainId,
    secret: &KeyPair,
    recipient: Address,
    amount: Amount,
    incoming_messages: Vec<Message>,
) -> BlockProposal {
    let block = Block {
        chain_id,
        incoming_messages,
        operation: Operation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        },
        previous_block_hash: None,
        height: BlockHeight::new(),
    };
    BlockProposal::new(
        BlockAndRound {
            block,
            round: RoundNumber::default(),
        },
        secret,
    )
}

fn make_certificate(
    committee: &Committee,
    state: &WorkerState<InMemoryStoreClient>,
    value: Value,
) -> Certificate {
    let vote = Vote::new(value.clone(), state.key_pair.as_ref().unwrap());
    let mut builder = SignatureAggregator::new(value, committee);
    builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap()
}

fn make_transfer_certificate(
    chain_id: ChainId,
    key_pair: &KeyPair,
    recipient: Address,
    amount: Amount,
    incoming_messages: Vec<Message>,
    committee: &Committee,
    state: &WorkerState<InMemoryStoreClient>,
) -> Certificate {
    let block =
        make_transfer_block_proposal(chain_id, key_pair, recipient, amount, incoming_messages)
            .content
            .block;
    let value = Value::Confirmed { block };
    make_certificate(committee, state, value)
}
