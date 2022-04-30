// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use std::collections::BTreeMap;
use zef_base::{
    base_types::*,
    chain::{ChainManager, ChainState},
    committee::Committee,
    messages::*,
};
use zef_storage::{InMemoryStoreClient, Storage};

#[tokio::test]
async fn test_handle_request_order_bad_signature() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::from(5));
    let unknown_key_pair = KeyPair::generate();
    let mut bad_signature_request_order = request_order.clone();
    bad_signature_request_order.signature =
        Signature::new(&request_order.request, &unknown_key_pair);
    assert!(state
        .handle_request_order(bad_signature_request_order)
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
async fn test_handle_request_order_zero_amount() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    // test request non-positive amount
    let zero_amount_request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::zero());
    assert!(state
        .handle_request_order(zero_amount_request_order)
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
async fn test_handle_request_order_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::from(5));
    let unknown_key = KeyPair::generate();

    let unknown_sender_request_order = RequestOrder::new(request_order.request, &unknown_key);
    assert!(state
        .handle_request_order(unknown_sender_request_order)
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
async fn test_handle_request_order_bad_sequence_number() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::from(5));

    let mut sender_chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    sender_chain
        .next_sequence_number
        .try_add_assign_one()
        .unwrap();
    state.storage.write_chain(sender_chain).await.unwrap();
    assert!(state.handle_request_order(request_order).await.is_err());
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
async fn test_handle_request_order_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order = make_transfer_request_order(
        dbg_chain(1),
        &sender_key_pair,
        recipient,
        Amount::from(1000),
    );
    assert!(state.handle_request_order(request_order).await.is_err());
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
async fn test_handle_request_order() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![(
        dbg_chain(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::from(5));

    let chain_info_response = state.handle_request_order(request_order).await.unwrap();
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
async fn test_handle_request_order_replay() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_chain(2));
    let (_, mut state) = init_state_with_chains(vec![
        (dbg_chain(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_chain(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_chain(1), &sender_key_pair, recipient, Amount::from(5));

    let response = state
        .handle_request_order(request_order.clone())
        .await
        .unwrap();
    response
        .check(state.key_pair.as_ref().unwrap().public())
        .as_ref()
        .unwrap();
    let replay_response = state.handle_request_order(request_order).await.unwrap();
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
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_err());
}

#[tokio::test]
async fn test_handle_certificate_bad_sequence_number() {
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
        &committee,
        &state,
    );
    // Replays are ignored.
    assert!(state
        .fully_handle_certificate(certificate.clone())
        .await
        .is_ok());
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    // TODO: test the case of a sequence number in the future (aka lagging validator)
}

#[tokio::test]
async fn test_handle_certificate_exceed_balance() {
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
        Amount::from(1000),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let sender_chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(-995), sender_chain.state.balance);
    assert_eq!(SequenceNumber::from(1), sender_chain.next_sequence_number);
    assert_eq!(sender_chain.confirmed_log.len(), 1);
    assert!(state.storage.read_active_chain(&dbg_chain(2)).await.is_ok());
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
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let new_sender_chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), new_sender_chain.state.balance);
    assert_eq!(
        SequenceNumber::from(1),
        new_sender_chain.next_sequence_number
    );
    assert_eq!(new_sender_chain.confirmed_log.len(), 1);
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
        Amount::from(10),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let chain = state
        .storage
        .read_active_chain(&dbg_chain(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(1), chain.state.balance);
    assert_eq!(SequenceNumber::from(1), chain.next_sequence_number);
    assert_eq!(chain.confirmed_log.len(), 1);
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
        &committee,
        &state,
    );
    let operation = certificate
        .value
        .confirmed_request()
        .unwrap()
        .operation
        .clone();
    assert!(state
        .update_recipient_chain(operation, committee, certificate)
        .await
        .is_ok());
    let chain = state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
    assert_eq!(Balance::from(11), chain.state.balance);
    assert_eq!(SequenceNumber::from(0), chain.next_sequence_number);
    assert_eq!(chain.confirmed_log.len(), 0);
    assert_eq!(chain.received_log.len(), 1);
}

#[tokio::test]
async fn test_handle_certificate_to_active_recipient() {
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
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        state
            .storage
            .read_active_chain(&dbg_chain(1))
            .await
            .unwrap()
            .confirmed_log,
        vec![certificate.hash]
    );

    let recipient_chain = state
        .storage
        .read_active_chain(&dbg_chain(2))
        .await
        .unwrap();
    assert_eq!(recipient_chain.state.balance, Balance::from(5));
    assert!(recipient_chain.state.manager.has_owner(&dbg_addr(2)));
    assert_eq!(recipient_chain.confirmed_log.len(), 0);
    assert_eq!(recipient_chain.received_log.len(), 1);

    let info_query = ChainInfoQuery {
        chain_id: dbg_chain(2),
        check_next_sequence_number: None,
        query_committee: false,
        query_sent_certificates_in_range: None,
        query_received_certificates_excluding_first_nth: Some(0),
    };
    let response = state.handle_chain_info_query(info_query).await.unwrap();
    assert_eq!(response.info.queried_received_certificates.len(), 1);
    assert!(matches!(response.info.queried_received_certificates[0]
            .value
            .confirmed_request()
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
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        state
            .storage
            .read_active_chain(&dbg_chain(1))
            .await
            .unwrap()
            .confirmed_log,
        vec![certificate.hash]
    );
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
    assert!(state
        .storage
        .read_active_chain(&unknown_chain_id)
        .await
        .is_ok());
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

async fn init_state_with_chains<I: IntoIterator<Item = (ChainId, ChainOwner, Balance)>>(
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
    owner: ChainOwner,
    balance: Balance,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    init_state_with_chains(std::iter::once((id, owner, balance))).await
}

fn make_transfer_request_order(
    chain_id: ChainId,
    secret: &KeyPair,
    recipient: Address,
    amount: Amount,
) -> RequestOrder {
    let request = Request {
        chain_id,
        operation: Operation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        },
        sequence_number: SequenceNumber::new(),
        round: RoundNumber::default(),
    };
    RequestOrder::new(request, secret)
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
    committee: &Committee,
    state: &WorkerState<InMemoryStoreClient>,
) -> Certificate {
    let request = make_transfer_request_order(chain_id, key_pair, recipient, amount).request;
    let value = Value::Confirmed { request };
    make_certificate(committee, state, value)
}
