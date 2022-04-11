// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{AuthorityWorker, WorkerState};
use std::collections::BTreeMap;
use zef_base::{
    account::{AccountManager, AccountState},
    base_types::*,
    committee::Committee,
    messages::*,
};
use zef_storage::{InMemoryStoreClient, Storage};

#[tokio::test]
async fn test_handle_request_order_bad_signature() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));
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
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_zero_amount() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    // test request non-positive amount
    let zero_amount_request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::zero());
    assert!(state
        .handle_request_order(zero_amount_request_order)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));
    let unknown_key = KeyPair::generate();

    let unknown_sender_request_order = RequestOrder::new(request_order.request, &unknown_key);
    assert!(state
        .handle_request_order(unknown_sender_request_order)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_bad_sequence_number() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));

    let mut sender_account = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
    sender_account
        .next_sequence_number
        .try_add_assign_one()
        .unwrap();
    state.storage.write_account(sender_account).await.unwrap();
    assert!(state.handle_request_order(request_order).await.is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order = make_transfer_request_order(
        dbg_account(1),
        &sender_key_pair,
        recipient,
        Amount::from(1000),
    );
    assert!(state.handle_request_order(request_order).await.is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![(
        dbg_account(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));

    let account_info_response = state.handle_request_order(request_order).await.unwrap();
    account_info_response
        .check(state.key_pair.unwrap().public())
        .unwrap();
    let pending = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .manager
        .pending()
        .cloned()
        .unwrap();
    assert_eq!(
        account_info_response.info.manager.pending().unwrap().value,
        pending.value
    );
}

#[tokio::test]
async fn test_handle_request_order_replay() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let (_, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));

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
        init_state_with_accounts(vec![(dbg_account(2), dbg_addr(2), Balance::from(0))]).await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(5),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_err());
}

#[tokio::test]
async fn test_handle_certificate_bad_sequence_number() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
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
    // TODO: test the case of a sequence number in the future (aka lagging authority)
}

#[tokio::test]
async fn test_handle_certificate_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(1000),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let sender_account = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(-995), sender_account.balance);
    assert_eq!(SequenceNumber::from(1), sender_account.next_sequence_number);
    assert_eq!(sender_account.confirmed_log.len(), 1);
    assert!(state
        .storage
        .read_active_account(&dbg_account(2))
        .await
        .is_ok());
}

#[tokio::test]
async fn test_handle_certificate_receiver_balance_overflow() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(1)),
        (dbg_account(2), dbg_addr(2), Balance::max()),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(1),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let new_sender_account = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), new_sender_account.balance);
    assert_eq!(
        SequenceNumber::from(1),
        new_sender_account.next_sequence_number
    );
    assert_eq!(new_sender_account.confirmed_log.len(), 1);
    let new_recipient_account = state
        .storage
        .read_active_account(&dbg_account(2))
        .await
        .unwrap();
    assert_eq!(Balance::max(), new_recipient_account.balance);
}

#[tokio::test]
async fn test_handle_certificate_receiver_equal_sender() {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut state) =
        init_state_with_account(dbg_account(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &key_pair,
        Address::Account(dbg_account(1)),
        Amount::from(10),
        &committee,
        &state,
    );
    assert!(state.fully_handle_certificate(certificate).await.is_ok());
    let account = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(1), account.balance);
    assert_eq!(SequenceNumber::from(1), account.next_sequence_number);
    assert_eq!(account.confirmed_log.len(), 1);
}

#[tokio::test]
async fn test_update_recipient_account() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) =
        init_state_with_accounts(vec![(dbg_account(2), dbg_addr(2), Balance::from(1))]).await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
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
        .update_recipient_account(operation, committee, certificate)
        .await
        .is_ok());
    let account = state
        .storage
        .read_active_account(&dbg_account(2))
        .await
        .unwrap();
    assert_eq!(Balance::from(11), account.balance);
    assert_eq!(SequenceNumber::from(0), account.next_sequence_number);
    assert_eq!(account.confirmed_log.len(), 0);
    assert_eq!(account.received_log.len(), 1);
}

#[tokio::test]
async fn test_handle_certificate_to_active_recipient() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut state) = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(5),
        &committee,
        &state,
    );

    let info = state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(dbg_account(1), info.account_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        state
            .storage
            .read_active_account(&dbg_account(1))
            .await
            .unwrap()
            .confirmed_log,
        vec![certificate.hash]
    );

    let recipient_account = state
        .storage
        .read_active_account(&dbg_account(2))
        .await
        .unwrap();
    assert_eq!(recipient_account.balance, Balance::from(5));
    assert!(recipient_account.manager.has_owner(&dbg_addr(2)));
    assert_eq!(recipient_account.confirmed_log.len(), 0);
    assert_eq!(recipient_account.received_log.len(), 1);

    let info_query = AccountInfoQuery {
        account_id: dbg_account(2),
        check_next_sequence_number: None,
        query_committee: false,
        query_sent_certificates_in_range: None,
        query_received_certificates_excluding_first_nth: Some(0),
    };
    let response = state.handle_account_info_query(info_query).await.unwrap();
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
    let (committee, mut state) = init_state_with_accounts(vec![(
        dbg_account(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)), // the recipient account does not exist
        Amount::from(5),
        &committee,
        &state,
    );

    let info = state
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(dbg_account(1), info.account_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert!(info.manager.pending().is_none());
    assert_eq!(
        state
            .storage
            .read_active_account(&dbg_account(1))
            .await
            .unwrap()
            .confirmed_log,
        vec![certificate.hash]
    );
}

#[tokio::test]
async fn test_read_account_state() {
    let sender = dbg_account(1);
    let (_, mut state) =
        init_state_with_account(sender.clone(), dbg_addr(1), Balance::from(5)).await;
    state.storage.read_active_account(&sender).await.unwrap();
}

#[tokio::test]
async fn test_read_account_state_unknown_account() {
    let sender = dbg_account(1);
    let unknown_account_id = dbg_account(99);
    let (committee, mut state) =
        init_state_with_account(sender, dbg_addr(1), Balance::from(5)).await;
    assert!(state
        .storage
        .read_active_account(&unknown_account_id)
        .await
        .is_err());
    let mut account = state
        .storage
        .read_account_or_default(&unknown_account_id)
        .await
        .unwrap();
    account.committee = Some(committee);
    account.manager = AccountManager::single(dbg_addr(4));
    state.storage.write_account(account).await.unwrap();
    assert!(state
        .storage
        .read_active_account(&unknown_account_id)
        .await
        .is_ok());
}

// helpers

fn init_state() -> (Committee, WorkerState<InMemoryStoreClient>) {
    let key_pair = KeyPair::generate();
    let mut authorities = BTreeMap::new();
    authorities.insert(key_pair.public(), /* voting right */ 1);
    let committee = Committee::new(authorities);
    let client = InMemoryStoreClient::default();
    let state = WorkerState::new(Some(key_pair), client);
    (committee, state)
}

async fn init_state_with_accounts<I: IntoIterator<Item = (AccountId, AccountOwner, Balance)>>(
    balances: I,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let (committee, mut state) = init_state();
    for (id, owner, balance) in balances {
        let account = AccountState::create(committee.clone(), id, owner, balance);
        state.storage.write_account(account).await.unwrap();
    }
    (committee, state)
}

async fn init_state_with_account(
    id: AccountId,
    owner: AccountOwner,
    balance: Balance,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    init_state_with_accounts(std::iter::once((id, owner, balance))).await
}

fn make_transfer_request_order(
    account_id: AccountId,
    secret: &KeyPair,
    recipient: Address,
    amount: Amount,
) -> RequestOrder {
    let request = Request {
        account_id,
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
        .append(vote.authority, vote.signature)
        .unwrap()
        .unwrap()
}

fn make_transfer_certificate(
    account_id: AccountId,
    key_pair: &KeyPair,
    recipient: Address,
    amount: Amount,
    committee: &Committee,
    state: &WorkerState<InMemoryStoreClient>,
) -> Certificate {
    let request = make_transfer_request_order(account_id, key_pair, recipient, amount).request;
    let value = Value::Confirmed { request };
    make_certificate(committee, state, value)
}
