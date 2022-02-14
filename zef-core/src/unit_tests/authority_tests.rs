// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//use super::*;
use crate::{
    account::AccountState,
    authority::{get_shard, Authority, WorkerState},
    base_types::*,
    committee::Committee,
    messages::*,
    storage::{InMemoryStoreClient, StorageClient},
};
use std::collections::BTreeMap;

#[tokio::test]
async fn test_handle_request_order_bad_signature() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));
    let unknown_key_pair = KeyPair::generate();
    let mut bad_signature_request_order = request_order.clone();
    bad_signature_request_order.signature = Signature::new(&request_order.value, &unknown_key_pair);
    assert!(state
        .handle_request_order(bad_signature_request_order)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .pending
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_zero_amount() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
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
        .pending
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));
    let unknown_key = KeyPair::generate();

    let unknown_sender_request_order = RequestOrder::new(request_order.value, &unknown_key);
    assert!(state
        .handle_request_order(unknown_sender_request_order)
        .await
        .is_err());
    assert!(state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .pending
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_bad_sequence_number() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
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
        .pending
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
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
        .pending
        .is_none());
}

#[tokio::test]
async fn test_handle_request_order() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![(
        dbg_account(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));

    let account_info = state.handle_request_order(request_order).await.unwrap();
    let pending = state
        .storage
        .read_active_account(&dbg_account(1))
        .await
        .unwrap()
        .pending
        .unwrap();
    assert_eq!(account_info.pending.unwrap(), pending);
}

#[tokio::test]
async fn test_handle_request_order_replay() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(dbg_account(2));
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let request_order =
        make_transfer_request_order(dbg_account(1), &sender_key_pair, recipient, Amount::from(5));

    let vote = state
        .handle_request_order(request_order.clone())
        .await
        .unwrap();
    let replay_vote = state.handle_request_order(request_order).await.unwrap();
    assert_eq!(vote, replay_vote);
}

#[tokio::test]
async fn test_handle_confirmation_order_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let mut state =
        init_state_with_accounts(vec![(dbg_account(2), dbg_addr(2), Balance::from(0))]).await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(5),
        &state,
    );
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate))
        .await
        .is_err());
}

#[tokio::test]
async fn test_handle_confirmation_order_bad_sequence_number() {
    let sender_key_pair = KeyPair::generate();
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(5),
        &state,
    );
    // Replays are ignored.
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate.clone()))
        .await
        .is_ok());
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate))
        .await
        .is_ok());
    // TODO: test the case of a sequence number in the future (aka lagging authority)
}

#[tokio::test]
async fn test_handle_confirmation_order_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(1000),
        &state,
    );
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate))
        .await
        .is_ok());
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
async fn test_handle_confirmation_order_receiver_balance_overflow() {
    let sender_key_pair = KeyPair::generate();
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(1)),
        (dbg_account(2), dbg_addr(2), Balance::max()),
    ])
    .await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(1),
        &state,
    );
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate))
        .await
        .is_ok());
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
async fn test_handle_confirmation_order_receiver_equal_sender() {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let mut state = init_state_with_account(dbg_account(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        dbg_account(1),
        &key_pair,
        Address::Account(dbg_account(1)),
        Amount::from(10),
        &state,
    );
    assert!(state
        .handle_confirmation_order(ConfirmationOrder::new(certificate))
        .await
        .is_ok());
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
    let mut state =
        init_state_with_accounts(vec![(dbg_account(2), dbg_addr(2), Balance::from(1))]).await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(10),
        &state,
    );
    let operation = certificate
        .value
        .confirm_request()
        .unwrap()
        .operation
        .clone();
    assert!(state
        .update_recipient_account(operation, certificate)
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
async fn test_handle_confirmation_order_to_active_recipient_in_the_same_shard() {
    let sender_key_pair = KeyPair::generate();
    let mut state = init_state_with_accounts(vec![
        (dbg_account(1), sender_key_pair.public(), Balance::from(5)),
        (dbg_account(2), dbg_addr(2), Balance::from(0)),
    ])
    .await;
    let certificate = make_transfer_certificate(
        dbg_account(1),
        &sender_key_pair,
        Address::Account(dbg_account(2)),
        Amount::from(5),
        &state,
    );

    let (info, continuation) = state
        .handle_confirmation_order(ConfirmationOrder::new(certificate.clone()))
        .await
        .unwrap();
    assert!(matches!(continuation, CrossShardContinuation::Done));
    assert_eq!(dbg_account(1), info.account_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert_eq!(None, info.pending);
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
    assert_eq!(recipient_account.owner, Some(dbg_addr(2)));
    assert_eq!(recipient_account.confirmed_log.len(), 0);
    assert_eq!(recipient_account.received_log.len(), 1);

    let info_query = AccountInfoQuery {
        account_id: dbg_account(2),
        query_sequence_number: None,
        query_received_certificates_excluding_first_nth: Some(0),
    };
    let response = state.handle_account_info_query(info_query).await.unwrap();
    assert_eq!(response.queried_received_certificates.len(), 1);
    assert_eq!(
        response.queried_received_certificates[0]
            .value
            .confirm_request()
            .unwrap()
            .amount()
            .unwrap(),
        Amount::from(5)
    );
}

#[tokio::test]
async fn test_handle_confirmation_order_to_inactive_recipient_in_the_same_shard() {
    let sender_key_pair = KeyPair::generate();
    let mut state = init_state_with_accounts(vec![(
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
        &state,
    );

    let (info, continuation) = state
        .handle_confirmation_order(ConfirmationOrder::new(certificate.clone()))
        .await
        .unwrap();
    assert!(matches!(continuation, CrossShardContinuation::Done));
    assert_eq!(dbg_account(1), info.account_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(SequenceNumber::from(1), info.next_sequence_number);
    assert_eq!(None, info.pending);
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
    let mut state = init_state_with_account(sender.clone(), dbg_addr(1), Balance::from(5)).await;
    state.storage.read_active_account(&sender).await.unwrap();
}

#[tokio::test]
async fn test_read_account_state_unknown_account() {
    let sender = dbg_account(1);
    let unknown_account_id = dbg_account(99);
    let mut state = init_state_with_account(sender, dbg_addr(1), Balance::from(5)).await;
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
    account.owner = Some(dbg_addr(4));
    state.storage.write_account(account).await.unwrap();
    assert!(state
        .storage
        .read_active_account(&unknown_account_id)
        .await
        .is_ok());
}

#[test]
fn test_get_shards() {
    let num_shards = 16u32;
    let mut found = vec![false; num_shards as usize];
    let mut left = num_shards;
    let mut i = 1;
    loop {
        let shard = get_shard(num_shards, &dbg_account(i)) as usize;
        println!("found {}", shard);
        if !found[shard] {
            found[shard] = true;
            left -= 1;
            if left == 0 {
                break;
            }
        }
        i += 1;
    }
}

// helpers

fn init_state() -> WorkerState<InMemoryStoreClient> {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let mut authorities = BTreeMap::new();
    authorities.insert(name, /* voting right */ 1);
    let committee = Committee::new(authorities);
    let client = InMemoryStoreClient::default();
    WorkerState::new(
        committee, name, key_pair, /* shard_id */ 0, /* number of shards */ 1, client,
    )
}

async fn init_state_with_accounts<I: IntoIterator<Item = (AccountId, AccountOwner, Balance)>>(
    balances: I,
) -> WorkerState<InMemoryStoreClient> {
    let mut state = init_state();
    for (id, owner, balance) in balances {
        let account = AccountState::create(id, owner, balance);
        state.storage.write_account(account).await.unwrap();
    }
    state
}

async fn init_state_with_account(
    id: AccountId,
    owner: AccountOwner,
    balance: Balance,
) -> WorkerState<InMemoryStoreClient> {
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
    };
    RequestOrder::new(request.into(), secret)
}

fn make_certificate(state: &WorkerState<InMemoryStoreClient>, value: Value) -> Certificate {
    let vote = Vote::new(value.clone(), &state.key_pair);
    let mut builder = SignatureAggregator::new(value, &state.committee);
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
    state: &WorkerState<InMemoryStoreClient>,
) -> Certificate {
    let request = make_transfer_request_order(account_id, key_pair, recipient, amount)
        .value
        .request;
    let value = Value::Confirm(request);
    make_certificate(state, value)
}
