// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    account::AccountState,
    authority::{fully_handle_certificate, Authority, WorkerState},
    base_types::Amount,
    storage::{InMemoryStoreClient, StorageClient},
};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    ops::DerefMut,
    sync::Arc,
};

#[derive(Clone)]
struct LocalAuthorityClient(Arc<Mutex<WorkerState<InMemoryStoreClient>>>);

#[async_trait]
impl AuthorityClient for LocalAuthorityClient {
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .handle_request_order(order)
            .await
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, Error> {
        let info =
            fully_handle_certificate(self.0.clone().lock().await.deref_mut(), certificate).await?;
        Ok(info)
    }

    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .handle_account_info_query(query)
            .await
    }
}

impl LocalAuthorityClient {
    fn new(state: WorkerState<InMemoryStoreClient>) -> Self {
        Self(Arc::new(Mutex::new(state)))
    }
}

fn init_local_authorities(
    count: usize,
) -> (HashMap<AuthorityName, LocalAuthorityClient>, Committee) {
    let mut key_pairs = Vec::new();
    let mut voting_rights = BTreeMap::new();
    for _ in 0..count {
        let key_pair = KeyPair::generate();
        voting_rights.insert(key_pair.public(), 1);
        key_pairs.push(key_pair);
    }
    let committee = Committee::new(voting_rights);
    let mut clients = HashMap::new();
    for key_pair in key_pairs {
        let name = key_pair.public();
        let state = WorkerState::new(
            committee.clone(),
            name,
            Some(key_pair),
            InMemoryStoreClient::default(),
        );
        clients.insert(name, LocalAuthorityClient::new(state));
    }
    (clients, committee)
}

fn init_local_authorities_bad_1(
    count: usize,
) -> (HashMap<AuthorityName, LocalAuthorityClient>, Committee) {
    let mut key_pairs = Vec::new();
    let mut voting_rights = BTreeMap::new();
    for i in 0..count {
        let key_pair = KeyPair::generate();
        voting_rights.insert(key_pair.public(), 1);
        if i + 1 < (count + 2) / 3 {
            // init 1 authority with a bad keypair
            key_pairs.push(KeyPair::generate());
        } else {
            key_pairs.push(key_pair);
        }
    }
    let committee = Committee::new(voting_rights);

    let mut clients = HashMap::new();
    for key_pair in key_pairs {
        let name = key_pair.public();
        let state = WorkerState::new(
            committee.clone(),
            name,
            Some(key_pair),
            InMemoryStoreClient::default(),
        );
        clients.insert(name, LocalAuthorityClient::new(state));
    }
    (clients, committee)
}

fn make_client(
    account_id: AccountId,
    authority_clients: HashMap<AuthorityName, LocalAuthorityClient>,
    committee: Committee,
) -> AccountClientState<LocalAuthorityClient> {
    let key_pair = KeyPair::generate();
    AccountClientState::new(
        account_id,
        Some(key_pair),
        committee,
        authority_clients,
        SequenceNumber::new(),
        Vec::new(),
        Vec::new(),
        Balance::from(0),
    )
}

async fn fund_account<I: IntoIterator<Item = i128>>(
    clients: &mut HashMap<AuthorityName, LocalAuthorityClient>,
    account_id: AccountId,
    owner: AccountOwner,
    balances: I,
) {
    let mut balances = balances.into_iter().map(Balance::from);
    for (_, client) in clients.iter_mut() {
        client
            .0
            .as_ref()
            .try_lock()
            .unwrap()
            .storage
            .write_account(AccountState::create(
                account_id.clone(),
                owner,
                balances.next().unwrap_or_else(Balance::zero),
            ))
            .await
            .unwrap();
    }
}

async fn init_local_client_state(balances: Vec<i128>) -> AccountClientState<LocalAuthorityClient> {
    let (mut authority_clients, committee) = init_local_authorities(balances.len());
    let zeroes = vec![0; balances.len()];
    let client1 = make_client(dbg_account(1), authority_clients.clone(), committee.clone());
    fund_account(
        &mut authority_clients,
        client1.account_id.clone(),
        client1.owner().unwrap(),
        balances,
    )
    .await;
    let client2 = make_client(dbg_account(2), authority_clients.clone(), committee);
    fund_account(
        &mut authority_clients,
        client2.account_id.clone(),
        client2.owner().unwrap(),
        zeroes,
    )
    .await;
    client1
}

async fn init_local_client_state_with_bad_authority(
    balances: Vec<i128>,
) -> AccountClientState<LocalAuthorityClient> {
    let (mut authority_clients, committee) = init_local_authorities_bad_1(balances.len());
    let zeroes = vec![0; balances.len()];
    let client1 = make_client(dbg_account(1), authority_clients.clone(), committee.clone());
    fund_account(
        &mut authority_clients,
        client1.account_id.clone(),
        client1.owner().unwrap(),
        balances,
    )
    .await;
    let client2 = make_client(dbg_account(2), authority_clients.clone(), committee);
    fund_account(
        &mut authority_clients,
        client2.account_id.clone(),
        client2.owner().unwrap(),
        zeroes,
    )
    .await;
    client1
}

#[tokio::test]
async fn test_get_strong_majority_balance() {
    let mut client = init_local_client_state(vec![3, 4, 4, 4]).await;
    assert_eq!(
        client.query_strong_majority_balance().await,
        Balance::from(4)
    );

    let mut client = init_local_client_state(vec![0, 3, 4, 4]).await;
    assert_eq!(
        client.query_strong_majority_balance().await,
        Balance::from(3)
    );

    let mut client = init_local_client_state(vec![0, 3, 4]).await;
    assert_eq!(
        client.query_strong_majority_balance().await,
        Balance::from(0)
    );
}

#[tokio::test]
async fn test_initiating_valid_transfer() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let certificate = sender
        .transfer_to_account(
            Amount::from(3),
            dbg_account(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(1)
    );
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
}

#[tokio::test]
async fn test_rotate_key_pair() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert_eq!(sender.owner().unwrap(), new_pubk);
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(4)
    );
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the account.
    sender
        .transfer_to_account(Amount::from(3), dbg_account(2), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_transfer_ownership() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.transfer_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert!(sender.key_pair.is_none());
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(4)
    );
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Cannot use the account any more.
    assert!(sender
        .transfer_to_account(Amount::from(3), dbg_account(2), UserData::default())
        .await
        .is_err());
}

#[tokio::test]
async fn test_share_ownership() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.share_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert!(sender.key_pair.is_some());
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(4)
    );
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the account wuth the old.
    assert!(sender
        .transfer_to_account(Amount::from(3), dbg_account(2), UserData::default())
        .await
        .is_ok());
    // Make a client to try the new key.
    let mut client = AccountClientState::new(
        sender.account_id,
        Some(new_key_pair),
        sender.committee.clone(),
        sender.authority_clients,
        SequenceNumber::from(2), // Latest sequence number must be given
        Vec::new(),
        Vec::new(),
        Balance::from(4), // Genesis balance must be correct
    );
    assert_eq!(
        client.query_strong_majority_balance().await,
        Balance::from(1)
    );
    assert_eq!(
        client.synchronize_balance().await.unwrap(),
        Balance::from(1)
    );
    client
        .transfer_to_account(Amount::from(1), dbg_account(3), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_open_account() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let new_id = AccountId::new(vec![SequenceNumber::from(1), SequenceNumber::from(1)]);
    // Transfer before creating the account.
    assert!(sender
        .transfer_to_account(Amount::from(3), new_id.clone(), UserData::default())
        .await
        .is_ok());
    // Open the new account.
    let certificate = sender.open_account(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(2));
    assert!(matches!(sender.pending_request, None));
    assert!(sender.key_pair.is_some());
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(1))
            .await
            .unwrap(),
        certificate
    );
    assert!(matches!(&certificate.value, Value::Confirmed{
        request: Request {
            operation: Operation::OpenAccount { new_id:id, .. },
            ..
        }} if &new_id == id
    ));
    // Make a client to try the new account.
    let mut client = AccountClientState::new(
        new_id,
        Some(new_key_pair),
        sender.committee.clone(),
        sender.authority_clients,
        SequenceNumber::from(0),
        Vec::new(),
        Vec::new(),
        Balance::from(0),
    );
    assert_eq!(
        client.query_strong_majority_balance().await,
        Balance::from(3)
    );
    assert_eq!(
        client.synchronize_balance().await.unwrap(),
        Balance::from(3)
    );
    assert!(client
        .transfer_to_account(Amount::from(3), dbg_account(3), UserData::default())
        .await
        .is_ok());
}

#[tokio::test]
async fn test_close_account() {
    let mut sender = init_local_client_state(vec![2, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let certificate = sender.close_account().await.unwrap();
    assert!(matches!(
        &certificate.value,
        Value::Confirmed {
            request: Request {
                operation: Operation::CloseAccount,
                ..
            }
        }
    ));
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert!(sender.key_pair.is_none());
    // Cannot query the certificate.
    assert!(sender
        .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
        .await
        .is_err());
    // Cannot use the account any more.
    assert!(sender
        .transfer_to_account(Amount::from(3), dbg_account(2), UserData::default())
        .await
        .is_err());
}

#[tokio::test]
async fn test_initiating_valid_transfer_despite_bad_authority() {
    let mut sender = init_local_client_state_with_bad_authority(vec![4, 4, 4, 4]).await;
    sender.balance = Balance::from(4);
    let certificate = sender
        .transfer_to_account(
            Amount::from(3),
            dbg_account(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(sender.pending_request, None));
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(1)
    );
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
}

#[tokio::test]
async fn test_initiating_transfer_low_funds() {
    let mut sender = init_local_client_state(vec![2, 2, 4, 4]).await;
    sender.balance = Balance::from(2);
    assert!(sender
        .transfer_to_account(Amount::from(3), dbg_account(2), UserData::default())
        .await
        .is_err());
    // Trying to overspend does not block an account.
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(0));
    assert!(matches!(sender.pending_request, None));
    assert_eq!(
        sender.query_strong_majority_balance().await,
        Balance::from(2)
    );
}

#[tokio::test]
async fn test_bidirectional_transfer() {
    let (mut authority_clients, committee) = init_local_authorities(4);
    let mut client1 = make_client(dbg_account(1), authority_clients.clone(), committee.clone());
    let mut client2 = make_client(dbg_account(2), authority_clients.clone(), committee);
    fund_account(
        &mut authority_clients,
        client1.account_id.clone(),
        client1.owner().unwrap(),
        vec![2, 3, 4, 4],
    )
    .await;
    fund_account(
        &mut authority_clients,
        client2.account_id.clone(),
        client2.owner().unwrap(),
        vec![0; 4],
    )
    .await;
    // Update client1's local balance accordingly.
    client1.balance = client1.query_strong_majority_balance().await;
    assert_eq!(client1.balance, Balance::from(3));

    let certificate = client1
        .transfer_to_account(
            Amount::from(3),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();

    assert_eq!(client1.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(client1.pending_request, None));
    assert_eq!(
        client1.query_strong_majority_balance().await,
        Balance::from(0)
    );
    assert_eq!(client1.balance, Balance::from(0));
    assert_eq!(
        client1
            .get_strong_majority_sequence_number(client1.account_id.clone())
            .await,
        SequenceNumber::from(1)
    );

    assert_eq!(
        client1
            .query_certificate(client1.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    // Our sender already confirmed.
    assert_eq!(
        client2.query_strong_majority_balance().await,
        Balance::from(3)
    );
    // But local balance is lagging.
    assert_eq!(client2.balance, Balance::from(0));
    // Force synchronization of local balance.
    assert_eq!(
        client2.synchronize_balance().await.unwrap(),
        Balance::from(3)
    );
    assert_eq!(client2.balance, Balance::from(3));

    // Send back some money.
    assert_eq!(client2.next_sequence_number, SequenceNumber::from(0));
    client2
        .transfer_to_account(
            Amount::from(1),
            client1.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    assert_eq!(client2.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(client2.pending_request, None));
    assert_eq!(
        client2.query_strong_majority_balance().await,
        Balance::from(2)
    );
    assert_eq!(
        client2
            .get_strong_majority_sequence_number(client2.account_id.clone())
            .await,
        SequenceNumber::from(1)
    );
    assert_eq!(
        client1.query_strong_majority_balance().await,
        Balance::from(1)
    );
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer() {
    let (mut authority_clients, committee) = init_local_authorities(4);
    let mut client1 = make_client(dbg_account(1), authority_clients.clone(), committee.clone());
    let mut client2 = make_client(dbg_account(2), authority_clients.clone(), committee);
    fund_account(
        &mut authority_clients,
        client1.account_id.clone(),
        client1.owner().unwrap(),
        vec![2, 3, 4, 4],
    )
    .await;
    fund_account(
        &mut authority_clients,
        client2.account_id.clone(),
        client2.owner().unwrap(),
        vec![0; 4],
    )
    .await;

    let certificate = client1
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(2),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    // Transfer was executed locally, creating negative balance.
    assert_eq!(client1.balance, Balance::from(-2));
    assert_eq!(client1.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(client1.pending_request, None));
    // ..but not confirmed remotely, hence an unchanged balance and sequence number.
    assert_eq!(
        client1.query_strong_majority_balance().await,
        Balance::from(3)
    );
    assert_eq!(
        client1
            .get_strong_majority_sequence_number(client1.account_id.clone())
            .await,
        SequenceNumber::from(0)
    );
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client2.query_strong_majority_balance().await,
        Balance::from(2)
    );
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer_with_lagging_sender_balances() {
    let (mut authority_clients, committee) = init_local_authorities(4);
    let mut client0 = make_client(dbg_account(1), authority_clients.clone(), committee.clone());
    let mut client1 = make_client(dbg_account(2), authority_clients.clone(), committee.clone());
    let mut client2 = make_client(dbg_account(3), authority_clients.clone(), committee);
    fund_account(
        &mut authority_clients,
        client0.account_id.clone(),
        client0.owner().unwrap(),
        vec![2, 3, 4, 4],
    )
    .await;
    fund_account(
        &mut authority_clients,
        client1.account_id.clone(),
        client1.owner().unwrap(),
        vec![0; 4],
    )
    .await;
    fund_account(
        &mut authority_clients,
        client2.account_id.clone(),
        client2.owner().unwrap(),
        vec![0; 4],
    )
    .await;

    // transferring funds from client0 to client1.
    // confirming to a quorum of node only at the end.
    client0
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(1),
            client1.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    client0
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(1),
            client1.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    client0
        .communicate_requests(
            client0.account_id.clone(),
            client0.sent_certificates.clone(),
            CommunicateAction::SynchronizeNextSequenceNumber(client0.next_sequence_number),
        )
        .await
        .unwrap();
    // requestring funds from client1 to client2 without confirmation
    let certificate = client1
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(2),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    // Requests were executed locally, possibly creating negative balances.
    assert_eq!(client0.balance, Balance::from(-2));
    assert_eq!(client0.next_sequence_number, SequenceNumber::from(2));
    assert!(matches!(client0.pending_request, None));
    assert_eq!(client1.balance, Balance::from(-2));
    assert_eq!(client1.next_sequence_number, SequenceNumber::from(1));
    assert!(matches!(client1.pending_request, None));
    // Last one was not confirmed remotely, hence an unchanged (remote) balance and sequence number.
    assert_eq!(
        client1.query_strong_majority_balance().await,
        Balance::from(2)
    );
    assert_eq!(
        client1
            .get_strong_majority_sequence_number(client1.account_id.clone())
            .await,
        SequenceNumber::from(0)
    );
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client2.query_strong_majority_balance().await,
        Balance::from(2)
    );
}
