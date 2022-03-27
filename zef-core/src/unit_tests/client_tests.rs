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

// NOTE:
// * To communicate with a quorum of authorities, account clients iterate over a copy of
// `authority_clients` to spawn I/O tasks.
// * When using `LocalAuthorityClient`, clients communicate with an exact quorum then stops.
// * Most tests have 1 faulty authority out 4 so that there is exactly only 1 quorum to
// communicate with.
struct TestBuilder {
    committee: Committee,
    genesis_store: InMemoryStoreClient,
    faulty_authorities: HashSet<AuthorityName>,
    authority_clients: HashMap<AuthorityName, LocalAuthorityClient>,
    authority_stores: HashMap<AuthorityName, InMemoryStoreClient>,
    account_client_stores: Vec<InMemoryStoreClient>,
}

impl TestBuilder {
    fn new(count: usize, with_faulty_authorities: usize) -> Self {
        let mut key_pairs = Vec::new();
        let mut voting_rights = BTreeMap::new();
        for _ in 0..count {
            let key_pair = KeyPair::generate();
            voting_rights.insert(key_pair.public(), 1);
            key_pairs.push(key_pair);
        }
        let committee = Committee::new(voting_rights);
        let mut authority_clients = HashMap::new();
        let mut authority_stores = HashMap::new();
        let mut faulty_authorities = HashSet::new();
        for (i, key_pair) in key_pairs.into_iter().enumerate() {
            let name = key_pair.public();
            let store = InMemoryStoreClient::default();
            let key_pair = if i < with_faulty_authorities {
                faulty_authorities.insert(name);
                KeyPair::generate()
            } else {
                key_pair
            };
            let state = WorkerState::new(committee.clone(), Some(key_pair), store.clone());
            authority_clients.insert(name, LocalAuthorityClient::new(state));
            authority_stores.insert(name, store);
        }
        Self {
            committee,
            genesis_store: InMemoryStoreClient::default(),
            faulty_authorities,
            authority_clients,
            authority_stores,
            account_client_stores: Vec::new(),
        }
    }

    async fn add_initial_account(
        &mut self,
        account_id: AccountId,
        balance: Balance,
    ) -> AccountClientState<LocalAuthorityClient, InMemoryStoreClient> {
        let key_pair = KeyPair::generate();
        let owner = key_pair.public();
        let account = AccountState::create(account_id.clone(), owner, balance);
        let account_bad = AccountState::create(account_id.clone(), owner, Balance::from(0));
        // Create genesis account in all the existing stores.
        self.genesis_store
            .write_account(account.clone())
            .await
            .unwrap();
        for (name, store) in self.authority_stores.iter_mut() {
            if self.faulty_authorities.contains(name) {
                store.write_account(account_bad.clone()).await.unwrap();
            } else {
                store.write_account(account.clone()).await.unwrap();
            }
        }
        for store in self.account_client_stores.iter_mut() {
            store.write_account(account.clone()).await.unwrap();
        }
        self.make_client(account_id, key_pair, SequenceNumber::from(0))
            .await
    }

    async fn make_client(
        &mut self,
        account_id: AccountId,
        key_pair: KeyPair,
        sequence_number: SequenceNumber,
    ) -> AccountClientState<LocalAuthorityClient, InMemoryStoreClient> {
        // Note that new clients are only given the genesis store: they must figure out
        // the rest by asking authorities.
        let store = self.genesis_store.copy().await;
        self.account_client_stores.push(store.clone());
        AccountClientState::new(
            account_id,
            Some(key_pair),
            self.committee.clone(),
            self.authority_clients.clone(),
            store,
            sequence_number,
        )
    }

    async fn single_account(
        count: usize,
        with_faulty_authorities: usize,
        balance: Balance,
    ) -> AccountClientState<LocalAuthorityClient, InMemoryStoreClient> {
        let mut builder = TestBuilder::new(count, with_faulty_authorities);
        builder.add_initial_account(dbg_account(1), balance).await
    }
}

#[tokio::test]
async fn test_query_balance() {
    let mut client = TestBuilder::single_account(4, 1, Balance::from(3)).await;
    assert_eq!(client.query_safe_balance().await, Balance::from(3));

    let mut client = TestBuilder::single_account(4, 2, Balance::from(3)).await;
    assert_eq!(client.query_safe_balance().await, Balance::from(3));

    let mut client = TestBuilder::single_account(4, 3, Balance::from(3)).await;
    assert_eq!(client.query_safe_balance().await, Balance::from(0));
}

#[tokio::test]
async fn test_initiating_valid_transfer() {
    let mut sender = TestBuilder::single_account(4, 1, Balance::from(4)).await;
    let certificate = sender
        .transfer_to_account(
            Amount::from(3),
            dbg_account(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(sender.pending_request.is_none());
    assert_eq!(sender.query_safe_balance().await, Balance::from(1));
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
    let mut sender = TestBuilder::single_account(4, 1, Balance::from(4)).await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(sender.pending_request.is_none());
    assert_eq!(sender.identity().unwrap(), new_pubk);
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(sender.query_safe_balance().await, Balance::from(4));
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
    let mut sender = TestBuilder::single_account(4, 1, Balance::from(4)).await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.transfer_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(sender.pending_request.is_none());
    assert!(sender.key_pair().is_err());
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(sender.query_safe_balance().await, Balance::from(4));
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
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_account(dbg_account(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.share_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(1));
    assert!(sender.pending_request.is_none());
    assert!(sender.key_pair().is_ok());
    assert_eq!(
        sender
            .query_certificate(sender.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    assert_eq!(sender.query_safe_balance().await, Balance::from(4));
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
    let mut client = builder
        .make_client(sender.account_id, new_key_pair, SequenceNumber::from(2))
        .await;
    assert_eq!(client.query_safe_balance().await, Balance::from(1));
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
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_account(dbg_account(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let new_id = AccountId::new(vec![1, 1].into_iter().map(SequenceNumber::from).collect());
    // Transfer before creating the account.
    assert!(sender
        .transfer_to_account(Amount::from(3), new_id.clone(), UserData::default())
        .await
        .is_ok());
    // Open the new account.
    let certificate = sender.open_account(new_pubk).await.unwrap();
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(2));
    assert!(sender.pending_request.is_none());
    assert!(sender.key_pair().is_ok());
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
    let mut client = builder
        .make_client(new_id, new_key_pair, SequenceNumber::from(0))
        .await;
    assert_eq!(client.query_safe_balance().await, Balance::from(3));
    assert_eq!(
        client.synchronize_balance().await.unwrap(),
        Balance::from(3)
    );
    client
        .transfer_to_account(Amount::from(3), dbg_account(3), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_close_account() {
    let mut sender = TestBuilder::single_account(4, 1, Balance::from(4)).await;
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
    assert!(sender.pending_request.is_none());
    assert!(sender.key_pair().is_err());
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
async fn test_initiating_valid_transfer_too_many_faults() {
    let mut sender = TestBuilder::single_account(4, 2, Balance::from(4)).await;
    assert!(sender
        .transfer_to_account(
            Amount::from(3),
            dbg_account(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .is_err());
    assert_eq!(sender.next_sequence_number, SequenceNumber::from(0));
    assert!(sender.pending_request.is_some());
    assert_eq!(sender.query_safe_balance().await, Balance::from(4));
}

#[tokio::test]
async fn test_bidirectional_transfer() {
    let mut builder = TestBuilder::new(4, 0); // TODO: 4, 1
    let mut client1 = builder
        .add_initial_account(dbg_account(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_account(dbg_account(2), Balance::from(0))
        .await;
    assert_eq!(client1.query_safe_balance().await, Balance::from(3));
    assert_eq!(client1.balance().await, Balance::from(3));

    let certificate = client1
        .transfer_to_account(
            Amount::from(3),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();

    assert_eq!(client1.next_sequence_number, SequenceNumber::from(1));
    assert!(client1.pending_request.is_none());
    assert_eq!(client1.balance().await, Balance::from(0));
    assert_eq!(client1.query_safe_balance().await, Balance::from(0));

    assert_eq!(
        client1
            .query_certificate(client1.account_id.clone(), SequenceNumber::from(0))
            .await
            .unwrap(),
        certificate
    );
    // Our sender already confirmed.
    assert_eq!(client2.query_safe_balance().await, Balance::from(3));
    // But local balance is lagging.
    assert_eq!(client2.balance().await, Balance::from(0));
    // Force synchronization of local balance.
    assert_eq!(
        client2.synchronize_balance().await.unwrap(),
        Balance::from(3)
    );
    assert_eq!(client2.balance().await, Balance::from(3));

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
    assert!(client2.pending_request.is_none());
    assert_eq!(client2.query_safe_balance().await, Balance::from(2));
    assert_eq!(client1.query_safe_balance().await, Balance::from(1));
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer() {
    let mut builder = TestBuilder::new(4, 1);
    let mut client1 = builder
        .add_initial_account(dbg_account(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_account(dbg_account(2), Balance::from(0))
        .await;
    let certificate = client1
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(2),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    // Transfer was executed locally.
    assert_eq!(client1.balance().await, Balance::from(1));
    assert_eq!(client1.next_sequence_number, SequenceNumber::from(1));
    assert!(client1.pending_request.is_none());
    // ..but not confirmed remotely, hence a conservative result.
    assert_eq!(client1.query_safe_balance().await, Balance::from(0));
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(client2.query_safe_balance().await, Balance::from(2));
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer_with_lagging_sender_balances() {
    let mut builder = TestBuilder::new(4, 0); // TODO: 4, 1
    let mut client1 = builder
        .add_initial_account(dbg_account(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_account(dbg_account(2), Balance::from(0))
        .await;
    let mut client3 = builder
        .add_initial_account(dbg_account(3), Balance::from(0))
        .await;

    // Transferring funds from client1 to client2.
    // Confirming to a quorum of nodes only at the end.
    client1
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(1),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(1),
            client2.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .communicate_account_updates(
            client1.account_id.clone(),
            CommunicateAction::SynchronizeNextSequenceNumber(client1.next_sequence_number),
        )
        .await
        .unwrap();
    // Requesting funds from client2 to client3 without confirmation.
    let certificate = client2
        .transfer_to_account_unsafe_unconfirmed(
            Amount::from(2),
            client3.account_id.clone(),
            UserData::default(),
        )
        .await
        .unwrap();
    // Requests were executed locally.
    assert_eq!(client1.balance().await, Balance::from(1));
    assert_eq!(client1.next_sequence_number, SequenceNumber::from(2));
    assert!(client1.pending_request.is_none());
    assert_eq!(client2.balance().await, Balance::from(-2));
    assert_eq!(client2.next_sequence_number, SequenceNumber::from(1));
    assert!(client2.pending_request.is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.query_safe_balance().await, Balance::from(0));
    // Let the receiver confirm in last resort.
    client3.receive_certificate(certificate).await.unwrap();
    assert_eq!(client3.query_safe_balance().await, Balance::from(2));
}
