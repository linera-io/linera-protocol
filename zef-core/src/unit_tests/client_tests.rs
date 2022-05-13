// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    client::{ChainClient, ChainClientState, CommunicateAction},
    node::ValidatorNode,
    worker::{ValidatorWorker, WorkerState},
};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use zef_base::{base_types::*, chain::ChainState, committee::Committee, error::Error, messages::*};
use zef_storage::{InMemoryStoreClient, Storage};

/// An validator used for testing. "Faulty" validators ignore block proposals (but not
/// certificates or info queries) and have the wrong initial balance for all chains.
struct LocalValidator {
    is_faulty: bool,
    state: WorkerState<InMemoryStoreClient>,
}

#[derive(Clone)]
struct LocalValidatorClient(Arc<Mutex<LocalValidator>>);

#[async_trait]
impl ValidatorNode for LocalValidatorClient {
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error> {
        let validator = self.0.clone();
        let mut validator = validator.lock().await;
        if validator.is_faulty {
            Err(Error::SequenceOverflow)
        } else {
            validator.state.handle_block_proposal(proposal).await
        }
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        let validator = self.0.clone();
        let mut validator = validator.lock().await;
        validator.state.fully_handle_certificate(certificate).await
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .state
            .handle_chain_info_query(query)
            .await
    }
}

impl LocalValidatorClient {
    fn new(is_faulty: bool, state: WorkerState<InMemoryStoreClient>) -> Self {
        let validator = LocalValidator { is_faulty, state };
        Self(Arc::new(Mutex::new(validator)))
    }
}

// NOTE:
// * To communicate with a quorum of validators, chain clients iterate over a copy of
// `validator_clients` to spawn I/O tasks.
// * When using `LocalValidatorClient`, clients communicate with an exact quorum then stops.
// * Most tests have 1 faulty validator out 4 so that there is exactly only 1 quorum to
// communicate with.
struct TestBuilder {
    committee: Committee,
    genesis_store: InMemoryStoreClient,
    faulty_validators: HashSet<ValidatorName>,
    validator_clients: Vec<(ValidatorName, LocalValidatorClient)>,
    validator_stores: HashMap<ValidatorName, InMemoryStoreClient>,
    chain_client_stores: Vec<InMemoryStoreClient>,
}

impl TestBuilder {
    fn new(count: usize, with_faulty_validators: usize) -> Self {
        let mut key_pairs = Vec::new();
        let mut voting_rights = BTreeMap::new();
        for _ in 0..count {
            let key_pair = KeyPair::generate();
            voting_rights.insert(key_pair.public(), 1);
            key_pairs.push(key_pair);
        }
        let committee = Committee::new(voting_rights);
        let mut validator_clients = Vec::new();
        let mut validator_stores = HashMap::new();
        let mut faulty_validators = HashSet::new();
        for (i, key_pair) in key_pairs.into_iter().enumerate() {
            let name = key_pair.public();
            let store = InMemoryStoreClient::default();
            let state = WorkerState::new(
                Some(key_pair),
                store.clone(),
                /* allow_inactive_chains */ false,
            );
            let validator = if i < with_faulty_validators {
                faulty_validators.insert(name);
                LocalValidatorClient::new(true, state)
            } else {
                LocalValidatorClient::new(false, state)
            };
            validator_clients.push((name, validator));
            validator_stores.insert(name, store);
        }
        eprintln!("faulty validators: {:?}", faulty_validators);
        Self {
            committee,
            genesis_store: InMemoryStoreClient::default(),
            faulty_validators,
            validator_clients,
            validator_stores,
            chain_client_stores: Vec::new(),
        }
    }

    async fn add_initial_chain(
        &mut self,
        description: ChainDescription,
        balance: Balance,
    ) -> ChainClientState<LocalValidatorClient, InMemoryStoreClient> {
        let key_pair = KeyPair::generate();
        let owner = key_pair.public();
        let chain = ChainState::create(self.committee.clone(), description, owner, balance);
        let chain_bad =
            ChainState::create(self.committee.clone(), description, owner, Balance::from(0));
        // Create genesis chain in all the existing stores.
        self.genesis_store.write_chain(chain.clone()).await.unwrap();
        for (name, store) in self.validator_stores.iter_mut() {
            if self.faulty_validators.contains(name) {
                store.write_chain(chain_bad.clone()).await.unwrap();
            } else {
                store.write_chain(chain.clone()).await.unwrap();
            }
        }
        for store in self.chain_client_stores.iter_mut() {
            store.write_chain(chain.clone()).await.unwrap();
        }
        self.make_client(description.into(), key_pair, None, BlockHeight::from(0))
            .await
    }

    async fn make_client(
        &mut self,
        chain_id: ChainId,
        key_pair: KeyPair,
        block_hash: Option<HashValue>,
        block_height: BlockHeight,
    ) -> ChainClientState<LocalValidatorClient, InMemoryStoreClient> {
        // Note that new clients are only given the genesis store: they must figure out
        // the rest by asking validators.
        let store = self.genesis_store.copy().await;
        self.chain_client_stores.push(store.clone());
        ChainClientState::new(
            chain_id,
            vec![key_pair],
            self.validator_clients.clone(),
            store,
            block_hash,
            block_height,
            std::time::Duration::from_millis(500),
            10,
        )
    }

    async fn single_chain(
        count: usize,
        with_faulty_validators: usize,
        balance: Balance,
    ) -> ChainClientState<LocalValidatorClient, InMemoryStoreClient> {
        let mut builder = TestBuilder::new(count, with_faulty_validators);
        builder
            .add_initial_chain(ChainDescription::Root(1), balance)
            .await
    }

    /// Try to find a (confirmation) certificate for the given chain_id and block height.
    async fn check_that_validators_have_certificate(
        &self,
        chain_id: ChainId,
        block_height: BlockHeight,
        target_count: usize,
    ) -> Option<Certificate> {
        let query = ChainInfoQuery {
            chain_id,
            check_next_block_height: None,
            query_committee: false,
            query_pending_messages: false,
            query_sent_certificates_in_range: Some(BlockHeightRange {
                start: block_height,
                limit: Some(1),
            }),
            query_received_certificates_excluding_first_nth: None,
        };
        let mut count = 0;
        let mut certificate = None;
        for (name, mut client) in self.validator_clients.clone() {
            if let Ok(response) = client.handle_chain_info_query(query.clone()).await {
                if response.check(name).is_ok() {
                    let ChainInfo {
                        mut queried_sent_certificates,
                        ..
                    } = response.info;
                    if let Some(cert) = queried_sent_certificates.pop() {
                        if let Value::Confirmed { block } = &cert.value {
                            if block.chain_id == chain_id && block.height == block_height {
                                cert.check(&self.committee).unwrap();
                                count += 1;
                                certificate = Some(cert);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(count, target_count);
        certificate
    }
}

#[tokio::test]
async fn test_initiating_valid_transfer() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let certificate = sender
        .transfer_to_chain(
            Amount::from(3),
            ChainId::root(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
}

#[tokio::test]
async fn test_rotate_key_pair() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.rotate_key_pair(new_key_pair).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert_eq!(sender.identity().await.unwrap(), new_pubk);
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the chain.
    sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_transfer_ownership() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;

    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.transfer_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Cannot use the chain any more.
    assert!(sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .is_err());
}

#[tokio::test]
async fn test_share_ownership() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let certificate = sender.share_ownership(new_pubk).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
    assert_eq!(
        sender.synchronize_balance().await.unwrap(),
        Balance::from(4)
    );
    // Can still use the chain with the old client.
    sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    // Make a client to try the new key.
    let mut client = builder
        .make_client(
            sender.chain_id,
            new_key_pair,
            sender.block_hash,
            BlockHeight::from(2),
        )
        .await;
    // Local balance fails because the client has block height 2 but we haven't downloaded
    // the blocks yet.
    assert!(client.local_balance().await.is_err());
    assert_eq!(
        client.synchronize_balance().await.unwrap(),
        Balance::from(1)
    );
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(1));
    client
        .transfer_to_chain(Amount::from(1), ChainId::root(3), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_open_chain_then_close_it() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let new_id = ChainId::child(OperationId {
        chain_id: ChainId::root(1),
        height: BlockHeight::from(0),
        index: 0,
    });
    // Open the new chain.
    let certificate = sender.open_chain(new_pubk).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(
        client.synchronize_balance().await.unwrap(),
        Balance::from(0)
    );
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
    client.close_chain().await.unwrap();
}

#[tokio::test]
async fn test_transfer_then_open_chain() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let new_id = ChainId::child(OperationId {
        chain_id: ChainId::root(1),
        height: BlockHeight::from(1),
        index: 0,
    });
    // Transfer before creating the chain.
    sender
        .transfer_to_chain(Amount::from(3), new_id, UserData::default())
        .await
        .unwrap();
    // Open the new chain.
    let certificate = sender.open_chain(new_pubk).await.unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(1), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    assert!(matches!(&certificate.value, Value::Confirmed{
        block: Block {
            operations,
            ..
        }} if matches!(&operations[..], &[Operation::OpenChain { id, .. }] if new_id == id)
    ));
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await;
    client.receive_certificate(certificate).await.unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(3));
    client
        .transfer_to_chain(Amount::from(3), ChainId::root(3), UserData::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_open_chain_then_transfer() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let new_key_pair = KeyPair::generate();
    let new_pubk = new_key_pair.public();
    let new_id = ChainId::child(OperationId {
        chain_id: ChainId::root(1),
        height: BlockHeight::from(0),
        index: 0,
    });
    // Open the new chain.
    let creation_certificate = sender.open_chain(new_pubk).await.unwrap();
    // Transfer after creating the chain.
    let transfer_certificate = sender
        .transfer_to_chain(Amount::from(3), new_id, UserData::default())
        .await
        .unwrap();
    assert_eq!(sender.next_block_height, BlockHeight::from(2));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_ok());
    // Make a client to try the new chain.
    let mut client = builder
        .make_client(new_id, new_key_pair, None, BlockHeight::from(0))
        .await;
    // Must process the creation certificate before using the new chain.
    client
        .receive_certificate(creation_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
    client
        .receive_certificate(transfer_certificate)
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(3));
    client
        .transfer_to_chain(Amount::from(3), ChainId::root(3), UserData::default())
        .await
        .unwrap();
    assert_eq!(client.local_balance().await.unwrap(), Balance::from(0));
}

#[tokio::test]
async fn test_close_chain() {
    let mut builder = TestBuilder::new(4, 1);
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(4))
        .await;
    let certificate = sender.close_chain().await.unwrap();
    assert!(matches!(
        &certificate.value,
        Value::Confirmed {
            block: Block {
                operations,
                ..
            }
        } if matches!(&operations[..], &[Operation::CloseChain])
    ));
    assert_eq!(sender.next_block_height, BlockHeight::from(1));
    assert!(sender.pending_block.is_none());
    assert!(sender.key_pair().await.is_err());
    assert_eq!(
        builder
            .check_that_validators_have_certificate(sender.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Cannot use the chain any more.
    assert!(sender
        .transfer_to_chain(Amount::from(3), ChainId::root(2), UserData::default())
        .await
        .is_err());
}

#[tokio::test]
async fn test_initiating_valid_transfer_too_many_faults() {
    let mut sender = TestBuilder::single_chain(4, 2, Balance::from(4)).await;
    assert!(sender
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(3),
            ChainId::root(2),
            UserData(Some(*b"hello...........hello...........")),
        )
        .await
        .is_err());
    assert_eq!(sender.next_block_height, BlockHeight::from(0));
    assert!(sender.pending_block.is_some());
    assert_eq!(sender.local_balance().await.unwrap(), Balance::from(4));
}

#[tokio::test]
async fn test_bidirectional_transfer() {
    let mut builder = TestBuilder::new(4, 1);
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await;
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(3));

    let certificate = client1
        .transfer_to_chain(Amount::from(3), client2.chain_id, UserData::default())
        .await
        .unwrap();

    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(0));

    assert_eq!(
        builder
            .check_that_validators_have_certificate(client1.chain_id, BlockHeight::from(0), 3)
            .await
            .unwrap()
            .value,
        certificate.value
    );
    // Local balance is lagging.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Force synchronization of local balance.
    assert_eq!(
        client2.synchronize_balance().await.unwrap(),
        Balance::from(3)
    );
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(3));

    // Send back some money.
    assert_eq!(client2.next_block_height, BlockHeight::from(0));
    client2
        .transfer_to_chain(Amount::from(1), client1.chain_id, UserData::default())
        .await
        .unwrap();
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(2));
    assert_eq!(
        client1.synchronize_balance().await.unwrap(),
        Balance::from(1)
    );
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer() {
    let mut builder = TestBuilder::new(4, 1);
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await;
    let certificate = client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(2),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    // Transfer was executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(client1.next_block_height, BlockHeight::from(1));
    assert!(client1.pending_block.is_none());
    // Let the receiver confirm in last resort.
    client2.receive_certificate(certificate).await.unwrap();
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(2));
}

#[tokio::test]
async fn test_receiving_unconfirmed_transfer_with_lagging_sender_balances() {
    let mut builder = TestBuilder::new(4, 1);
    let mut client1 = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(3))
        .await;
    let mut client2 = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await;
    let mut client3 = builder
        .add_initial_chain(ChainDescription::Root(3), Balance::from(0))
        .await;

    // Transferring funds from client1 to client2.
    // Confirming to a quorum of nodes only at the end.
    client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(1),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(1),
            client2.chain_id,
            UserData::default(),
        )
        .await
        .unwrap();
    client1
        .communicate_chain_updates(
            &builder.committee,
            client1.chain_id,
            CommunicateAction::AdvanceToNextBlockHeight(client1.next_block_height),
        )
        .await
        .unwrap();
    // Client2 does not know about the money yet.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Sending money from client2 fails, as a consequence.
    assert!(client2
        .transfer_to_chain_unsafe_unconfirmed(
            Amount::from(2),
            client3.chain_id,
            UserData::default(),
        )
        .await
        .is_err());
    // Retrying the same block doesn't work.
    assert!(client2.retry_pending_block().await.is_err());
    client2.clear_pending_block().await;
    // Retrying the whole command works after synchronization.
    assert_eq!(
        client2.synchronize_balance().await.unwrap(),
        Balance::from(2)
    );
    let certificate = client2
        .transfer_to_chain(Amount::from(2), client3.chain_id, UserData::default())
        .await
        .unwrap();
    // Blocks were executed locally.
    assert_eq!(client1.local_balance().await.unwrap(), Balance::from(1));
    assert_eq!(client1.next_block_height, BlockHeight::from(2));
    assert!(client1.pending_block.is_none());
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    assert_eq!(client2.next_block_height, BlockHeight::from(1));
    assert!(client2.pending_block.is_none());
    // Last one was not confirmed remotely, hence a conservative balance.
    assert_eq!(client2.local_balance().await.unwrap(), Balance::from(0));
    // Let the receiver confirm in last resort.
    client3.receive_certificate(certificate).await.unwrap();
    assert_eq!(client3.local_balance().await.unwrap(), Balance::from(2));
}
