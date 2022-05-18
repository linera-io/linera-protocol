// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use std::collections::BTreeMap;
use zef_base::{
    base_types::*,
    chain::{ChainState, ChainStatus, Event, ExecutionState},
    committee::Committee,
    error::Error,
    manager::ChainManager,
    messages::*,
};
use zef_storage::{InMemoryStoreClient, Storage};

/// Instantiate the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker(allow_inactive_chains: bool) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let key_pair = KeyPair::generate();
    let mut validators = BTreeMap::new();
    validators.insert(key_pair.public(), /* voting right */ 1);
    let committee = Committee::new(validators);
    let client = InMemoryStoreClient::default();
    let worker = WorkerState::new(Some(key_pair), client, allow_inactive_chains);
    (committee, worker)
}

/// Same as `init_worker` but also instantiate some initial chains.
async fn init_worker_with_chains<I: IntoIterator<Item = (ChainDescription, Owner, Balance)>>(
    balances: I,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ false);
    for (description, owner, balance) in balances {
        let chain = ChainState::create(
            committee.clone(),
            ChainId::root(0),
            description,
            owner,
            balance,
        );
        worker.storage.write_chain(chain).await.unwrap();
    }
    (committee, worker)
}

/// Same as `init_worker` but also instantiate a single initial chain.
async fn init_worker_with_chain(
    description: ChainDescription,
    owner: Owner,
    balance: Balance,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    init_worker_with_chains(std::iter::once((description, owner, balance))).await
}

fn make_block(
    chain_id: ChainId,
    operations: Vec<Operation>,
    incoming_messages: Vec<MessageGroup>,
    previous_confirmed_block: Option<&Certificate>,
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
        chain_id,
        incoming_messages,
        operations,
        previous_block_hash,
        height,
    }
}

fn make_transfer_block_proposal(
    chain_id: ChainId,
    key_pair: &KeyPair,
    recipient: Address,
    amount: Amount,
    incoming_messages: Vec<MessageGroup>,
    previous_confirmed_block: Option<&Certificate>,
) -> BlockProposal {
    let block = make_block(
        chain_id,
        vec![Operation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        }],
        incoming_messages,
        previous_confirmed_block,
    );
    BlockProposal::new(
        BlockAndRound {
            block,
            round: RoundNumber::default(),
        },
        key_pair,
    )
}

fn make_certificate(
    committee: &Committee,
    worker: &WorkerState<InMemoryStoreClient>,
    value: Value,
) -> Certificate {
    let vote = Vote::new(value.clone(), worker.key_pair.as_ref().unwrap());
    let mut builder = SignatureAggregator::new(value, committee);
    builder
        .append(vote.validator, vote.signature)
        .unwrap()
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn make_transfer_certificate(
    chain_id: ChainId,
    key_pair: &KeyPair,
    recipient: Address,
    amount: Amount,
    incoming_messages: Vec<MessageGroup>,
    committee: &Committee,
    balance: Balance,
    worker: &WorkerState<InMemoryStoreClient>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    let state = ExecutionState {
        status: Some(ChainStatus::ManagedBy {
            admin_id: ChainId::root(0),
        }),
        committee: Some(committee.clone()),
        manager: ChainManager::single(key_pair.public()),
        balance,
    };
    let block = make_block(
        chain_id,
        vec![Operation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        }],
        incoming_messages,
        previous_confirmed_block,
    );
    let state_hash = HashValue::new(&state);
    let value = Value::Confirmed { block, state_hash };
    make_certificate(committee, worker, value)
}

#[tokio::test]
async fn test_read_chain_state() {
    let sender = ChainDescription::Root(1);
    let (_, mut worker) =
        init_worker_with_chain(sender, PublicKey::debug(1), Balance::from(5)).await;
    worker
        .storage
        .read_active_chain(sender.into())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_read_chain_state_unknown_chain() {
    let sender = ChainDescription::Root(1);
    let unknown_chain_id = ChainId::root(99);
    let (committee, mut worker) =
        init_worker_with_chain(sender, PublicKey::debug(1), Balance::from(5)).await;
    assert!(worker
        .storage
        .read_active_chain(unknown_chain_id)
        .await
        .is_err());
    let mut chain = worker
        .storage
        .read_chain_or_default(unknown_chain_id)
        .await
        .unwrap();
    chain.description = Some(ChainDescription::Root(99));
    chain.state.committee = Some(committee);
    chain.state.status = Some(ChainStatus::Managing {
        subscribers: Vec::new(),
    });
    chain.state.manager = ChainManager::single(PublicKey::debug(4));
    worker.storage.write_chain(chain).await.unwrap();
    worker
        .storage
        .read_active_chain(unknown_chain_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_handle_block_proposal_bad_signature() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![
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
    ])
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
        .read_active_chain(ChainId::root(1))
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
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![
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
    ])
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
        .read_active_chain(ChainId::root(1))
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
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![
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
    ])
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
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());
}

#[tokio::test]
async fn test_handle_block_proposal_with_chaining() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(ChainId::root(2));
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
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
        ChainId::root(1),
        &sender_key_pair,
        recipient,
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(4),
        &worker,
        None,
    );
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
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_none());

    worker
        .handle_block_proposal(block_proposal0.clone())
        .await
        .unwrap();
    assert!(worker
        .storage
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_some());
    worker.handle_certificate(certificate0).await.unwrap();
    worker.handle_block_proposal(block_proposal1).await.unwrap();
    assert!(worker
        .storage
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .state
        .manager
        .pending()
        .is_some());
    assert!(worker.handle_block_proposal(block_proposal0).await.is_err());
}

#[tokio::test]
async fn test_handle_block_proposal_with_incoming_messages() {
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let recipient = Address::Account(ChainId::root(2));
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
    .await;
    let block0 = Block {
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![
            Operation::Transfer {
                recipient,
                amount: Amount::from(1),
                user_data: UserData::default(),
            },
            Operation::Transfer {
                recipient,
                amount: Amount::from(2),
                user_data: UserData::default(),
            },
        ],
        previous_block_hash: None,
        height: BlockHeight::from(0),
    };
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::Confirmed {
            block: block0,
            state_hash: HashValue::new(&ExecutionState {
                status: Some(ChainStatus::ManagedBy {
                    admin_id: ChainId::root(0),
                }),
                committee: Some(committee.clone()),
                manager: ChainManager::single(sender_key_pair.public()),
                balance: Balance::from(3),
            }),
        },
    );
    let block1 = Block {
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::Transfer {
            recipient,
            amount: Amount::from(3),
            user_data: UserData::default(),
        }],
        previous_block_hash: Some(certificate0.hash),
        height: BlockHeight::from(1),
    };
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::Confirmed {
            block: block1,
            state_hash: HashValue::new(&ExecutionState {
                status: Some(ChainStatus::ManagedBy {
                    admin_id: ChainId::root(0),
                }),
                committee: Some(committee.clone()),
                manager: ChainManager::single(sender_key_pair.public()),
                balance: Balance::from(0),
            }),
        },
    );
    // Missing earlier blocks
    assert!(worker
        .handle_certificate(certificate1.clone())
        .await
        .is_err());

    worker.fully_handle_certificate(certificate0).await.unwrap();
    worker.fully_handle_certificate(certificate1).await.unwrap();
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
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
            Address::Account(ChainId::root(3)),
            Amount::from(5),
            vec![
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(0),
                    operations: vec![
                        (
                            0,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(1),
                                user_data: UserData::default(),
                            },
                        ),
                        (
                            1,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(2),
                                user_data: UserData::default(),
                            },
                        ),
                    ],
                },
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(1),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(2), // wrong
                            user_data: UserData::default(),
                        },
                    )],
                },
            ],
            None,
        );
        // Inconsistent received messages.
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(Error::InvalidMessageContent { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(6),
            vec![
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(0),
                    operations: vec![
                        (
                            1,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(2),
                                user_data: UserData::default(),
                            },
                        ),
                        (
                            0,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(1),
                                user_data: UserData::default(),
                            },
                        ),
                    ],
                },
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(1),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(3),
                            user_data: UserData::default(),
                        },
                    )],
                },
            ],
            None,
        );
        // Inconsistent order in received messages (indices).
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(Error::InvalidMessageOrder { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(6),
            vec![
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(1),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(3),
                            user_data: UserData::default(),
                        },
                    )],
                },
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(0),
                    operations: vec![
                        (
                            0,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(1),
                                user_data: UserData::default(),
                            },
                        ),
                        (
                            1,
                            Operation::Transfer {
                                recipient: Address::Account(ChainId::root(2)),
                                amount: Amount::from(2),
                                user_data: UserData::default(),
                            },
                        ),
                    ],
                },
            ],
            None,
        );
        // Inconsistent order in received messages (heights).
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(Error::InvalidMessageOrder { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(4),
            vec![
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(0),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(1),
                            user_data: UserData::default(),
                        },
                    )], // missing message
                },
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(1),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(3),
                            user_data: UserData::default(),
                        },
                    )],
                },
            ],
            None,
        );
        // Cannot skip messages.
        assert!(matches!(
            worker.handle_block_proposal(block_proposal).await,
            Err(Error::InvalidMessageOrder { .. })
        ));
    }
    {
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(1),
            vec![MessageGroup {
                sender_id: ChainId::root(1),
                height: BlockHeight::from(0),
                operations: vec![(
                    0,
                    Operation::Transfer {
                        recipient: Address::Account(ChainId::root(2)),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    },
                )],
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
            Value::Confirmed {
                block: block_proposal.content.block,
                state_hash: HashValue::new(&ExecutionState {
                    status: Some(ChainStatus::ManagedBy {
                        admin_id: ChainId::root(0),
                    }),
                    committee: Some(committee.clone()),
                    manager: ChainManager::single(recipient_key_pair.public()),
                    balance: Balance::from(0),
                }),
            },
        );
        worker
            .handle_certificate(certificate.clone())
            .await
            .unwrap();
        // Then receive the last two messages.
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(5),
            vec![
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(0),
                    operations: vec![(
                        1,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(2),
                            user_data: UserData::default(),
                        },
                    )],
                },
                MessageGroup {
                    sender_id: ChainId::root(1),
                    height: BlockHeight::from(1),
                    operations: vec![(
                        0,
                        Operation::Transfer {
                            recipient: Address::Account(ChainId::root(2)),
                            amount: Amount::from(3),
                            user_data: UserData::default(),
                        },
                    )],
                },
            ],
            Some(&certificate),
        );
        worker
            .handle_block_proposal(block_proposal.clone())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_handle_block_proposal_exceed_balance() {
    let sender_key_pair = KeyPair::generate();
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![
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
    ])
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
        .read_active_chain(ChainId::root(1))
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
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
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
        .check(worker.key_pair.unwrap().public())
        .unwrap();
    let pending = worker
        .storage
        .read_active_chain(ChainId::root(1))
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
    let recipient = Address::Account(ChainId::root(2));
    let (_, mut worker) = init_worker_with_chains(vec![
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
    ])
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
        .check(worker.key_pair.as_ref().unwrap().public())
        .as_ref()
        .unwrap();
    let replay_response = worker.handle_block_proposal(block_proposal).await.unwrap();
    // Workaround lack of equality.
    assert_eq!(
        HashValue::new(&response.info),
        HashValue::new(&replay_response.info)
    );
}

#[tokio::test]
async fn test_handle_certificate_unknown_sender() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(2),
        PublicKey::debug(2),
        Balance::from(0),
    )])
    .await;
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    assert!(worker.fully_handle_certificate(certificate).await.is_err());
}

#[tokio::test]
async fn test_handle_certificate_bad_block_height() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
    .await;
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    // Replays are ignored.
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    worker.fully_handle_certificate(certificate).await.unwrap();
}

#[tokio::test]
async fn test_handle_certificate_with_early_incoming_message() {
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
    .await;

    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(1000),
        vec![MessageGroup {
            sender_id: ChainId::root(3),
            height: BlockHeight::from(0),
            operations: vec![(
                0,
                Operation::Transfer {
                    recipient: Address::Account(ChainId::root(1)),
                    amount: Amount::from(995),
                    user_data: UserData::default(),
                },
            )],
        }],
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let chain = worker
        .storage
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.state.balance);
    assert_eq!(BlockHeight::from(1), chain.next_block_height);
    assert_eq!(
        BlockHeight::from(0),
        chain
            .inboxes
            .get(&ChainId::root(3))
            .unwrap()
            .next_height_to_receive
    );
    assert!(chain
        .inboxes
        .get(&ChainId::root(3))
        .unwrap()
        .received_events
        .is_empty(),);
    assert!(matches!(
        chain.inboxes.get(&ChainId::root(3)).unwrap().expected_events.front().unwrap(),
        Event { height, index: 0, operation: Operation::Transfer { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(995),
    ));
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
    worker
        .storage
        .read_active_chain(ChainId::root(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_handle_certificate_receiver_balance_overflow() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
    .await;

    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let new_sender_chain = worker
        .storage
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), new_sender_chain.state.balance);
    assert_eq!(BlockHeight::from(1), new_sender_chain.next_block_height);
    assert_eq!(new_sender_chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), new_sender_chain.block_hash);
    let new_recipient_chain = worker
        .storage
        .read_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(Balance::max(), new_recipient_chain.state.balance);
}

#[tokio::test]
async fn test_handle_certificate_receiver_equal_sender() {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut worker) =
        init_worker_with_chain(ChainDescription::Root(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &key_pair,
        Address::Account(ChainId::root(1)),
        Amount::from(1),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();
    let chain = worker
        .storage
        .read_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.state.balance);
    assert_eq!(
        BlockHeight::from(1),
        chain
            .inboxes
            .get(&ChainId::root(1))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&ChainId::root(1)).unwrap().received_events.front().unwrap(),
        Event { height, index: 0, operation: Operation::Transfer { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(1),
    ));
    assert_eq!(BlockHeight::from(1), chain.next_block_height);
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
}

#[tokio::test]
async fn test_handle_cross_chain_request() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(2),
        PublicKey::debug(2),
        Balance::from(1),
    )])
    .await;
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            sender: ChainId::root(1),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap();
    let chain = worker
        .storage
        .read_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(Balance::from(1), chain.state.balance);
    assert_eq!(BlockHeight::from(0), chain.next_block_height);
    assert_eq!(
        BlockHeight::from(1),
        chain
            .inboxes
            .get(&ChainId::root(1))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&ChainId::root(1)).unwrap().received_events.front().unwrap(),
        Event { height, index: 0, operation: Operation::Transfer { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(10),
    ));
    assert_eq!(chain.confirmed_log.len(), 0);
    assert_eq!(None, chain.block_hash);
    assert_eq!(chain.received_log.len(), 1);
}

#[tokio::test]
async fn test_handle_cross_chain_request_no_recipient_chain() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ false);
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    assert!(worker
        .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
            sender: ChainId::root(1),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap()
        .is_empty());
    let chain = worker
        .storage
        .read_chain_or_default(ChainId::root(2))
        .await
        .unwrap();
    // The target chain did not receive the message
    assert!(chain.inboxes.is_empty());
}

#[tokio::test]
async fn test_handle_cross_chain_request_no_recipient_chain_with_inactive_chains_allowed() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ true);
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(10),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );
    // An inactive target chain is created and it acknowledges the message.
    assert!(matches!(
        worker
            .handle_cross_chain_request(CrossChainRequest::UpdateRecipient {
                sender: ChainId::root(1),
                recipient: ChainId::root(2),
                certificates: vec![certificate],
            })
            .await
            .unwrap()
            .as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    ));
    let chain = worker
        .storage
        .read_chain_or_default(ChainId::root(2))
        .await
        .unwrap();
    assert!(!chain.inboxes.is_empty());
}

#[tokio::test]
async fn test_handle_certificate_to_active_recipient() {
    let sender_key_pair = KeyPair::generate();
    let recipient_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
    .await;
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );

    let info = worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_transfer_certificate(
        ChainId::root(2),
        &recipient_key_pair,
        Address::Account(ChainId::root(3)),
        Amount::from(1),
        vec![MessageGroup {
            sender_id: ChainId::root(1),
            height: BlockHeight::from(0),
            operations: vec![(
                0,
                Operation::Transfer {
                    recipient: Address::Account(ChainId::root(2)),
                    amount: Amount::from(5),
                    user_data: UserData::default(),
                },
            )],
        }],
        &committee,
        Balance::from(4),
        &worker,
        None,
    );
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();

    let recipient_chain = worker
        .storage
        .read_active_chain(ChainId::root(2))
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
        chain_id: ChainId::root(2),
        check_next_block_height: None,
        query_committee: false,
        query_pending_messages: false,
        query_sent_certificates_in_range: None,
        query_received_certificates_excluding_first_nth: Some(0),
    };
    let response = worker.handle_chain_info_query(info_query).await.unwrap();
    assert_eq!(response.info.queried_received_certificates.len(), 1);
    assert!(matches!(response.info.queried_received_certificates[0]
            .value
            .confirmed_block()
            .unwrap()
            .operations[..], [Operation::Transfer { amount, .. }] if amount == Amount::from(5)));
}

#[tokio::test]
async fn test_handle_certificate_to_inactive_recipient() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let certificate = make_transfer_certificate(
        ChainId::root(1),
        &sender_key_pair,
        Address::Account(ChainId::root(2)), // the recipient chain does not exist
        Amount::from(5),
        Vec::new(),
        &committee,
        Balance::from(0),
        &worker,
        None,
    );

    let info = worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(1), info.chain_id);
    assert_eq!(Balance::from(0), info.balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());
}

#[tokio::test]
async fn test_chain_creation() {
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(0),
        key_pair.public(),
        Balance::from(0),
    )])
    .await;
    let root_id = ChainId::root(0);
    let child_id = ChainId::child(OperationId {
        chain_id: root_id,
        height: BlockHeight::from(0),
        index: 0,
    });
    let block = Block {
        chain_id: root_id,
        incoming_messages: Vec::new(),
        operations: vec![Operation::OpenChain {
            id: child_id,
            owner: key_pair.public(),
            committee: committee.clone(),
            admin_id: root_id,
        }],
        previous_block_hash: None,
        height: BlockHeight::from(0),
    };
    let certificate = make_certificate(
        &committee,
        &worker,
        Value::Confirmed {
            block,
            state_hash: HashValue::new(&ExecutionState {
                status: Some(ChainStatus::Managing {
                    subscribers: Vec::new(),
                }),
                committee: Some(committee.clone()),
                manager: ChainManager::single(key_pair.public()),
                balance: Balance::from(0),
            }),
        },
    );
    worker
        .fully_handle_certificate(certificate.clone())
        .await
        .unwrap();

    let root_chain = worker.storage.read_active_chain(root_id).await.unwrap();
    assert_eq!(BlockHeight::from(1), root_chain.next_block_height);
    assert!(!root_chain.outboxes.contains_key(&child_id));
    assert!(
        matches!(root_chain.state.status, Some(ChainStatus::Managing { subscribers }) if subscribers.is_empty())
    );

    let child_chain = worker.storage.read_active_chain(child_id).await.unwrap();
    assert_eq!(BlockHeight::from(0), child_chain.next_block_height);
    assert!(
        matches!(child_chain.state.status, Some(ChainStatus::ManagedBy { admin_id }) if admin_id == root_id)
    );
    assert!(!child_chain
        .inboxes
        .get(&root_id)
        .unwrap()
        .received_events
        .is_empty());

    // Make the root receive the subscription.
    let block = Block {
        chain_id: root_id,
        incoming_messages: vec![MessageGroup {
            sender_id: ChainId::root(1),
            height: BlockHeight::from(0),
            operations: vec![(
                0,
                Operation::OpenChain {
                    id: child_id,
                    owner: key_pair.public(),
                    committee: committee.clone(),
                    admin_id: root_id,
                },
            )],
        }],
        operations: Vec::new(),
        previous_block_hash: Some(certificate.hash),
        height: BlockHeight::from(1),
    };
    let certificate = make_certificate(
        &committee,
        &worker,
        Value::Confirmed {
            block,
            state_hash: HashValue::new(&ExecutionState {
                status: Some(ChainStatus::Managing {
                    subscribers: vec![child_id], // was just added
                }),
                committee: Some(committee.clone()),
                manager: ChainManager::single(key_pair.public()),
                balance: Balance::from(0),
            }),
        },
    );
    worker.fully_handle_certificate(certificate).await.unwrap();
}
