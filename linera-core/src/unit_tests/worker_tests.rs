// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use linera_base::{
    chain::{ChainState, Event},
    committee::Committee,
    crypto::*,
    error::Error,
    execution::{
        Address, Amount, Balance, ChainAdminStatus, Effect, ExecutionState, Operation, UserData,
        ADMIN_CHANNEL,
    },
    manager::ChainManager,
    messages::*,
};
use linera_storage::{InMemoryStoreClient, Storage};
use std::collections::BTreeMap;
use test_log::test;

/// Instantiate the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker(allow_inactive_chains: bool) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let key_pair = KeyPair::generate();
    let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
    let client = InMemoryStoreClient::default();
    let worker = WorkerState::new("Single validator node".to_string(), Some(key_pair), client)
        .allow_inactive_chains(allow_inactive_chains);
    (committee, worker)
}

/// Same as `init_worker` but also instantiate some initial chains.
async fn init_worker_with_chains<I: IntoIterator<Item = (ChainDescription, PublicKey, Balance)>>(
    balances: I,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ false);
    for (description, pubk, balance) in balances {
        let chain = ChainState::create(
            committee.clone(),
            ChainId::root(0),
            description,
            pubk.into(),
            balance,
        );
        worker.storage.write_chain(chain).await.unwrap();
    }
    (committee, worker)
}

/// Same as `init_worker` but also instantiate a single initial chain.
async fn init_worker_with_chain(
    description: ChainDescription,
    owner: PublicKey,
    balance: Balance,
) -> (Committee, WorkerState<InMemoryStoreClient>) {
    init_worker_with_chains([(description, owner, balance)]).await
}

fn make_block(
    epoch: Epoch,
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
        epoch,
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
        Epoch::from(0),
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
        epoch: Some(Epoch::from(0)),
        chain_id,
        admin_status: Some(ChainAdminStatus::ManagedBy {
            admin_id: ChainId::root(0),
        }),
        subscriptions: BTreeMap::new(),
        committees: [(Epoch::from(0), committee.clone())].into_iter().collect(),
        manager: ChainManager::single(key_pair.public().into()),
        balance,
    };
    let block = make_block(
        Epoch::from(0),
        chain_id,
        vec![Operation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        }],
        incoming_messages,
        previous_confirmed_block,
    );
    let effects = match recipient {
        Address::Account(id) => vec![Effect::Credit {
            recipient: id,
            amount,
        }],
        Address::Burn => Vec::new(),
    };
    let state_hash = HashValue::new(&state);
    let value = Value::ConfirmedBlock {
        block,
        effects,
        state_hash,
    };
    make_certificate(committee, worker, value)
}

#[test(tokio::test)]
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

#[test(tokio::test)]
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
    chain.state.committees.insert(Epoch(0), committee);
    chain.state.epoch = Some(Epoch(0));
    chain.state.admin_status = Some(ChainAdminStatus::Managing);
    chain.state.manager = ChainManager::single(PublicKey::debug(4).into());
    worker.storage.write_chain(chain).await.unwrap();
    worker
        .storage
        .read_active_chain(unknown_chain_id)
        .await
        .unwrap();
}

#[test(tokio::test)]
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

#[test(tokio::test)]
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

#[test(tokio::test)]
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

#[test(tokio::test)]
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

#[test(tokio::test)]
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
            },
            effects: vec![
                Effect::Credit {
                    recipient: ChainId::root(2),
                    amount: Amount::from(1),
                },
                Effect::Credit {
                    recipient: ChainId::root(2),
                    amount: Amount::from(2),
                },
            ],
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(epoch),
                chain_id: ChainId::root(1),
                admin_status: Some(ChainAdminStatus::ManagedBy {
                    admin_id: ChainId::root(0),
                }),
                subscriptions: BTreeMap::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                manager: ChainManager::single(sender_key_pair.public().into()),
                balance: Balance::from(3),
            }),
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
                operations: vec![Operation::Transfer {
                    recipient,
                    amount: Amount::from(3),
                    user_data: UserData::default(),
                }],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
            },
            effects: vec![Effect::Credit {
                recipient: ChainId::root(2),
                amount: Amount::from(3),
            }],
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(epoch),
                chain_id: ChainId::root(1),
                admin_status: Some(ChainAdminStatus::ManagedBy {
                    admin_id: ChainId::root(0),
                }),
                subscriptions: BTreeMap::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                manager: ChainManager::single(sender_key_pair.public().into()),
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
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            0,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            },
                        ),
                        (
                            1,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
                            },
                        ),
                    ],
                },
                MessageGroup {
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(2), // wrong
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
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            1,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
                            },
                        ),
                        (
                            0,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            },
                        ),
                    ],
                },
                MessageGroup {
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(3),
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
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(3),
                        },
                    )],
                },
                MessageGroup {
                    origin: Origin::Chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            0,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            },
                        ),
                        (
                            1,
                            Effect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
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
            Amount::from(1),
            vec![MessageGroup {
                origin: Origin::Chain(ChainId::root(1)),
                height: BlockHeight::from(0),
                effects: vec![(
                    0,
                    Effect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(1),
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
            Value::ConfirmedBlock {
                block: block_proposal.content.block,
                effects: vec![Effect::Credit {
                    recipient: ChainId::root(3),
                    amount: Amount::from(1),
                }],
                state_hash: HashValue::new(&ExecutionState {
                    epoch: Some(epoch),
                    chain_id: ChainId::root(2),
                    admin_status: Some(ChainAdminStatus::ManagedBy {
                        admin_id: ChainId::root(0),
                    }),
                    subscriptions: BTreeMap::new(),
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    manager: ChainManager::single(recipient_key_pair.public().into()),
                    balance: Balance::from(0),
                }),
            },
        );
        worker
            .handle_certificate(certificate.clone())
            .await
            .unwrap();
        // Then skip the second message and receive the last one.
        let block_proposal = make_transfer_block_proposal(
            ChainId::root(2),
            &recipient_key_pair,
            Address::Account(ChainId::root(3)),
            Amount::from(3),
            vec![MessageGroup {
                origin: Origin::Chain(ChainId::root(1)),
                height: BlockHeight::from(1),
                effects: vec![(
                    0,
                    Effect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(3),
                    },
                )],
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

#[test(tokio::test)]
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
        .check(ValidatorName(worker.key_pair.unwrap().public()))
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

#[test(tokio::test)]
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
        .check(ValidatorName(worker.key_pair.as_ref().unwrap().public()))
        .as_ref()
        .unwrap();
    let replay_response = worker.handle_block_proposal(block_proposal).await.unwrap();
    // Workaround lack of equality.
    assert_eq!(
        HashValue::new(&response.info),
        HashValue::new(&replay_response.info)
    );
}

#[test(tokio::test)]
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

#[test(tokio::test)]
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

#[test(tokio::test)]
async fn test_handle_certificate_with_anticipated_incoming_message() {
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
            origin: Origin::Chain(ChainId::root(3)),
            height: BlockHeight::from(0),
            effects: vec![(
                0,
                Effect::Credit {
                    recipient: ChainId::root(1),
                    amount: Amount::from(995),
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
            .get(&Origin::Chain(ChainId::root(3)))
            .unwrap()
            .next_height_to_receive
    );
    assert!(chain
        .inboxes
        .get(&Origin::Chain(ChainId::root(3)))
        .unwrap()
        .received_events
        .is_empty(),);
    assert!(matches!(
        chain.inboxes.get(&Origin::Chain(ChainId::root(3))).unwrap().expected_events.front().unwrap(),
        Event { height, index: 0, effect: Effect::Credit { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(995),
    ));
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
    worker
        .storage
        .read_active_chain(ChainId::root(2))
        .await
        .unwrap();
}

#[test(tokio::test)]
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

#[test(tokio::test)]
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
            .get(&Origin::Chain(ChainId::root(1)))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&Origin::Chain(ChainId::root(1))).unwrap().received_events.front().unwrap(),
        Event { height, index: 0, effect: Effect::Credit { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(1),
    ));
    assert_eq!(BlockHeight::from(1), chain.next_block_height);
    assert_eq!(chain.confirmed_log.len(), 1);
    assert_eq!(Some(certificate.hash), chain.block_hash);
}

#[test(tokio::test)]
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
            origin: Origin::Chain(ChainId::root(1)),
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
            .get(&Origin::Chain(ChainId::root(1)))
            .unwrap()
            .next_height_to_receive
    );
    assert!(matches!(
        chain.inboxes.get(&Origin::Chain(ChainId::root(1))).unwrap().received_events.front().unwrap(),
        Event { height, index: 0, effect: Effect::Credit { amount, .. }} if *height == BlockHeight::from(0) && *amount == Amount::from(10),
    ));
    assert_eq!(chain.confirmed_log.len(), 0);
    assert_eq!(None, chain.block_hash);
    assert_eq!(chain.received_log.len(), 1);
}

#[test(tokio::test)]
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
            origin: Origin::Chain(ChainId::root(1)),
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

#[test(tokio::test)]
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
                origin: Origin::Chain(ChainId::root(1)),
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

#[test(tokio::test)]
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
            origin: Origin::Chain(ChainId::root(1)),
            height: BlockHeight::from(0),
            effects: vec![(
                0,
                Effect::Credit {
                    recipient: ChainId::root(2),
                    amount: Amount::from(5),
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
        .has_owner(&recipient_key_pair.public().into()));
    assert_eq!(recipient_chain.confirmed_log.len(), 1);
    assert_eq!(recipient_chain.block_hash, Some(certificate.hash));
    assert_eq!(recipient_chain.received_log.len(), 1);

    let query =
        ChainInfoQuery::new(ChainId::root(2)).with_received_certificates_excluding_first_nth(0);
    let response = worker.handle_chain_info_query(query).await.unwrap();
    assert_eq!(response.info.requested_received_certificates.len(), 1);
    assert!(matches!(response.info.requested_received_certificates[0]
            .value
            .confirmed_block()
            .unwrap()
            .operations[..], [Operation::Transfer { amount, .. }] if amount == Amount::from(5)));
}

#[test(tokio::test)]
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

#[test(tokio::test)]
async fn test_chain_creation_with_committee_creation() {
    let key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(0),
        key_pair.public(),
        Balance::from(0),
    )])
    .await;
    let root_id = ChainId::root(0);
    // Have the root create a new chain.
    let child_id0 = ChainId::child(EffectId {
        chain_id: root_id,
        height: BlockHeight::from(0),
        index: 0,
    });
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::from(0), committee.clone());
    let certificate0 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: root_id,
                incoming_messages: Vec::new(),
                operations: vec![Operation::OpenChain {
                    id: child_id0,
                    owner: key_pair.public().into(),
                    epoch: Epoch::from(0),
                    committees: committees.clone(),
                    admin_id: root_id,
                }],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![
                Effect::OpenChain {
                    id: child_id0,
                    owner: key_pair.public().into(),
                    epoch: Epoch::from(0),
                    committees: committees.clone(),
                    admin_id: root_id,
                },
                Effect::Subscribe {
                    id: child_id0,
                    channel: ChannelId {
                        chain_id: root_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                },
            ],
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(Epoch::from(0)),
                chain_id: root_id,
                admin_status: Some(ChainAdminStatus::Managing),
                subscriptions: BTreeMap::new(),
                committees: committees.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            }),
        },
    );
    worker
        .fully_handle_certificate(certificate0.clone())
        .await
        .unwrap();
    {
        let root_chain = worker.storage.read_active_chain(root_id).await.unwrap();
        assert_eq!(BlockHeight::from(1), root_chain.next_block_height);
        assert!(root_chain.outboxes.is_empty());
        assert!(matches!(
            root_chain.state.admin_status,
            Some(ChainAdminStatus::Managing)
        ));
        // The root chain has 1 subscriber already.
        assert_eq!(
            root_chain
                .channels
                .get(ADMIN_CHANNEL)
                .unwrap()
                .subscribers
                .len(),
            1
        );
    }

    // Create a committee before receiving the subscription of the new chain.
    let committees2: BTreeMap<_, _> = [
        (Epoch::from(0), committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]
    .into_iter()
    .collect();
    let certificate1 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: root_id,
                incoming_messages: vec![MessageGroup {
                    origin: Origin::Chain(ChainId::root(0)),
                    height: BlockHeight::from(0),
                    effects: vec![(
                        1,
                        Effect::Subscribe {
                            id: child_id0,
                            channel: ChannelId {
                                chain_id: root_id,
                                name: ADMIN_CHANNEL.into(),
                            },
                        },
                    )],
                }],
                // Create the new committee.
                operations: vec![Operation::CreateCommittee {
                    admin_id: root_id,
                    epoch: Epoch::from(1),
                    committee: committee.clone(),
                }],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
            },
            effects: vec![Effect::SetCommittees {
                admin_id: root_id,
                epoch: Epoch::from(1),
                committees: committees2.clone(),
            }],
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(Epoch::from(1)),
                chain_id: root_id,
                admin_status: Some(ChainAdminStatus::Managing),
                subscriptions: BTreeMap::new(),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            }),
        },
    );
    worker
        .fully_handle_certificate(certificate1.clone())
        .await
        .unwrap();

    // Let the derived chain create another chain before migrating to the new committee.
    let child_id = ChainId::child(EffectId {
        chain_id: child_id0,
        height: BlockHeight::from(0),
        index: 0,
    });
    let certificate00 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: child_id0,
                incoming_messages: Vec::new(),
                operations: vec![Operation::OpenChain {
                    id: child_id,
                    owner: key_pair.public().into(),
                    epoch: Epoch::from(0),
                    committees: committees.clone(),
                    admin_id: root_id,
                }],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![
                Effect::OpenChain {
                    id: child_id,
                    owner: key_pair.public().into(),
                    epoch: Epoch::from(0),
                    committees: committees.clone(),
                    admin_id: root_id,
                },
                Effect::Subscribe {
                    id: child_id,
                    channel: ChannelId {
                        chain_id: root_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                },
            ],
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(Epoch::from(0)),
                chain_id: child_id0,
                admin_status: Some(ChainAdminStatus::ManagedBy { admin_id: root_id }),
                subscriptions: [(
                    ChannelId {
                        chain_id: root_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                    (),
                )]
                .into_iter()
                .collect(),
                committees: committees.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            }),
        },
    );
    worker
        .fully_handle_certificate(certificate00.clone())
        .await
        .unwrap();
    {
        // The root chain has 2 subscribers.
        let root_chain = worker.storage.read_active_chain(root_id).await.unwrap();
        assert_eq!(
            root_chain
                .channels
                .get(ADMIN_CHANNEL)
                .unwrap()
                .subscribers
                .len(),
            2
        );
    }
    {
        // The second child chain is active and has not migrated yet.
        let child_chain = worker.storage.read_active_chain(child_id).await.unwrap();
        assert_eq!(BlockHeight::from(0), child_chain.next_block_height);
        assert!(
            matches!(child_chain.state.admin_status, Some(ChainAdminStatus::ManagedBy { admin_id }) if admin_id == root_id)
        );
        assert_eq!(child_chain.state.subscriptions.len(), 1);
        assert!(!child_chain
            .inboxes
            .get(&Origin::Chain(child_id0))
            .unwrap()
            .received_events
            .is_empty());
        assert_eq!(child_chain.state.committees.len(), 1);
    }
    // Make the child receive the pending notification from root.
    let certificate000 = make_certificate(
        &committee,
        &worker,
        Value::ConfirmedBlock {
            block: Block {
                epoch: Epoch::from(0),
                chain_id: child_id,
                incoming_messages: vec![MessageGroup {
                    origin: Origin::Channel(ChannelId {
                        chain_id: root_id,
                        name: ADMIN_CHANNEL.into(),
                    }),
                    height: BlockHeight::from(2),
                    effects: vec![(
                        0,
                        Effect::SetCommittees {
                            admin_id: root_id,
                            epoch: Epoch::from(1),
                            committees: committees2.clone(),
                        },
                    )],
                }],
                operations: Vec::new(),
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: Vec::new(),
            state_hash: HashValue::new(&ExecutionState {
                epoch: Some(Epoch::from(1)),
                chain_id: child_id,
                admin_status: Some(ChainAdminStatus::ManagedBy { admin_id: root_id }),
                subscriptions: [(
                    ChannelId {
                        chain_id: root_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                    (),
                )]
                .into_iter()
                .collect(),
                // Finally the child knows about both committees.
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            }),
        },
    );
    worker
        .fully_handle_certificate(certificate000)
        .await
        .unwrap();
}
