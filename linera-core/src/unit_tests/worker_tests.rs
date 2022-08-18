// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{ValidatorWorker, WorkerState};
use linera_base::{
    committee::Committee,
    crypto::*,
    error::Error,
    execution::{ExecutionState, SYSTEM},
    manager::ChainManager,
    messages::*,
    system::{
        Address, Amount, Balance, SystemEffect, SystemExecutionState, SystemOperation, UserData,
        ADMIN_CHANNEL,
    },
};
use linera_storage2::{chain::Event, MemoryStoreClient, Store};
use std::collections::BTreeMap;
use test_log::test;

/// Instantiate the protocol with a single validator. Returns the corresponding committee
/// and the (non-sharded, in-memory) "worker" that we can interact with.
fn init_worker(allow_inactive_chains: bool) -> (Committee, WorkerState<MemoryStoreClient>) {
    let key_pair = KeyPair::generate();
    let committee = Committee::make_simple(vec![ValidatorName(key_pair.public())]);
    let client = MemoryStoreClient::default();
    let worker = WorkerState::new("Single validator node".to_string(), Some(key_pair), client)
        .allow_inactive_chains(allow_inactive_chains);
    (committee, worker)
}

/// Same as `init_worker` but also instantiate some initial chains.
async fn init_worker_with_chains<I: IntoIterator<Item = (ChainDescription, PublicKey, Balance)>>(
    balances: I,
) -> (Committee, WorkerState<MemoryStoreClient>) {
    let (committee, worker) = init_worker(/* allow_inactive_chains */ false);
    for (description, pubk, balance) in balances {
        worker
            .storage
            .create_chain(
                committee.clone(),
                ChainId::root(0),
                description,
                pubk.into(),
                balance,
            )
            .await
            .unwrap();
    }
    (committee, worker)
}

/// Same as `init_worker` but also instantiate a single initial chain.
async fn init_worker_with_chain(
    description: ChainDescription,
    owner: PublicKey,
    balance: Balance,
) -> (Committee, WorkerState<MemoryStoreClient>) {
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
        operations: operations.into_iter().map(|op| (SYSTEM, op)).collect(),
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
        vec![Operation::System(SystemOperation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        })],
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
    worker: &WorkerState<MemoryStoreClient>,
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
    chain_description: ChainDescription,
    key_pair: &KeyPair,
    recipient: Address,
    amount: Amount,
    incoming_messages: Vec<MessageGroup>,
    committee: &Committee,
    balance: Balance,
    worker: &WorkerState<MemoryStoreClient>,
    previous_confirmed_block: Option<&Certificate>,
) -> Certificate {
    let chain_id = chain_description.into();
    let state = ExecutionState::from(SystemExecutionState {
        epoch: Some(Epoch::from(0)),
        description: Some(chain_description),
        admin_id: Some(ChainId::root(0)),
        subscriptions: BTreeMap::new(),
        committees: [(Epoch::from(0), committee.clone())].into_iter().collect(),
        manager: ChainManager::single(key_pair.public().into()),
        balance,
    });
    let block = make_block(
        Epoch::from(0),
        chain_id,
        vec![Operation::System(SystemOperation::Transfer {
            recipient,
            amount,
            user_data: UserData::default(),
        })],
        incoming_messages,
        previous_confirmed_block,
    );
    let effects = match recipient {
        Address::Account(id) => vec![(
            SYSTEM,
            Destination::Recipient(id),
            Effect::System(SystemEffect::Credit {
                recipient: id,
                amount,
            }),
        )],
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
        ChainDescription::Root(1),
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
        .manager
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
        .execution_state
        .get()
        .system
        .manager
        .pending()
        .is_some());
    worker.handle_certificate(certificate0).await.unwrap();
    worker.handle_block_proposal(block_proposal1).await.unwrap();
    assert!(worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
                    (
                        SYSTEM,
                        Operation::System(SystemOperation::Transfer {
                            recipient,
                            amount: Amount::from(1),
                            user_data: UserData::default(),
                        }),
                    ),
                    (
                        SYSTEM,
                        Operation::System(SystemOperation::Transfer {
                            recipient,
                            amount: Amount::from(2),
                            user_data: UserData::default(),
                        }),
                    ),
                ],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![
                (
                    SYSTEM,
                    Destination::Recipient(ChainId::root(2)),
                    Effect::System(SystemEffect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(1),
                    }),
                ),
                (
                    SYSTEM,
                    Destination::Recipient(ChainId::root(2)),
                    Effect::System(SystemEffect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(2),
                    }),
                ),
            ],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(epoch),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(ChainId::root(0)),
                subscriptions: BTreeMap::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                manager: ChainManager::single(sender_key_pair.public().into()),
                balance: Balance::from(3),
            })),
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
                    SYSTEM,
                    Operation::System(SystemOperation::Transfer {
                        recipient,
                        amount: Amount::from(3),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
            },
            effects: vec![(
                SYSTEM,
                Destination::Recipient(ChainId::root(2)),
                Effect::System(SystemEffect::Credit {
                    recipient: ChainId::root(2),
                    amount: Amount::from(3),
                }),
            )],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(epoch),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(ChainId::root(0)),
                subscriptions: BTreeMap::new(),
                committees: [(epoch, committee.clone())].into_iter().collect(),
                manager: ChainManager::single(sender_key_pair.public().into()),
                balance: Balance::from(0),
            })),
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
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            0,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            }),
                        ),
                        (
                            1,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
                            }),
                        ),
                    ],
                },
                MessageGroup {
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::System(SystemEffect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(2), // wrong
                        }),
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
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            1,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
                            }),
                        ),
                        (
                            0,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            }),
                        ),
                    ],
                },
                MessageGroup {
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::System(SystemEffect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(3),
                        }),
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
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(1),
                    effects: vec![(
                        0,
                        Effect::System(SystemEffect::Credit {
                            recipient: ChainId::root(2),
                            amount: Amount::from(3),
                        }),
                    )],
                },
                MessageGroup {
                    application_id: SYSTEM,
                    origin: Origin::chain(ChainId::root(1)),
                    height: BlockHeight::from(0),
                    effects: vec![
                        (
                            0,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(1),
                            }),
                        ),
                        (
                            1,
                            Effect::System(SystemEffect::Credit {
                                recipient: ChainId::root(2),
                                amount: Amount::from(2),
                            }),
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
                application_id: SYSTEM,
                origin: Origin::chain(ChainId::root(1)),
                height: BlockHeight::from(0),
                effects: vec![(
                    0,
                    Effect::System(SystemEffect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(1),
                    }),
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
                effects: vec![(
                    SYSTEM,
                    Destination::Recipient(ChainId::root(3)),
                    Effect::System(SystemEffect::Credit {
                        recipient: ChainId::root(3),
                        amount: Amount::from(1),
                    }),
                )],
                state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                    epoch: Some(epoch),
                    description: Some(ChainDescription::Root(2)),
                    admin_id: Some(ChainId::root(0)),
                    subscriptions: BTreeMap::new(),
                    committees: [(epoch, committee.clone())].into_iter().collect(),
                    manager: ChainManager::single(recipient_key_pair.public().into()),
                    balance: Balance::from(0),
                })),
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
                application_id: SYSTEM,
                origin: Origin::chain(ChainId::root(1)),
                height: BlockHeight::from(1),
                effects: vec![(
                    0,
                    Effect::System(SystemEffect::Credit {
                        recipient: ChainId::root(2),
                        amount: Amount::from(3),
                    }),
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap()
        .execution_state
        .get()
        .system
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
        ChainDescription::Root(1),
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
        ChainDescription::Root(1),
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
        ChainDescription::Root(1),
        &key_pair,
        Address::Account(ChainId::root(2)),
        Amount::from(1000),
        vec![MessageGroup {
            application_id: SYSTEM,
            origin: Origin::chain(ChainId::root(3)),
            height: BlockHeight::from(0),
            effects: vec![(
                0,
                Effect::System(SystemEffect::Credit {
                    recipient: ChainId::root(1),
                    amount: Amount::from(995),
                }),
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
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.execution_state.get().system.balance);
    assert_eq!(
        BlockHeight::from(1),
        chain.chaining_state.get().next_block_height
    );
    assert_eq!(
        BlockHeight::from(0),
        *chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .inboxes
            .load_entry(Origin::chain(ChainId::root(3)))
            .await
            .unwrap()
            .next_height_to_receive
            .get()
    );
    assert_eq!(
        chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .inboxes
            .load_entry(Origin::chain(ChainId::root(3)))
            .await
            .unwrap()
            .received_events
            .count(),
        0
    );
    assert!(matches!(
        chain.communication_states.load_entry(SYSTEM).await.unwrap().inboxes.load_entry(Origin::chain(ChainId::root(3))).await.unwrap().expected_events.front().await.unwrap().unwrap(),
        Event { height, index: 0, effect: Effect::System(SystemEffect::Credit { amount, .. })} if height == BlockHeight::from(0) && amount == Amount::from(995),
    ));
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash),
        chain.chaining_state.get().block_hash
    );
    worker
        .storage
        .load_active_chain(ChainId::root(2))
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
        ChainDescription::Root(1),
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
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(
        Balance::from(0),
        new_sender_chain.execution_state.get().system.balance
    );
    assert_eq!(
        BlockHeight::from(1),
        new_sender_chain.chaining_state.get().next_block_height
    );
    assert_eq!(new_sender_chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash),
        new_sender_chain.chaining_state.get().block_hash
    );
    let new_recipient_chain = worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(
        Balance::max(),
        new_recipient_chain.execution_state.get().system.balance
    );
}

#[test(tokio::test)]
async fn test_handle_certificate_receiver_equal_sender() {
    let key_pair = KeyPair::generate();
    let name = key_pair.public();
    let (committee, mut worker) =
        init_worker_with_chain(ChainDescription::Root(1), name, Balance::from(1)).await;

    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
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
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(1))
        .await
        .unwrap();
    assert_eq!(Balance::from(0), chain.execution_state.get().system.balance);
    assert_eq!(
        BlockHeight::from(1),
        *chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .inboxes
            .load_entry(Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
            .next_height_to_receive
            .get()
    );
    assert!(matches!(
        chain.communication_states.load_entry(SYSTEM).await.unwrap().inboxes.load_entry(Origin::chain(ChainId::root(1))).await.unwrap().received_events.front().await.unwrap().unwrap(),
        Event { height, index: 0, effect: Effect::System(SystemEffect::Credit { amount, .. })} if height == BlockHeight::from(0) && amount == Amount::from(1),
    ));
    assert_eq!(
        BlockHeight::from(1),
        chain.chaining_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash),
        chain.chaining_state.get().block_hash
    );
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
        ChainDescription::Root(1),
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
            application_id: SYSTEM,
            origin: Origin::chain(ChainId::root(1)),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap();
    let mut chain = worker
        .storage
        .load_active_chain(ChainId::root(2))
        .await
        .unwrap();
    assert_eq!(Balance::from(1), chain.execution_state.get().system.balance);
    assert_eq!(
        BlockHeight::from(0),
        chain.chaining_state.get().next_block_height
    );
    assert_eq!(
        BlockHeight::from(1),
        *chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .inboxes
            .load_entry(Origin::chain(ChainId::root(1)))
            .await
            .unwrap()
            .next_height_to_receive
            .get()
    );
    assert!(matches!(
        chain.communication_states.load_entry(SYSTEM).await.unwrap().inboxes.load_entry(Origin::chain(ChainId::root(1))).await.unwrap().received_events.front().await.unwrap().unwrap(),
        Event { height, index: 0, effect: Effect::System(SystemEffect::Credit { amount, .. })} if height == BlockHeight::from(0) && amount == Amount::from(10),
    ));
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.chaining_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
}

#[test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ false);
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
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
            application_id: SYSTEM,
            origin: Origin::chain(ChainId::root(1)),
            recipient: ChainId::root(2),
            certificates: vec![certificate],
        })
        .await
        .unwrap()
        .is_empty());
    let mut chain = worker.storage.load_chain(ChainId::root(2)).await.unwrap();
    // The target chain did not receive the message
    assert!(!chain
        .communication_states
        .indices()
        .await
        .unwrap()
        .contains(&SYSTEM));
}

#[test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain_with_inactive_chains_allowed() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker(/* allow_inactive_chains */ true);
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
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
                application_id: SYSTEM,
                origin: Origin::chain(ChainId::root(1)),
                recipient: ChainId::root(2),
                certificates: vec![certificate],
            })
            .await
            .unwrap()
            .as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    ));
    let mut chain = worker.storage.load_chain(ChainId::root(2)).await.unwrap();
    assert!(!chain
        .communication_states
        .load_entry(SYSTEM)
        .await
        .unwrap()
        .inboxes
        .indices()
        .await
        .unwrap()
        .is_empty());
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
        ChainDescription::Root(1),
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
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = make_transfer_certificate(
        ChainDescription::Root(2),
        &recipient_key_pair,
        Address::Account(ChainId::root(3)),
        Amount::from(1),
        vec![MessageGroup {
            application_id: SYSTEM,
            origin: Origin::chain(ChainId::root(1)),
            height: BlockHeight::from(0),
            effects: vec![(
                0,
                Effect::System(SystemEffect::Credit {
                    recipient: ChainId::root(2),
                    amount: Amount::from(5),
                }),
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

    {
        let recipient_chain = worker
            .storage
            .load_active_chain(ChainId::root(2))
            .await
            .unwrap();
        assert_eq!(
            recipient_chain.execution_state.get().system.balance,
            Balance::from(4)
        );
        assert!(recipient_chain
            .execution_state
            .get()
            .system
            .manager
            .has_owner(&recipient_key_pair.public().into()));
        assert_eq!(recipient_chain.confirmed_log.count(), 1);
        assert_eq!(
            recipient_chain.chaining_state.get().block_hash,
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
async fn test_handle_certificate_to_inactive_recipient() {
    let sender_key_pair = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![(
        ChainDescription::Root(1),
        sender_key_pair.public(),
        Balance::from(5),
    )])
    .await;
    let certificate = make_transfer_certificate(
        ChainDescription::Root(1),
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
    assert_eq!(Balance::from(0), info.system_balance);
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
        Balance::from(2),
    )])
    .await;
    let mut committees = BTreeMap::new();
    committees.insert(Epoch::from(0), committee.clone());
    let admin_id = ChainId::root(0);
    let admin_channel = ChannelId {
        chain_id: admin_id,
        name: ADMIN_CHANNEL.into(),
    };
    let admin_channel_origin = Origin::channel(admin_id, ADMIN_CHANNEL.into());
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
                    SYSTEM,
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
            },
            effects: vec![
                (
                    SYSTEM,
                    Destination::Recipient(user_id),
                    Effect::System(SystemEffect::OpenChain {
                        id: user_id,
                        owner: key_pair.public().into(),
                        epoch: Epoch::from(0),
                        committees: committees.clone(),
                        admin_id,
                    }),
                ),
                (
                    SYSTEM,
                    Destination::Recipient(admin_id),
                    Effect::System(SystemEffect::Subscribe {
                        id: user_id,
                        channel: admin_channel.clone(),
                    }),
                ),
            ],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                committees: committees.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(2),
            })),
        },
    );
    worker
        .fully_handle_certificate(certificate0.clone())
        .await
        .unwrap();
    {
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
        assert_eq!(
            BlockHeight::from(1),
            admin_chain.chaining_state.get().next_block_height
        );
        // FIXME: we do not clean up empty outboxes yet.
        assert!(!admin_chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .outboxes
            .indices()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            admin_chain.execution_state.get().system.admin_id,
            Some(admin_id)
        );
        // The root chain has no subscribers yet.
        assert!(!admin_chain
            .communication_states
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .channels
            .indices()
            .await
            .unwrap()
            .contains(&ADMIN_CHANNEL.to_string()));
    }

    // Create a new committee and transfer money before accepting the subscription.
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
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![
                    (
                        SYSTEM,
                        Operation::System(SystemOperation::CreateCommittee {
                            admin_id,
                            epoch: Epoch::from(1),
                            committee: committee.clone(),
                        }),
                    ),
                    (
                        SYSTEM,
                        Operation::System(SystemOperation::Transfer {
                            recipient: Address::Account(user_id),
                            amount: Amount::from(2),
                            user_data: UserData::default(),
                        }),
                    ),
                ],
                previous_block_hash: Some(certificate0.hash),
                height: BlockHeight::from(1),
            },
            effects: vec![
                (
                    SYSTEM,
                    Destination::Subscribers(ADMIN_CHANNEL.to_string()),
                    Effect::System(SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    }),
                ),
                (
                    SYSTEM,
                    Destination::Recipient(user_id),
                    Effect::System(SystemEffect::Credit {
                        recipient: user_id,
                        amount: Amount::from(2),
                    }),
                ),
            ],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            })),
        },
    );
    worker
        .fully_handle_certificate(certificate1.clone())
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
                incoming_messages: vec![MessageGroup {
                    application_id: SYSTEM,
                    origin: Origin::chain(admin_id),
                    height: BlockHeight::from(0),
                    effects: vec![(
                        1,
                        Effect::System(SystemEffect::Subscribe {
                            id: user_id,
                            channel: admin_channel.clone(),
                        }),
                    )],
                }],
                operations: Vec::new(),
                previous_block_hash: Some(certificate1.hash),
                height: BlockHeight::from(2),
            },
            effects: vec![(
                SYSTEM,
                Destination::Recipient(user_id),
                Effect::System(SystemEffect::Notify { id: user_id }),
            )],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(0),
            })),
        },
    );
    worker
        .fully_handle_certificate(certificate2.clone())
        .await
        .unwrap();
    {
        // The root chain has 1 subscribers.
        let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
        admin_chain.validate_incoming_messages().await.unwrap();
        assert_eq!(
            admin_chain
                .communication_states
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .channels
                .load_entry(ADMIN_CHANNEL.to_string())
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
            user_chain.chaining_state.get().next_block_height
        );
        assert_eq!(
            user_chain.execution_state.get().system.admin_id,
            Some(admin_id)
        );
        assert_eq!(
            user_chain.execution_state.get().system.subscriptions.len(),
            1
        );
        user_chain.validate_incoming_messages().await.unwrap();
        matches!(
            user_chain
                .communication_states
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .inboxes
                .load_entry(Origin::chain(admin_id))
                .await
                .unwrap()
                .received_events
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
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .inboxes
                .load_entry(admin_channel_origin.clone())
                .await
                .unwrap()
                .received_events
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
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .inboxes
                .load_entry(admin_channel_origin.clone())
                .await
                .unwrap()
                .expected_events
                .count(),
            0
        );
        assert_eq!(user_chain.execution_state.get().system.committees.len(), 1);
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
                    MessageGroup {
                        application_id: SYSTEM,
                        origin: admin_channel_origin.clone(),
                        height: BlockHeight::from(1),
                        effects: vec![(
                            0,
                            Effect::System(SystemEffect::SetCommittees {
                                admin_id,
                                epoch: Epoch::from(1),
                                committees: committees2.clone(),
                            }),
                        )],
                    },
                    MessageGroup {
                        application_id: SYSTEM,
                        origin: Origin::chain(admin_id),
                        height: BlockHeight::from(1),
                        effects: vec![(
                            1,
                            Effect::System(SystemEffect::Credit {
                                recipient: user_id,
                                amount: Amount::from(2),
                            }),
                        )],
                    },
                    MessageGroup {
                        application_id: SYSTEM,
                        origin: Origin::chain(admin_id),
                        height: BlockHeight::from(2),
                        effects: vec![(0, Effect::System(SystemEffect::Notify { id: user_id }))],
                    },
                ],
                operations: Vec::new(),
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: Vec::new(),
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(user_description),
                admin_id: Some(admin_id),
                subscriptions: [(
                    ChannelId {
                        chain_id: admin_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                    (),
                )]
                .into_iter()
                .collect(),
                // Finally the child knows about both committees and has the money.
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair.public().into()),
                balance: Balance::from(2),
            })),
        },
    );
    worker.fully_handle_certificate(certificate3).await.unwrap();
    {
        let mut user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
        assert_eq!(
            BlockHeight::from(1),
            user_chain.chaining_state.get().next_block_height
        );
        assert_eq!(
            user_chain.execution_state.get().system.admin_id,
            Some(admin_id)
        );
        assert_eq!(
            user_chain.execution_state.get().system.subscriptions.len(),
            1
        );
        assert_eq!(user_chain.execution_state.get().system.committees.len(), 2);
        user_chain.validate_incoming_messages().await.unwrap();
        {
            let inbox = user_chain
                .communication_states
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .inboxes
                .load_entry(Origin::chain(admin_id))
                .await
                .unwrap();
            assert_eq!(*inbox.next_height_to_receive.get(), BlockHeight(3));
            assert_eq!(inbox.received_events.count(), 0);
            assert_eq!(inbox.expected_events.count(), 0);
        }
        {
            let inbox = user_chain
                .communication_states
                .load_entry(SYSTEM)
                .await
                .unwrap()
                .inboxes
                .load_entry(admin_channel_origin)
                .await
                .unwrap();
            assert_eq!(*inbox.next_height_to_receive.get(), BlockHeight(2));
            assert_eq!(inbox.received_events.count(), 0);
            assert_eq!(inbox.expected_events.count(), 0);
        }
    }
}

#[test(tokio::test)]
async fn test_transfers_and_committee_creation() {
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
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
                    SYSTEM,
                    Operation::System(SystemOperation::Transfer {
                        recipient: Address::Account(admin_id),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![(
                SYSTEM,
                Destination::Recipient(admin_id),
                Effect::System(SystemEffect::Credit {
                    recipient: admin_id,
                    amount: Amount::from(1),
                }),
            )],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                committees: committees.clone(),
                manager: ChainManager::single(key_pair1.public().into()),
                balance: Balance::from(2),
            })),
        },
    );
    // Have the admin chain create a new epoch without retiring the old one.
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
                chain_id: admin_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    SYSTEM,
                    Operation::System(SystemOperation::CreateCommittee {
                        admin_id,
                        epoch: Epoch::from(1),
                        committee: committee.clone(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![(
                SYSTEM,
                Destination::Subscribers(ADMIN_CHANNEL.to_string()),
                Effect::System(SystemEffect::SetCommittees {
                    admin_id,
                    epoch: Epoch::from(1),
                    committees: committees2.clone(),
                }),
            )],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                committees: committees2.clone(),
                manager: ChainManager::single(key_pair0.public().into()),
                balance: Balance::from(0),
            })),
        },
    );
    worker
        .fully_handle_certificate(certificate1.clone())
        .await
        .unwrap();

    // Try to execute the transfer.
    worker
        .fully_handle_certificate(certificate0.clone())
        .await
        .unwrap();

    // The transfer was started..
    let user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
    assert_eq!(
        BlockHeight::from(1),
        user_chain.chaining_state.get().next_block_height
    );
    assert_eq!(
        user_chain.execution_state.get().system.balance,
        Balance::from(2)
    );
    assert_eq!(
        user_chain.execution_state.get().system.epoch,
        Some(Epoch::from(0))
    );

    // .. and the message has gone through.
    let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
    assert_eq!(
        admin_chain
            .communication_states
            .load_entry(SYSTEM)
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
            .load_entry(SYSTEM)
            .await
            .unwrap()
            .inboxes
            .load_entry(Origin::chain(user_id))
            .await
            .unwrap()
            .received_events
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
async fn test_transfers_and_committee_removal() {
    let key_pair0 = KeyPair::generate();
    let key_pair1 = KeyPair::generate();
    let (committee, mut worker) = init_worker_with_chains(vec![
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
    ])
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
                    SYSTEM,
                    Operation::System(SystemOperation::Transfer {
                        recipient: Address::Account(admin_id),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![(
                SYSTEM,
                Destination::Recipient(admin_id),
                Effect::System(SystemEffect::Credit {
                    recipient: admin_id,
                    amount: Amount::from(1),
                }),
            )],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(0)),
                description: Some(ChainDescription::Root(1)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                committees: committees.clone(),
                manager: ChainManager::single(key_pair1.public().into()),
                balance: Balance::from(2),
            })),
        },
    );
    // Have the admin chain create a new epoch and retire the old one immediately.
    let committees2: BTreeMap<_, _> = [
        (Epoch::from(0), committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]
    .into_iter()
    .collect();
    let committees3: BTreeMap<_, _> = [(Epoch::from(1), committee.clone())].into_iter().collect();
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
                        SYSTEM,
                        Operation::System(SystemOperation::CreateCommittee {
                            admin_id,
                            epoch: Epoch::from(1),
                            committee: committee.clone(),
                        }),
                    ),
                    (
                        SYSTEM,
                        Operation::System(SystemOperation::RemoveCommittee {
                            admin_id,
                            epoch: Epoch::from(0),
                        }),
                    ),
                ],
                previous_block_hash: None,
                height: BlockHeight::from(0),
            },
            effects: vec![
                (
                    SYSTEM,
                    Destination::Subscribers(ADMIN_CHANNEL.to_string()),
                    Effect::System(SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees2.clone(),
                    }),
                ),
                (
                    SYSTEM,
                    Destination::Subscribers(ADMIN_CHANNEL.to_string()),
                    Effect::System(SystemEffect::SetCommittees {
                        admin_id,
                        epoch: Epoch::from(1),
                        committees: committees3.clone(),
                    }),
                ),
            ],
            state_hash: HashValue::new(&ExecutionState::from(SystemExecutionState {
                epoch: Some(Epoch::from(1)),
                description: Some(ChainDescription::Root(0)),
                admin_id: Some(admin_id),
                subscriptions: BTreeMap::new(),
                committees: committees3.clone(),
                manager: ChainManager::single(key_pair0.public().into()),
                balance: Balance::from(0),
            })),
        },
    );
    worker
        .fully_handle_certificate(certificate1.clone())
        .await
        .unwrap();

    // Try to execute the transfer.
    worker
        .fully_handle_certificate(certificate0.clone())
        .await
        .unwrap();

    // The transfer was started..
    let user_chain = worker.storage.load_active_chain(user_id).await.unwrap();
    assert_eq!(
        BlockHeight::from(1),
        user_chain.chaining_state.get().next_block_height
    );
    assert_eq!(
        user_chain.execution_state.get().system.balance,
        Balance::from(2)
    );
    assert_eq!(
        user_chain.execution_state.get().system.epoch,
        Some(Epoch::from(0))
    );

    // .. but the message hasn't gone through.
    let mut admin_chain = worker.storage.load_active_chain(admin_id).await.unwrap();
    assert!(admin_chain
        .communication_states
        .load_entry(SYSTEM)
        .await
        .unwrap()
        .inboxes
        .indices()
        .await
        .unwrap()
        .is_empty());
}
