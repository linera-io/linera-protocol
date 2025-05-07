// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{
        AccountPublicKey, AccountSecretKey, CryptoHash, InMemorySigner, Signer, ValidatorKeypair,
    },
    data_types::*,
    identifiers::{Account, AccountOwner, ChainId, EventId, StreamId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ChainAndHeight, IncomingBundle, LiteValue, LiteVote,
        MessageAction, MessageBundle, OperationResult, PostedMessage, ProposedBlock,
        SignatureAggregator,
    },
    manager::LockingBlock,
    test::{make_child_block, make_first_block, BlockTestExt, MessageTestExt, VoteTestExt},
    types::{
        CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate, Timeout,
        ValidatedBlock,
    },
    ChainError, ChainExecutionContext,
};
use linera_execution::{
    committee::Committee,
    system::{
        AdminOperation, OpenChainConfig, Recipient, SystemMessage, SystemOperation,
        EPOCH_STREAM_NAME as NEW_EPOCH_STREAM_NAME, REMOVED_EPOCH_STREAM_NAME,
    },
    test_utils::{
        dummy_chain_description, ExpectedCall, RegisterMockApplication, SystemExecutionState,
    },
    ExecutionError, Message, MessageKind, OutgoingMessage, Query, QueryContext, QueryOutcome,
    QueryResponse, SystemQuery, SystemResponse,
};
use linera_storage::{DbStorage, Storage, TestClock};
use linera_views::{
    memory::MemoryStore,
    random::generate_test_namespace,
    store::TestKeyValueStore as _,
    views::{CryptoHashView, RootView},
};
use test_case::test_case;
use test_log::test;

#[cfg(feature = "dynamodb")]
use crate::test_utils::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
use crate::{
    chain_worker::CrossChainUpdateHelper,
    data_types::*,
    test_utils::{MemoryStorageBuilder, StorageBuilder},
    worker::{
        Notification,
        Reason::{self, NewBlock, NewIncomingBundle},
        WorkerError, WorkerState,
    },
};

/// The test worker accepts blocks with a timestamp this far in the future.
const TEST_GRACE_PERIOD_MICROS: u64 = 500_000;

fn serialize_committees(
    committees: impl IntoIterator<Item = (Epoch, Committee)>,
) -> BTreeMap<Epoch, Vec<u8>> {
    committees
        .into_iter()
        .map(|(epoch, committee)| {
            (
                epoch,
                bcs::to_bytes(&committee).expect("serializing a committee should not fail"),
            )
        })
        .collect()
}

struct TestEnvironment<S: Storage> {
    committee: Committee,
    worker: WorkerState<S>,
    admin_keypair: AccountSecretKey,
    admin_description: ChainDescription,
    other_chains: BTreeMap<ChainId, ChainDescription>,
}

impl<S> TestEnvironment<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn new(storage: S, is_client: bool, has_long_lived_services: bool) -> Self {
        Self::new_with_amount(
            storage,
            is_client,
            has_long_lived_services,
            Amount::from_tokens(1_000_000),
        )
        .await
    }

    async fn new_with_amount(
        storage: S,
        is_client: bool,
        has_long_lived_services: bool,
        amount: Amount,
    ) -> Self {
        let validator_keypair = ValidatorKeypair::generate();
        let account_secret = AccountSecretKey::generate();
        let committee = Committee::make_simple(vec![(
            validator_keypair.public_key,
            account_secret.public(),
        )]);

        let origin = ChainOrigin::Root(0);
        let config = InitialChainConfig {
            admin_id: None,
            balance: amount,
            ownership: ChainOwnership::single(account_secret.public().into()),
            epoch: Epoch::ZERO,
            committees: serialize_committees([(Epoch::ZERO, committee.clone())]),
            application_permissions: Default::default(),
        };
        let admin_description = ChainDescription::new(origin, config, Timestamp::from(0));
        storage
            .write_blob(&Blob::new_chain_description(&admin_description))
            .await
            .expect("writing a blob should not fail");

        let worker = WorkerState::new(
            "Single validator node".to_string(),
            Some(validator_keypair.secret_key),
            storage,
            NonZeroUsize::new(10).expect("Chain worker limit should not be zero"),
        )
        .with_allow_inactive_chains(is_client)
        .with_allow_messages_from_deprecated_epochs(is_client)
        .with_long_lived_services(has_long_lived_services)
        .with_grace_period(Duration::from_micros(TEST_GRACE_PERIOD_MICROS));
        Self {
            committee,
            worker,
            admin_description,
            admin_keypair: account_secret,
            other_chains: BTreeMap::new(),
        }
    }

    fn admin_id(&self) -> ChainId {
        self.admin_description.id()
    }

    fn committee(&self) -> &Committee {
        &self.committee
    }

    fn worker(&self) -> &WorkerState<S> {
        &self.worker
    }

    fn admin_public_key(&self) -> AccountPublicKey {
        self.admin_keypair.public()
    }

    async fn add_root_chain(
        &mut self,
        index: u32,
        owner: AccountOwner,
        balance: Amount,
    ) -> ChainDescription {
        self.add_root_chain_with_ownership(index, balance, ChainOwnership::single(owner))
            .await
    }

    async fn add_root_chain_with_ownership(
        &mut self,
        index: u32,
        balance: Amount,
        ownership: ChainOwnership,
    ) -> ChainDescription {
        let origin = ChainOrigin::Root(index);
        let config = InitialChainConfig {
            admin_id: Some(self.admin_id()),
            epoch: self.admin_description.config().epoch,
            ownership,
            committees: self.admin_description.config().committees.clone(),
            balance,
            application_permissions: Default::default(),
        };
        let description = ChainDescription::new(origin, config, Timestamp::from(0));
        self.other_chains
            .insert(description.id(), description.clone());
        self.worker
            .storage
            .create_chain(description.clone())
            .await
            .unwrap();
        description
    }

    async fn add_child_chain(
        &mut self,
        parent_id: ChainId,
        owner: AccountOwner,
        balance: Amount,
    ) -> ChainDescription {
        let origin = ChainOrigin::Child {
            parent: parent_id,
            block_height: BlockHeight(0),
            chain_index: 0,
        };
        let config = InitialChainConfig {
            admin_id: Some(self.admin_id()),
            epoch: self.admin_description.config().epoch,
            ownership: ChainOwnership::single(owner),
            committees: self.admin_description.config().committees.clone(),
            balance,
            application_permissions: Default::default(),
        };
        let description = ChainDescription::new(origin, config, Timestamp::from(0));
        self.other_chains
            .insert(description.id(), description.clone());
        self.worker
            .storage
            .create_chain(description.clone())
            .await
            .unwrap();
        description
    }

    fn make_certificate<T>(&self, value: T) -> GenericCertificate<T>
    where
        T: CertificateValue,
    {
        self.make_certificate_with_round(value, Round::MultiLeader(0))
    }

    fn make_certificate_with_round<T>(&self, value: T, round: Round) -> GenericCertificate<T>
    where
        T: CertificateValue,
    {
        let vote = LiteVote::new(
            LiteValue::new(&value),
            round,
            self.worker.chain_worker_config.key_pair().unwrap(),
        );
        let mut builder = SignatureAggregator::new(value, round, &self.committee);
        builder
            .append(vote.public_key, vote.signature)
            .unwrap()
            .unwrap()
    }
    #[expect(clippy::too_many_arguments)]
    async fn make_simple_transfer_certificate(
        &self,
        chain_description: ChainDescription,
        chain_owner_pubkey: AccountPublicKey,
        target_id: ChainId,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        balance: Amount,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        self.make_transfer_certificate_for_epoch(
            chain_description,
            chain_owner_pubkey,
            chain_owner_pubkey.into(),
            AccountOwner::CHAIN,
            Recipient::chain(target_id),
            amount,
            incoming_bundles,
            Epoch::ZERO,
            balance,
            BTreeMap::new(),
            previous_confirmed_block,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn make_transfer_certificate(
        &self,
        chain_description: ChainDescription,
        chain_owner_pubkey: AccountPublicKey,
        authenticated_signer: AccountOwner,
        source: AccountOwner,
        recipient: Recipient,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        balance: Amount,
        balances: BTreeMap<AccountOwner, Amount>,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        self.make_transfer_certificate_for_epoch(
            chain_description,
            chain_owner_pubkey,
            authenticated_signer,
            source,
            recipient,
            amount,
            incoming_bundles,
            Epoch::ZERO,
            balance,
            balances,
            previous_confirmed_block,
        )
        .await
    }

    /// Creates a certificate with a transfer.
    ///
    /// This does not work for blocks with ancestors that sent a message to the same recipient, unless
    /// the `previous_confirmed_block` also did.
    #[expect(clippy::too_many_arguments)]
    async fn make_transfer_certificate_for_epoch(
        &self,
        chain_description: ChainDescription,
        chain_owner_pubkey: AccountPublicKey,
        authenticated_signer: AccountOwner,
        source: AccountOwner,
        recipient: Recipient,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        epoch: Epoch,
        balance: Amount,
        balances: BTreeMap<AccountOwner, Amount>,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        let chain_id = chain_description.id();
        let system_state = SystemExecutionState {
            committees: [(epoch, self.committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(chain_owner_pubkey.into()),
            balance,
            balances,
            ..SystemExecutionState::new(chain_description)
        };
        let block_template = match &previous_confirmed_block {
            None => make_first_block(chain_id),
            Some(cert) => make_child_block(cert.value()),
        };

        let mut messages = incoming_bundles
            .iter()
            .flat_map(|incoming_bundle| {
                incoming_bundle
                    .bundle
                    .messages
                    .iter()
                    .map(|posted_message| {
                        if matches!(incoming_bundle.action, MessageAction::Reject)
                            && matches!(posted_message.kind, MessageKind::Tracked)
                        {
                            vec![OutgoingMessage {
                                authenticated_signer: posted_message.authenticated_signer,
                                destination: incoming_bundle.origin,
                                grant: Amount::ZERO,
                                refund_grant_to: None,
                                kind: MessageKind::Bouncing,
                                message: posted_message.message.clone(),
                            }]
                        } else {
                            Vec::new()
                        }
                    })
            })
            .collect::<Vec<_>>();

        let block = ProposedBlock {
            epoch,
            incoming_bundles,
            authenticated_signer: Some(authenticated_signer),
            ..block_template
        }
        .with_transfer(source, recipient, amount);
        match recipient {
            Recipient::Account(account) => {
                messages.push(vec![direct_outgoing_message(
                    account.chain_id,
                    MessageKind::Tracked,
                    SystemMessage::Credit {
                        source,
                        target: account.owner,
                        amount,
                    },
                )]);
            }
            Recipient::Burn => messages.push(Vec::new()),
        }
        let tx_count = block.operations.len() + block.incoming_bundles.len();
        let oracle_responses = iter::repeat_with(Vec::new).take(tx_count).collect();
        let events = iter::repeat_with(Vec::new).take(tx_count).collect();
        let blobs = iter::repeat_with(Vec::new).take(tx_count).collect();
        let operation_results = iter::repeat_with(Vec::new)
            .map(OperationResult)
            .take(block.operations.len())
            .collect();
        let state_hash = system_state.into_hash().await;
        let previous_message_blocks = messages
            .iter()
            .flatten()
            .map(|message| message.destination)
            .filter(|recipient| {
                previous_confirmed_block
                    .iter()
                    .flat_map(|block| block.inner().block().body.messages.iter().flatten())
                    .any(|message| message.destination == *recipient)
            })
            .map(|recipient| (recipient, previous_confirmed_block.unwrap().hash()))
            .collect();
        let value = ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages,
                previous_message_blocks,
                events,
                blobs,
                state_hash,
                oracle_responses,
                operation_results,
            }
            .with(block),
        );
        self.make_certificate(value)
    }
}

fn direct_outgoing_message(
    recipient: ChainId,
    kind: MessageKind,
    message: SystemMessage,
) -> OutgoingMessage {
    OutgoingMessage {
        destination: recipient,
        authenticated_signer: None,
        grant: Amount::ZERO,
        refund_grant_to: None,
        kind,
        message: Message::System(message),
    }
}

fn system_credit_message(amount: Amount) -> Message {
    Message::System(SystemMessage::Credit {
        source: AccountOwner::CHAIN,
        target: AccountOwner::CHAIN,
        amount,
    })
}

fn direct_credit_message(recipient: ChainId, amount: Amount) -> OutgoingMessage {
    let message = SystemMessage::Credit {
        source: AccountOwner::CHAIN,
        target: AccountOwner::CHAIN,
        amount,
    };
    direct_outgoing_message(recipient, MessageKind::Tracked, message)
}

/// Creates `count` key pairs and returns them, sorted by the `AccountOwner` created from their public key.
fn generate_key_pairs(signer: &mut InMemorySigner, count: usize) -> Vec<AccountPublicKey> {
    let mut public_keys = iter::repeat_with(|| signer.generate_new())
        .take(count)
        .collect::<Vec<_>>();
    public_keys.sort_by_key(|pk| AccountOwner::from(*pk));
    public_keys
}

/// Creates a `CrossChainRequest` with the messages sent by the certificate to the recipient.
fn update_recipient_direct(
    recipient: ChainId,
    certificate: &ConfirmedBlockCertificate,
) -> CrossChainRequest {
    let sender = certificate.inner().block().header.chain_id;
    let bundles = certificate.message_bundles_for(recipient).collect();
    CrossChainRequest::UpdateRecipient {
        sender,
        recipient,
        bundles,
    }
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_bad_signature<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_public_key = signer.generate_new();
    let sender_owner = sender_public_key.into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(5))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    let unknown_key_pair = AccountSecretKey::generate();
    let mut bad_signature_block_proposal = block_proposal.clone();
    bad_signature_block_proposal.signature = unknown_key_pair.sign(&block_proposal.content);
    assert_matches!(
        env.worker()
            .handle_block_proposal(bad_signature_block_proposal)
            .await,
            Err(WorkerError::CryptoError(error))
                if matches!(error, linera_base::crypto::CryptoError::InvalidSignature {..})
    );
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_zero_amount<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_owner = signer.generate_new().into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    // test block non-positive amount
    let zero_amount_block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::ZERO)
        .with_authenticated_signer(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    assert_matches!(
    env.worker()
        .handle_block_proposal(zero_amount_block_proposal)
        .await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
            execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::IncorrectTransferAmount))
    );
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_ticks<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let balance = Amount::from_tokens(5);
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner, balance).await;
    let epoch = Epoch::ZERO;
    let chain_id = chain_1_desc.id();

    {
        let block_proposal = make_first_block(chain_id)
            .with_timestamp(Timestamp::from(TEST_GRACE_PERIOD_MICROS + 1_000_000))
            .into_first_proposal(owner, &signer)
            .await
            .unwrap();
        // Timestamp too far in the future
        assert_matches!(
            env.worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::InvalidTimestamp)
        );
    }

    let block_0_time = Timestamp::from(TEST_GRACE_PERIOD_MICROS);
    let certificate = {
        let block = make_first_block(chain_id).with_timestamp(block_0_time);
        let block_proposal = block
            .clone()
            .into_first_proposal(owner, &signer)
            .await
            .unwrap();
        let future = env.worker().handle_block_proposal(block_proposal);
        clock.set(block_0_time);
        future.await?;

        let system_state = SystemExecutionState {
            committees: [(epoch, env.committee().clone())].into_iter().collect(),
            ownership: ChainOwnership::single(owner),
            balance,
            timestamp: block_0_time,
            ..SystemExecutionState::new(chain_1_desc.clone())
        };
        let state_hash = system_state.into_hash().await;
        let value = ConfirmedBlock::new(
            BlockExecutionOutcome {
                state_hash,
                ..BlockExecutionOutcome::default()
            }
            .with(block),
        );
        env.make_certificate(value)
    };
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    {
        let block_proposal = make_child_block(&certificate.into_value())
            .with_timestamp(block_0_time.saturating_sub_micros(1))
            .into_first_proposal(owner, &signer)
            .await
            .unwrap();
        // Timestamp older than previous one
        assert_matches!(
            env.worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(error))
                if matches!(*error, ChainError::InvalidBlockTimestamp)
        );
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_unknown_sender<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_public_key = signer.generate_new();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_public_key.into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let unknown_key = AccountSecretKey::generate();
    let unknown_owner = unknown_key.public().into();
    let new_signer: Box<dyn Signer> = Box::new(InMemorySigner::from_iter(vec![(
        unknown_owner,
        unknown_key,
    )]));
    let unknown_sender_block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(5))
        .into_first_proposal(unknown_owner, &new_signer)
        .await
        .unwrap();
    assert_matches!(
        env.worker()
            .handle_block_proposal(unknown_sender_block_proposal)
            .await,
        Err(WorkerError::InvalidOwner)
    );
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_with_chaining<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_public_key = signer.generate_new();
    let sender_owner = sender_public_key.into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_1 = chain_desc.id();
    let chain_2 = env.add_root_chain(2, sender_owner, Amount::ZERO).await.id();
    let block_proposal0 = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::ONE)
        .with_authenticated_signer(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    let certificate0 = env
        .make_simple_transfer_certificate(
            chain_desc.clone(),
            sender_public_key,
            chain_2,
            Amount::ONE,
            Vec::new(),
            Amount::from_tokens(4),
            None,
        )
        .await;
    let block_proposal1 = make_child_block(certificate0.value())
        .with_simple_transfer(chain_2, Amount::from_tokens(2))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    assert_matches!(
        env.worker().handle_block_proposal(block_proposal1.clone()).await,
        Err(WorkerError::ChainError(error)) if matches!(
            *error,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: BlockHeight(0),
                found_block_height: BlockHeight(1)
            })
    );
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());

    drop(chain);
    env.worker()
        .handle_block_proposal(block_proposal0.clone())
        .await?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    let block = chain.manager.validated_vote().unwrap().value().block();
    // Multi-leader round - it's not confirmed yet.
    assert!(block.matches_proposed_block(&block_proposal0.content.block));
    assert!(chain.manager.confirmed_vote().is_none());
    let block_certificate0 =
        env.make_certificate(chain.manager.validated_vote().unwrap().value().clone());
    drop(chain);
    env.worker()
        .handle_validated_certificate(block_certificate0)
        .await?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    let block = chain.manager.confirmed_vote().unwrap().value().block();
    // Should be confirmed after handling the certificate.
    assert!(block.matches_proposed_block(&block_proposal0.content.block));
    assert!(chain.manager.validated_vote().is_none());
    drop(chain);

    env.worker()
        .handle_confirmed_certificate(certificate0, None)
        .await?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    drop(chain);
    env.worker()
        .handle_block_proposal(block_proposal1.clone())
        .await?;

    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    let block = chain.manager.validated_vote().unwrap().value().block();
    assert!(block.matches_proposed_block(&block_proposal1.content.block));
    assert!(chain.manager.confirmed_vote().is_none());
    drop(chain);
    assert_matches!(
        env.worker().handle_block_proposal(block_proposal0).await,
        Err(WorkerError::ChainError(error)) if matches!(
            *error,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: BlockHeight(1),
                found_block_height: BlockHeight(0),
            })
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_with_incoming_bundles<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_public_key = signer.generate_new();
    let sender_owner = sender_public_key.into();
    let recipient_public_key = signer.generate_new();
    let recipient_owner = recipient_public_key.into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(6))
        .await;
    let chain_2_desc = env.add_root_chain(2, recipient_owner, Amount::ZERO).await;
    let chain_3_desc = env.add_root_chain(3, recipient_owner, Amount::ZERO).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let chain_3 = chain_3_desc.id();
    let certificate0 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![
                vec![direct_credit_message(chain_2, Amount::ONE)],
                vec![direct_credit_message(chain_2, Amount::from_tokens(2))],
            ],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new(); 2],
            blobs: vec![Vec::new(); 2],
            state_hash: SystemExecutionState {
                balance: Amount::from_tokens(3),
                ..SystemExecutionState::new(chain_1_desc.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new(); 2],
            operation_results: vec![OperationResult::default(); 2],
        }
        .with(
            make_first_block(chain_1)
                .with_simple_transfer(chain_2, Amount::ONE)
                .with_simple_transfer(chain_2, Amount::from_tokens(2))
                .with_authenticated_signer(Some(sender_owner)),
        ),
    ));

    let certificate1 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![direct_credit_message(chain_2, Amount::from_tokens(3))]],
            previous_message_blocks: BTreeMap::from([(chain_2, certificate0.hash())]),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: SystemExecutionState {
                balance: Amount::ZERO,
                ..SystemExecutionState::new(chain_1_desc.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_child_block(&certificate0.clone().into_value())
                .with_simple_transfer(chain_2, Amount::from_tokens(3))
                .with_authenticated_signer(Some(sender_owner)),
        ),
    ));
    // Missing earlier blocks
    assert_matches!(
        env.worker()
            .handle_confirmed_certificate(certificate1.clone(), None)
            .await,
        Err(WorkerError::MissingEarlierBlocks { .. })
    );

    // Run transfers
    let notifications = Arc::new(Mutex::new(Vec::new()));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &notifications)
        .await?;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &notifications)
        .await?;
    assert_eq!(
        *notifications.lock().unwrap(),
        vec![
            Notification {
                chain_id: chain_1,
                reason: NewBlock {
                    height: BlockHeight(0),
                    hash: certificate0.hash(),
                }
            },
            Notification {
                chain_id: chain_2,
                reason: NewIncomingBundle {
                    origin: chain_1,
                    height: BlockHeight(0)
                }
            },
            Notification {
                chain_id: chain_1,
                reason: NewBlock {
                    height: BlockHeight(1),
                    hash: certificate1.hash(),
                }
            },
            Notification {
                chain_id: chain_2,
                reason: NewIncomingBundle {
                    origin: chain_1,
                    height: BlockHeight(1)
                }
            }
        ]
    );
    {
        let block_proposal = make_first_block(chain_2)
            .with_simple_transfer(chain_3, Amount::from_tokens(6))
            .with_authenticated_signer(Some(recipient_owner))
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Insufficient funding
        assert_matches!(
                env.worker().handle_block_proposal(block_proposal).await,
                Err(
                    WorkerError::ChainError(error)
                ) if matches!(&*error, ChainError::ExecutionError(
                    execution_error, ChainExecutionContext::Operation(_)
                ) if matches!(**execution_error, ExecutionError::InsufficientFunding { .. }))
        );
    }
    {
        let block_proposal = make_first_block(chain_2)
            .with_simple_transfer(chain_3, Amount::from_tokens(5))
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::from_tokens(2)) // wrong amount
                            .to_posted(0, MessageKind::Tracked),
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_owner))
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Inconsistent received messages.
        assert_matches!(
            env.worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::UnexpectedMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(chain_2)
            .with_simple_transfer(chain_3, Amount::from_tokens(6))
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_owner))
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Skipped message.
        assert_matches!(
            env.worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::CannotSkipMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(chain_2)
            .with_simple_transfer(chain_3, Amount::from_tokens(6))
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(3))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_owner))
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Inconsistent order in received messages (heights).
        assert_matches!(
            env.worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(chain_error))
                if matches!(*chain_error, ChainError::CannotSkipMessage { .. })
        );
    }
    {
        let block_proposal = make_first_block(chain_2)
            .with_simple_transfer(chain_3, Amount::ONE)
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![
                        system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                    ],
                },
                action: MessageAction::Accept,
            })
            .with_authenticated_signer(Some(recipient_owner))
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Taking the first message only is ok.
        env.worker()
            .handle_block_proposal(block_proposal.clone())
            .await?;
        let certificate: ConfirmedBlockCertificate = env.make_certificate(ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages: vec![
                    Vec::new(),
                    vec![direct_credit_message(chain_3, Amount::ONE)],
                ],
                previous_message_blocks: BTreeMap::new(),
                events: vec![Vec::new(); 2],
                blobs: vec![Vec::new(); 2],
                state_hash: SystemExecutionState::new(chain_2_desc.clone())
                    .into_hash()
                    .await,
                oracle_responses: vec![Vec::new(); 2],
                operation_results: vec![OperationResult::default()],
            }
            .with(block_proposal.content.block),
        ));
        env.worker()
            .handle_confirmed_certificate(certificate.clone(), None)
            .await?;

        // Then receive the next two messages.
        let block_proposal = make_child_block(&certificate.into_value())
            .with_simple_transfer(chain_3, Amount::from_tokens(3))
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate0.hash(),
                    height: BlockHeight::from(0),
                    timestamp: Timestamp::from(0),
                    transaction_index: 1,
                    messages: vec![system_credit_message(Amount::from_tokens(2))
                        .to_posted(1, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate1.hash(),
                    height: BlockHeight::from(1),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(3))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            })
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        env.worker()
            .handle_block_proposal(block_proposal.clone())
            .await?;
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_exceed_balance<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_owner = signer.generate_new().into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(1000))
        .with_authenticated_signer(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    assert_matches!(
        env.worker().handle_block_proposal(block_proposal).await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
                execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::InsufficientFunding { .. }))
    );
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none());
    assert!(chain.manager.validated_vote().is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_owner = signer.generate_new().into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(5))
        .with_authenticated_signer(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    let (chain_info_response, _actions) =
        env.worker().handle_block_proposal(block_proposal).await?;
    chain_info_response.check(&env.worker().public_key())?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none()); // It was a multi-leader
                                                       // round.
    let validated_certificate =
        env.make_certificate(chain.manager.validated_vote().unwrap().value().clone());
    drop(chain);

    let (chain_info_response, _actions) = env
        .worker()
        .handle_validated_certificate(validated_certificate)
        .await?;
    chain_info_response.check(&env.worker().public_key())?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.validated_vote().is_none()); // Should be confirmed by now.
    let pending_vote = chain.manager.confirmed_vote().unwrap().lite();
    assert_eq!(
        chain_info_response.info.manager.pending.unwrap(),
        pending_vote
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_replay<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_owner = signer.generate_new().into();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(5))
        .with_authenticated_signer(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    let (response, _actions) = env
        .worker()
        .handle_block_proposal(block_proposal.clone())
        .await?;
    response.check(&env.worker().public_key())?;
    let (replay_response, _actions) = env.worker().handle_block_proposal(block_proposal).await?;
    // Workaround lack of equality.
    assert_eq!(
        CryptoHash::new(&*response.info),
        CryptoHash::new(&*replay_response.info)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_unknown_sender<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_pubkey = signer.generate_new();
    let test_pubkey = signer.generate_new();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_2_desc = env
        .add_root_chain(2, test_pubkey.into(), Amount::ZERO)
        .await;
    let chain_2 = chain_2_desc.id();
    let chain_1_desc = dummy_chain_description(1);
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            chain_2,
            Amount::from_tokens(5),
            Vec::new(),
            Amount::from_tokens(5),
            None,
        )
        .await;
    assert_matches!(
        env.worker()
            .fully_handle_certificate_with_notifications(certificate, &())
            .await,
        Err(WorkerError::BlobsNotFound(error))
            if error == vec![Blob::new_chain_description(&chain_1_desc).id()]
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_open_chain<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let balance = Amount::from_tokens(42);
    let description = env
        .add_child_chain(chain_2_desc.id(), sender_key_pair.public().into(), balance)
        .await;
    let chain_id = description.id();
    let state = SystemExecutionState::new(description);
    let value = ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![],
            previous_message_blocks: BTreeMap::new(),
            events: vec![],
            blobs: vec![],
            state_hash: state.into_hash().await,
            oracle_responses: vec![],
            operation_results: vec![],
        }
        .with(make_first_block(chain_id)),
    );
    let certificate = env.make_certificate(value);
    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?
        .info;
    assert_eq!(info.next_block_height, BlockHeight::from(1));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_wrong_owner<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let chain_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_2_desc = env
        .add_root_chain(2, chain_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2 = chain_2_desc.id();
    let certificate = env
        .make_transfer_certificate_for_epoch(
            chain_2_desc.clone(),
            sender_key_pair.public(),
            chain_key_pair.public().into(),
            AccountOwner::CHAIN,
            Recipient::chain(chain_2),
            Amount::from_tokens(5),
            Vec::new(),
            Epoch::ZERO,
            Amount::ZERO,
            BTreeMap::new(),
            None,
        )
        .await;
    // This fails because `make_simple_transfer_certificate` uses `sender_key_pair.public()` to
    // compute the hash of the execution state.
    assert_matches!(
        env.worker()
            .fully_handle_certificate_with_notifications(certificate, &())
            .await,
        Err(WorkerError::IncorrectOutcome { .. })
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_bad_block_height<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_2 = chain_2_desc.id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(5),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    // Replays are ignored.
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?;
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_anticipated_incoming_bundle<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_3_desc = env
        .add_root_chain(3, AccountPublicKey::test_key(3).into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let chain_3 = chain_3_desc.id();

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            key_pair.public(),
            chain_2,
            Amount::from_tokens(1000),
            vec![IncomingBundle {
                origin: chain_3,
                bundle: MessageBundle {
                    certificate_hash: CryptoHash::test_hash("certificate"),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(995))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            }],
            Amount::ZERO,
            None,
        )
        .await;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get());
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    let inbox = chain
        .inboxes
        .try_load_entry(&chain_3)
        .await?
        .expect("Missing inbox for `chain_3` in `ChainId::root(1)`");
    assert_eq!(BlockHeight::ZERO, inbox.next_block_height_to_receive()?);
    assert_eq!(inbox.added_bundles.count(), 0);
    assert_matches!(
        inbox
            .removed_bundles
            .front()
            .await?
            .unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == CryptoHash::test_hash("certificate")
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && matches!(messages[..], [PostedMessage {
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Tracked,
                index: 0,
                message: Message::System(SystemMessage::Credit { amount, .. }),
            }] if amount == Amount::from_tokens(995)),
        "Unexpected bundle",
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
    let chain = env.worker().chain_state_view(chain_2).await?;
    assert!(chain.is_active());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_receiver_balance_overflow<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::ONE)
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::MAX)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            sender_key_pair.public(),
            chain_2,
            Amount::ONE,
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let new_sender_chain = env.worker().chain_state_view(chain_1).await?;
    assert!(new_sender_chain.is_active());
    assert_eq!(
        Amount::ZERO,
        *new_sender_chain.execution_state.system.balance.get()
    );
    assert_eq!(
        BlockHeight::from(1),
        new_sender_chain.tip_state.get().next_block_height
    );
    assert_eq!(new_sender_chain.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash()),
        new_sender_chain.tip_state.get().block_hash
    );
    let new_recipient_chain = env.worker().chain_state_view(chain_2).await?;
    assert!(new_recipient_chain.is_active());
    assert_eq!(
        Amount::MAX,
        *new_recipient_chain.execution_state.system.balance.get()
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_receiver_equal_sender<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let key_pair = AccountSecretKey::generate();
    let owner = key_pair.public().into();
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::ONE).await;
    let chain_1 = chain_1_desc.id();

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            key_pair.public(),
            chain_1,
            Amount::ONE,
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let chain = env.worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ZERO, *chain.execution_state.system.balance.get());
    let inbox = chain
        .inboxes
        .try_load_entry(&chain_1)
        .await?
        .expect("Missing inbox for `ChainId::root(1)` in `ChainId::root(1)`");
    assert_eq!(BlockHeight::from(1), inbox.next_block_height_to_receive()?);
    assert_matches!(
        inbox.added_bundles.front().await?.unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == certificate.hash()
        && height == BlockHeight::ZERO
        && timestamp == Timestamp::from(0)
        && matches!(messages[..], [PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            index: 0,
            message: Message::System(SystemMessage::Credit { amount, .. })
        }] if amount == Amount::ONE),
        "Unexpected bundle",
    );
    assert_eq!(
        BlockHeight::from(1),
        chain.tip_state.get().next_block_height
    );
    assert_eq!(chain.confirmed_log.count(), 1);
    assert_eq!(Some(certificate.hash()), chain.tip_state.get().block_hash);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ONE)
        .await;
    let chain_2 = chain_2_desc.id();
    let chain_1_desc = dummy_chain_description(1);
    let chain_1 = chain_1_desc.id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc,
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    env.worker()
        .handle_cross_chain_request(update_recipient_direct(chain_2, &certificate.clone()))
        .await?;
    let chain = env.worker().chain_state_view(chain_2).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ONE, *chain.execution_state.system.balance.get());
    assert_eq!(BlockHeight::ZERO, chain.tip_state.get().next_block_height);
    let inbox = chain
        .inboxes
        .try_load_entry(&chain_1)
        .await?
        .expect("Missing inbox for `ChainId::root(1)` in `chain_2`");
    assert_eq!(BlockHeight::from(1), inbox.next_block_height_to_receive()?);
    assert_matches!(
        inbox
            .added_bundles
            .front()
            .await?
            .unwrap(),
        MessageBundle {
            certificate_hash,
            height,
            timestamp,
            transaction_index: 0,
            messages,
        } if certificate_hash == certificate.hash()
        && height == BlockHeight::ZERO
        && timestamp == Timestamp::from(0)
        && matches!(messages[..], [PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Tracked,
            index: 0,
            message: Message::System(SystemMessage::Credit { amount, .. })
        }] if amount == Amount::from_tokens(10)),
        "Unexpected bundle",
    );
    assert_eq!(chain.confirmed_log.count(), 0);
    assert_eq!(None, chain.tip_state.get().block_hash);
    assert_eq!(chain.received_log.count(), 1);
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(10))
        .await;
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc,
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    assert!(env
        .worker()
        .handle_cross_chain_request(update_recipient_direct(chain_2, &certificate))
        .await?
        .cross_chain_requests
        .is_empty());
    let chain = env.worker().chain_state_view(chain_2).await?;
    // The target chain did not receive the message
    assert!(chain.inboxes.indices().await?.is_empty());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_cross_chain_request_no_recipient_chain_on_client<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage, true, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(10))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc,
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;
    // An inactive target chain is created and it acknowledges the message.
    let actions = env
        .worker()
        .handle_cross_chain_request(update_recipient_direct(chain_2, &certificate))
        .await?;
    assert_matches!(
        actions.cross_chain_requests.as_slice(),
        &[CrossChainRequest::ConfirmUpdatedRecipient { .. }]
    );
    assert_eq!(
        actions.notifications,
        vec![Notification {
            chain_id: chain_2,
            reason: Reason::NewIncomingBundle {
                origin: chain_1,
                height: BlockHeight::ZERO,
            }
        }]
    );
    let chain = env.worker().chain_state_view(chain_2).await?;
    assert!(!chain.inboxes.indices().await?.is_empty());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_to_active_recipient<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let recipient_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, recipient_key_pair.public().into(), Amount::ZERO)
        .await;
    let chain_3_desc = env
        .add_root_chain(3, recipient_key_pair.public().into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let chain_3 = chain_3_desc.id();
    assert_eq!(
        env.worker()
            .query_application(chain_1, Query::System(SystemQuery))
            .await?,
        QueryOutcome {
            response: QueryResponse::System(SystemResponse {
                chain_id: chain_1,
                balance: Amount::from_tokens(5),
            }),
            operations: vec![],
        }
    );
    assert_eq!(
        env.worker()
            .query_application(chain_2, Query::System(SystemQuery))
            .await?,
        QueryOutcome {
            response: QueryResponse::System(SystemResponse {
                chain_id: chain_2,
                balance: Amount::ZERO,
            }),
            operations: vec![],
        }
    );

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(5),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;

    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?
        .info;
    assert_eq!(chain_1, info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    assert_eq!(
        env.worker()
            .query_application(chain_1, Query::System(SystemQuery))
            .await?,
        QueryOutcome {
            response: QueryResponse::System(SystemResponse {
                chain_id: chain_1,
                balance: Amount::ZERO,
            }),
            operations: vec![],
        }
    );

    // Try to use the money. This requires selecting the incoming message in a next block.
    let certificate = env
        .make_simple_transfer_certificate(
            chain_2_desc.clone(),
            recipient_key_pair.public(),
            chain_3,
            Amount::ONE,
            vec![IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(5))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            }],
            Amount::from_tokens(4),
            None,
        )
        .await;
    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    assert_eq!(
        env.worker()
            .query_application(chain_2, Query::System(SystemQuery))
            .await?,
        QueryOutcome {
            response: QueryResponse::System(SystemResponse {
                chain_id: chain_2,
                balance: Amount::from_tokens(4),
            }),
            operations: vec![],
        }
    );

    {
        let recipient_chain = env.worker().chain_state_view(chain_2).await?;
        assert!(recipient_chain.is_active());
        assert_eq!(
            *recipient_chain.execution_state.system.balance.get(),
            Amount::from_tokens(4)
        );
        let ownership = &recipient_chain.manager.ownership.get();
        assert!(
            ownership
                .owners
                .contains_key(&recipient_key_pair.public().into())
                && ownership.super_owners.is_empty()
                && ownership.owners.len() == 1
        );
        assert_eq!(recipient_chain.confirmed_log.count(), 1);
        assert_eq!(
            recipient_chain.tip_state.get().block_hash,
            Some(certificate.hash())
        );
        assert_eq!(recipient_chain.received_log.count(), 1);
    }
    let query = ChainInfoQuery::new(chain_2).with_received_log_excluding_first_n(0);
    let (response, _actions) = env.worker().handle_chain_info_query(query).await?;
    assert_eq!(response.info.requested_received_log.len(), 1);
    assert_eq!(
        response.info.requested_received_log[0],
        ChainAndHeight {
            chain_id: chain_1,
            height: BlockHeight::ZERO
        }
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_to_inactive_recipient<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.clone(),
            sender_key_pair.public(),
            chain_2, // the recipient chain does not exist
            Amount::from_tokens(5),
            Vec::new(),
            Amount::ZERO,
            None,
        )
        .await;

    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?
        .info;
    assert_eq!(chain_1, info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_certificate_with_rejected_transfer<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let sender_pubkey = sender_key_pair.public();
    let sender = AccountOwner::from(sender_pubkey);

    let recipient_key_pair = AccountSecretKey::generate();
    let recipient_pubkey = recipient_key_pair.public();
    let recipient = AccountOwner::from(recipient_pubkey);

    let mut env = TestEnvironment::new(storage_builder.build().await?, false, false).await;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(6))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, recipient_key_pair.public().into(), Amount::ZERO)
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    let sender_account = Account {
        chain_id: chain_1,
        owner: sender,
    };
    let recipient_account = Account {
        chain_id: chain_2,
        owner: recipient,
    };

    // First move the money from the public balance to the sender's account.
    // This takes two certificates (sending, receiving) sadly.
    let certificate00 = env
        .make_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            sender,
            AccountOwner::CHAIN,
            Recipient::Account(sender_account),
            Amount::from_tokens(5),
            Vec::new(),
            Amount::ONE,
            BTreeMap::new(),
            None,
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate00.clone(), &())
        .await?;

    let certificate01 = env
        .make_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            sender,
            AccountOwner::CHAIN,
            Recipient::Burn,
            Amount::ONE,
            vec![IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: certificate00.hash(),
                    height: BlockHeight::from(0),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![Message::System(SystemMessage::Credit {
                        source: AccountOwner::CHAIN,
                        target: sender,
                        amount: Amount::from_tokens(5),
                    })
                    .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            }],
            Amount::ZERO,
            BTreeMap::from_iter([(sender, Amount::from_tokens(5))]),
            Some(&certificate00),
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate01.clone(), &())
        .await?;

    {
        let chain = env.worker.chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
    }

    // Then, make two transfers to the recipient.
    let certificate1 = env
        .make_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            sender,
            sender,
            Recipient::Account(recipient_account),
            Amount::from_tokens(3),
            Vec::new(),
            Amount::ZERO,
            BTreeMap::from_iter([(sender, Amount::from_tokens(2))]),
            Some(&certificate01),
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    let certificate2 = env
        .make_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            sender,
            sender,
            Recipient::Account(recipient_account),
            Amount::from_tokens(2),
            Vec::new(),
            Amount::ZERO,
            BTreeMap::new(),
            Some(&certificate1),
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    // Reject the first transfer and try to use the money of the second one.
    let certificate = env
        .make_transfer_certificate(
            chain_2_desc.clone(),
            recipient_pubkey,
            recipient,
            recipient,
            Recipient::Burn,
            Amount::ONE,
            vec![
                IncomingBundle {
                    origin: chain_1,
                    bundle: MessageBundle {
                        certificate_hash: certificate1.hash(),
                        height: BlockHeight::from(2),
                        timestamp: Timestamp::from(0),
                        transaction_index: 0,
                        messages: vec![Message::System(SystemMessage::Credit {
                            source: sender,
                            target: recipient,
                            amount: Amount::from_tokens(3),
                        })
                        .to_posted(0, MessageKind::Tracked)],
                    },
                    action: MessageAction::Reject,
                },
                IncomingBundle {
                    origin: chain_1,
                    bundle: MessageBundle {
                        certificate_hash: certificate2.hash(),
                        height: BlockHeight::from(3),
                        timestamp: Timestamp::from(0),
                        transaction_index: 0,
                        messages: vec![Message::System(SystemMessage::Credit {
                            source: sender,
                            target: recipient,
                            amount: Amount::from_tokens(2),
                        })
                        .to_posted(0, MessageKind::Tracked)],
                    },
                    action: MessageAction::Accept,
                },
            ],
            Amount::ZERO,
            BTreeMap::from_iter([(recipient, Amount::from_tokens(1))]),
            None,
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    {
        let chain = env.worker().chain_state_view(chain_2).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
    }

    // Process the bounced message and try to use the refund.
    let certificate3 = env
        .make_transfer_certificate(
            chain_1_desc.clone(),
            sender_pubkey,
            sender,
            sender,
            Recipient::Burn,
            Amount::from_tokens(3),
            vec![IncomingBundle {
                origin: chain_2,
                bundle: MessageBundle {
                    certificate_hash: certificate.hash(),
                    height: BlockHeight::from(0),
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![Message::System(SystemMessage::Credit {
                        source: sender,
                        target: recipient,
                        amount: Amount::from_tokens(3),
                    })
                    .to_posted(0, MessageKind::Bouncing)],
                },
                action: MessageAction::Accept,
            }],
            Amount::ZERO,
            BTreeMap::new(),
            Some(&certificate2),
        )
        .await;

    env.worker()
        .fully_handle_certificate_with_notifications(certificate3.clone(), &())
        .await?;

    {
        let chain = env.worker.chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        chain.validate_incoming_bundles().await?;
    }
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn run_test_chain_creation_with_committee_creation<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let mut env =
        TestEnvironment::new_with_amount(storage.clone(), false, false, Amount::from_tokens(2))
            .await;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = env.admin_id();
    // Have the admin chain create a user chain.
    let user_description = env
        .add_child_chain(admin_id, env.admin_public_key().into(), Amount::ZERO)
        .await;
    let user_id = user_description.id();
    let certificate0 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![]],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![vec![Blob::new_chain_description(&user_description)]],
            state_hash: SystemExecutionState {
                admin_id: Some(admin_id),
                ..SystemExecutionState::new(env.admin_description.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(admin_id)
                .with_operation(SystemOperation::OpenChain(OpenChainConfig {
                    ownership: ChainOwnership::single(env.admin_public_key().into()),
                    balance: Amount::ZERO,
                    application_permissions: Default::default(),
                }))
                .with_authenticated_signer(Some(env.admin_public_key().into())),
        ),
    ));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;
    {
        let admin_chain = env.worker().chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        admin_chain.validate_incoming_bundles().await?;
        assert_eq!(
            BlockHeight::from(1),
            admin_chain.tip_state.get().next_block_height
        );
        assert!(admin_chain.outboxes.indices().await?.is_empty());
        assert_eq!(
            *admin_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
    }

    // Create a new committee and transfer money before accepting the subscription.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let event_id = EventId {
        chain_id: admin_id,
        stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
        index: 1,
    };
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    // `PublishCommitteeBlob` is tested e.g. in `client_tests::test_change_voting_rights`, so we
    // just write it directly to storage here for simplicity.
    storage.write_blob(&committee_blob).await?;
    let blob_hash = committee_blob.id().hash;
    let certificate1 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![
                vec![],
                vec![direct_credit_message(user_id, Amount::from_tokens(2))],
            ],
            previous_message_blocks: BTreeMap::new(),
            events: vec![
                vec![Event {
                    stream_id: event_id.stream_id.clone(),
                    index: event_id.index,
                    value: bcs::to_bytes(&blob_hash).unwrap(),
                }],
                Vec::new(),
            ],
            blobs: vec![Vec::new(); 2],
            state_hash: SystemExecutionState {
                // The root chain knows both committees at the end.
                committees: committees2.clone(),
                used_blobs: BTreeSet::from([committee_blob.id()]),
                epoch: Some(Epoch::from(1)),
                admin_id: Some(admin_id),
                balance: Amount::ZERO,
                ..SystemExecutionState::new(env.admin_description.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![vec![OracleResponse::Blob(committee_blob.id())], vec![]],
            operation_results: vec![OperationResult::default(); 2],
        }
        .with(
            make_child_block(&certificate0.clone().into_value())
                .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    blob_hash,
                }))
                .with_simple_transfer(user_id, Amount::from_tokens(2)),
        ),
    ));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;
    {
        // The child is active and has not migrated yet.
        let user_chain = env.worker().chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
        assert_eq!(
            BlockHeight::ZERO,
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        user_chain.validate_incoming_bundles().await?;
        matches!(
            &user_chain
                .inboxes
                .try_load_entry(&admin_id)
                .await?
                .expect("Missing inbox for admin chain in user chain")
                .added_bundles
                .read_front(10)
                .await?[..],
            [bundle1]
            if matches!(bundle1.messages[..], [PostedMessage {
                message: Message::System(SystemMessage::Credit { .. }), ..
            }])
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 1);
    }
    // Make the child receive the pending messages.
    let certificate3 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![Vec::new(); 2],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new(); 2],
            blobs: vec![Vec::new(); 2],
            state_hash: SystemExecutionState {
                // Finally the child knows about both committees and has the money.
                committees: committees2.clone(),
                balance: Amount::from_tokens(2),
                used_blobs: BTreeSet::from([committee_blob.id()]),
                epoch: Some(Epoch::from(1)),
                ..SystemExecutionState::new(user_description)
            }
            .into_hash()
            .await,
            oracle_responses: vec![
                vec![],
                vec![
                    OracleResponse::Event(
                        EventId {
                            chain_id: admin_id,
                            stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
                            index: 1,
                        },
                        bcs::to_bytes(&blob_hash).unwrap(),
                    ),
                    OracleResponse::Blob(committee_blob.id()),
                ],
            ],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(user_id)
                .with_incoming_bundle(IncomingBundle {
                    origin: admin_id,
                    bundle: MessageBundle {
                        certificate_hash: certificate1.hash(),
                        height: BlockHeight::from(1),
                        timestamp: Timestamp::from(0),
                        transaction_index: 1,
                        messages: vec![system_credit_message(Amount::from_tokens(2))
                            .to_posted(0, MessageKind::Tracked)],
                    },
                    action: MessageAction::Accept,
                })
                .with_operation(SystemOperation::ProcessNewEpoch(Epoch::from(1))),
        ),
    ));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate3, &())
        .await?;
    {
        let user_chain = env.worker().chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
        assert_eq!(
            BlockHeight::from(1),
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.admin_id.get(),
            Some(admin_id)
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 2);
        user_chain.validate_incoming_bundles().await?;
        Ok(())
    }
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfers_and_committee_creation<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let owner1 = AccountSecretKey::generate().public().into();
    let storage = storage_builder.build().await?;
    let mut env = TestEnvironment::new(storage.clone(), false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner1, Amount::from_tokens(3)).await;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = env.admin_id();
    let user_id = chain_1_desc.id();

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![direct_credit_message(admin_id, Amount::ONE)]],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: SystemExecutionState {
                committees: committees.clone(),
                ownership: ChainOwnership::single(owner1),
                balance: Amount::from_tokens(2),
                ..SystemExecutionState::new(chain_1_desc.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(user_id)
                .with_simple_transfer(admin_id, Amount::ONE)
                .with_authenticated_signer(Some(owner1)),
        ),
    ));
    // Have the admin chain create a new epoch without retiring the old one.
    let committees2 = BTreeMap::from_iter([
        (Epoch::ZERO, committee.clone()),
        (Epoch::from(1), committee.clone()),
    ]);
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    let blob_hash = committee_blob.id().hash;
    storage.write_blob(&committee_blob).await?;
    let certificate1 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![]],
            previous_message_blocks: BTreeMap::new(),
            events: vec![vec![Event {
                stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
                index: 1,
                value: bcs::to_bytes(&committee_blob.id().hash).unwrap(),
            }]],
            blobs: vec![Vec::new()],
            state_hash: SystemExecutionState {
                committees: committees2.clone(),
                used_blobs: BTreeSet::from([committee_blob.id()]),
                admin_id: Some(admin_id),
                epoch: Some(Epoch::from(1)),
                ..SystemExecutionState::new(env.admin_description.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![vec![OracleResponse::Blob(committee_blob.id())]],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(admin_id).with_operation(SystemOperation::Admin(
                AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    blob_hash,
                },
            )),
        ),
    ));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    // Try to execute the transfer.
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    // The transfer was started..
    let user_chain = env.worker().chain_state_view(user_id).await?;
    assert!(user_chain.is_active());
    assert_eq!(
        BlockHeight::from(1),
        user_chain.tip_state.get().next_block_height
    );
    assert_eq!(
        *user_chain.execution_state.system.balance.get(),
        Amount::from_tokens(2)
    );
    assert_eq!(
        *user_chain.execution_state.system.epoch.get(),
        Some(Epoch::ZERO)
    );

    // .. and the message has gone through.
    let admin_chain = env.worker().chain_state_view(admin_id).await?;
    assert!(admin_chain.is_active());
    assert_eq!(admin_chain.inboxes.indices().await?.len(), 1);
    matches!(
        &admin_chain
            .inboxes
            .try_load_entry(&user_id)
            .await?
            .expect("Missing inbox for user chain in admin chain")
            .added_bundles
            .read_front(10)
            .await?[..],
        [bundle] if matches!(bundle.messages[..], [PostedMessage {
            message: Message::System(SystemMessage::Credit { .. }),
            ..
        }])
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_transfers_and_committee_removal<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let mut env =
        TestEnvironment::new_with_amount(storage.clone(), false, false, Amount::ZERO).await;
    let owner1 = AccountSecretKey::generate().public().into();
    let chain_1_desc = env.add_root_chain(1, owner1, Amount::from_tokens(3)).await;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_id = env.admin_id();
    let user_id = chain_1_desc.id();

    // Have the user chain start a transfer to the admin chain.
    let certificate0 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![direct_credit_message(admin_id, Amount::ONE)]],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: SystemExecutionState {
                balance: Amount::from_tokens(2),
                ..SystemExecutionState::new(chain_1_desc.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(user_id)
                .with_simple_transfer(admin_id, Amount::ONE)
                .with_authenticated_signer(Some(owner1)),
        ),
    ));
    // Have the admin chain create a new epoch and retire the old one immediately.
    let committees3 = BTreeMap::from_iter([(Epoch::from(1), committee.clone())]);
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    let blob_hash = committee_blob.id().hash;
    storage.write_blob(&committee_blob).await?;

    let certificate1 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![]; 2],
            previous_message_blocks: BTreeMap::new(),
            events: vec![
                vec![Event {
                    stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
                    index: 1,
                    value: bcs::to_bytes(&committee_blob.id().hash).unwrap(),
                }],
                vec![Event {
                    stream_id: StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                    index: 0,
                    value: Vec::new(),
                }],
            ],
            blobs: vec![Vec::new(); 2],
            state_hash: SystemExecutionState {
                committees: committees3.clone(),
                used_blobs: BTreeSet::from([committee_blob.id()]),
                epoch: Some(Epoch::from(1)),
                admin_id: Some(admin_id),
                ..SystemExecutionState::new(env.admin_description.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![vec![OracleResponse::Blob(committee_blob.id())], vec![]],
            operation_results: vec![OperationResult::default(); 2],
        }
        .with(
            make_first_block(admin_id)
                .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
                    epoch: Epoch::from(1),
                    blob_hash,
                }))
                .with_operation(SystemOperation::Admin(AdminOperation::RemoveCommittee {
                    epoch: Epoch::ZERO,
                })),
        ),
    ));

    env.worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    // Try to execute the transfer from the user chain to the admin chain.
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    {
        // The transfer was started..
        let user_chain = env.worker().chain_state_view(user_id).await?;
        assert!(user_chain.is_active());
        assert_eq!(
            BlockHeight::from(1),
            user_chain.tip_state.get().next_block_height
        );
        assert_eq!(
            *user_chain.execution_state.system.balance.get(),
            Amount::from_tokens(2)
        );
        assert_eq!(
            *user_chain.execution_state.system.epoch.get(),
            Some(Epoch::ZERO)
        );

        // .. but the message hasn't gone through.
        let admin_chain = env.worker().chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        assert!(admin_chain.inboxes.indices().await?.is_empty());
    }

    // Force the admin chain to receive the money nonetheless by anticipation.
    let certificate2 = env.make_certificate(ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![Vec::new()],
            previous_message_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: SystemExecutionState {
                committees: committees3.clone(),
                balance: Amount::ONE,
                used_blobs: BTreeSet::from([committee_blob.id()]),
                admin_id: Some(admin_id),
                epoch: Some(Epoch::from(1)),
                ..SystemExecutionState::new(env.admin_description.clone())
            }
            .into_hash()
            .await,
            oracle_responses: vec![Vec::new()],
            operation_results: vec![],
        }
        .with(
            make_child_block(&certificate1.into_value())
                .with_epoch(1)
                .with_incoming_bundle(IncomingBundle {
                    origin: user_id,
                    bundle: MessageBundle {
                        certificate_hash: certificate0.hash(),
                        height: BlockHeight::ZERO,
                        timestamp: Timestamp::from(0),
                        transaction_index: 0,
                        messages: vec![
                            system_credit_message(Amount::ONE).to_posted(0, MessageKind::Tracked)
                        ],
                    },
                    action: MessageAction::Accept,
                }),
        ),
    ));
    env.worker()
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    {
        // The admin chain has an anticipated message.
        let admin_chain = env.worker().chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        assert_matches!(
            admin_chain.validate_incoming_bundles().await,
            Err(ChainError::MissingCrossChainUpdate { .. })
        );
    }

    // Try again to execute the transfer from the user chain to the admin chain.
    // This time, the epoch verification should be overruled.
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    {
        // The admin chain has no more anticipated messages.
        let admin_chain = env.worker().chain_state_view(admin_id).await?;
        assert!(admin_chain.is_active());
        admin_chain.validate_incoming_bundles().await?;
    }
    Ok(())
}

#[test(tokio::test)]
async fn test_cross_chain_helper() -> anyhow::Result<()> {
    let store_config = MemoryStore::new_test_config().await?;
    let namespace = generate_test_namespace();
    let store = DbStorage::<MemoryStore, _>::new_for_testing(
        store_config,
        &namespace,
        None,
        TestClock::new(),
    )
    .await?;
    let env = TestEnvironment::new(store, true, false).await;
    let committees = BTreeMap::from([(Epoch::from(1), env.committee().clone())]);

    let chain_0 = env.admin_description.clone();
    let chain_1 = dummy_chain_description(1);

    let key_pair0 = AccountSecretKey::generate();
    let id0 = chain_0.id();
    let id1 = chain_1.id();

    let certificate0 = env
        .make_transfer_certificate_for_epoch(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Recipient::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            None,
        )
        .await;
    let certificate1 = env
        .make_transfer_certificate_for_epoch(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Recipient::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            Some(&certificate0),
        )
        .await;
    let certificate2 = env
        .make_transfer_certificate_for_epoch(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Recipient::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::from(1),
            Amount::ONE,
            BTreeMap::new(),
            Some(&certificate1),
        )
        .await;
    // Weird case: epoch going backward.
    let certificate3 = env
        .make_transfer_certificate_for_epoch(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Recipient::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            Some(&certificate2),
        )
        .await;
    let bundles0 = certificate0.message_bundles_for(id1).collect::<Vec<_>>();
    let bundles1 = certificate1.message_bundles_for(id1).collect::<Vec<_>>();
    let bundles2 = certificate2.message_bundles_for(id1).collect::<Vec<_>>();
    let bundles3 = certificate3.message_bundles_for(id1).collect::<Vec<_>>();
    let bundles01 = Vec::from_iter(bundles0.iter().cloned().chain(bundles1.iter().cloned()));
    let bundles012 = Vec::from_iter(bundles01.iter().cloned().chain(bundles2.iter().cloned()));
    let bundles0123 = Vec::from_iter(bundles012.iter().cloned().chain(bundles3.iter().cloned()));

    fn without_epochs<'a>(
        bundles: impl IntoIterator<Item = &'a (Epoch, MessageBundle)>,
    ) -> Vec<MessageBundle> {
        bundles
            .into_iter()
            .map(|(_, bundle)| bundle.clone())
            .collect()
    }

    let helper = CrossChainUpdateHelper {
        allow_messages_from_deprecated_epochs: true,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is not tested when `allow_messages_from_deprecated_epochs` is true.
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::ZERO, None, bundles01.clone())?,
        without_epochs(&bundles01)
    );
    // Received heights is removing prefixes.
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::from(1), None, bundles01.clone())?,
        without_epochs(&bundles1)
    );
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::from(2), None, bundles01.clone())?,
        vec![]
    );
    // Order of certificates is checked.
    assert_matches!(
        helper.select_message_bundles(
            &id0,
            id1,
            BlockHeight::ZERO,
            None,
            Vec::from_iter(bundles1.iter().cloned().chain(bundles0.iter().cloned()))
        ),
        Err(WorkerError::InvalidCrossChainRequest)
    );

    let helper = CrossChainUpdateHelper {
        allow_messages_from_deprecated_epochs: false,
        current_epoch: Some(Epoch::from(1)),
        committees: &committees,
    };
    // Epoch is tested when `allow_messages_from_deprecated_epochs` is false.
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::ZERO, None, bundles01.clone())?,
        vec![]
    );
    // A certificate with a recent epoch certifies all the previous blocks.
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::ZERO, None, bundles0123.clone())?,
        without_epochs(&bundles012)
    );
    // Received heights is still removing prefixes.
    assert_eq!(
        helper.select_message_bundles(&id0, id1, BlockHeight::from(1), None, bundles012.clone())?,
        without_epochs(bundles1.iter().chain(&bundles2))
    );
    // Anticipated messages re-certify blocks up to the given height.
    assert_eq!(
        helper.select_message_bundles(
            &id0,
            id1,
            BlockHeight::from(1),
            Some(BlockHeight::from(1)),
            bundles01.clone()
        )?,
        without_epochs(&bundles1)
    );
    assert_eq!(
        helper.select_message_bundles(
            &id0,
            id1,
            BlockHeight::ZERO,
            Some(BlockHeight::from(1)),
            bundles01.clone()
        )?,
        without_epochs(&bundles01)
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_timeouts<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let mut signer = InMemorySigner::new(None);
    let clock = storage_builder.clock();
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let chain_1 = chain_1_desc.id();

    // Add another owner and use the leader-based protocol in all rounds.
    let proposed_block0 = make_first_block(chain_1)
        .with_operation(SystemOperation::ChangeOwnership {
            super_owners: Vec::new(),
            owners: vec![(owner0, 100), (owner1, 100)],
            multi_leader_rounds: 0,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        })
        .with_authenticated_signer(Some(owner0));
    let (block0, _) = env
        .worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The leader sequence is pseudorandom but deterministic. The first leader is owner 1.
    assert_eq!(response.info.manager.leader, Some(owner1));

    // So owner 0 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0.clone())
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(0))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal = make_child_block(&value0.clone())
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal).await;

    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::SingleLeader(0)))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_1).with_timeout();
    let (response, _) = env.worker().handle_chain_info_query(query).await?;
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_1).with_timeout();
    let (response, _) = env.worker().handle_chain_info_query(query).await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Timeout::new(chain_1, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts, where owner
    // 0 happens to be the leader.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));

    // Now owner 0 can propose a block, but owner 1 can't.
    let proposed_block1 = make_child_block(&value0.clone());
    let (block1, _) = env
        .worker()
        .stage_block_execution(proposed_block1.clone(), None, vec![])
        .await?;
    let proposal1_wrong_owner = proposed_block1
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let result = env
        .worker()
        .handle_block_proposal(proposal1_wrong_owner)
        .await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal1 = proposed_block1
        .clone()
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let (response, _) = env.worker().handle_block_proposal(proposal1).await?;
    let value1 = ValidatedBlock::new(block1.clone());

    // If we send the validated block certificate to the worker, it votes to confirm.
    let vote = response.info.manager.pending.clone().unwrap();
    let certificate1 = vote.with_value(value1.clone()).unwrap().into_certificate();
    let (response, _) = env
        .worker()
        .handle_validated_certificate(certificate1.clone())
        .await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    let value = ConfirmedBlock::new(block1.clone());
    assert_eq!(vote.value, LiteValue::new(&value));

    // Instead of submitting the confirmed block certificate, let rounds 2 to 4 time out, too.
    let certificate_timeout =
        env.make_certificate_with_round(value_timeout.clone(), Round::SingleLeader(4));
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner1));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(5));

    // Create block2, also at height 1, but different from block 1.
    let amount = Amount::from_tokens(1);
    let proposed_block2 = make_child_block(&value0.clone()).with_simple_transfer(chain_1, amount);
    let (block2, _) = env
        .worker()
        .stage_block_execution(proposed_block2.clone(), None, vec![])
        .await?;

    // Since round 3 is already over, the validator won't vote for a validated block from round 3.
    let value2 = ValidatedBlock::new(block2.clone());
    let certificate = env.make_certificate_with_round(value2.clone(), Round::SingleLeader(2));
    env.worker()
        .handle_validated_certificate(certificate)
        .await?;
    let query_values = ChainInfoQuery::new(chain_1).with_manager_values();
    let (response, _) = env
        .worker()
        .handle_chain_info_query(query_values.clone())
        .await?;
    let manager = response.info.manager;
    assert_eq!(
        manager.requested_confirmed.unwrap().block(),
        certificate1.block()
    );

    // Proposing block2 now would fail.
    let proposal = proposed_block2
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::SingleLeader(5))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal.clone()).await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasIncompatibleConfirmedVote(_, _))
    );

    // But with the validated block certificate for block2, it is allowed.
    let certificate2 = env.make_certificate_with_round(value2.clone(), Round::SingleLeader(4));

    let proposal = BlockProposal::new_retry(
        owner1,
        Round::SingleLeader(5),
        certificate2.clone(),
        &signer,
    )
    .await
    .unwrap();
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = env.worker().handle_block_proposal(proposal).await?;
    let (response, _) = env
        .worker()
        .handle_chain_info_query(query_values.clone())
        .await?;
    assert_eq!(
        response.info.manager.requested_locking,
        Some(Box::new(LockingBlock::Regular(certificate2)))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::SingleLeader(5));

    // Let round 5 time out, too.
    let certificate_timeout =
        env.make_certificate_with_round(value_timeout.clone(), Round::SingleLeader(5));
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(6));

    // Since the validator now voted for block2, it can't vote for block1 anymore.
    let proposal = proposed_block1
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(6))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal.clone()).await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasIncompatibleConfirmedVote(_, _))
    );

    // Let rounds 6 and 7 time out.
    let certificate_timeout =
        env.make_certificate_with_round(value_timeout, Round::SingleLeader(7));
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(8));

    // The worker updates its locking block even if it's from a past round.
    let certificate = env.make_certificate_with_round(value1, Round::SingleLeader(7));
    let worker = env.worker().clone().with_key_pair(None).await; // Forget validator keys.
    worker
        .handle_validated_certificate(certificate.clone())
        .await?;
    let (response, _) = worker.handle_chain_info_query(query_values).await?;
    assert_eq!(
        response.info.manager.requested_locking,
        Some(Box::new(LockingBlock::Regular(certificate)))
    );
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_round_types<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let mut signer = InMemorySigner::new(None);
    let clock = storage_builder.clock();
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let chain_id = chain_1_desc.id();

    // Add another owner and configure two multi-leader rounds.
    let proposed_block0 =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![owner0],
            owners: vec![(owner0, 100), (owner1, 100)],
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                ..TimeoutConfig::default()
            },
        });
    let (block0, _) = env
        .worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // So owner 1 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0)
        .into_proposal_with_round(owner1, &signer, Round::Fast)
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal = make_child_block(&value0)
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::Fast))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = env.worker().handle_chain_info_query(query).await?;
    assert!(response.info.manager.timeout_vote.is_none());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let query = ChainInfoQuery::new(chain_id).with_timeout();
    let (response, _) = env.worker().handle_chain_info_query(query).await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts.
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate();
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. And multi-leader rounds can be skipped without timeout.
    let block1 = make_child_block(&value0);
    let proposal1 = block1
        .clone()
        .with_authenticated_signer(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(1))
        .await
        .unwrap();
    let _ = env.worker().handle_block_proposal(proposal1).await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = env.worker().handle_chain_info_query(query_values).await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(1));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_open_multi_leader_rounds<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::from_tokens(2)).await;
    let chain_id = chain_1_desc.id();

    // Configure open multi-leader rounds.
    let change_ownership_block =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![],
            owners: vec![(owner, 100)],
            multi_leader_rounds: 2,
            open_multi_leader_rounds: true,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                ..TimeoutConfig::default()
            },
        });
    let (change_ownership_block, _) = env
        .worker()
        .stage_block_execution(change_ownership_block, None, vec![])
        .await?;
    let change_ownership_value = ConfirmedBlock::new(change_ownership_block);
    let change_ownership_certificate = env.make_certificate(change_ownership_value.clone());
    env.worker()
        .fully_handle_certificate_with_notifications(change_ownership_certificate, &())
        .await?;

    // The first round is the multi-leader round 0. Anyone is allowed to propose.
    // But non-owners are not allowed to transfer the chain's funds.
    let proposal = make_child_block(&change_ownership_value)
        .with_transfer(AccountOwner::CHAIN, Recipient::Burn, Amount::from_tokens(1))
        .into_proposal_with_round(owner, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(error)) if matches!(&*error,
        ChainError::ExecutionError(error, _) if matches!(&**error,
        ExecutionError::UnauthenticatedTransferOwner
    )));

    // Without the transfer, a random key pair can propose a block.
    let proposal = make_child_block(&change_ownership_value)
        .into_proposal_with_round(owner, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let (block, _) = env
        .worker()
        .stage_block_execution(proposal.content.block.clone(), None, vec![])
        .await?;
    let value = ConfirmedBlock::new(block);
    let (response, _) = env.worker().handle_block_proposal(proposal).await?;
    let vote = response.info.manager.pending.unwrap();
    assert_eq!(vote.round, Round::MultiLeader(0));
    assert_eq!(vote.value.value_hash, value.hash());
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_fast_proposal_is_locked<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let mut signer = InMemorySigner::new(None);
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(storage, false, false).await;
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let chain_id = chain_1_desc.id();

    // Add another owner and configure two multi-leader rounds.
    let proposed_block0 =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![owner0],
            owners: vec![(owner0, 100), (owner1, 100)],
            multi_leader_rounds: 3,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_millis(5)),
                ..TimeoutConfig::default()
            },
        });
    let (block0, _) = env
        .worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // Owner 0 proposes another block. The validator votes to confirm.
    let proposed_block1 = make_child_block(&value0.clone());
    let proposal1 = proposed_block1
        .clone()
        .into_proposal_with_round(owner0, &signer, Round::Fast)
        .await
        .unwrap();
    let (block1, _) = env
        .worker()
        .stage_block_execution(proposed_block1.clone(), None, vec![])
        .await?;
    let value1 = ConfirmedBlock::new(block1);
    let (response, _) = env.worker().handle_block_proposal(proposal1).await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::Fast);
    assert_eq!(vote.value.value_hash, value1.hash());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Once we provide the validator with a timeout certificate, the next round starts.
    let value_timeout = Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0));
    let certificate_timeout = env.make_certificate_with_round(value_timeout.clone(), Round::Fast);
    let (response, _) = env
        .worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. But block1 is locked. Re-proposing it is allowed.
    let proposal1b = proposed_block1
        .clone()
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let (response, _) = env.worker().handle_block_proposal(proposal1b).await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::MultiLeader(0));
    assert_eq!(vote.value.value_hash, value1.hash());

    // Proposing a different block is not.
    let proposed_block2 = make_child_block(&value0)
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_authenticated_signer(Some(owner1));
    let proposal2 = proposed_block2
        .clone()
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(1))
        .await
        .unwrap();
    let result = env.worker().handle_block_proposal(proposal2).await;
    assert_matches!(result, Err(WorkerError::ChainError(err))
        if matches!(*err, ChainError::HasIncompatibleConfirmedVote(_, Round::Fast))
    );
    let proposal3 = proposed_block1
        .clone()
        .into_proposal_with_round(owner0, &signer, Round::MultiLeader(2))
        .await
        .unwrap();
    env.worker().handle_block_proposal(proposal3).await?;

    // A validated block certificate from a later round can override the locked fast block.
    let (block2, _) = env
        .worker()
        .stage_block_execution(proposed_block2.clone(), None, vec![])
        .await?;
    let value2 = ValidatedBlock::new(block2.clone());
    let certificate2 = env.make_certificate_with_round(value2.clone(), Round::MultiLeader(0));
    let proposal =
        BlockProposal::new_retry(owner1, Round::MultiLeader(3), certificate2.clone(), &signer)
            .await
            .unwrap();
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = env.worker().handle_block_proposal(proposal).await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = env.worker().handle_chain_info_query(query_values).await?;
    assert_eq!(
        response.info.manager.requested_locking,
        Some(Box::new(LockingBlock::Regular(certificate2)))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::MultiLeader(3));
    Ok(())
}

#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_fallback<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let mut env = TestEnvironment::new(storage, false, false).await;
    let balance = Amount::from_tokens(5);
    let mut ownership = ChainOwnership::single(public_key.into());
    // Configure a fallback duration. (The default is `MAX`, i.e. never.)
    ownership.timeout_config.fallback_duration = TimeDelta::from_secs(5);
    let chain_1_desc = env
        .add_root_chain_with_ownership(1, balance, ownership)
        .await;
    let chain_id = chain_1_desc.id();

    // At time 0 we don't vote for fallback mode.
    let query = ChainInfoQuery::new(chain_id)
        .with_fallback()
        .with_committees();
    let (response, _) = env.worker().handle_chain_info_query(query.clone()).await?;
    let manager = response.info.manager;
    assert!(manager.fallback_vote.is_none());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    assert!(manager.leader.is_none());
    let fallback_duration = manager.ownership.timeout_config.fallback_duration;

    // Even if a long time passes: Without an incoming message there's no fallback mode.
    clock.add(fallback_duration);
    let (response, _) = env.worker().handle_chain_info_query(query.clone()).await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // Make a tracked message to ourselves. It's in the inbox now.
    let proposed_block = make_first_block(chain_id)
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_authenticated_signer(Some(public_key.into()));
    let (block, _) = env
        .worker()
        .stage_block_execution(proposed_block, None, vec![])
        .await?;
    let value = ConfirmedBlock::new(block);
    let certificate = env.make_certificate(value);
    env.worker()
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?;

    // The message only just arrived: No fallback mode.
    let (response, _) = env.worker().handle_chain_info_query(query.clone()).await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // If for a long time the message isn't handled, we vote for fallback mode.
    clock.add(fallback_duration);
    let (response, _) = env.worker().handle_chain_info_query(query.clone()).await?;
    let vote = response.info.manager.fallback_vote.unwrap();
    let value = Timeout::new(chain_id, BlockHeight(1), Epoch::ZERO);
    let round = Round::SingleLeader(u32::MAX);
    assert_eq!(vote.value.value_hash, value.hash());
    assert_eq!(vote.round, round);
    let certificate = env.make_certificate_with_round(value, round);
    env.worker().handle_timeout_certificate(certificate).await?;

    // Now we are in fallback mode, and the validator is the leader.
    let (response, _) = env.worker().handle_chain_info_query(query.clone()).await?;
    let manager = response.info.manager;
    let expected_key = response
        .info
        .requested_committees
        .unwrap()
        .get(&response.info.epoch.unwrap())
        .unwrap()
        .validators
        .get(&vote.public_key)
        .unwrap()
        .account_public_key;
    assert_eq!(manager.current_round, Round::Validator(0));
    assert_eq!(manager.leader, Some(AccountOwner::from(expected_key)));
    Ok(())
}

/// Tests if a service is able to handle more than one query without restarting.
///
/// If the service is restarted, a new [`MockApplicationInstance`] is created with an empty list of
/// expected calls, and the test fails because the first [`MockApplicationInstance`] still expects
/// some calls.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_long_lived_service<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    const NUM_QUERIES: usize = 5;

    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let mut env = TestEnvironment::new(storage, false, true).await;

    let chain_description = env
        .add_root_chain(
            1,
            AccountSecretKey::generate().public().into(),
            Amount::from_tokens(2),
        )
        .await;
    let chain_id = chain_description.id();

    let (application_id, application);
    {
        let mut chain = env.worker().storage.load_chain(chain_id).await?;
        (application_id, application, _) =
            chain.execution_state.register_mock_application(0).await?;
        chain.save().await?;
    }

    let query_times = (0..NUM_QUERIES as u64).map(Timestamp::from);
    let query_contexts = query_times.clone().map(|local_time| QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time,
    });

    for _ in query_contexts {
        application.expect_call(ExpectedCall::handle_query(move |_runtime, query| {
            assert!(query.is_empty());
            Ok(vec![])
        }));
    }

    let query = Query::User {
        application_id,
        bytes: vec![],
    };
    for query_time in query_times {
        clock.set(query_time);

        assert_eq!(
            env.worker()
                .query_application(chain_id, query.clone())
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    drop(env);
    linera_base::time::timer::sleep(Duration::from_millis(10)).await;
    application.assert_no_more_expected_calls();
    application.assert_no_active_instances();

    Ok(())
}

/// Tests if a service is restarted when a block is added to the chain.
///
/// A new block must force the service to restart, because the context will have changed and the
/// application state may have changed.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_new_block_causes_service_restart<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    const NUM_QUERIES: usize = 2;
    const BLOCK_TIMESTAMP: u64 = 10;

    let storage = storage_builder.build().await?;
    let clock = storage_builder.clock();
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let balance = Amount::ZERO;

    let mut env = TestEnvironment::new(storage.clone(), false, true).await;
    let chain_description = env.add_root_chain(1, owner, balance).await;
    let chain_id = chain_description.id();

    let (application_id, application);
    {
        let mut chain = storage.load_chain(chain_id).await?;
        (application_id, application, _) =
            chain.execution_state.register_mock_application(0).await?;
        chain.save().await?;
    }

    let queries_before_proposal = (0..NUM_QUERIES as u64).map(Timestamp::from);
    let queries_before_confirmation =
        (0..NUM_QUERIES as u64).map(|delta| Timestamp::from(NUM_QUERIES as u64 + delta));

    let queries_before_new_block = queries_before_proposal
        .clone()
        .chain(queries_before_confirmation.clone());
    let queries_after_new_block =
        (1..=NUM_QUERIES as u64).map(|delta| Timestamp::from(BLOCK_TIMESTAMP + delta));

    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    let query_contexts_before_new_block =
        queries_before_new_block
            .clone()
            .map(|local_time| QueryContext {
                chain_id,
                next_block_height: BlockHeight(0),
                local_time,
            });
    let query_contexts_after_new_block =
        queries_after_new_block
            .clone()
            .map(|local_time| QueryContext {
                chain_id,
                next_block_height: BlockHeight(1),
                local_time,
            });

    for _ in query_contexts_before_new_block.clone() {
        application.expect_call(ExpectedCall::handle_query(move |_runtime, query| {
            assert!(query.is_empty());
            Ok(vec![])
        }));
    }

    for local_time in queries_before_proposal {
        clock.set(local_time);

        assert_eq!(
            env.worker()
                .query_application(chain_id, query.clone())
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    clock.set(Timestamp::from(BLOCK_TIMESTAMP));
    let block = make_first_block(chain_id).with_timestamp(Timestamp::from(BLOCK_TIMESTAMP));

    let block_proposal = block
        .clone()
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    let _ = env.worker().handle_block_proposal(block_proposal).await?;

    for local_time in queries_before_confirmation {
        clock.set(local_time);

        assert_eq!(
            env.worker()
                .query_application(chain_id, query.clone())
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    let mut state = SystemExecutionState {
        timestamp: Timestamp::from(BLOCK_TIMESTAMP),
        ..SystemExecutionState::new(chain_description)
    }
    .into_view()
    .await;
    let _ = state.register_mock_application(0).await?;

    let value = ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![],
            previous_message_blocks: BTreeMap::new(),
            events: vec![],
            blobs: vec![],
            state_hash: state.crypto_hash_mut().await?,
            oracle_responses: vec![],
            operation_results: vec![],
        }
        .with(block),
    );
    let certificate = env.make_certificate(value);
    env.worker()
        .handle_confirmed_certificate(certificate, None)
        .await?;

    for _ in query_contexts_after_new_block.clone() {
        application.expect_call(ExpectedCall::handle_query(move |_runtime, query| {
            assert!(query.is_empty());
            Ok(vec![])
        }));
    }

    for local_time in queries_after_new_block {
        clock.set(local_time);

        assert_eq!(
            env.worker()
                .query_application(chain_id, query.clone())
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    drop(env);
    linera_base::time::timer::sleep(Duration::from_millis(10)).await;
    application.assert_no_more_expected_calls();
    application.assert_no_active_instances();

    Ok(())
}
