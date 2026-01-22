// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::large_futures)]

#[path = "./wasm_worker_tests.rs"]
mod wasm;

use std::{
    collections::BTreeMap,
    iter,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{
        AccountPublicKey, AccountSecretKey, AccountSignature, CryptoHash, InMemorySigner,
        ValidatorKeypair,
    },
    data_types::*,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, EventId, StreamId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_chain::{
    data_types::{
        BlockExecutionOutcome, BlockProposal, ChainAndHeight, IncomingBundle, LiteValue, LiteVote,
        MessageAction, MessageBundle, OperationResult, PostedMessage, ProposedBlock,
        SignatureAggregator, Transaction,
    },
    manager::LockingBlock,
    test::{make_child_block, make_first_block, BlockTestExt, MessageTestExt, VoteTestExt},
    types::{
        CertificateKind, CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate,
        GenericCertificate, Timeout, ValidatedBlock,
    },
    ChainError, ChainExecutionContext, ChainStateView,
};
use linera_execution::{
    committee::Committee,
    system::{
        AdminOperation, EpochEventData, OpenChainConfig, SystemMessage, SystemOperation,
        EPOCH_STREAM_NAME as NEW_EPOCH_STREAM_NAME, REMOVED_EPOCH_STREAM_NAME,
    },
    test_utils::{
        dummy_chain_description, ExpectedCall, MockApplication, RegisterMockApplication,
        SystemExecutionState,
    },
    ExecutionError, ExecutionRuntimeContext, Message, MessageKind, OutgoingMessage, Query,
    QueryContext, QueryOutcome, QueryResponse, SystemQuery, SystemResponse,
};
use linera_storage::Storage;
use linera_views::{context::Context, views::RootView};
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

struct TestEnvironment<S: Storage> {
    committee: Committee,
    // The main worker used for assertions.
    worker: WorkerState<S>,
    // This second worker is mostly used to create certificates that can then be handled by the
    // main worker, but some tests depend on the worker handling proposals to be a validator, so
    // they need to use this worker for everything.
    // The certificates have to be created by a worker due to the difficulty of computing
    // historical hashes for manually prepared certificates.
    executing_worker: WorkerState<S>,
    admin_keypair: AccountSecretKey,
    admin_description: ChainDescription,
    other_chains: BTreeMap<ChainId, ChainDescription>,
}

impl<S> TestEnvironment<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn new<B: StorageBuilder<Storage = S>>(
        builder: &mut B,
        is_client: bool,
        has_long_lived_services: bool,
    ) -> Result<Self, anyhow::Error> {
        Self::new_with_amount(
            builder,
            is_client,
            has_long_lived_services,
            Amount::from_tokens(1_000_000),
        )
        .await
    }

    async fn new_with_amount<B: StorageBuilder<Storage = S>>(
        builder: &mut B,
        is_client: bool,
        has_long_lived_services: bool,
        amount: Amount,
    ) -> Result<Self, anyhow::Error> {
        let validator_keypair = ValidatorKeypair::generate();
        let account_secret = AccountSecretKey::generate();
        let committee = Committee::make_simple(vec![(
            validator_keypair.public_key,
            account_secret.public(),
        )]);

        let origin = ChainOrigin::Root(0);
        let config = InitialChainConfig {
            balance: amount,
            ownership: ChainOwnership::single(account_secret.public().into()),
            epoch: Epoch::ZERO,
            min_active_epoch: Epoch::ZERO,
            max_active_epoch: Epoch::ZERO,
            application_permissions: Default::default(),
        };
        let admin_description = ChainDescription::new(origin, config, Timestamp::from(0));
        let committee_blob = Blob::new_committee(bcs::to_bytes(&committee).unwrap());

        let mut make_worker = async |keypair: ValidatorKeypair| {
            let storage = builder
                .build()
                .await
                .expect("building storage should not fail");
            storage
                .write_blob(&Blob::new_chain_description(&admin_description))
                .await
                .expect("writing a blob should not fail");
            storage
                .write_blob(&committee_blob)
                .await
                .expect("writing a blob should succeed");
            storage
                .write_network_description(&NetworkDescription {
                    admin_chain_id: admin_description.id(),
                    genesis_config_hash: CryptoHash::test_hash("genesis config"),
                    genesis_timestamp: Timestamp::from(0),
                    genesis_committee_blob_hash: committee_blob.id().hash,
                    name: "test network".to_string(),
                })
                .await
                .expect("writing a network description should not fail");

            WorkerState::new(
                "Single validator node".to_string(),
                Some(keypair.secret_key),
                storage,
                5_000,
                10_000,
            )
            .with_allow_inactive_chains(is_client)
            .with_allow_messages_from_deprecated_epochs(is_client)
            .with_long_lived_services(has_long_lived_services)
            .with_block_time_grace_period(Duration::from_micros(TEST_GRACE_PERIOD_MICROS))
        };

        let worker = make_worker(ValidatorKeypair::generate()).await;
        let executing_worker = make_worker(validator_keypair).await;

        Ok(Self {
            committee,
            worker,
            executing_worker,
            admin_description,
            admin_keypair: account_secret,
            other_chains: BTreeMap::new(),
        })
    }

    fn admin_chain_id(&self) -> ChainId {
        self.admin_description.id()
    }

    fn committee(&self) -> &Committee {
        &self.committee
    }

    fn worker(&self) -> &WorkerState<S> {
        &self.worker
    }

    fn executing_worker(&self) -> &WorkerState<S> {
        &self.executing_worker
    }

    fn admin_public_key(&self) -> AccountPublicKey {
        self.admin_keypair.public()
    }

    pub async fn write_blobs(&mut self, blobs: &[Blob]) -> Result<(), linera_views::ViewError> {
        self.worker.storage.write_blobs(blobs).await?;
        self.executing_worker.storage.write_blobs(blobs).await?;
        Ok(())
    }

    pub async fn register_mock_application(
        &mut self,
        chain_id: ChainId,
        index: u32,
    ) -> Result<(ApplicationId, MockApplication), anyhow::Error> {
        let mut chain = self.worker.storage.load_chain(chain_id).await?;
        chain
            .execution_state
            .register_mock_application(index)
            .await?;
        chain.save().await?;
        let mut chain = self.executing_worker.storage.load_chain(chain_id).await?;
        let (application_id, application, _) = chain
            .execution_state
            .register_mock_application(index)
            .await?;
        chain.save().await?;
        Ok((application_id, application))
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
            epoch: self.admin_description.config().epoch,
            ownership,
            min_active_epoch: self.admin_description.config().min_active_epoch,
            max_active_epoch: self.admin_description.config().max_active_epoch,
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
        self.executing_worker
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
            epoch: self.admin_description.config().epoch,
            ownership: ChainOwnership::single(owner),
            min_active_epoch: self.admin_description.config().min_active_epoch,
            max_active_epoch: self.admin_description.config().max_active_epoch,
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
        self.executing_worker
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
            self.executing_worker
                .chain_worker_config
                .key_pair()
                .unwrap(),
        );
        let mut builder = SignatureAggregator::new(value, round, &self.committee);
        builder
            .append(self.executing_worker.public_key(), vote.signature)
            .unwrap()
            .unwrap()
    }

    async fn make_simple_transfer_certificate(
        &mut self,
        chain_id: ChainId,
        chain_owner_pubkey: AccountPublicKey,
        target_id: ChainId,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        self.make_transfer_certificate_for_epoch(
            chain_id,
            chain_owner_pubkey.into(),
            AccountOwner::CHAIN,
            Account::chain(target_id),
            amount,
            incoming_bundles,
            Epoch::ZERO,
            previous_confirmed_block,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn make_transfer_certificate(
        &mut self,
        chain_id: ChainId,
        authenticated_owner: AccountOwner,
        source: AccountOwner,
        recipient: Account,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        self.make_transfer_certificate_for_epoch(
            chain_id,
            authenticated_owner,
            source,
            recipient,
            amount,
            incoming_bundles,
            Epoch::ZERO,
            previous_confirmed_block,
        )
        .await
    }

    /// Creates a certificate with a transfer.
    #[expect(clippy::too_many_arguments)]
    async fn make_transfer_certificate_for_epoch(
        &mut self,
        chain_id: ChainId,
        authenticated_owner: AccountOwner,
        source: AccountOwner,
        recipient: Account,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        epoch: Epoch,
        previous_confirmed_block: Option<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        let block = match previous_confirmed_block {
            None => make_first_block(chain_id),
            Some(cert) => make_child_block(cert.value()),
        }
        .with_incoming_bundles(incoming_bundles)
        .with_transfer(source, recipient, amount)
        .with_epoch(epoch)
        .with_authenticated_owner(Some(authenticated_owner));

        self.execute_proposal(block, vec![]).await.unwrap()
    }

    /// Creates a certificate with a transfer.
    ///
    /// This does not work for blocks with ancestors that sent a message to the same recipient, unless
    /// the `previous_confirmed_block` also did.
    /// It also does not work as a certificate that can be processed - it doesn't do
    /// proper hashing of the execution state, so such a certificate will be rejected by
    /// the worker.
    #[expect(clippy::too_many_arguments)]
    async fn make_transfer_certificate_for_epoch_unprocessable(
        &self,
        chain_description: ChainDescription,
        chain_owner_pubkey: AccountPublicKey,
        authenticated_owner: AccountOwner,
        source: AccountOwner,
        recipient: Account,
        amount: Amount,
        incoming_bundles: Vec<IncomingBundle>,
        epoch: Epoch,
        balance: Amount,
        balances: BTreeMap<AccountOwner, Amount>,
        previous_confirmed_blocks: Vec<&ConfirmedBlockCertificate>,
    ) -> ConfirmedBlockCertificate {
        let chain_id = chain_description.id();
        let system_state = SystemExecutionState {
            committees: [(epoch, self.committee.clone())].into_iter().collect(),
            ownership: ChainOwnership::single(chain_owner_pubkey.into()),
            balance,
            balances,
            admin_chain_id: Some(self.admin_chain_id()),
            ..SystemExecutionState::new(chain_description)
        };
        let mut block = match previous_confirmed_blocks.first() {
            None => make_first_block(chain_id),
            Some(cert) => make_child_block(cert.value()),
        }
        .with_transfer(source, recipient, amount);
        block.authenticated_owner = Some(authenticated_owner);
        block.epoch = epoch;

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
                                authenticated_owner: posted_message.authenticated_owner,
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

        block.transactions = incoming_bundles
            .into_iter()
            .map(Transaction::ReceiveMessages)
            .chain(block.transactions)
            .collect();

        if chain_id != recipient.chain_id {
            messages.push(vec![direct_outgoing_message(
                recipient.chain_id,
                MessageKind::Tracked,
                SystemMessage::Credit {
                    source,
                    target: recipient.owner,
                    amount,
                },
            )]);
        } else {
            messages.push(Vec::new());
        }
        let tx_count = block.transactions.len();
        let oracle_responses = iter::repeat_with(Vec::new).take(tx_count).collect();
        let events = iter::repeat_with(Vec::new).take(tx_count).collect();
        let blobs = iter::repeat_with(Vec::new).take(tx_count).collect();
        let operation_results = vec![OperationResult(Vec::new()); block.operations().count()];
        let state_hash = system_state.into_hash().await;
        let previous_message_blocks = messages
            .iter()
            .flatten()
            .map(|message| message.destination)
            .filter_map(|recipient| {
                previous_confirmed_blocks
                    .iter()
                    .find(|block| {
                        block
                            .inner()
                            .block()
                            .body
                            .messages
                            .iter()
                            .flatten()
                            .any(|message| message.destination == recipient)
                    })
                    .map(|block| {
                        (
                            recipient,
                            (block.hash(), block.inner().block().header.height),
                        )
                    })
            })
            .collect();
        let value = ConfirmedBlock::new(
            BlockExecutionOutcome {
                messages,
                previous_message_blocks,
                previous_event_blocks: BTreeMap::new(),
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

    pub fn system_execution_state(&self, chain_id: &ChainId) -> SystemExecutionState {
        let description = if *chain_id == self.admin_chain_id() {
            self.admin_description.clone()
        } else {
            self.other_chains
                .get(chain_id)
                .expect("Unknown chain")
                .clone()
        };
        SystemExecutionState {
            admin_chain_id: Some(self.admin_chain_id()),
            timestamp: description.timestamp(),
            committees: [(Epoch::ZERO, self.committee.clone())]
                .into_iter()
                .collect(),
            ..SystemExecutionState::new(description.clone())
        }
    }

    /// A method creating a `ConfirmedBlockCertificate` for a proposal by executing it on the
    /// `executing_worker`.
    async fn execute_proposal(
        &mut self,
        proposal: ProposedBlock,
        blobs: Vec<Blob>,
    ) -> Result<ConfirmedBlockCertificate, anyhow::Error> {
        let (block, _, _) = self
            .executing_worker
            .stage_block_execution(proposal, None, blobs)
            .await?;
        let certificate = self.make_certificate(ConfirmedBlock::new(block));
        self.executing_worker
            .fully_handle_certificate_with_notifications(certificate.clone(), &())
            .await?;
        Ok(certificate)
    }
}

/// Asserts that there are no "removed" bundles in the inbox, that have been included as
/// incoming in a block but not received from the sender chain yet.
async fn assert_no_removed_bundles<C>(chain: &ChainStateView<C>)
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    for (_, inbox) in chain.inboxes.try_load_all_entries().await.unwrap() {
        assert_eq!(inbox.removed_bundles.front().await.unwrap(), None);
    }
}

fn direct_outgoing_message(
    recipient: ChainId,
    kind: MessageKind,
    message: SystemMessage,
) -> OutgoingMessage {
    OutgoingMessage {
        destination: recipient,
        authenticated_owner: None,
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
    let original_public_key = match block_proposal.signature {
        AccountSignature::Ed25519 { public_key, .. } => public_key,
        _ => {
            panic!(
                "Expected an Ed25519 signature, found: {:?}",
                block_proposal.signature
            );
        }
    };
    let mut bad_signature_block_proposal = block_proposal.clone();
    let bad_signature = match unknown_key_pair.sign(&block_proposal.content) {
        AccountSignature::Ed25519 { signature, .. } => AccountSignature::Ed25519 {
            public_key: original_public_key,
            signature,
        },
        _ => panic!("Expected an Ed25519 signature"),
    };
    bad_signature_block_proposal.signature = bad_signature;
    assert_matches!(
        env.executing_worker()
            .handle_block_proposal(bad_signature_block_proposal)
            .await,
            Err(WorkerError::CryptoError(error))
                if matches!(error, linera_base::crypto::CryptoError::InvalidSignature {..})
    );
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
        .with_authenticated_owner(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    assert_matches!(
    env.executing_worker()
        .handle_block_proposal(zero_amount_block_proposal)
        .await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
            execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::IncorrectTransferAmount))
    );
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
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
async fn test_handle_block_proposal_valid_timestamps<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let balance = Amount::from_tokens(5);
    let small_transfer = Amount::from_micros(1);
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner, balance).await;
    let chain_2_desc = env.add_root_chain(2, owner, balance).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    {
        let block_proposal = make_first_block(chain_1)
            .with_simple_transfer(chain_2, small_transfer)
            .with_authenticated_owner(Some(owner))
            .with_timestamp(Timestamp::from(TEST_GRACE_PERIOD_MICROS + 1_000_000))
            .into_first_proposal(owner, &signer)
            .await
            .unwrap();
        // Timestamp too far in the future
        assert_matches!(
            env.executing_worker()
                .handle_block_proposal(block_proposal)
                .await,
            Err(WorkerError::InvalidTimestamp { .. })
        );
    }

    let block_0_time = Timestamp::from(TEST_GRACE_PERIOD_MICROS);
    let block = make_first_block(chain_1)
        .with_timestamp(block_0_time)
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner));
    let certificate = {
        let future = env.execute_proposal(block.clone(), vec![]);
        clock.set(block_0_time);
        future.await?
    };

    assert!(certificate.value().matches_proposed_block(&block));
    assert!(certificate.block().outcome_matches(
        vec![vec![direct_credit_message(chain_2, small_transfer)]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult::default()],
    ));

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
            env.executing_worker().handle_block_proposal(block_proposal).await,
            Err(WorkerError::ChainError(error))
                if matches!(*error, ChainError::InvalidBlockTimestamp { .. })
        );
    }
    Ok(())
}

/// Tests that proposals with future timestamps are delayed until the timestamp is reached,
/// while other requests can still be processed.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_handle_block_proposal_timestamp_delay<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    use std::task::Poll;

    use futures::{future::poll_fn, Future as _};
    use tokio::task::yield_now;

    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let balance = Amount::from_tokens(5);
    let small_transfer = Amount::from_micros(1);
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner, balance).await;
    let chain_2_desc = env.add_root_chain(2, owner, balance).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    // Start at time 0.
    clock.set(Timestamp::from(0));

    // Case 1: Timestamp in the past - should be handled immediately.
    let past_timestamp = Timestamp::from(0);
    clock.set(Timestamp::from(1000)); // Current time is 1000.
    let proposed_block = make_first_block(chain_1)
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner))
        .with_timestamp(past_timestamp);
    let block_proposal = proposed_block
        .clone()
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    // Stage execution to get the block for certificate creation.
    let (block, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block, None, vec![])
        .await?;
    // Past timestamp should be handled immediately (and succeed).
    let result = env
        .executing_worker()
        .handle_block_proposal(block_proposal)
        .await;
    assert!(result.is_ok(), "Past timestamp should be accepted");
    let certificate = env.make_certificate(ConfirmedBlock::new(block));
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    // Case 2: Timestamp at current time - should be handled immediately.
    let current_timestamp = Timestamp::from(2000);
    clock.set(current_timestamp);
    let proposed_block = make_child_block(&certificate.clone().into_value())
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner))
        .with_timestamp(current_timestamp);
    let block_proposal = proposed_block
        .clone()
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    let (block, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block, None, vec![])
        .await?;
    let result = env
        .executing_worker()
        .handle_block_proposal(block_proposal)
        .await;
    assert!(result.is_ok(), "Current timestamp should be accepted");
    let certificate = env.make_certificate(ConfirmedBlock::new(block));
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    // Case 3: Timestamp in the future (within grace period) - should be delayed.
    let future_timestamp = Timestamp::from(3000 + TEST_GRACE_PERIOD_MICROS / 2);
    clock.set(Timestamp::from(3000));
    let proposed_block = make_child_block(&certificate.clone().into_value())
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner))
        .with_timestamp(future_timestamp);
    let block_proposal = proposed_block
        .clone()
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    let (block, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block, None, vec![])
        .await?;

    // Spawn the proposal handling. It should not complete immediately.
    let worker = env.executing_worker().clone();
    let mut future = Box::pin(worker.handle_block_proposal(block_proposal));

    // Give the task a chance to run.
    yield_now().await;

    // The future should not be ready yet (the proposal is delayed).
    let is_pending = poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(true),
        Poll::Ready(_) => Poll::Ready(false),
    })
    .await;
    assert!(is_pending, "Future-timestamp proposal should be delayed");

    // Advance the clock to the block timestamp.
    clock.set(future_timestamp);

    // Now the future should complete.
    let result = future.as_mut().await;
    assert!(
        result.is_ok(),
        "Future timestamp within grace period should succeed after delay"
    );
    let certificate = env.make_certificate(ConfirmedBlock::new(block));
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    // Case 4: Timestamp far in the future (beyond grace period) - should be rejected immediately.
    let far_future_timestamp = Timestamp::from(4000 + TEST_GRACE_PERIOD_MICROS + 1_000_000);
    clock.set(Timestamp::from(4000));
    let proposed_block = make_child_block(&certificate.into_value())
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner))
        .with_timestamp(far_future_timestamp);
    let block_proposal = proposed_block
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    // Far-future timestamp should be rejected immediately.
    assert_matches!(
        env.executing_worker()
            .handle_block_proposal(block_proposal)
            .await,
        Err(WorkerError::InvalidTimestamp { .. })
    );

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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
    let new_signer: InMemorySigner = InMemorySigner::from_iter(vec![(unknown_owner, unknown_key)]);
    let unknown_sender_block_proposal = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::from_tokens(5))
        .into_first_proposal(unknown_owner, &new_signer)
        .await
        .unwrap();
    assert_matches!(
        env.executing_worker()
            .handle_block_proposal(unknown_sender_block_proposal)
            .await,
        Err(WorkerError::InvalidOwner)
    );
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_1 = chain_desc.id();
    let chain_2 = env.add_root_chain(2, sender_owner, Amount::ZERO).await.id();
    let block_proposal0 = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::ONE)
        .with_authenticated_owner(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    let certificate0 = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_public_key,
            chain_2,
            Amount::ONE,
            vec![],
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
async fn test_handle_block_proposal_sparse_chain<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let sender_public_key = signer.generate_new();
    let sender_owner = sender_public_key.into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(5))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2_desc = env.add_root_chain(2, sender_owner, Amount::ZERO).await;
    let chain_2 = chain_2_desc.id();
    let chain_3_desc = env.add_root_chain(3, sender_owner, Amount::ZERO).await;
    let chain_3 = chain_3_desc.id();

    let certificate0 = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_public_key,
            chain_2,
            Amount::ONE,
            Vec::new(),
            None,
        )
        .await;

    let certificate1 = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_public_key,
            chain_3,
            Amount::ONE,
            Vec::new(),
            Some(&certificate0),
        )
        .await;

    let certificate2 = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_public_key,
            chain_2,
            Amount::ONE,
            Vec::new(),
            Some(&certificate1),
        )
        .await;

    let block_proposal1 = make_child_block(certificate2.value())
        .with_simple_transfer(chain_2, Amount::from_tokens(1))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    // The worker handles certificates 0 and 2 - this should succeed, and the worker
    // should now have block 0 fully processed, and block 2 preprocessed.
    env.worker()
        .handle_confirmed_certificate(certificate0, None)
        .await?;

    env.worker()
        .handle_confirmed_certificate(certificate2.clone(), None)
        .await?;

    {
        let chain = env.worker().chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        assert_eq!(chain.tip_state.get().next_block_height, BlockHeight(1));
    }

    // The proposal is at height 3 - it should fail until the chain is fully processed up
    // to height 2.
    let proposal_result = env
        .worker()
        .handle_block_proposal(block_proposal1.clone())
        .await;
    assert_matches!(
        proposal_result,
        Err(WorkerError::ChainError(err)) if matches!(*err, ChainError::UnexpectedBlockHeight {
            expected_block_height: BlockHeight(1),
            found_block_height: BlockHeight(3)
        })
    );

    // Handle the certificate in the gap.
    env.worker()
        .handle_confirmed_certificate(certificate1, None)
        .await?;

    {
        let chain = env.worker().chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        assert_eq!(chain.tip_state.get().next_block_height, BlockHeight(2));
    }

    // ...and the one that has been preprocessed before, again, as it is not automatically
    // re-processed.
    env.worker()
        .handle_confirmed_certificate(certificate2, None)
        .await?;

    {
        let chain = env.worker().chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        assert_eq!(chain.tip_state.get().next_block_height, BlockHeight(3));
    }

    // The proposal should now succeed.
    let proposal_result = env
        .worker()
        .handle_block_proposal(block_proposal1.clone())
        .await;
    assert_matches!(proposal_result, Ok(_));

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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_owner, Amount::from_tokens(6))
        .await;
    let chain_2_desc = env.add_root_chain(2, recipient_owner, Amount::ZERO).await;
    let chain_3_desc = env.add_root_chain(3, recipient_owner, Amount::ZERO).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let chain_3 = chain_3_desc.id();
    let proposal0 = make_first_block(chain_1)
        .with_simple_transfer(chain_2, Amount::ONE)
        .with_simple_transfer(chain_2, Amount::from_tokens(2))
        .with_authenticated_owner(Some(sender_owner));
    let certificate0 = env.execute_proposal(proposal0.clone(), vec![]).await?;

    assert!(certificate0.value().matches_proposed_block(&proposal0));
    assert!(certificate0.block().outcome_matches(
        vec![
            vec![direct_credit_message(chain_2, Amount::ONE)],
            vec![direct_credit_message(chain_2, Amount::from_tokens(2))],
        ],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]; 2],
        vec![vec![]; 2],
        vec![vec![]; 2],
        vec![OperationResult::default(); 2],
    ));

    let proposal1 = make_child_block(&certificate0.clone().into_value())
        .with_simple_transfer(chain_2, Amount::from_tokens(3))
        .with_authenticated_owner(Some(sender_owner));
    let certificate1 = env.execute_proposal(proposal1.clone(), vec![]).await?;

    assert!(certificate1.value().matches_proposed_block(&proposal1));
    assert!(certificate1.block().outcome_matches(
        vec![vec![direct_credit_message(chain_2, Amount::from_tokens(3))]],
        BTreeMap::from([(chain_2, (certificate0.hash(), BlockHeight(0)),)]),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult::default()],
    ));

    // Missing earlier blocks, but the certificate will be preprocessed.
    assert_matches!(
        env.worker()
            .handle_confirmed_certificate(certificate1.clone(), None)
            .await,
        Ok(_)
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
            .with_authenticated_owner(Some(recipient_owner))
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
                ) if matches!(**execution_error, ExecutionError::InsufficientBalance { .. }))
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
            .with_authenticated_owner(Some(recipient_owner))
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
            .with_authenticated_owner(Some(recipient_owner))
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
            .with_authenticated_owner(Some(recipient_owner))
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
        let proposed_block = make_first_block(chain_2)
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
            .with_simple_transfer(chain_3, Amount::ONE)
            .with_authenticated_owner(Some(recipient_owner));
        let block_proposal = proposed_block
            .clone()
            .into_first_proposal(recipient_owner, &signer)
            .await
            .unwrap();
        // Taking the first message only is ok.
        env.worker()
            .handle_block_proposal(block_proposal.clone())
            .await?;
        let certificate = env
            .execute_proposal(block_proposal.content.block, vec![])
            .await?;

        assert!(certificate.value().matches_proposed_block(&proposed_block));
        assert!(certificate.block().outcome_matches(
            vec![vec![], vec![direct_credit_message(chain_3, Amount::ONE)],],
            BTreeMap::new(),
            BTreeMap::new(),
            vec![vec![]; 2],
            vec![vec![]; 2],
            vec![vec![]; 2],
            vec![OperationResult::default()],
        ));

        env.worker()
            .handle_confirmed_certificate(certificate.clone(), None)
            .await?;

        // Then receive the next two messages.
        let block_proposal = make_child_block(&certificate.into_value())
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
            .with_simple_transfer(chain_3, Amount::from_tokens(3))
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
        .with_authenticated_owner(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();
    assert_matches!(
        env.executing_worker().handle_block_proposal(block_proposal).await,
        Err(
            WorkerError::ChainError(error)
        ) if matches!(&*error, ChainError::ExecutionError(
                execution_error, ChainExecutionContext::Operation(_)
        ) if matches!(**execution_error, ExecutionError::InsufficientBalance { .. }))
    );
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
        .with_authenticated_owner(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    let (chain_info_response, _actions) = env
        .executing_worker()
        .handle_block_proposal(block_proposal)
        .await?;
    chain_info_response.check(env.executing_worker().public_key())?;
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert!(chain.manager.confirmed_vote().is_none()); // It was a multi-leader
                                                       // round.
    let validated_certificate =
        env.make_certificate(chain.manager.validated_vote().unwrap().value().clone());
    drop(chain);

    let (chain_info_response, _actions) = env
        .executing_worker()
        .handle_validated_certificate(validated_certificate)
        .await?;
    chain_info_response.check(env.executing_worker().public_key())?;
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
        .with_authenticated_owner(Some(sender_owner))
        .into_first_proposal(sender_owner, &signer)
        .await
        .unwrap();

    let (response, _actions) = env
        .executing_worker()
        .handle_block_proposal(block_proposal.clone())
        .await?;
    response.check(env.executing_worker().public_key())?;
    let (replay_response, _actions) = env
        .executing_worker()
        .handle_block_proposal(block_proposal)
        .await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_2_desc = env
        .add_root_chain(2, test_pubkey.into(), Amount::ZERO)
        .await;
    let chain_2 = chain_2_desc.id();
    let chain_1_desc = dummy_chain_description(1);
    let certificate = env
        .make_transfer_certificate_for_epoch_unprocessable(
            chain_1_desc.clone(),
            sender_pubkey,
            sender_pubkey.into(),
            test_pubkey.into(),
            Account::chain(chain_2),
            Amount::from_tokens(5),
            Vec::new(),
            Epoch::ZERO,
            Amount::ZERO,
            BTreeMap::new(),
            vec![],
        )
        .await;
    assert_matches!(
        env.executing_worker()
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
async fn test_handle_certificate_bad_block_height<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_2 = chain_2_desc.id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.id(),
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(5),
            Vec::new(),
            None,
        )
        .await;
    // Replays are ignored.
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    env.executing_worker()
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ZERO)
        .await;
    let chain_3_desc = env
        .add_root_chain(3, key_pair.public().into(), Amount::from_tokens(995))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();
    let chain_3 = chain_3_desc.id();

    let transfer_proposal = make_first_block(chain_3)
        .with_simple_transfer(chain_1, Amount::from_tokens(995))
        .with_authenticated_owner(Some(key_pair.public().into()));
    let transfer_certificate = env
        .execute_proposal(transfer_proposal.clone(), vec![])
        .await?;

    assert!(transfer_certificate
        .value()
        .matches_proposed_block(&transfer_proposal));

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            key_pair.public(),
            chain_2,
            Amount::from_tokens(1000),
            vec![IncomingBundle {
                origin: chain_3,
                bundle: MessageBundle {
                    certificate_hash: transfer_certificate.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![system_credit_message(Amount::from_tokens(995))
                        .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            }],
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
        .expect("Missing inbox for `chain_3` in `chain_1`");
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
        } if certificate_hash == transfer_certificate.hash()
            && height == BlockHeight::ZERO
            && timestamp == Timestamp::from(0)
            && matches!(messages[..], [PostedMessage {
                authenticated_owner: None,
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
            chain_1,
            sender_key_pair.public(),
            chain_2,
            Amount::ONE,
            Vec::new(),
            None,
        )
        .await;
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let new_sender_chain = env.executing_worker().chain_state_view(chain_1).await?;
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
    let new_recipient_chain = env.executing_worker().chain_state_view(chain_2).await?;
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
async fn test_handle_certificate_same_chain_same_owner_no_messages<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let key_pair = AccountSecretKey::generate();
    let owner = key_pair.public().into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::ONE).await;
    let chain_1 = chain_1_desc.id();

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            key_pair.public(),
            chain_1,
            Amount::ONE,
            Vec::new(),
            None,
        )
        .await;
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;
    let chain = env.executing_worker().chain_state_view(chain_1).await?;
    assert!(chain.is_active());
    assert_eq!(Amount::ONE, *chain.execution_state.system.balance.get());

    // With the new optimization, transfers to the same chain should not create messages.
    let inbox = chain.inboxes.try_load_entry(&chain_1).await?;
    assert!(inbox.is_none());

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
async fn test_handle_certificate_different_chain_with_messages<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let key_pair = AccountSecretKey::generate();
    let owner = key_pair.public().into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::ONE).await;
    let chain_1 = chain_1_desc.id();
    let chain_2_desc = env.add_root_chain(2, owner, Amount::ZERO).await;
    let chain_2 = chain_2_desc.id();

    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            key_pair.public(),
            chain_2,
            Amount::ONE,
            Vec::new(),
            None,
        )
        .await;
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    // Check the sender chain
    let chain_1_state = env.executing_worker().chain_state_view(chain_1).await?;
    assert!(chain_1_state.is_active());
    assert_eq!(
        Amount::ZERO,
        *chain_1_state.execution_state.system.balance.get()
    );

    // Check the receiver chain has the inbox with the expected message
    let chain_2_state = env.executing_worker().chain_state_view(chain_2).await?;
    let inbox = chain_2_state
        .inboxes
        .try_load_entry(&chain_1)
        .await?
        .expect("Missing inbox for chain_1 in chain_2");
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
            authenticated_owner: None,
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
        chain_1_state.tip_state.get().next_block_height
    );
    assert_eq!(chain_1_state.confirmed_log.count(), 1);
    assert_eq!(
        Some(certificate.hash()),
        chain_1_state.tip_state.get().block_hash
    );
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
    let sender_public_key = sender_key_pair.public();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_public_key.into(), Amount::from_tokens(20))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2_desc = env
        .add_root_chain(2, AccountPublicKey::test_key(2).into(), Amount::ONE)
        .await;
    let chain_2 = chain_2_desc.id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_public_key,
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
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
        .expect("Missing inbox for `chain_1` in `chain_2`");
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
            authenticated_owner: None,
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
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(10))
        .await;
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1_desc.id(),
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
            None,
        )
        .await;
    assert!(env
        .executing_worker()
        .handle_cross_chain_request(update_recipient_direct(chain_2, &certificate))
        .await?
        .cross_chain_requests
        .is_empty());
    let chain = env.executing_worker().chain_state_view(chain_2).await?;
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
    let sender_key_pair = AccountSecretKey::generate();
    let mut env = TestEnvironment::new(&mut storage_builder, true, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(10))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(10),
            Vec::new(),
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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
        env.executing_worker()
            .query_application(chain_1, Query::System(SystemQuery), None)
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
        env.executing_worker()
            .query_application(chain_2, Query::System(SystemQuery), None)
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
            chain_1,
            sender_key_pair.public(),
            chain_2,
            Amount::from_tokens(5),
            Vec::new(),
            None,
        )
        .await;

    let info = env
        .executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?
        .info;
    assert_eq!(chain_1, info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Some(certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());
    assert_eq!(
        env.executing_worker()
            .query_application(chain_1, Query::System(SystemQuery), None)
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
            chain_2,
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
            None,
        )
        .await;
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    assert_eq!(
        env.executing_worker()
            .query_application(chain_2, Query::System(SystemQuery), None)
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
        let recipient_chain = env.executing_worker().chain_state_view(chain_2).await?;
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
    let (response, _actions) = env
        .executing_worker()
        .handle_chain_info_query(query)
        .await?;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env
        .add_root_chain(1, sender_key_pair.public().into(), Amount::from_tokens(5))
        .await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = dummy_chain_description(2).id();
    let certificate = env
        .make_simple_transfer_certificate(
            chain_1,
            sender_key_pair.public(),
            chain_2, // the recipient chain does not exist
            Amount::from_tokens(5),
            Vec::new(),
            None,
        )
        .await;

    let info = env
        .executing_worker()
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

    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
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

    // Transfer money from the CHAIN account to the sender's account.
    let certificate0 = env
        .make_transfer_certificate(
            chain_1,
            sender,
            AccountOwner::CHAIN,
            sender_account,
            Amount::from_tokens(5),
            Vec::new(),
            None,
        )
        .await;

    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    // Then, make two transfers to the recipient.
    let certificate1 = env
        .make_transfer_certificate(
            chain_1,
            sender,
            sender,
            recipient_account,
            Amount::from_tokens(3),
            Vec::new(),
            Some(&certificate0),
        )
        .await;

    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate1.clone(), &())
        .await?;

    let certificate2 = env
        .make_transfer_certificate(
            chain_1,
            sender,
            sender,
            recipient_account,
            Amount::from_tokens(2),
            Vec::new(),
            Some(&certificate1),
        )
        .await;

    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    // Reject the first transfer and try to use the money of the second one.
    let certificate = env
        .make_transfer_certificate(
            chain_2,
            recipient,
            recipient,
            Account::chain(chain_2_desc.id()),
            Amount::ONE,
            vec![
                IncomingBundle {
                    origin: chain_1,
                    bundle: MessageBundle {
                        certificate_hash: certificate1.hash(),
                        height: BlockHeight::from(1),
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
                        height: BlockHeight::from(2),
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
            None,
        )
        .await;

    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate.clone(), &())
        .await?;

    {
        let chain = env.executing_worker().chain_state_view(chain_2).await?;
        assert!(chain.is_active());
        assert_no_removed_bundles(&chain).await;
    }

    // Process the bounced message and try to use the refund.
    let certificate3 = env
        .make_transfer_certificate(
            chain_1,
            sender,
            sender,
            Account::chain(chain_2_desc.id()),
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
            Some(&certificate2),
        )
        .await;

    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate3.clone(), &())
        .await?;

    {
        let chain = env.worker.chain_state_view(chain_1).await?;
        assert!(chain.is_active());
        assert_no_removed_bundles(&chain).await;
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
    let mut env = TestEnvironment::new_with_amount(
        &mut storage_builder,
        false,
        false,
        Amount::from_tokens(2),
    )
    .await?;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_chain_id = env.admin_chain_id();
    // Have the admin chain create a user chain.
    let user_description = env
        .add_child_chain(admin_chain_id, env.admin_public_key().into(), Amount::ZERO)
        .await;
    let user_id = user_description.id();
    let proposal0 = make_first_block(admin_chain_id)
        .with_operation(SystemOperation::OpenChain(OpenChainConfig {
            ownership: ChainOwnership::single(env.admin_public_key().into()),
            balance: Amount::ZERO,
            application_permissions: Default::default(),
        }))
        .with_authenticated_owner(Some(env.admin_public_key().into()));
    let certificate0 = env.execute_proposal(proposal0.clone(), vec![]).await?;

    assert!(certificate0.value().matches_proposed_block(&proposal0));
    assert!(certificate0.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![Vec::new()],
        vec![Vec::new()],
        vec![vec![Blob::new_chain_description(&user_description)]],
        vec![OperationResult::default()],
    ));

    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;
    {
        let admin_chain = env.worker().chain_state_view(admin_chain_id).await?;
        assert!(admin_chain.is_active());
        assert_no_removed_bundles(&admin_chain).await;
        assert_eq!(
            BlockHeight::from(1),
            admin_chain.tip_state.get().next_block_height
        );
        assert!(admin_chain.outboxes.indices().await?.is_empty());
        assert_eq!(
            *admin_chain.execution_state.system.admin_chain_id.get(),
            Some(admin_chain_id)
        );
    }

    // Create a new committee and transfer money before accepting the subscription.
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    // `PublishCommitteeBlob` is tested e.g. in `client_tests::test_change_voting_rights`, so we
    // just write it directly to storage here for simplicity.
    env.write_blobs(std::slice::from_ref(&committee_blob))
        .await?;
    let blob_hash = committee_blob.id().hash;
    let proposal1 = make_child_block(&certificate0.clone().into_value())
        .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
            epoch: Epoch::from(1),
            blob_hash,
        }))
        .with_simple_transfer(user_id, Amount::from_tokens(2));
    let certificate1 = env.execute_proposal(proposal1.clone(), vec![]).await?;

    let event_id = EventId {
        chain_id: admin_chain_id,
        stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
        index: 1,
    };

    assert!(certificate1.value().matches_proposed_block(&proposal1));
    assert!(certificate1.block().outcome_matches(
        vec![
            vec![],
            vec![direct_credit_message(user_id, Amount::from_tokens(2))],
        ],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![OracleResponse::Blob(committee_blob.id())], vec![]],
        vec![
            vec![Event {
                stream_id: event_id.stream_id.clone(),
                index: event_id.index,
                value: bcs::to_bytes(&EpochEventData {
                    blob_hash,
                    timestamp: Timestamp::from(0),
                })
                .unwrap(),
            }],
            vec![],
        ],
        vec![vec![]; 2],
        vec![OperationResult::default(); 2],
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
            *user_chain.execution_state.system.admin_chain_id.get(),
            Some(admin_chain_id)
        );
        assert_no_removed_bundles(&user_chain).await;
        matches!(
            &user_chain
                .inboxes
                .try_load_entry(&admin_chain_id)
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
    let proposal3 = make_first_block(user_id)
        .with_incoming_bundle(IncomingBundle {
            origin: admin_chain_id,
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
        .with_operation(SystemOperation::ProcessNewEpoch(Epoch::from(1)));
    // Make the child receive the pending messages.
    let certificate3 = env.execute_proposal(proposal3.clone(), vec![]).await?;

    assert!(certificate3.value().matches_proposed_block(&proposal3));
    assert!(certificate3.block().outcome_matches(
        vec![vec![]; 2],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![
            vec![],
            vec![
                OracleResponse::Event(
                    EventId {
                        chain_id: admin_chain_id,
                        stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
                        index: 1,
                    },
                    bcs::to_bytes(&EpochEventData {
                        blob_hash,
                        timestamp: Timestamp::from(0),
                    })
                    .unwrap(),
                ),
                OracleResponse::Blob(committee_blob.id()),
            ],
        ],
        vec![vec![]; 2],
        vec![vec![]; 2],
        vec![OperationResult::default()],
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
            *user_chain.execution_state.system.admin_chain_id.get(),
            Some(admin_chain_id)
        );
        assert_eq!(user_chain.execution_state.system.committees.get().len(), 2);
        assert_no_removed_bundles(&user_chain).await;
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
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env.add_root_chain(1, owner1, Amount::from_tokens(3)).await;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_chain_id = env.admin_chain_id();
    let user_id = chain_1_desc.id();

    // Have the user chain start a transfer to the admin chain.
    let proposal0 = make_first_block(user_id)
        .with_simple_transfer(admin_chain_id, Amount::ONE)
        .with_authenticated_owner(Some(owner1));
    let certificate0 = env.execute_proposal(proposal0.clone(), vec![]).await?;

    assert!(certificate0.value().matches_proposed_block(&proposal0));
    assert!(certificate0.block().outcome_matches(
        vec![vec![direct_credit_message(admin_chain_id, Amount::ONE)]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult::default()],
    ));

    // Have the admin chain create a new epoch without retiring the old one.
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    let blob_hash = committee_blob.id().hash;
    env.write_blobs(std::slice::from_ref(&committee_blob))
        .await?;
    let proposal1 = make_first_block(admin_chain_id).with_operation(SystemOperation::Admin(
        AdminOperation::CreateCommittee {
            epoch: Epoch::from(1),
            blob_hash,
        },
    ));
    let certificate1 = env.execute_proposal(proposal1.clone(), vec![]).await?;

    assert!(certificate1.value().matches_proposed_block(&proposal1));
    assert!(certificate1.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![OracleResponse::Blob(committee_blob.id())]],
        vec![vec![Event {
            stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
            index: 1,
            value: bcs::to_bytes(&EpochEventData {
                blob_hash: committee_blob.id().hash,
                timestamp: Timestamp::from(0),
            })
            .unwrap(),
        }]],
        vec![vec![]],
        vec![OperationResult::default()],
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
    assert_eq!(*user_chain.execution_state.system.epoch.get(), Epoch::ZERO);

    // .. and the message has gone through.
    let admin_chain = env.worker().chain_state_view(admin_chain_id).await?;
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
    let mut env =
        TestEnvironment::new_with_amount(&mut storage_builder, false, false, Amount::ZERO).await?;
    let owner1 = AccountSecretKey::generate().public().into();
    let chain_1_desc = env.add_root_chain(1, owner1, Amount::from_tokens(3)).await;
    let mut committees = BTreeMap::new();
    let committee = env.committee().clone();
    committees.insert(Epoch::ZERO, committee.clone());
    let admin_chain_id = env.admin_chain_id();
    let user_id = chain_1_desc.id();

    // Have the user chain start a transfer to the admin chain.
    let proposal0 = make_first_block(user_id)
        .with_simple_transfer(admin_chain_id, Amount::ONE)
        .with_authenticated_owner(Some(owner1));
    let certificate0 = env.execute_proposal(proposal0.clone(), vec![]).await?;

    assert!(certificate0.value().matches_proposed_block(&proposal0));
    assert!(certificate0.block().outcome_matches(
        vec![vec![direct_credit_message(admin_chain_id, Amount::ONE)]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![Vec::new()],
        vec![Vec::new()],
        vec![Vec::new()],
        vec![OperationResult::default()]
    ));

    // Have the admin chain create a new epoch and retire the old one immediately.
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    let blob_hash = committee_blob.id().hash;
    env.write_blobs(std::slice::from_ref(&committee_blob))
        .await?;

    let proposal1 = make_first_block(admin_chain_id)
        .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
            epoch: Epoch::from(1),
            blob_hash,
        }))
        .with_operation(SystemOperation::Admin(AdminOperation::RemoveCommittee {
            epoch: Epoch::ZERO,
        }));

    let certificate1 = env.execute_proposal(proposal1.clone(), vec![]).await?;

    assert!(certificate1.value().matches_proposed_block(&proposal1));
    assert!(certificate1.block().outcome_matches(
        vec![vec![]; 2],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![OracleResponse::Blob(committee_blob.id())], vec![]],
        vec![
            vec![Event {
                stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
                index: 1,
                value: bcs::to_bytes(&EpochEventData {
                    blob_hash: committee_blob.id().hash,
                    timestamp: Timestamp::from(0),
                })
                .unwrap(),
            }],
            vec![Event {
                stream_id: StreamId::system(REMOVED_EPOCH_STREAM_NAME),
                index: 0,
                value: Vec::new(),
            }],
        ],
        vec![vec![]; 2],
        vec![OperationResult::default(); 2],
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
        assert_eq!(*user_chain.execution_state.system.epoch.get(), Epoch::ZERO);

        // .. but the message hasn't gone through.
        let admin_chain = env.worker().chain_state_view(admin_chain_id).await?;
        assert!(admin_chain.is_active());
        assert!(admin_chain.inboxes.indices().await?.is_empty());
    }

    // Force the admin chain to receive the money nonetheless by anticipation.
    let proposal2 = make_child_block(&certificate1.clone().into_value())
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
        });
    let certificate2 = env.execute_proposal(proposal2.clone(), vec![]).await?;

    assert!(certificate2.value().matches_proposed_block(&proposal2));
    assert!(certificate2.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![],
    ));

    env.worker()
        .fully_handle_certificate_with_notifications(certificate2.clone(), &())
        .await?;

    {
        // The admin chain has an anticipated message.
        let admin_chain = env.worker().chain_state_view(admin_chain_id).await?;
        assert!(admin_chain.is_active());
        assert!(admin_chain
            .inboxes
            .try_load_entry(&user_id)
            .await?
            .unwrap()
            .removed_bundles
            .front()
            .await?
            .is_some());
    }

    // Try again to execute the transfer from the user chain to the admin chain.
    // This time, the epoch verification should be overruled.
    env.worker()
        .fully_handle_certificate_with_notifications(certificate0.clone(), &())
        .await?;

    {
        // The admin chain has no more anticipated messages.
        let admin_chain = env.worker().chain_state_view(admin_chain_id).await?;
        assert!(admin_chain.is_active());
        assert_no_removed_bundles(&admin_chain).await;
    }

    // Let's make a certificate for a block creating another epoch.
    // This one should contain previous_event_blocks.
    let proposal3 = make_child_block(&certificate2.into_value())
        .with_epoch(1)
        .with_operation(SystemOperation::Admin(AdminOperation::CreateCommittee {
            epoch: Epoch::from(2),
            blob_hash,
        }));
    let certificate3 = env.execute_proposal(proposal3.clone(), vec![]).await?;

    assert!(certificate3.value().matches_proposed_block(&proposal3));
    assert!(certificate3.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::from([(
            StreamId::system(NEW_EPOCH_STREAM_NAME),
            (certificate1.hash(), BlockHeight(0)),
        )]),
        vec![vec![]],
        vec![vec![Event {
            stream_id: StreamId::system(NEW_EPOCH_STREAM_NAME),
            index: 2,
            value: bcs::to_bytes(&EpochEventData {
                blob_hash: committee_blob.id().hash,
                timestamp: Timestamp::from(0),
            })
            .unwrap(),
        }]],
        vec![vec![]],
        vec![OperationResult::default()],
    ));

    env.worker()
        .fully_handle_certificate_with_notifications(certificate3, &())
        .await?;

    Ok(())
}

#[test(tokio::test)]
async fn test_cross_chain_helper() -> anyhow::Result<()> {
    let mut storage_builder = MemoryStorageBuilder::default();
    let env = TestEnvironment::new(&mut storage_builder, true, false).await?;
    let committees = BTreeMap::from([(Epoch::from(1), env.committee().clone())]);

    let chain_0 = env.admin_description.clone();
    let chain_1 = dummy_chain_description(1);

    let key_pair0 = AccountSecretKey::generate();
    let id0 = chain_0.id();
    let id1 = chain_1.id();

    let certificate0 = env
        .make_transfer_certificate_for_epoch_unprocessable(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Account::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            vec![],
        )
        .await;
    let certificate1 = env
        .make_transfer_certificate_for_epoch_unprocessable(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Account::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            vec![&certificate0],
        )
        .await;
    let certificate2 = env
        .make_transfer_certificate_for_epoch_unprocessable(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Account::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::from(1),
            Amount::ONE,
            BTreeMap::new(),
            vec![&certificate1, &certificate0],
        )
        .await;
    // Weird case: epoch going backward.
    let certificate3 = env
        .make_transfer_certificate_for_epoch_unprocessable(
            chain_0.clone(),
            key_pair0.public(),
            key_pair0.public().into(),
            AccountOwner::CHAIN,
            Account::chain(id1),
            Amount::ONE,
            Vec::new(),
            Epoch::ZERO,
            Amount::ONE,
            BTreeMap::new(),
            vec![&certificate2, &certificate1, &certificate0],
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
        current_epoch: Epoch::from(1),
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
        current_epoch: Epoch::from(1),
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
    let mut signer = InMemorySigner::new(None);
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let small_transfer = Amount::from_micros(1);
    let chain_1 = chain_1_desc.id();

    // Add another owner and use the leader-based protocol in all rounds.
    // Set owner0 as the first leader to test the first_leader configuration.
    let proposed_block0 = make_first_block(chain_1)
        .with_operation(SystemOperation::ChangeOwnership {
            super_owners: Vec::new(),
            owners: vec![(owner0, 100), (owner1, 100)],
            first_leader: Some(owner0),
            multi_leader_rounds: 0,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig::default(),
        })
        .with_authenticated_owner(Some(owner0));
    let (block0, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .executing_worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // We explicitly configured owner0 as the first leader.
    assert_eq!(response.info.manager.leader, Some(owner0));

    // So owner 1 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0)
        .with_simple_transfer(chain_1, small_transfer)
        .with_authenticated_owner(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::SingleLeader(0))
        .await
        .unwrap();
    let result = env.executing_worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal = make_child_block(&value0)
        .with_simple_transfer(chain_1, small_transfer)
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let result = env.executing_worker().handle_block_proposal(proposal).await;

    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::SingleLeader(0)))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_1).with_timeout(BlockHeight(1), Round::SingleLeader(0));
    let result = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::NotTimedOutYet(_))
    );

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query)
        .await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Timeout::new(chain_1, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts. The leader
    // for round 1 is determined by the pseudorandom selection (not first_leader).
    let certificate_timeout = vote
        .with_value(value_timeout.clone())
        .unwrap()
        .into_certificate(env.executing_worker().public_key());
    let (response, _) = env
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));

    // Now owner 0 can propose a block, but owner 1 can't.
    let proposed_block1 = make_child_block(&value0).with_simple_transfer(chain_1, small_transfer);
    let (block1, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block1.clone(), None, vec![])
        .await?;
    let proposal1_wrong_owner = proposed_block1
        .clone()
        .with_authenticated_owner(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let result = env
        .executing_worker()
        .handle_block_proposal(proposal1_wrong_owner)
        .await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal1 = proposed_block1
        .clone()
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(1))
        .await
        .unwrap();
    let (response, _) = env
        .executing_worker()
        .handle_block_proposal(proposal1)
        .await?;
    let value1 = ValidatedBlock::new(block1.clone());

    // If we send the validated block certificate to the worker, it votes to confirm.
    let vote = response.info.manager.pending.clone().unwrap();
    let certificate1 = vote
        .with_value(value1.clone())
        .unwrap()
        .into_certificate(env.executing_worker().public_key());
    let (response, _) = env
        .executing_worker()
        .handle_validated_certificate(certificate1.clone())
        .await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    let value = ConfirmedBlock::new(block1.clone());
    assert_eq!(vote.value, LiteValue::new(&value));

    // Instead of submitting the confirmed block certificate, let rounds 2 to 4 time out, too.
    let certificate_timeout =
        env.make_certificate_with_round(value_timeout.clone(), Round::SingleLeader(4));
    let (response, _) = env
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner1));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(5));

    // Create block2, also at height 1, but different from block 1.
    let amount = Amount::from_tokens(1);
    let proposed_block2 = make_child_block(&value0.clone()).with_simple_transfer(chain_1, amount);
    let (block2, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block2.clone(), None, vec![])
        .await?;

    // Since round 3 is already over, the validator won't vote for a validated block from round 3.
    let value2 = ValidatedBlock::new(block2.clone());
    let certificate = env.make_certificate_with_round(value2.clone(), Round::SingleLeader(2));
    env.executing_worker()
        .handle_validated_certificate(certificate)
        .await?;
    let query_values = ChainInfoQuery::new(chain_1).with_manager_values();
    let (response, _) = env
        .executing_worker()
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
        .with_authenticated_owner(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::SingleLeader(5))
        .await
        .unwrap();
    let result = env
        .executing_worker()
        .handle_block_proposal(proposal.clone())
        .await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasIncompatibleConfirmedVote(_, _))
    );

    // But with the validated block certificate for block2, it is allowed.
    let certificate2 = env.make_certificate_with_round(value2.clone(), Round::SingleLeader(4));

    let proposal = BlockProposal::new_retry_regular(
        owner1,
        Round::SingleLeader(5),
        certificate2.clone(),
        &signer,
    )
    .await
    .unwrap();
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = env
        .executing_worker()
        .handle_block_proposal(proposal)
        .await?;
    let (response, _) = env
        .executing_worker()
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
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.leader, Some(owner0));
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(6));

    // Since the validator now voted for block2, it can't vote for block1 anymore.
    let proposal = proposed_block1
        .into_proposal_with_round(owner0, &signer, Round::SingleLeader(6))
        .await
        .unwrap();
    let result = env
        .executing_worker()
        .handle_block_proposal(proposal.clone())
        .await;
    assert_matches!(result, Err(WorkerError::ChainError(error))
         if matches!(*error, ChainError::HasIncompatibleConfirmedVote(_, _))
    );

    // Let rounds 6 and 7 time out.
    let certificate_timeout =
        env.make_certificate_with_round(value_timeout, Round::SingleLeader(7));
    let (response, _) = env
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::SingleLeader(8));

    // The worker updates its locking block even if it's from a past round and it doesn't sign
    // to confirm.
    let certificate = env.make_certificate_with_round(value1, Round::SingleLeader(7));
    let worker = env.executing_worker().clone();
    worker
        .handle_validated_certificate(certificate.clone())
        .await?;
    let (response, _) = worker.handle_chain_info_query(query_values).await?;
    assert_eq!(
        response.info.manager.requested_locking,
        Some(Box::new(LockingBlock::Regular(certificate)))
    );
    assert_ne!(
        response.info.manager.pending.unwrap().kind(),
        CertificateKind::Confirmed
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
    let mut signer = InMemorySigner::new(None);
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let small_transfer = Amount::from_micros(1);
    let chain_id = chain_1_desc.id();

    // Add another owner and configure two multi-leader rounds.
    let proposed_block0 =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![owner0],
            owners: vec![(owner0, 100), (owner1, 100)],
            first_leader: None,
            multi_leader_rounds: 2,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                ..TimeoutConfig::default()
            },
        });
    let (block0, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .executing_worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // So owner 1 cannot propose a block in this round. And the next round hasn't started yet.
    let proposal = make_child_block(&value0)
        .with_simple_transfer(chain_id, small_transfer)
        .into_proposal_with_round(owner1, &signer, Round::Fast)
        .await
        .unwrap();
    let result = env.executing_worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::InvalidOwner));
    let proposal = make_child_block(&value0)
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let result = env.executing_worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::WrongRound(Round::Fast))
    );

    // The round hasn't timed out yet, so the validator won't sign a leader timeout vote yet.
    let query = ChainInfoQuery::new(chain_id).with_timeout(BlockHeight(1), Round::Fast);
    let result = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await;
    assert_matches!(result, Err(WorkerError::ChainError(ref error))
        if matches!(**error, ChainError::NotTimedOutYet(_))
    );

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Now the validator will sign a leader timeout vote.
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query)
        .await?;
    let vote = response.info.manager.timeout_vote.clone().unwrap();
    let value_timeout = Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0));

    // Once we provide the validator with a timeout certificate, the next round starts.
    let certificate_timeout = vote
        .with_value(value_timeout)
        .unwrap()
        .into_certificate(env.executing_worker().public_key());
    let (response, _) = env
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. And multi-leader rounds can be skipped without timeout.
    let block1 = make_child_block(&value0).with_simple_transfer(chain_id, small_transfer);
    let proposal1 = block1
        .clone()
        .with_authenticated_owner(Some(owner1))
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(1))
        .await
        .unwrap();
    let (_, actions) = env
        .executing_worker()
        .handle_block_proposal(proposal1)
        .await?;
    assert_matches!(actions.notifications[0].reason, Reason::NewRound { .. });
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query_values)
        .await?;
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
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::from_tokens(2)).await;
    let small_transfer = Amount::from_micros(1);
    let chain_id = chain_1_desc.id();

    // Configure open multi-leader rounds.
    let change_ownership_block =
        make_first_block(chain_id).with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![],
            owners: vec![(owner, 100)],
            first_leader: None,
            multi_leader_rounds: 2,
            open_multi_leader_rounds: true,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                ..TimeoutConfig::default()
            },
        });
    let (change_ownership_block, _, _) = env
        .executing_worker()
        .stage_block_execution(change_ownership_block, None, vec![])
        .await?;
    let change_ownership_value = ConfirmedBlock::new(change_ownership_block);
    let change_ownership_certificate = env.make_certificate(change_ownership_value.clone());
    env.executing_worker()
        .fully_handle_certificate_with_notifications(change_ownership_certificate, &())
        .await?;

    // The first round is the multi-leader round 0. Anyone is allowed to propose.
    // But non-owners are not allowed to transfer the chain's funds.
    let proposal = make_child_block(&change_ownership_value)
        .with_burn(Amount::from_tokens(1))
        .into_proposal_with_round(owner, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let result = env.executing_worker().handle_block_proposal(proposal).await;
    assert_matches!(result, Err(WorkerError::ChainError(error)) if matches!(&*error,
        ChainError::ExecutionError(error, _) if matches!(&**error,
        ExecutionError::UnauthenticatedTransferOwner
    )));

    // Without the transfer, a random key pair can propose a block.
    let proposal = make_child_block(&change_ownership_value)
        .with_simple_transfer(chain_id, small_transfer)
        .with_authenticated_owner(Some(owner))
        .into_proposal_with_round(owner, &signer, Round::MultiLeader(0))
        .await
        .unwrap();
    let (block, _, _) = env
        .executing_worker()
        .stage_block_execution(proposal.content.block.clone(), None, vec![])
        .await?;
    let value = ConfirmedBlock::new(block);
    let (response, _) = env
        .executing_worker()
        .handle_block_proposal(proposal)
        .await?;
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
    let mut signer = InMemorySigner::new(None);
    let key_pairs = generate_key_pairs(&mut signer, 2);
    let owner0 = AccountOwner::from(key_pairs[0]);
    let owner1 = AccountOwner::from(key_pairs[1]);
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner0, Amount::from_tokens(2)).await;
    let chain_id = chain_1_desc.id();

    // Add another owner and configure two multi-leader rounds.
    let proposed_block0 = make_first_block(chain_id)
        .with_transfer(
            AccountOwner::CHAIN,
            Account::new(chain_id, owner0),
            Amount::from_tokens(1),
        )
        .with_authenticated_owner(Some(owner0))
        .with_operation(SystemOperation::ChangeOwnership {
            super_owners: vec![owner0],
            owners: vec![(owner0, 100), (owner1, 100)],
            first_leader: None,
            multi_leader_rounds: 3,
            open_multi_leader_rounds: false,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(TimeDelta::from_millis(5)),
                ..TimeoutConfig::default()
            },
        });
    let (block0, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block0, None, vec![])
        .await?;
    let value0 = ConfirmedBlock::new(block0);
    let certificate0 = env.make_certificate(value0.clone());
    let response = env
        .executing_worker()
        .fully_handle_certificate_with_notifications(certificate0, &())
        .await?;

    // The first round is the fast-block round, and owner 0 is a super owner.
    assert_eq!(response.info.manager.current_round, Round::Fast);
    assert_eq!(response.info.manager.leader, None);

    // Owner 0 proposes another block. The validator votes to confirm.
    let proposed_block1 = make_child_block(&value0)
        .with_transfer(
            AccountOwner::CHAIN,
            Account::new(chain_id, owner0),
            Amount::from_micros(1),
        )
        .with_authenticated_owner(Some(owner0));
    let proposal1 = proposed_block1
        .clone()
        .into_proposal_with_round(owner0, &signer, Round::Fast)
        .await
        .unwrap();
    let (block1, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block1.clone(), None, vec![])
        .await?;
    let value1 = ConfirmedBlock::new(block1);
    let (response, _) = env
        .executing_worker()
        .handle_block_proposal(proposal1.clone())
        .await?;
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::Fast);
    assert_eq!(vote.value.value_hash, value1.hash());

    // Set the clock to the end of the round.
    clock.set(response.info.manager.round_timeout.unwrap());

    // Once we provide the validator with a timeout certificate, the next round starts.
    let value_timeout = Timeout::new(chain_id, BlockHeight::from(1), Epoch::from(0));
    let certificate_timeout = env.make_certificate_with_round(value_timeout.clone(), Round::Fast);
    let (response, _) = env
        .executing_worker()
        .handle_timeout_certificate(certificate_timeout)
        .await?;
    assert_eq!(response.info.manager.current_round, Round::MultiLeader(0));
    assert_eq!(response.info.manager.leader, None);

    // Now any owner can propose a block. But block1 is locked. Re-proposing it is allowed.
    let proposal1b =
        BlockProposal::new_retry_fast(owner1, Round::MultiLeader(0), proposal1.clone(), &signer)
            .await
            .unwrap();
    let (response, _) = env
        .executing_worker()
        .handle_block_proposal(proposal1b)
        .await?;

    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.round, Round::MultiLeader(0));
    assert_eq!(vote.value.value_hash, value1.hash());

    // Proposing a different block is not.
    let proposed_block2 = make_child_block(&value0)
        .with_simple_transfer(chain_id, Amount::ONE)
        .with_authenticated_owner(Some(owner1));
    let proposal2 = proposed_block2
        .clone()
        .into_proposal_with_round(owner1, &signer, Round::MultiLeader(1))
        .await
        .unwrap();
    let result = env
        .executing_worker()
        .handle_block_proposal(proposal2)
        .await;
    assert_matches!(result, Err(WorkerError::ChainError(err))
        if matches!(*err, ChainError::HasIncompatibleConfirmedVote(_, Round::Fast))
    );
    let proposal3 =
        BlockProposal::new_retry_fast(owner0, Round::MultiLeader(2), proposal1.clone(), &signer)
            .await
            .unwrap();
    env.executing_worker()
        .handle_block_proposal(proposal3)
        .await?;

    // A validated block certificate from a later round can override the locked fast block.
    let (block2, _, _) = env
        .executing_worker()
        .stage_block_execution(proposed_block2.clone(), None, vec![])
        .await?;
    let value2 = ValidatedBlock::new(block2.clone());
    let certificate2 = env.make_certificate_with_round(value2.clone(), Round::MultiLeader(0));
    let proposal = BlockProposal::new_retry_regular(
        owner1,
        Round::MultiLeader(3),
        certificate2.clone(),
        &signer,
    )
    .await
    .unwrap();
    let lite_value2 = LiteValue::new(&value2);
    let (_, _) = env
        .executing_worker()
        .handle_block_proposal(proposal)
        .await?;
    let query_values = ChainInfoQuery::new(chain_id).with_manager_values();
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query_values)
        .await?;
    assert_eq!(
        response.info.manager.requested_locking,
        Some(Box::new(LockingBlock::Regular(certificate2)))
    );
    let vote = response.info.manager.pending.as_ref().unwrap();
    assert_eq!(vote.value, lite_value2);
    assert_eq!(vote.round, Round::MultiLeader(3));
    Ok(())
}

/// Tests that fallback mode is triggered when epoch e+1 exists on the admin chain
/// and is older than the fallback_duration.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_fallback<B>(mut storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let mut env = TestEnvironment::new(&mut storage_builder, true, false).await?;
    let clock = storage_builder.clock();
    let committee = env.committee().clone();
    let admin_chain_id = env.admin_description.id();

    // Create a non-admin chain with fallback_duration configured.
    let balance = Amount::from_tokens(5);
    let mut ownership = ChainOwnership::single(public_key.into());
    // Configure a fallback duration. (The default is `MAX`, i.e. never.)
    ownership.timeout_config.fallback_duration = TimeDelta::from_secs(5);
    let chain_desc = env
        .add_root_chain_with_ownership(1, balance, ownership)
        .await;
    let chain_id = chain_desc.id();

    // At time 0 we don't vote for fallback mode.
    let query = ChainInfoQuery::new(chain_id)
        .with_fallback()
        .with_committees();
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await?;
    let manager = response.info.manager;
    assert!(manager.fallback_vote.is_none());
    assert_eq!(manager.current_round, Round::MultiLeader(0));
    assert!(manager.leader.is_none());
    let fallback_duration = manager.ownership.timeout_config.fallback_duration;

    // Even if a long time passes: Without a new epoch there's no fallback mode.
    clock.add(fallback_duration);
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // Reset clock to 0 before creating the epoch, so that the epoch timestamp is 0.
    clock.set(Timestamp::from(0));

    // Create epoch 1 on the admin chain.
    let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
    let blob_hash = committee_blob.id().hash;
    env.write_blobs(&[committee_blob]).await?;
    let proposal = make_first_block(admin_chain_id).with_operation(SystemOperation::Admin(
        AdminOperation::CreateCommittee {
            epoch: Epoch::from(1),
            blob_hash,
        },
    ));
    let certificate = env.execute_proposal(proposal, vec![]).await?;
    env.executing_worker()
        .fully_handle_certificate_with_notifications(certificate, &())
        .await?;

    // Epoch was just created at time 0: No fallback mode yet.
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await?;
    assert!(response.info.manager.fallback_vote.is_none());

    // Advance time past the fallback_duration. Now we should vote for fallback.
    clock.add(fallback_duration);
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await?;
    let vote = response.info.manager.fallback_vote.unwrap();
    let value = Timeout::new(chain_id, BlockHeight(0), Epoch::ZERO);
    let round = Round::SingleLeader(u32::MAX);
    assert_eq!(vote.value.value_hash, value.hash());
    assert_eq!(vote.round, round);
    let certificate = env.make_certificate_with_round(value, round);
    env.executing_worker()
        .handle_timeout_certificate(certificate)
        .await?;

    // Now we are in fallback mode, and the validator is the leader.
    let (response, _) = env
        .executing_worker()
        .handle_chain_info_query(query.clone())
        .await?;
    let manager = response.info.manager;
    let expected_key = response
        .info
        .requested_committees
        .unwrap()
        .get(&response.info.epoch)
        .unwrap()
        .validators
        .get(&env.executing_worker().public_key())
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

    let mut env = TestEnvironment::new(&mut storage_builder, false, true).await?;
    let clock = storage_builder.clock();

    let chain_description = env
        .add_root_chain(
            1,
            AccountSecretKey::generate().public().into(),
            Amount::from_tokens(2),
        )
        .await;
    let chain_id = chain_description.id();

    let (application_id, application) = env.register_mock_application(chain_id, 0).await?;

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
            env.executing_worker()
                .query_application(chain_id, query.clone(), None)
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

    let mut signer = InMemorySigner::new(None);
    let public_key = signer.generate_new();
    let owner = public_key.into();
    let balance = Amount::from_tokens(1);
    let small_transfer = Amount::from_micros(1);

    let mut env = TestEnvironment::new(&mut storage_builder, false, true).await?;
    let clock = storage_builder.clock();
    let chain_1_desc = env.add_root_chain(1, owner, balance).await;
    let chain_2_desc = env.add_root_chain(2, owner, balance).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    let (application_id, application) = env.register_mock_application(chain_1, 0).await?;

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
                chain_id: chain_1,
                next_block_height: BlockHeight(0),
                local_time,
            });
    let query_contexts_after_new_block =
        queries_after_new_block
            .clone()
            .map(|local_time| QueryContext {
                chain_id: chain_1,
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
            env.executing_worker()
                .query_application(chain_1, query.clone(), None)
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    clock.set(Timestamp::from(BLOCK_TIMESTAMP));
    let block = make_first_block(chain_1)
        .with_timestamp(Timestamp::from(BLOCK_TIMESTAMP))
        .with_simple_transfer(chain_2, small_transfer)
        .with_authenticated_owner(Some(owner));

    let block_proposal = block
        .clone()
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();
    env.executing_worker()
        .handle_block_proposal(block_proposal)
        .await?;

    for local_time in queries_before_confirmation {
        clock.set(local_time);

        assert_eq!(
            env.executing_worker()
                .query_application(chain_1, query.clone(), None)
                .await?,
            QueryOutcome {
                response: QueryResponse::User(vec![]),
                operations: vec![],
            }
        );
    }

    let mut state = SystemExecutionState {
        timestamp: Timestamp::from(BLOCK_TIMESTAMP),
        balance: balance - small_transfer,
        ..env.system_execution_state(&chain_1_desc.id())
    }
    .into_view()
    .await;
    state.register_mock_application(0).await?;

    let certificate = env.execute_proposal(block.clone(), vec![]).await?;

    assert!(certificate.value().matches_proposed_block(&block));
    assert!(certificate.block().outcome_matches(
        vec![vec![direct_credit_message(chain_2, small_transfer)]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult::default()],
    ));

    for _ in query_contexts_after_new_block.clone() {
        application.expect_call(ExpectedCall::handle_query(move |_runtime, query| {
            assert!(query.is_empty());
            Ok(vec![])
        }));
    }

    for local_time in queries_after_new_block {
        clock.set(local_time);

        assert_eq!(
            env.executing_worker()
                .query_application(chain_1, query.clone(), None)
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
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[cfg_attr(feature = "rocksdb", test_case(RocksDbStorageBuilder::new().await; "rocks_db"))]
#[cfg_attr(feature = "dynamodb", test_case(DynamoDbStorageBuilder::default(); "dynamo_db"))]
#[cfg_attr(feature = "scylladb", test_case(ScyllaDbStorageBuilder::default(); "scylla_db"))]
#[test_log::test(tokio::test)]
async fn test_stage_block_with_message_earlier_than_cursor<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut signer = InMemorySigner::new(None);
    let receiver_public_key = signer.generate_new();
    let owner = receiver_public_key.into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let chain_1_desc = env.add_root_chain(1, owner, Amount::from_tokens(10)).await;
    let chain_2_desc = env.add_root_chain(2, owner, Amount::ZERO).await;
    let chain_1 = chain_1_desc.id();
    let chain_2 = chain_2_desc.id();

    // Transfer some funds to `owner` on chain 2, so that we can claim it later.
    let transfer_cert = env
        .execute_proposal(
            make_first_block(chain_1)
                .with_transfer(
                    AccountOwner::CHAIN,
                    Account::new(chain_2, owner),
                    Amount::from_tokens(2),
                )
                .with_authenticated_owner(Some(owner)),
            vec![],
        )
        .await?;

    // Accept the transfer on chain 2.
    let chain_2_first_block = env
        .execute_proposal(
            make_first_block(chain_2).with_incoming_bundle(IncomingBundle {
                origin: chain_1,
                bundle: MessageBundle {
                    certificate_hash: transfer_cert.hash(),
                    height: BlockHeight::ZERO,
                    timestamp: Timestamp::from(0),
                    transaction_index: 0,
                    messages: vec![Message::System(SystemMessage::Credit {
                        source: AccountOwner::CHAIN,
                        target: owner,
                        amount: Amount::from_tokens(2),
                    })
                    .to_posted(0, MessageKind::Tracked)],
                },
                action: MessageAction::Accept,
            }),
            vec![],
        )
        .await?;

    // Simulate a certificate sending two messages from chain_1 to chain_2.
    // The first message is a skippable `Withdraw` message, which makes it possible to receive only
    // the second one.
    let sender_cert = env
        .execute_proposal(
            make_child_block(transfer_cert.value())
                .with_operation(SystemOperation::Claim {
                    owner,
                    target_id: chain_2,
                    recipient: Account::chain(chain_1),
                    amount: Amount::from_tokens(2),
                })
                .with_simple_transfer(chain_2, Amount::from_tokens(2))
                .with_authenticated_owner(Some(owner)),
            vec![],
        )
        .await?;

    // Process the second message bundle on chain_2. This advances next_cursor_to_remove
    // to height=0, index=1.
    let block_proposal =
        make_child_block(chain_2_first_block.value()).with_incoming_bundle(IncomingBundle {
            origin: chain_1,
            bundle: MessageBundle {
                certificate_hash: sender_cert.hash(),
                height: BlockHeight::from(1),
                timestamp: Timestamp::from(0),
                transaction_index: 1,
                messages: vec![system_credit_message(Amount::from_tokens(2))
                    .to_posted(1, MessageKind::Tracked)],
            },
            action: MessageAction::Accept,
        });

    let certificate_chain_2 = env.execute_proposal(block_proposal.clone(), vec![]).await?;

    assert!(certificate_chain_2
        .value()
        .matches_proposed_block(&block_proposal));
    assert!(certificate_chain_2.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![],
    ));

    // Now try to stage a block with the earlier message (transaction_index: 0).
    // This should fail with IncorrectMessageOrder because next_cursor_to_remove
    // is now at index 1, but we're trying to process index 0.
    let bad_proposed_block = make_child_block(&certificate_chain_2.into_value())
        .with_incoming_bundle(IncomingBundle {
            origin: chain_1,
            bundle: MessageBundle {
                certificate_hash: sender_cert.hash(),
                height: BlockHeight::from(1),
                timestamp: Timestamp::from(0),
                transaction_index: 0,
                messages: vec![Message::System(SystemMessage::Withdraw {
                    owner,
                    recipient: Account::chain(chain_2),
                    amount: Amount::from_tokens(2),
                })
                .to_posted(0, MessageKind::Simple)],
            },
            action: MessageAction::Accept,
        });

    // Test stage_block_execution directly - this should fail with IncorrectMessageOrder.
    assert_matches!(
        env.executing_worker()
            .stage_block_execution(bad_proposed_block.clone(), None, vec![])
            .await,
        Err(WorkerError::ChainError(chain_error))
            if matches!(*chain_error, ChainError::IncorrectMessageOrder { .. })
    );

    // Also test handle_block_proposal for completeness.
    let bad_proposal = bad_proposed_block
        .into_first_proposal(owner, &signer)
        .await
        .unwrap();

    assert_matches!(
        env.executing_worker().handle_block_proposal(bad_proposal).await,
        Err(WorkerError::ChainError(chain_error))
            if matches!(*chain_error, ChainError::IncorrectMessageOrder { .. })
    );

    Ok(())
}
