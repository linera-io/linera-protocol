// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeSet, HashSet},
    fmt::Debug,
};

use async_graphql::SimpleObject;
use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::{Blob, BlockHeight, OracleResponse, Timestamp},
    hashed::Hashed,
    identifiers::{BlobId, BlobType, ChainId, MessageId, Owner},
};
use linera_execution::{committee::Epoch, system::OpenChainConfig, Operation, SystemOperation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    data_types::{
        BlockExecutionOutcome, EventRecord, ExecutedBlock, IncomingBundle, Medium, MessageAction,
        MessageBundle, OutgoingMessage, PostedMessage, Proposal, Transaction,
    },
    ChainError,
};

/// Wrapper around an `ExecutedBlock` that has been validated.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ValidatedBlock(Hashed<Block>);

impl ValidatedBlock {
    /// Creates a new `ValidatedBlock` from an `ExecutedBlock`.
    pub fn new(block: ExecutedBlock) -> Self {
        Self(Hashed::new(block.into()))
    }

    pub fn from_hashed(block: Hashed<Block>) -> Self {
        Self(block)
    }

    pub fn inner(&self) -> &Hashed<Block> {
        &self.0
    }

    /// Returns a reference to the [`Block`] contained in this `ValidatedBlock`.
    pub fn block(&self) -> &Block {
        self.0.inner()
    }

    /// Consumes this `ValidatedBlock`, returning the [`Block`] it contains.
    pub fn into_inner(self) -> Block {
        self.0.into_inner()
    }

    pub fn to_log_str(&self) -> &'static str {
        "validated_block"
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().header.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().header.height
    }

    pub fn epoch(&self) -> Epoch {
        self.0.inner().header.epoch
    }
}

impl<'de> BcsHashable<'de> for ValidatedBlock {}

/// Wrapper around an `ExecutedBlock` that has been confirmed.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfirmedBlock(Hashed<Block>);

#[async_graphql::Object(cache_control(no_cache))]
impl ConfirmedBlock {
    #[graphql(derived(name = "block"))]
    async fn _block(&self) -> Block {
        self.0.inner().clone()
    }

    async fn status(&self) -> String {
        "confirmed".to_string()
    }
}

impl<'de> BcsHashable<'de> for ConfirmedBlock {}

impl ConfirmedBlock {
    pub fn new(block: ExecutedBlock) -> Self {
        Self(Hashed::new(block.into()))
    }

    pub fn from_hashed(block: Hashed<Block>) -> Self {
        Self(block)
    }

    pub fn inner(&self) -> &Hashed<Block> {
        &self.0
    }

    pub fn take_inner(self) -> Hashed<Block> {
        self.0
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ConfirmedBlock`.
    pub fn block(&self) -> &Block {
        self.0.inner()
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> Block {
        self.0.into_inner()
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().header.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().header.height
    }

    pub fn to_log_str(&self) -> &'static str {
        "confirmed_block"
    }

    /// Creates a `HashedCertificateValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> Hashed<ConfirmedBlock> {
        Hashed::unchecked_new(self, hash)
    }

    fn with_hash(self) -> Hashed<Self> {
        let hash = CryptoHash::new(&self);
        Hashed::unchecked_new(self, hash)
    }

    /// Creates a `HashedCertificateValue` checking that this is the correct hash.
    pub fn with_hash_checked(self, hash: CryptoHash) -> Result<Hashed<ConfirmedBlock>, ChainError> {
        let hashed_certificate_value = self.with_hash();
        if hashed_certificate_value.hash() == hash {
            Ok(hashed_certificate_value)
        } else {
            Err(ChainError::CertificateValueHashMismatch {
                expected: hash,
                actual: hashed_certificate_value.hash(),
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct Timeout {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub epoch: Epoch,
}

impl Timeout {
    pub fn new(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> Self {
        Self {
            chain_id,
            height,
            epoch,
        }
    }

    pub fn to_log_str(&self) -> &'static str {
        "timeout"
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.height
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl<'de> BcsHashable<'de> for Timeout {}

/// Failure to convert a `Certificate` into one of the expected certificate types.
#[derive(Clone, Copy, Debug, Error)]
pub enum ConversionError {
    /// Failure to convert to [`ConfirmedBlock`] certificate.
    #[error("Expected a `ConfirmedBlockCertificate` value")]
    ConfirmedBlock,

    /// Failure to convert to [`ValidatedBlock`] certificate.
    #[error("Expected a `ValidatedBlockCertificate` value")]
    ValidatedBlock,

    /// Failure to convert to [`Timeout`] certificate.
    #[error("Expected a `TimeoutCertificate` value")]
    Timeout,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct Block {
    pub header: BlockHeader,
    pub body: BlockBody,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockHeader {
    pub version: u8, // TODO: More granular versioning. #3078
    pub chain_id: ChainId,
    pub epoch: Epoch,
    pub height: BlockHeight,
    pub timestamp: Timestamp,
    pub state_hash: CryptoHash,
    pub previous_block_hash: Option<CryptoHash>,
    pub authenticated_signer: Option<Owner>,

    // Inputs to the block, chosen by the block proposer.
    pub bundles_hash: CryptoHash,
    pub operations_hash: CryptoHash,

    // Outcome of the block execution.
    pub messages_hash: CryptoHash,
    pub oracle_responses_hash: CryptoHash,
    pub events_hash: CryptoHash,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockBody {
    pub incoming_bundles: Vec<IncomingBundle>,
    pub operations: Vec<Operation>,
    pub messages: Vec<Vec<OutgoingMessage>>,
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    pub events: Vec<Vec<EventRecord>>,
}

impl BlockHeader {
    /// Returns the message ID belonging to the `index`th outgoing message in this block.
    pub fn message_id(&self, index: u32) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index,
        }
    }
}

impl BlockBody {
    pub fn oracle_blob_ids(&self) -> HashSet<BlobId> {
        let mut required_blob_ids = HashSet::new();
        for responses in &self.oracle_responses {
            for response in responses {
                if let OracleResponse::Blob(blob_id) = response {
                    required_blob_ids.insert(*blob_id);
                }
            }
        }

        required_blob_ids
    }

    pub fn has_oracle_responses(&self) -> bool {
        self.oracle_responses
            .iter()
            .any(|responses| !responses.is_empty())
    }
}

impl Block {
    pub fn messages(&self) -> &Vec<Vec<OutgoingMessage>> {
        &self.body.messages
    }

    /// Returns the bundles of messages sent via the given medium to the specified
    /// recipient. Messages originating from different transactions of the original block
    /// are kept in separate bundles. If the medium is a channel, does not verify that the
    /// recipient is actually subscribed to that channel.
    pub fn message_bundles_for<'a>(
        &'a self,
        medium: &'a Medium,
        recipient: ChainId,
        certificate_hash: CryptoHash,
    ) -> impl Iterator<Item = (Epoch, MessageBundle)> + 'a {
        let mut index = 0u32;
        let block_height = self.header.height;
        let block_timestamp = self.header.timestamp;
        let block_epoch = self.header.epoch;

        (0u32..)
            .zip(self.messages())
            .filter_map(move |(transaction_index, txn_messages)| {
                let messages = (index..)
                    .zip(txn_messages)
                    .filter(|(_, message)| message.has_destination(medium, recipient))
                    .map(|(idx, message)| message.clone().into_posted(idx))
                    .collect::<Vec<_>>();
                index += txn_messages.len() as u32;
                (!messages.is_empty()).then(|| {
                    let bundle = MessageBundle {
                        height: block_height,
                        timestamp: block_timestamp,
                        certificate_hash,
                        transaction_index,
                        messages,
                    };
                    (block_epoch, bundle)
                })
            })
    }

    /// Returns the `message_index`th outgoing message created by the `operation_index`th operation,
    /// or `None` if there is no such operation or message.
    pub fn message_id_for_operation(
        &self,
        operation_index: usize,
        message_index: u32,
    ) -> Option<MessageId> {
        let block = &self.body;
        let transaction_index = block.incoming_bundles.len().checked_add(operation_index)?;
        if message_index >= u32::try_from(self.body.messages.get(transaction_index)?.len()).ok()? {
            return None;
        }
        let first_message_index = u32::try_from(
            self.body
                .messages
                .iter()
                .take(transaction_index)
                .map(Vec::len)
                .sum::<usize>(),
        )
        .ok()?;
        let index = first_message_index.checked_add(message_index)?;
        Some(self.header.message_id(index))
    }

    pub fn message_by_id(&self, message_id: &MessageId) -> Option<&OutgoingMessage> {
        let MessageId {
            chain_id,
            height,
            index,
        } = message_id;
        if self.header.chain_id != *chain_id || self.header.height != *height {
            return None;
        }
        let mut index = usize::try_from(*index).ok()?;
        for messages in self.messages() {
            if let Some(message) = messages.get(index) {
                return Some(message);
            }
            index -= messages.len();
        }
        None
    }

    pub fn required_blob_ids(&self) -> HashSet<BlobId> {
        let mut blob_ids = self.body.oracle_blob_ids();
        blob_ids.extend(self.published_blob_ids());
        blob_ids
    }

    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.body.oracle_blob_ids().contains(blob_id) || self.published_blob_ids().contains(blob_id)
    }

    /// Returns all the published blob IDs in this block's operations.
    pub fn published_blob_ids(&self) -> BTreeSet<BlobId> {
        let mut blob_ids = BTreeSet::new();
        for operation in &self.body.operations {
            if let Operation::System(SystemOperation::PublishDataBlob { blob_hash }) = operation {
                blob_ids.insert(BlobId::new(*blob_hash, BlobType::Data));
            }
            if let Operation::System(SystemOperation::PublishBytecode { bytecode_id }) = operation {
                blob_ids.extend([
                    BlobId::new(bytecode_id.contract_blob_hash, BlobType::ContractBytecode),
                    BlobId::new(bytecode_id.service_blob_hash, BlobType::ServiceBytecode),
                ]);
            }
        }

        blob_ids
    }

    /// Returns whether the block contains only rejected incoming messages, which
    /// makes it admissible even on closed chains.
    pub fn has_only_rejected_messages(&self) -> bool {
        self.body.operations.is_empty()
            && self
                .body
                .incoming_bundles
                .iter()
                .all(|message| message.action == MessageAction::Reject)
    }

    /// Returns an iterator over all incoming [`PostedMessage`]s in this block.
    pub fn incoming_messages(&self) -> impl Iterator<Item = &PostedMessage> {
        self.body
            .incoming_bundles
            .iter()
            .flat_map(|incoming_bundle| &incoming_bundle.bundle.messages)
    }

    /// Returns the number of incoming messages.
    pub fn message_count(&self) -> usize {
        self.body
            .incoming_bundles
            .iter()
            .map(|im| im.bundle.messages.len())
            .sum()
    }

    /// Returns an iterator over all transactions, by index.
    pub fn transactions(&self) -> impl Iterator<Item = (u32, Transaction<'_>)> {
        let bundles = self
            .body
            .incoming_bundles
            .iter()
            .map(Transaction::ReceiveMessages);
        let operations = self
            .body
            .operations
            .iter()
            .map(Transaction::ExecuteOperation);
        (0u32..).zip(bundles.chain(operations))
    }

    /// If the block's first message is `OpenChain`, returns the bundle, the message and
    /// the configuration for the new chain.
    pub fn starts_with_open_chain_message(
        &self,
    ) -> Option<(&IncomingBundle, &PostedMessage, &OpenChainConfig)> {
        let in_bundle = self.body.incoming_bundles.first()?;
        if in_bundle.action != MessageAction::Accept {
            return None;
        }
        let posted_message = in_bundle.bundle.messages.first()?;
        let config = posted_message.message.matches_open_chain()?;
        Some((in_bundle, posted_message, config))
    }

    pub fn check_proposal_size(
        &self,
        maximum_block_proposal_size: u64,
        blobs: &[Blob],
    ) -> Result<(), ChainError> {
        let size = linera_base::bcs::serialized_size(&(self, blobs))?;
        linera_base::ensure!(
            size <= usize::try_from(maximum_block_proposal_size).unwrap_or(usize::MAX),
            ChainError::BlockProposalTooLarge
        );
        Ok(())
    }

    pub fn eq_proposal(&self, proposal: &Proposal) -> bool {
        self.header.chain_id == proposal.chain_id
            && self.header.epoch == proposal.epoch
            && self.header.height == proposal.height
            && self.header.timestamp == proposal.timestamp
            && self.body.incoming_bundles == proposal.incoming_bundles
            && self.body.operations == proposal.operations
            && self.header.authenticated_signer == proposal.authenticated_signer
            && self.header.previous_block_hash == proposal.previous_block_hash
    }

    pub fn new(proposal: Proposal, outcome: BlockExecutionOutcome) -> Self {
        fn hash_vec<'de, T: BcsHashable<'de>>(it: &Vec<T>) -> CryptoHash {
            CryptoHash::new(&it.iter().map(CryptoHash::new).collect::<Vec<_>>())
        }
        let bundles_hash = hash_vec(&proposal.incoming_bundles);
        let messages_hash =
            CryptoHash::new(&outcome.messages.iter().map(hash_vec).collect::<Vec<_>>());
        let operations_hash = hash_vec(&proposal.operations);
        let oracle_responses_hash = CryptoHash::new(
            &outcome
                .oracle_responses
                .iter()
                .map(hash_vec)
                .collect::<Vec<_>>(),
        );
        let events_hash = CryptoHash::new(&outcome.events.iter().map(hash_vec).collect::<Vec<_>>());

        let header = BlockHeader {
            version: 0,
            chain_id: proposal.chain_id,
            epoch: proposal.epoch,
            height: proposal.height,
            timestamp: proposal.timestamp,
            state_hash: outcome.state_hash,
            previous_block_hash: proposal.previous_block_hash,
            authenticated_signer: proposal.authenticated_signer,
            bundles_hash,
            operations_hash,
            messages_hash,
            oracle_responses_hash,
            events_hash,
        };

        let body = BlockBody {
            incoming_bundles: proposal.incoming_bundles,
            operations: proposal.operations,
            messages: outcome.messages,
            oracle_responses: outcome.oracle_responses,
            events: outcome.events,
        };

        Self { header, body }
    }
}

impl From<ExecutedBlock> for Block {
    fn from(executed_block: ExecutedBlock) -> Self {
        Block::new(executed_block.block, executed_block.outcome)
    }
}

impl From<Block> for ExecutedBlock {
    fn from(block: Block) -> Self {
        let proposal = Proposal {
            chain_id: block.header.chain_id,
            epoch: block.header.epoch,
            height: block.header.height,
            timestamp: block.header.timestamp,
            incoming_bundles: block.body.incoming_bundles,
            operations: block.body.operations,
            authenticated_signer: block.header.authenticated_signer,
            previous_block_hash: block.header.previous_block_hash,
        };

        let outcome = BlockExecutionOutcome {
            state_hash: block.header.state_hash,
            messages: block.body.messages,
            oracle_responses: block.body.oracle_responses,
            events: block.body.events,
        };

        ExecutedBlock {
            block: proposal,
            outcome,
        }
    }
}

impl<'de> BcsHashable<'de> for Block {}
