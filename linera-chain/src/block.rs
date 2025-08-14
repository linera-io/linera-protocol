// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use async_graphql::SimpleObject;
use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::{Blob, BlockHeight, Epoch, Event, OracleResponse, Timestamp},
    hashed::Hashed,
    identifiers::{AccountOwner, BlobId, BlobType, ChainId, StreamId},
};
use linera_execution::{BlobState, Operation, OutgoingMessage};
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use thiserror::Error;

use crate::{
    data_types::{
        BlockExecutionOutcome, IncomingBundle, MessageBundle, OperationResult, OutgoingMessageExt,
        ProposedBlock, Transaction,
    },
    types::CertificateValue,
};

/// Wrapper around a `Block` that has been validated.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ValidatedBlock(Hashed<Block>);

impl ValidatedBlock {
    /// Creates a new `ValidatedBlock` from a `Block`.
    pub fn new(block: Block) -> Self {
        Self(Hashed::new(block))
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

/// Wrapper around a `Block` that has been confirmed.
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

    async fn hash(&self) -> CryptoHash {
        self.0.hash()
    }
}

impl ConfirmedBlock {
    pub fn new(block: Block) -> Self {
        Self(Hashed::new(block))
    }

    pub fn from_hashed(block: Hashed<Block>) -> Self {
        Self(block)
    }

    pub fn inner(&self) -> &Hashed<Block> {
        &self.0
    }

    pub fn into_inner(self) -> Hashed<Block> {
        self.0
    }

    /// Returns a reference to the `Block` contained in this `ConfirmedBlock`.
    pub fn block(&self) -> &Block {
        self.0.inner()
    }

    /// Consumes this `ConfirmedBlock`, returning the `Block` it contains.
    pub fn into_block(self) -> Block {
        self.0.into_inner()
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().header.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().header.height
    }

    pub fn timestamp(&self) -> Timestamp {
        self.0.inner().header.timestamp
    }

    pub fn to_log_str(&self) -> &'static str {
        "confirmed_block"
    }

    /// Returns whether this block matches the proposal.
    pub fn matches_proposed_block(&self, block: &ProposedBlock) -> bool {
        self.block().matches_proposed_block(block)
    }

    /// Returns a blob state that applies to all blobs used by this block.
    pub fn to_blob_state(&self, is_stored_block: bool) -> BlobState {
        BlobState {
            last_used_by: is_stored_block.then_some(self.0.hash()),
            chain_id: self.chain_id(),
            block_height: self.height(),
            epoch: is_stored_block.then_some(self.epoch()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timeout(Hashed<TimeoutInner>);

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename = "Timeout")]
pub(crate) struct TimeoutInner {
    chain_id: ChainId,
    height: BlockHeight,
    epoch: Epoch,
}

impl Timeout {
    pub fn new(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> Self {
        let inner = TimeoutInner {
            chain_id,
            height,
            epoch,
        };
        Self(Hashed::new(inner))
    }

    pub fn to_log_str(&self) -> &'static str {
        "timeout"
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().height
    }

    pub fn epoch(&self) -> Epoch {
        self.0.inner().epoch
    }

    pub(crate) fn inner(&self) -> &Hashed<TimeoutInner> {
        &self.0
    }
}

impl BcsHashable<'_> for Timeout {}
impl BcsHashable<'_> for TimeoutInner {}

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

/// Block defines the atomic unit of growth of the Linera chain.
///
/// As part of the block body, contains all the incoming messages
/// and operations to execute which define a state transition of the chain.
/// Resulting messages produced by the operations are also included in the block body,
/// together with oracle responses and events.
#[derive(Debug, PartialEq, Eq, Hash, Clone, SimpleObject)]
pub struct Block {
    /// Header of the block containing metadata of the block.
    pub header: BlockHeader,
    /// Body of the block containing all of the data.
    pub body: BlockBody,
}

impl Serialize for Block {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Block", 2)?;

        let header = SerializedHeader {
            chain_id: self.header.chain_id,
            epoch: self.header.epoch,
            height: self.header.height,
            timestamp: self.header.timestamp,
            state_hash: self.header.state_hash,
            previous_block_hash: self.header.previous_block_hash,
            authenticated_signer: self.header.authenticated_signer,
        };
        state.serialize_field("header", &header)?;
        state.serialize_field("body", &self.body)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Block {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "Block")]
        struct Inner {
            header: SerializedHeader,
            body: BlockBody,
        }
        let inner = Inner::deserialize(deserializer)?;

        let transactions_hash = hashing::hash_vec(&inner.body.transactions);
        let messages_hash = hashing::hash_vec_vec(&inner.body.messages);
        let previous_message_blocks_hash = CryptoHash::new(&PreviousMessageBlocksMap {
            inner: Cow::Borrowed(&inner.body.previous_message_blocks),
        });
        let previous_event_blocks_hash = CryptoHash::new(&PreviousEventBlocksMap {
            inner: Cow::Borrowed(&inner.body.previous_event_blocks),
        });
        let oracle_responses_hash = hashing::hash_vec_vec(&inner.body.oracle_responses);
        let events_hash = hashing::hash_vec_vec(&inner.body.events);
        let blobs_hash = hashing::hash_vec_vec(&inner.body.blobs);
        let operation_results_hash = hashing::hash_vec(&inner.body.operation_results);

        let header = BlockHeader {
            chain_id: inner.header.chain_id,
            epoch: inner.header.epoch,
            height: inner.header.height,
            timestamp: inner.header.timestamp,
            state_hash: inner.header.state_hash,
            previous_block_hash: inner.header.previous_block_hash,
            authenticated_signer: inner.header.authenticated_signer,
            transactions_hash,
            messages_hash,
            previous_message_blocks_hash,
            previous_event_blocks_hash,
            oracle_responses_hash,
            events_hash,
            blobs_hash,
            operation_results_hash,
        };

        Ok(Self {
            header,
            body: inner.body,
        })
    }
}

/// Succinct representation of a block.
/// Contains all the metadata to follow the chain of blocks or verifying
/// inclusion (event, message, oracle response, etc.) in the block's body.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockHeader {
    /// The chain to which this block belongs.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Epoch,
    /// The block height.
    pub height: BlockHeight,
    /// The timestamp when this block was created.
    pub timestamp: Timestamp,
    /// The hash of the chain's execution state after this block.
    pub state_hash: CryptoHash,
    /// Certified hash of the previous block in the chain, if any.
    pub previous_block_hash: Option<CryptoHash>,
    /// The user signing for the operations in the block and paying for their execution
    /// fees. If set, this must be the `owner` in the block proposal. `None` means that
    /// the default account of the chain is used. This value is also used as recipient of
    /// potential refunds for the message grants created by the operations.
    pub authenticated_signer: Option<AccountOwner>,

    // Inputs to the block, chosen by the block proposer.
    /// Cryptographic hash of all the transactions in the block.
    pub transactions_hash: CryptoHash,

    // Outcome of the block execution.
    /// Cryptographic hash of all the messages in the block.
    pub messages_hash: CryptoHash,
    /// Cryptographic hash of the lookup table for previous sending blocks.
    pub previous_message_blocks_hash: CryptoHash,
    /// Cryptographic hash of the lookup table for previous blocks publishing events.
    pub previous_event_blocks_hash: CryptoHash,
    /// Cryptographic hash of all the oracle responses in the block.
    pub oracle_responses_hash: CryptoHash,
    /// Cryptographic hash of all the events in the block.
    pub events_hash: CryptoHash,
    /// Cryptographic hash of all the created blobs in the block.
    pub blobs_hash: CryptoHash,
    /// A cryptographic hash of the execution results of all operations in a block.
    pub operation_results_hash: CryptoHash,
}

/// The body of a block containing all the data included in the block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
#[graphql(complex)]
pub struct BlockBody {
    /// The transactions to execute in this block. Each transaction can be either
    /// incoming messages or an operation.
    #[graphql(skip)]
    pub transactions: Vec<Transaction>,
    /// The list of outgoing messages for each transaction.
    pub messages: Vec<Vec<OutgoingMessage>>,
    /// The hashes and heights of previous blocks that sent messages to the same recipients.
    pub previous_message_blocks: BTreeMap<ChainId, (CryptoHash, BlockHeight)>,
    /// The hashes and heights of previous blocks that published events to the same channels.
    pub previous_event_blocks: BTreeMap<StreamId, (CryptoHash, BlockHeight)>,
    /// The record of oracle responses for each transaction.
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    /// The list of events produced by each transaction.
    pub events: Vec<Vec<Event>>,
    /// The list of blobs produced by each transaction.
    pub blobs: Vec<Vec<Blob>>,
    /// The execution result for each operation.
    pub operation_results: Vec<OperationResult>,
}

impl BlockBody {
    /// Returns all operations in this block body.
    pub fn operations(&self) -> impl Iterator<Item = &Operation> {
        self.transactions.iter().filter_map(|tx| match tx {
            Transaction::ExecuteOperation(operation) => Some(operation),
            Transaction::ReceiveMessages(_) => None,
        })
    }

    /// Returns all incoming bundles in this block body.
    pub fn incoming_bundles(&self) -> impl Iterator<Item = &IncomingBundle> {
        self.transactions.iter().filter_map(|tx| match tx {
            Transaction::ReceiveMessages(bundle) => Some(bundle),
            Transaction::ExecuteOperation(_) => None,
        })
    }
}

#[async_graphql::ComplexObject]
impl BlockBody {
    /// Metadata about the transactions in this block.
    async fn transaction_metadata(&self) -> Vec<crate::data_types::TransactionMetadata> {
        self.transactions
            .iter()
            .map(crate::data_types::TransactionMetadata::from_transaction)
            .collect()
    }
}

impl Block {
    pub fn new(block: ProposedBlock, outcome: BlockExecutionOutcome) -> Self {
        let transactions_hash = hashing::hash_vec(&block.transactions);
        let messages_hash = hashing::hash_vec_vec(&outcome.messages);
        let previous_message_blocks_hash = CryptoHash::new(&PreviousMessageBlocksMap {
            inner: Cow::Borrowed(&outcome.previous_message_blocks),
        });
        let previous_event_blocks_hash = CryptoHash::new(&PreviousEventBlocksMap {
            inner: Cow::Borrowed(&outcome.previous_event_blocks),
        });
        let oracle_responses_hash = hashing::hash_vec_vec(&outcome.oracle_responses);
        let events_hash = hashing::hash_vec_vec(&outcome.events);
        let blobs_hash = hashing::hash_vec_vec(&outcome.blobs);
        let operation_results_hash = hashing::hash_vec(&outcome.operation_results);

        let header = BlockHeader {
            chain_id: block.chain_id,
            epoch: block.epoch,
            height: block.height,
            timestamp: block.timestamp,
            state_hash: outcome.state_hash,
            previous_block_hash: block.previous_block_hash,
            authenticated_signer: block.authenticated_signer,
            transactions_hash,
            messages_hash,
            previous_message_blocks_hash,
            previous_event_blocks_hash,
            oracle_responses_hash,
            events_hash,
            blobs_hash,
            operation_results_hash,
        };

        let body = BlockBody {
            transactions: block.transactions,
            messages: outcome.messages,
            previous_message_blocks: outcome.previous_message_blocks,
            previous_event_blocks: outcome.previous_event_blocks,
            oracle_responses: outcome.oracle_responses,
            events: outcome.events,
            blobs: outcome.blobs,
            operation_results: outcome.operation_results,
        };

        Self { header, body }
    }

    /// Returns the bundles of messages sent via the given medium to the specified
    /// recipient. Messages originating from different transactions of the original block
    /// are kept in separate bundles. If the medium is a channel, does not verify that the
    /// recipient is actually subscribed to that channel.
    pub fn message_bundles_for(
        &self,
        recipient: ChainId,
        certificate_hash: CryptoHash,
    ) -> impl Iterator<Item = (Epoch, MessageBundle)> + '_ {
        let mut index = 0u32;
        let block_height = self.header.height;
        let block_timestamp = self.header.timestamp;
        let block_epoch = self.header.epoch;

        (0u32..)
            .zip(self.messages())
            .filter_map(move |(transaction_index, txn_messages)| {
                let messages = (index..)
                    .zip(txn_messages)
                    .filter(|(_, message)| message.destination == recipient)
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

    /// Returns all the blob IDs required by this block.
    /// Either as oracle responses or as published blobs.
    pub fn required_blob_ids(&self) -> BTreeSet<BlobId> {
        let mut blob_ids = self.oracle_blob_ids();
        blob_ids.extend(self.published_blob_ids());
        blob_ids.extend(self.created_blob_ids());
        if self.header.height == BlockHeight(0) {
            // the initial block implicitly depends on the chain description blob
            blob_ids.insert(BlobId::new(
                self.header.chain_id.0,
                BlobType::ChainDescription,
            ));
        }
        blob_ids
    }

    /// Returns whether this block requires the blob with the specified ID.
    pub fn requires_or_creates_blob(&self, blob_id: &BlobId) -> bool {
        self.oracle_blob_ids().contains(blob_id)
            || self.published_blob_ids().contains(blob_id)
            || self.created_blob_ids().contains(blob_id)
            || (self.header.height == BlockHeight(0)
                && (blob_id.blob_type == BlobType::ChainDescription
                    && blob_id.hash == self.header.chain_id.0))
    }

    /// Returns all the published blob IDs in this block's operations.
    pub fn published_blob_ids(&self) -> BTreeSet<BlobId> {
        self.body
            .operations()
            .flat_map(Operation::published_blob_ids)
            .collect()
    }

    /// Returns all the blob IDs created by the block's operations.
    pub fn created_blob_ids(&self) -> BTreeSet<BlobId> {
        self.body
            .blobs
            .iter()
            .flatten()
            .map(|blob| blob.id())
            .collect()
    }

    /// Returns all the blobs created by the block's operations.
    pub fn created_blobs(&self) -> BTreeMap<BlobId, Blob> {
        self.body
            .blobs
            .iter()
            .flatten()
            .map(|blob| (blob.id(), blob.clone()))
            .collect()
    }

    /// Returns set of blob IDs that were a result of an oracle call.
    pub fn oracle_blob_ids(&self) -> BTreeSet<BlobId> {
        let mut required_blob_ids = BTreeSet::new();
        for responses in &self.body.oracle_responses {
            for response in responses {
                if let OracleResponse::Blob(blob_id) = response {
                    required_blob_ids.insert(*blob_id);
                }
            }
        }

        required_blob_ids
    }

    /// Returns reference to the outgoing messages in the block.
    pub fn messages(&self) -> &Vec<Vec<OutgoingMessage>> {
        &self.body.messages
    }

    /// Returns all recipients of messages in this block.
    pub fn recipients(&self) -> BTreeSet<ChainId> {
        self.body
            .messages
            .iter()
            .flat_map(|messages| messages.iter().map(|message| message.destination))
            .collect()
    }

    /// Returns whether there are any oracle responses in this block.
    pub fn has_oracle_responses(&self) -> bool {
        self.body
            .oracle_responses
            .iter()
            .any(|responses| !responses.is_empty())
    }

    /// Returns whether this block matches the proposal.
    pub fn matches_proposed_block(&self, block: &ProposedBlock) -> bool {
        let ProposedBlock {
            chain_id,
            epoch,
            transactions,
            height,
            timestamp,
            authenticated_signer,
            previous_block_hash,
        } = block;
        *chain_id == self.header.chain_id
            && *epoch == self.header.epoch
            && *transactions == self.body.transactions
            && *height == self.header.height
            && *timestamp == self.header.timestamp
            && *authenticated_signer == self.header.authenticated_signer
            && *previous_block_hash == self.header.previous_block_hash
    }

    pub fn into_proposal(self) -> (ProposedBlock, BlockExecutionOutcome) {
        let proposed_block = ProposedBlock {
            chain_id: self.header.chain_id,
            epoch: self.header.epoch,
            transactions: self.body.transactions,
            height: self.header.height,
            timestamp: self.header.timestamp,
            authenticated_signer: self.header.authenticated_signer,
            previous_block_hash: self.header.previous_block_hash,
        };
        let outcome = BlockExecutionOutcome {
            state_hash: self.header.state_hash,
            messages: self.body.messages,
            previous_message_blocks: self.body.previous_message_blocks,
            previous_event_blocks: self.body.previous_event_blocks,
            oracle_responses: self.body.oracle_responses,
            events: self.body.events,
            blobs: self.body.blobs,
            operation_results: self.body.operation_results,
        };
        (proposed_block, outcome)
    }

    pub fn iter_created_blobs(&self) -> impl Iterator<Item = (BlobId, Blob)> + '_ {
        self.body
            .blobs
            .iter()
            .flatten()
            .map(|blob| (blob.id(), blob.clone()))
    }
}

impl BcsHashable<'_> for Block {}

#[derive(Serialize, Deserialize)]
pub struct PreviousMessageBlocksMap<'a> {
    inner: Cow<'a, BTreeMap<ChainId, (CryptoHash, BlockHeight)>>,
}

impl<'de> BcsHashable<'de> for PreviousMessageBlocksMap<'de> {}

#[derive(Serialize, Deserialize)]
pub struct PreviousEventBlocksMap<'a> {
    inner: Cow<'a, BTreeMap<StreamId, (CryptoHash, BlockHeight)>>,
}

impl<'de> BcsHashable<'de> for PreviousEventBlocksMap<'de> {}

#[derive(Serialize, Deserialize)]
#[serde(rename = "BlockHeader")]
struct SerializedHeader {
    chain_id: ChainId,
    epoch: Epoch,
    height: BlockHeight,
    timestamp: Timestamp,
    state_hash: CryptoHash,
    previous_block_hash: Option<CryptoHash>,
    authenticated_signer: Option<AccountOwner>,
}

mod hashing {
    use linera_base::crypto::{BcsHashable, CryptoHash, CryptoHashVec};

    pub(super) fn hash_vec<'de, T: BcsHashable<'de>>(it: impl AsRef<[T]>) -> CryptoHash {
        let v = CryptoHashVec(it.as_ref().iter().map(CryptoHash::new).collect::<Vec<_>>());
        CryptoHash::new(&v)
    }

    pub(super) fn hash_vec_vec<'de, T: BcsHashable<'de>>(it: impl AsRef<[Vec<T>]>) -> CryptoHash {
        let v = CryptoHashVec(it.as_ref().iter().map(hash_vec).collect::<Vec<_>>());
        CryptoHash::new(&v)
    }
}
