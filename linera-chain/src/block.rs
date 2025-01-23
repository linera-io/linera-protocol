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
    data_types::{BlockHeight, OracleResponse, Timestamp},
    hashed::Hashed,
    identifiers::{BlobId, BlobType, ChainId, MessageId, Owner},
};
use linera_execution::{committee::Epoch, Operation, SystemOperation};
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use thiserror::Error;

use crate::{
    data_types::{
        BlockExecutionOutcome, EventRecord, ExecutedBlock, IncomingBundle, Medium, MessageBundle,
        OutgoingMessage, ProposedBlock,
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
        Self(Hashed::new(Block::new(block.block, block.outcome)))
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
        Self(Hashed::new(Block::new(block.block, block.outcome)))
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

    /// Returns a reference to the `ExecutedBlock` contained in this `ConfirmedBlock`.
    pub fn block(&self) -> &Block {
        self.0.inner()
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_block(self) -> Block {
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

        let bundles_hash = hashing::hash_vec(&inner.body.incoming_bundles);
        let messages_hash = hashing::hash_vec_vec(&inner.body.messages);
        let operations_hash = hashing::hash_vec(&inner.body.operations);
        let oracle_responses_hash = hashing::hash_vec_vec(&inner.body.oracle_responses);
        let events_hash = hashing::hash_vec_vec(&inner.body.events);

        let header = BlockHeader {
            chain_id: inner.header.chain_id,
            epoch: inner.header.epoch,
            height: inner.header.height,
            timestamp: inner.header.timestamp,
            state_hash: inner.header.state_hash,
            previous_block_hash: inner.header.previous_block_hash,
            authenticated_signer: inner.header.authenticated_signer,
            bundles_hash,
            operations_hash,
            messages_hash,
            oracle_responses_hash,
            events_hash,
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
    pub authenticated_signer: Option<Owner>,

    // Inputs to the block, chosen by the block proposer.
    /// Cryptographic hash of all the incoming bundles in the block.
    pub bundles_hash: CryptoHash,
    /// Cryptographic hash of all the operations in the block.
    pub operations_hash: CryptoHash,

    // Outcome of the block execution.
    /// Cryptographic hash of all the messages in the block.
    pub messages_hash: CryptoHash,
    /// Cryptographic hash of all the oracle responses in the block.
    pub oracle_responses_hash: CryptoHash,
    /// Cryptographic hash of all the events in the block.
    pub events_hash: CryptoHash,
}

/// The body of a block containing all the data included in the block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockBody {
    /// A selection of incoming messages to be executed first. Successive messages of same
    /// sender and height are grouped together for conciseness.
    pub incoming_bundles: Vec<IncomingBundle>,
    /// The operations to execute.
    pub operations: Vec<Operation>,
    /// The list of outgoing messages for each transaction.
    pub messages: Vec<Vec<OutgoingMessage>>,
    /// The record of oracle responses for each transaction.
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    /// The list of events produced by each transaction.
    pub events: Vec<Vec<EventRecord>>,
}

impl Block {
    pub fn new(block: ProposedBlock, outcome: BlockExecutionOutcome) -> Self {
        let bundles_hash = hashing::hash_vec(&block.incoming_bundles);
        let messages_hash = hashing::hash_vec_vec(&outcome.messages);
        let operations_hash = hashing::hash_vec(&block.operations);
        let oracle_responses_hash = hashing::hash_vec_vec(&outcome.oracle_responses);
        let events_hash = hashing::hash_vec_vec(&outcome.events);

        let header = BlockHeader {
            chain_id: block.chain_id,
            epoch: block.epoch,
            height: block.height,
            timestamp: block.timestamp,
            state_hash: outcome.state_hash,
            previous_block_hash: block.previous_block_hash,
            authenticated_signer: block.authenticated_signer,
            bundles_hash,
            operations_hash,
            messages_hash,
            oracle_responses_hash,
            events_hash,
        };

        let body = BlockBody {
            incoming_bundles: block.incoming_bundles,
            operations: block.operations,
            messages: outcome.messages,
            oracle_responses: outcome.oracle_responses,
            events: outcome.events,
        };

        Self { header, body }
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
        Some(self.message_id(index))
    }

    /// Returns the message ID belonging to the `index`th outgoing message in this block.
    pub fn message_id(&self, index: u32) -> MessageId {
        MessageId {
            chain_id: self.header.chain_id,
            height: self.header.height,
            index,
        }
    }

    /// Returns the outgoing message with the specified id, or `None` if there is no such message.
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

    /// Returns all the blob IDs required by this block.
    /// Either as oracle responses or as published blobs.
    pub fn required_blob_ids(&self) -> HashSet<BlobId> {
        let mut blob_ids = self.oracle_blob_ids();
        blob_ids.extend(self.published_blob_ids());
        blob_ids
    }

    /// Returns whether this block requires the blob with the specified ID.
    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.oracle_blob_ids().contains(blob_id) || self.published_blob_ids().contains(blob_id)
    }

    /// Returns all the published blob IDs in this block's operations.
    fn published_blob_ids(&self) -> BTreeSet<BlobId> {
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

    /// Returns set of blob ids that were a result of an oracle call.
    pub fn oracle_blob_ids(&self) -> HashSet<BlobId> {
        let mut required_blob_ids = HashSet::new();
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
}

impl From<Block> for ExecutedBlock {
    fn from(block: Block) -> Self {
        let Block {
            header:
                BlockHeader {
                    chain_id,
                    epoch,
                    height,
                    timestamp,
                    state_hash,
                    previous_block_hash,
                    authenticated_signer,
                    bundles_hash: _,
                    operations_hash: _,
                    messages_hash: _,
                    oracle_responses_hash: _,
                    events_hash: _,
                },
            body:
                BlockBody {
                    incoming_bundles,
                    operations,
                    messages,
                    oracle_responses,
                    events,
                },
        } = block;

        let block = ProposedBlock {
            chain_id,
            epoch,
            height,
            timestamp,
            incoming_bundles,
            operations,
            authenticated_signer,
            previous_block_hash,
        };

        let outcome = BlockExecutionOutcome {
            state_hash,
            messages,
            oracle_responses,
            events,
        };

        ExecutedBlock { block, outcome }
    }
}

impl<'de> BcsHashable<'de> for Block {}

#[derive(Serialize, Deserialize)]
#[serde(rename = "BlockHeader")]
struct SerializedHeader {
    chain_id: ChainId,
    epoch: Epoch,
    height: BlockHeight,
    timestamp: Timestamp,
    state_hash: CryptoHash,
    previous_block_hash: Option<CryptoHash>,
    authenticated_signer: Option<Owner>,
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
