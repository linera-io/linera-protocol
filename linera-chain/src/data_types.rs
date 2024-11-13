// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::HashSet, fmt};

use async_graphql::SimpleObject;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::{BcsHashable, BcsSignable, CryptoError, CryptoHash, KeyPair, PublicKey, Signature},
    data_types::{Amount, Blob, BlockHeight, OracleResponse, Round, Timestamp},
    doc_scalar, ensure, hex_debug,
    identifiers::{
        Account, BlobId, BlobType, ChainId, ChannelName, Destination, GenericApplicationId,
        MessageId, Owner, StreamId,
    },
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    system::OpenChainConfig,
    Message, MessageKind, Operation, SystemMessage, SystemOperation,
};
use serde::{de::Deserializer, Deserialize, Serialize};

use crate::{
    block::{ConfirmedBlock, Timeout, ValidatedBlock},
    types::{GenericCertificate, Hashed, ValidatedBlockCertificate},
    ChainError,
};

#[cfg(test)]
#[path = "unit_tests/data_types_tests.rs"]
mod data_types_tests;

/// A block containing operations to apply on a given chain, as well as the
/// acknowledgment of a number of incoming messages from other chains.
/// * Incoming messages must be selected in the order they were
///   produced by the sending chain, but can be skipped.
/// * When a block is proposed to a validator, all cross-chain messages must have been
///   received ahead of time in the inbox of the chain.
/// * This constraint does not apply to the execution of confirmed blocks.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct Block {
    /// The chain to which this block belongs.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Epoch,
    /// A selection of incoming messages to be executed first. Successive messages of same
    /// sender and height are grouped together for conciseness.
    #[debug(skip_if = Vec::is_empty)]
    pub incoming_bundles: Vec<IncomingBundle>,
    /// The operations to execute.
    #[debug(skip_if = Vec::is_empty)]
    pub operations: Vec<Operation>,
    /// The block height.
    pub height: BlockHeight,
    /// The timestamp when this block was created. This must be later than all messages received
    /// in this block, but no later than the current time.
    pub timestamp: Timestamp,
    /// The user signing for the operations in the block and paying for their execution
    /// fees. If set, this must be the `owner` in the block proposal. `None` means that
    /// the default account of the chain is used. This value is also used as recipient of
    /// potential refunds for the message grants created by the operations.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// Certified hash (see `Certificate` below) of the previous block in the
    /// chain, if any.
    pub previous_block_hash: Option<CryptoHash>,
}

impl Block {
    /// Returns all the published blob IDs in this block's operations.
    pub fn published_blob_ids(&self) -> HashSet<BlobId> {
        let mut blob_ids = HashSet::new();
        for operation in &self.operations {
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
        self.operations.is_empty()
            && self
                .incoming_bundles
                .iter()
                .all(|message| message.action == MessageAction::Reject)
    }

    /// Returns an iterator over all incoming [`PostedMessage`]s in this block.
    pub fn incoming_messages(&self) -> impl Iterator<Item = &PostedMessage> {
        self.incoming_bundles
            .iter()
            .flat_map(|incoming_bundle| &incoming_bundle.bundle.messages)
    }

    /// Returns the number of incoming messages.
    pub fn message_count(&self) -> usize {
        self.incoming_bundles
            .iter()
            .map(|im| im.bundle.messages.len())
            .sum()
    }

    /// Returns an iterator over all transactions, by index.
    pub fn transactions(&self) -> impl Iterator<Item = (u32, Transaction<'_>)> {
        let bundles = self
            .incoming_bundles
            .iter()
            .map(Transaction::ReceiveMessages);
        let operations = self.operations.iter().map(Transaction::ExecuteOperation);
        (0u32..).zip(bundles.chain(operations))
    }

    /// If the block's first message is `OpenChain`, returns the bundle, the message and
    /// the configuration for the new chain.
    pub fn starts_with_open_chain_message(
        &self,
    ) -> Option<(&IncomingBundle, &PostedMessage, &OpenChainConfig)> {
        let in_bundle = self.incoming_bundles.first()?;
        if in_bundle.action != MessageAction::Accept {
            return None;
        }
        let posted_message = in_bundle.bundle.messages.first()?;
        let config = posted_message.message.matches_open_chain()?;
        Some((in_bundle, posted_message, config))
    }
}

/// A transaction in a block: incoming messages or an operation.
#[derive(Debug, Clone)]
pub enum Transaction<'a> {
    /// Receive a bundle of incoming messages.
    ReceiveMessages(&'a IncomingBundle),
    /// Execute an operation.
    ExecuteOperation(&'a Operation),
}

/// A chain ID with a block height.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, SimpleObject)]
pub struct ChainAndHeight {
    pub chain_id: ChainId,
    pub height: BlockHeight,
}

impl ChainAndHeight {
    /// Returns the ID of the `index`-th message sent by the block at that height.
    pub fn to_message_id(&self, index: u32) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index,
        }
    }
}

/// A bundle of cross-chain messages.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct IncomingBundle {
    /// The origin of the messages (chain and channel if any).
    pub origin: Origin,
    /// The messages to be delivered to the inbox identified by `origin`.
    pub bundle: MessageBundle,
    /// What to do with the message.
    pub action: MessageAction,
}

impl IncomingBundle {
    /// Returns an iterator over all posted messages in this bundle, together with their ID.
    pub fn messages_and_ids(&self) -> impl Iterator<Item = (MessageId, &PostedMessage)> {
        let chain_and_height = ChainAndHeight {
            chain_id: self.origin.sender,
            height: self.bundle.height,
        };
        let messages = self.bundle.messages.iter();
        messages.map(move |posted_message| {
            let message_id = chain_and_height.to_message_id(posted_message.index);
            (message_id, posted_message)
        })
    }

    /// Rearranges the messages in the bundle so that the first message is an `OpenChain` message.
    /// Returns whether the `OpenChain` message was found at all.
    pub fn put_openchain_at_front(bundles: &mut [IncomingBundle]) -> bool {
        let Some(index) = bundles.iter().position(|msg| {
            matches!(
                msg.bundle.messages.first(),
                Some(PostedMessage {
                    message: Message::System(SystemMessage::OpenChain(_)),
                    ..
                })
            )
        }) else {
            return false;
        };

        bundles[0..=index].rotate_right(1);
        true
    }
}

/// What to do with a message picked from the inbox.
#[derive(Copy, Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum MessageAction {
    /// Execute the incoming message.
    Accept,
    /// Do not execute the incoming message.
    Reject,
}

/// The origin of a message, relative to a particular application. Used to identify each inbox.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub struct Origin {
    /// The chain ID of the sender.
    pub sender: ChainId,
    /// The medium.
    pub medium: Medium,
}

/// The target of a message, relative to a particular application. Used to identify each outbox.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub struct Target {
    /// The chain ID of the recipient.
    pub recipient: ChainId,
    /// The medium.
    pub medium: Medium,
}

/// A set of messages from a single block, for a single destination.
#[derive(Debug, Eq, PartialEq, Clone, Hash, Serialize, Deserialize, SimpleObject)]
pub struct MessageBundle {
    /// The block height.
    pub height: BlockHeight,
    /// The block's timestamp.
    pub timestamp: Timestamp,
    /// The confirmed block certificate hash.
    pub certificate_hash: CryptoHash,
    /// The index of the transaction in the block that is sending this bundle.
    pub transaction_index: u32,
    /// The relevant messages.
    pub messages: Vec<PostedMessage>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
/// A channel name together with its application ID.
pub struct ChannelFullName {
    /// The application owning the channel.
    pub application_id: GenericApplicationId,
    /// The name of the channel.
    pub name: ChannelName,
}

impl fmt::Display for ChannelFullName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = hex::encode(&self.name);
        match self.application_id {
            GenericApplicationId::System => write!(f, "system channel {name}"),
            GenericApplicationId::User(app_id) => write!(f, "user channel {name} for app {app_id}"),
        }
    }
}

/// The origin of a message coming from a particular chain. Used to identify each inbox.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Medium {
    /// The message is a direct message.
    Direct,
    /// The message is a channel broadcast.
    Channel(ChannelFullName),
}

/// An authenticated proposal for a new block.
// TODO(#456): the signature of the block owner is currently lost but it would be useful
// to have it for auditing purposes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct BlockProposal {
    pub content: ProposalContent,
    pub owner: Owner,
    pub signature: Signature,
    #[debug(skip_if = Vec::is_empty)]
    pub blobs: Vec<Blob>,
    #[debug(skip_if = Option::is_none)]
    pub validated_block_certificate: Option<LiteCertificate<'static>>,
}

/// A posted message together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OutgoingMessage {
    /// The destination of the message.
    pub destination: Destination,
    /// The user authentication carried by the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// A grant to pay for the message execution.
    #[debug(skip_if = Amount::is_zero)]
    pub grant: Amount,
    /// Where to send a refund for the unused part of the grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    pub refund_grant_to: Option<Account>,
    /// The kind of message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
}

/// A message together with kind, authentication and grant information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct PostedMessage {
    /// The user authentication carried by the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<Owner>,
    /// A grant to pay for the message execution.
    #[debug(skip_if = Amount::is_zero)]
    pub grant: Amount,
    /// Where to send a refund for the unused part of the grant after execution, if any.
    #[debug(skip_if = Option::is_none)]
    pub refund_grant_to: Option<Account>,
    /// The kind of message being sent.
    pub kind: MessageKind,
    /// The index of the message in the sending block.
    pub index: u32,
    /// The message itself.
    pub message: Message,
}

impl OutgoingMessage {
    /// Returns whether this message is sent via the given medium to the specified
    /// recipient. If the medium is a channel, does not verify that the recipient is
    /// actually subscribed to that channel.
    pub fn has_destination(&self, medium: &Medium, recipient: ChainId) -> bool {
        match (&self.destination, medium) {
            (Destination::Recipient(_), Medium::Channel(_))
            | (Destination::Subscribers(_), Medium::Direct) => false,
            (Destination::Recipient(id), Medium::Direct) => *id == recipient,
            (
                Destination::Subscribers(dest_name),
                Medium::Channel(ChannelFullName {
                    application_id,
                    name,
                }),
            ) => *application_id == self.message.application_id() && name == dest_name,
        }
    }

    /// Returns the posted message, i.e. the outgoing message without the destination.
    pub fn into_posted(self, index: u32) -> PostedMessage {
        let OutgoingMessage {
            destination: _,
            authenticated_signer,
            grant,
            refund_grant_to,
            kind,
            message,
        } = self;
        PostedMessage {
            authenticated_signer,
            grant,
            refund_grant_to,
            kind,
            index,
            message,
        }
    }
}

/// A [`Block`], together with the outcome from its execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct ExecutedBlock {
    pub block: Block,
    pub outcome: BlockExecutionOutcome,
}

/// The messages and the state hash resulting from a [`Block`]'s execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
#[cfg_attr(with_testing, derive(Default))]
pub struct BlockExecutionOutcome {
    /// The list of outgoing messages for each transaction.
    pub messages: Vec<Vec<OutgoingMessage>>,
    /// The hash of the chain's execution state after this block.
    pub state_hash: CryptoHash,
    /// The record of oracle responses for each transaction.
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    /// The list of events produced by each transaction.
    pub events: Vec<Vec<EventRecord>>,
}

/// An event recorded in an executed block.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct EventRecord {
    /// The ID of the stream this event belongs to.
    pub stream_id: StreamId,
    /// The event key.
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub key: Vec<u8>,
    /// The payload data.
    #[serde(with = "serde_bytes")]
    #[debug(with = "hex_debug")]
    pub value: Vec<u8>,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum CertificateValue {
    ValidatedBlock(ValidatedBlock),
    ConfirmedBlock(ConfirmedBlock),
    Timeout(Timeout),
}

#[async_graphql::Object(cache_control(no_cache))]
impl CertificateValue {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> Option<ExecutedBlock> {
        self.executed_block().cloned()
    }

    async fn status(&self) -> String {
        match self {
            CertificateValue::ValidatedBlock { .. } => "validated".to_string(),
            CertificateValue::ConfirmedBlock { .. } => "confirmed".to_string(),
            CertificateValue::Timeout { .. } => "timeout".to_string(),
        }
    }
}

/// A statement to be certified by the validators, with its hash.
pub type HashedCertificateValue = Hashed<CertificateValue>;

#[async_graphql::Object(cache_control(no_cache))]
impl HashedCertificateValue {
    #[graphql(derived(name = "hash"))]
    async fn _hash(&self) -> CryptoHash {
        self.hash()
    }

    #[graphql(derived(name = "value"))]
    async fn _value(&self) -> CertificateValue {
        self.inner().clone()
    }
}

#[cfg(with_testing)]
impl<T> PartialEq for Hashed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
    }
}

#[cfg(with_testing)]
impl<T> Eq for Hashed<T> {}

/// The hash and chain ID of a `CertificateValue`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct LiteValue {
    pub value_hash: CryptoHash,
    pub chain_id: ChainId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
struct ValueHashAndRound(CryptoHash, Round);

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vote {
    pub value: HashedCertificateValue,
    pub round: Round,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: HashedCertificateValue, round: Round, key_pair: &KeyPair) -> Self {
        let hash_and_round = ValueHashAndRound(value.hash(), round);
        let signature = Signature::new(&hash_and_round, key_pair);
        Self {
            value,
            round,
            validator: ValidatorName(key_pair.public()),
            signature,
        }
    }

    /// Returns the vote, with a `LiteValue` instead of the full value.
    pub fn lite(&self) -> LiteVote {
        LiteVote {
            value: self.value.lite(),
            round: self.round,
            validator: self.validator,
            signature: self.signature,
        }
    }

    /// Returns the value this vote is for.
    pub fn value(&self) -> &CertificateValue {
        self.value.inner()
    }
}

/// A vote on a statement from a validator, represented as a `LiteValue`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LiteVote {
    pub value: LiteValue,
    pub round: Round,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl LiteVote {
    /// Returns the full vote, with the value, if it matches.
    pub fn with_value(self, value: HashedCertificateValue) -> Option<Vote> {
        if self.value != value.lite() {
            return None;
        }
        Some(Vote {
            value,
            round: self.round,
            validator: self.validator,
            signature: self.signature,
        })
    }
}

/// A certified statement from the committee, without the value.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LiteCertificate<'a> {
    /// Hash and chain ID of the certified value (used as key for storage).
    pub value: LiteValue,
    /// The round in which the value was certified.
    pub round: Round,
    /// Signatures on the value.
    pub signatures: Cow<'a, [(ValidatorName, Signature)]>,
}

impl<'a> LiteCertificate<'a> {
    pub fn new(
        value: LiteValue,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        let signatures = Cow::Owned(signatures);
        Self {
            value,
            round,
            signatures,
        }
    }

    /// Creates a `LiteCertificate` from a list of votes, without cryptographically checking the
    /// signatures. Returns `None` if the votes are empty or don't have matching values and rounds.
    pub fn try_from_votes(votes: impl IntoIterator<Item = LiteVote>) -> Option<Self> {
        let mut votes = votes.into_iter();
        let LiteVote {
            value,
            round,
            validator,
            signature,
        } = votes.next()?;
        let mut signatures = vec![(validator, signature)];
        for vote in votes {
            if vote.value.value_hash != value.value_hash || vote.round != round {
                return None;
            }
            signatures.push((vote.validator, vote.signature));
        }
        Some(LiteCertificate::new(value, round, signatures))
    }

    /// Verifies the certificate.
    pub fn check(&self, committee: &Committee) -> Result<&LiteValue, ChainError> {
        check_signatures(
            self.value.value_hash,
            self.round,
            &self.signatures,
            committee,
        )?;
        Ok(&self.value)
    }

    /// Returns the `Certificate` with the specified value, if it matches.
    pub fn with_value(self, value: HashedCertificateValue) -> Option<Certificate> {
        if self.value.chain_id != value.inner().chain_id() || self.value.value_hash != value.hash()
        {
            return None;
        }
        Some(Certificate::new(
            value,
            self.round,
            self.signatures.into_owned(),
        ))
    }

    /// Returns a `LiteCertificate` that owns the list of signatures.
    pub fn cloned(&self) -> LiteCertificate<'static> {
        LiteCertificate {
            value: self.value.clone(),
            round: self.round,
            signatures: Cow::Owned(self.signatures.clone().into_owned()),
        }
    }
}

/// A certified statement from the committee.
pub type Certificate = GenericCertificate<CertificateValue>;

impl Serialize for Certificate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Debug, Serialize)]
        #[serde(rename = "Certificate")]
        struct CertificateHelper<'a> {
            value: &'a CertificateValue,
            round: Round,
            signatures: &'a Vec<(ValidatorName, Signature)>,
        }

        let helper = CertificateHelper {
            value: self.inner(),
            round: self.round,
            signatures: self.signatures(),
        };

        helper.serialize(serializer)
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.medium {
            Medium::Direct => write!(f, "{:.8} (direct)", self.sender),
            Medium::Channel(full_name) => write!(f, "{:.8} via {full_name:.8}", self.sender),
        }
    }
}

impl Origin {
    pub fn chain(sender: ChainId) -> Self {
        Self {
            sender,
            medium: Medium::Direct,
        }
    }

    pub fn channel(sender: ChainId, name: ChannelFullName) -> Self {
        Self {
            sender,
            medium: Medium::Channel(name),
        }
    }
}

impl Target {
    pub fn chain(recipient: ChainId) -> Self {
        Self {
            recipient,
            medium: Medium::Direct,
        }
    }

    pub fn channel(recipient: ChainId, name: ChannelFullName) -> Self {
        Self {
            recipient,
            medium: Medium::Channel(name),
        }
    }
}

impl Serialize for HashedCertificateValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for HashedCertificateValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        Ok(CertificateValue::deserialize(deserializer)?.into())
    }
}

impl From<CertificateValue> for HashedCertificateValue {
    fn from(value: CertificateValue) -> HashedCertificateValue {
        value.with_hash()
    }
}

impl From<HashedCertificateValue> for CertificateValue {
    fn from(hv: HashedCertificateValue) -> CertificateValue {
        hv.into_inner()
    }
}

impl CertificateValue {
    pub fn chain_id(&self) -> ChainId {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.chain_id,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.chain_id,
            CertificateValue::Timeout(Timeout { chain_id, .. }) => *chain_id,
        }
    }

    pub fn height(&self) -> BlockHeight {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.height,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.height,
            CertificateValue::Timeout(Timeout { height, .. }) => *height,
        }
    }

    pub fn epoch(&self) -> Epoch {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.epoch,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.epoch,
            CertificateValue::Timeout(Timeout { epoch, .. }) => *epoch,
        }
    }

    /// Creates a `HashedCertificateValue` checking that this is the correct hash.
    pub fn with_hash_checked(self, hash: CryptoHash) -> Result<HashedCertificateValue, ChainError> {
        let hashed_certificate_value = self.with_hash();
        ensure!(
            hashed_certificate_value.hash() == hash,
            ChainError::CertificateValueHashMismatch {
                expected: hash,
                actual: hashed_certificate_value.hash()
            }
        );
        Ok(hashed_certificate_value)
    }

    /// Creates a `HashedCertificateValue` by hashing `self`. No hash checks are made!
    pub fn with_hash(self) -> HashedCertificateValue {
        let hash = CryptoHash::new(&self);
        HashedCertificateValue::unchecked_new(self, hash)
    }

    /// Creates a `HashedCertificateValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> HashedCertificateValue {
        HashedCertificateValue::unchecked_new(self, hash)
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self, CertificateValue::ConfirmedBlock { .. })
    }

    pub fn is_validated(&self) -> bool {
        matches!(self, CertificateValue::ValidatedBlock { .. })
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, CertificateValue::Timeout { .. })
    }

    #[cfg(with_testing)]
    pub fn messages(&self) -> Option<&Vec<Vec<OutgoingMessage>>> {
        Some(self.executed_block()?.messages())
    }

    pub fn executed_block(&self) -> Option<&ExecutedBlock> {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => Some(confirmed.inner()),
            CertificateValue::ValidatedBlock(validated) => Some(validated.inner()),
            CertificateValue::Timeout(_) => None,
        }
    }

    pub fn block(&self) -> Option<&Block> {
        self.executed_block()
            .map(|executed_block| &executed_block.block)
    }

    pub fn to_log_str(&self) -> &'static str {
        match self {
            CertificateValue::ConfirmedBlock { .. } => "confirmed_block",
            CertificateValue::ValidatedBlock { .. } => "validated_block",
            CertificateValue::Timeout { .. } => "timeout",
        }
    }
}

impl MessageBundle {
    pub fn is_skippable(&self) -> bool {
        self.messages.iter().all(PostedMessage::is_skippable)
    }

    pub fn is_tracked(&self) -> bool {
        let mut tracked = false;
        for posted_message in &self.messages {
            match posted_message.kind {
                MessageKind::Simple | MessageKind::Bouncing => {}
                MessageKind::Protected => return false,
                MessageKind::Tracked => tracked = true,
            }
        }
        tracked
    }

    pub fn is_protected(&self) -> bool {
        self.messages.iter().any(PostedMessage::is_protected)
    }

    /// Returns whether this bundle must be added to the inbox.
    ///
    /// If this is `false`, it gets handled immediately and should never be received in a block.
    pub fn goes_to_inbox(&self) -> bool {
        self.messages
            .iter()
            .any(|posted_message| posted_message.message.goes_to_inbox())
    }
}

impl PostedMessage {
    pub fn is_skippable(&self) -> bool {
        match self.kind {
            MessageKind::Protected | MessageKind::Tracked => false,
            MessageKind::Simple | MessageKind::Bouncing => self.grant == Amount::ZERO,
        }
    }

    pub fn is_protected(&self) -> bool {
        matches!(self.kind, MessageKind::Protected)
    }

    pub fn is_tracked(&self) -> bool {
        matches!(self.kind, MessageKind::Tracked)
    }

    pub fn is_bouncing(&self) -> bool {
        matches!(self.kind, MessageKind::Bouncing)
    }
}

impl ExecutedBlock {
    pub fn messages(&self) -> &Vec<Vec<OutgoingMessage>> {
        &self.outcome.messages
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
        let block_height = self.block.height;
        let block_timestamp = self.block.timestamp;
        let block_epoch = self.block.epoch;

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
        let block = &self.block;
        let transaction_index = block.incoming_bundles.len().checked_add(operation_index)?;
        if message_index
            >= u32::try_from(self.outcome.messages.get(transaction_index)?.len()).ok()?
        {
            return None;
        }
        let first_message_index = u32::try_from(
            self.outcome
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

    pub fn message_by_id(&self, message_id: &MessageId) -> Option<&OutgoingMessage> {
        let MessageId {
            chain_id,
            height,
            index,
        } = message_id;
        if self.block.chain_id != *chain_id || self.block.height != *height {
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

    /// Returns the message ID belonging to the `index`th outgoing message in this block.
    pub fn message_id(&self, index: u32) -> MessageId {
        MessageId {
            chain_id: self.block.chain_id,
            height: self.block.height,
            index,
        }
    }

    pub fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.outcome.required_blob_ids()
    }

    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.required_blob_ids().contains(blob_id)
    }
}

impl BlockExecutionOutcome {
    pub fn with(self, block: Block) -> ExecutedBlock {
        ExecutedBlock {
            block,
            outcome: self,
        }
    }

    pub fn required_blob_ids(&self) -> HashSet<BlobId> {
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
}

impl HashedCertificateValue {
    /// Creates a [`ConfirmedBlock`](CertificateValue::ConfirmedBlock) value.
    pub fn new_confirmed(executed_block: ExecutedBlock) -> HashedCertificateValue {
        CertificateValue::ConfirmedBlock(ConfirmedBlock::new(executed_block)).into()
    }

    /// Creates a [`ValidatedBlock`](CertificateValue::ValidatedBlock) value.
    pub fn new_validated(executed_block: ExecutedBlock) -> HashedCertificateValue {
        CertificateValue::ValidatedBlock(ValidatedBlock::new(executed_block)).into()
    }

    /// Creates a [`Timeout`](CertificateValue::Timeout) value.
    pub fn new_timeout(
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
    ) -> HashedCertificateValue {
        CertificateValue::Timeout(Timeout::new(chain_id, height, epoch)).into()
    }

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.inner().chain_id(),
        }
    }

    /// Returns the corresponding `ConfirmedBlock`, if this is a `ValidatedBlock`.
    pub fn validated_to_confirmed(&self) -> Option<HashedCertificateValue> {
        match self.inner() {
            CertificateValue::ValidatedBlock(validated) => {
                Some(ConfirmedBlock::from_validated(validated.clone()).into())
            }
            CertificateValue::ConfirmedBlock(_) | CertificateValue::Timeout(_) => None,
        }
    }
}

/// The data a block proposer signs.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProposalContent {
    /// The proposed block.
    pub block: Block,
    /// The consensus round in which this proposal is made.
    pub round: Round,
    /// If this is a retry from an earlier round, the oracle responses from when the block was
    /// first validated. These are reused so the execution outcome remains the same.
    #[debug(skip_if = Option::is_none)]
    pub forced_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
}

impl BlockProposal {
    pub fn new_initial(round: Round, block: Block, secret: &KeyPair, blobs: Vec<Blob>) -> Self {
        let content = ProposalContent {
            round,
            block,
            forced_oracle_responses: None,
        };
        let signature = Signature::new(&content, secret);
        Self {
            content,
            owner: secret.public().into(),
            signature,
            blobs,
            validated_block_certificate: None,
        }
    }

    pub fn new_retry(
        round: Round,
        validated_block_certificate: ValidatedBlockCertificate,
        secret: &KeyPair,
        blobs: Vec<Blob>,
    ) -> Self {
        let lite_cert = validated_block_certificate.lite_certificate().cloned();
        let executed_block = validated_block_certificate.into_inner().into_inner();
        let content = ProposalContent {
            block: executed_block.block,
            round,
            forced_oracle_responses: Some(executed_block.outcome.oracle_responses),
        };
        let signature = Signature::new(&content, secret);
        Self {
            content,
            owner: secret.public().into(),
            signature,
            blobs,
            validated_block_certificate: Some(lite_cert),
        }
    }

    pub fn check_signature(&self, public_key: PublicKey) -> Result<(), CryptoError> {
        self.signature.check(&self.content, public_key)
    }
}

impl LiteVote {
    /// Uses the signing key to create a signed object.
    pub fn new(value: LiteValue, round: Round, key_pair: &KeyPair) -> Self {
        let hash_and_round = ValueHashAndRound(value.value_hash, round);
        let signature = Signature::new(&hash_and_round, key_pair);
        Self {
            value,
            round,
            validator: ValidatorName(key_pair.public()),
            signature,
        }
    }

    /// Verifies the signature in the vote.
    pub fn check(&self) -> Result<(), ChainError> {
        let hash_and_round = ValueHashAndRound(self.value.value_hash, self.round);
        Ok(self.signature.check(&hash_and_round, self.validator.0)?)
    }
}

pub struct SignatureAggregator<'a> {
    committee: &'a Committee,
    weight: u64,
    used_validators: HashSet<ValidatorName>,
    partial: Certificate,
}

impl<'a> SignatureAggregator<'a> {
    /// Starts aggregating signatures for the given value into a certificate.
    pub fn new(value: HashedCertificateValue, round: Round, committee: &'a Committee) -> Self {
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
            partial: Certificate::new(value, round, Vec::new()),
        }
    }

    /// Tries to append a signature to a (partial) certificate. Returns Some(certificate) if a
    /// quorum was reached. The resulting final certificate is guaranteed to be valid in the sense
    /// of `check` below. Returns an error if the signed value cannot be aggregated.
    pub fn append(
        &mut self,
        validator: ValidatorName,
        signature: Signature,
    ) -> Result<Option<Certificate>, ChainError> {
        let hash_and_round = ValueHashAndRound(self.partial.hash(), self.partial.round);
        signature.check(&hash_and_round, validator.0)?;
        // Check that each validator only appears once.
        ensure!(
            !self.used_validators.contains(&validator),
            ChainError::CertificateValidatorReuse
        );
        self.used_validators.insert(validator);
        // Update weight.
        let voting_rights = self.committee.weight(&validator);
        ensure!(voting_rights > 0, ChainError::InvalidSigner);
        self.weight += voting_rights;
        // Update certificate.
        self.partial.add_signature((validator, signature));

        if self.weight >= self.committee.quorum_threshold() {
            self.weight = 0; // Prevent from creating the certificate twice.
            Ok(Some(self.partial.clone()))
        } else {
            Ok(None)
        }
    }
}

// Checks if the array slice is strictly ordered. That means that if the array
// has duplicates, this will return False, even if the array is sorted
pub(crate) fn is_strictly_ordered(values: &[(ValidatorName, Signature)]) -> bool {
    values.windows(2).all(|pair| pair[0].0 < pair[1].0)
}

impl<'de> Deserialize<'de> for Certificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(rename = "Certificate")]
        struct CertificateHelper {
            value: HashedCertificateValue,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }

        let helper: CertificateHelper = Deserialize::deserialize(deserializer)?;
        if !is_strictly_ordered(&helper.signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::new(helper.value, helper.round, helper.signatures))
        }
    }
}

impl Certificate {
    /// Returns the certificate without the full value.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        LiteCertificate {
            value: self.lite_value(),
            round: self.round,
            signatures: Cow::Borrowed(self.signatures()),
        }
    }

    /// Returns the `LiteValue` corresponding to the certified value.
    pub fn lite_value(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.inner().chain_id(),
        }
    }

    /// Returns the bundles of messages sent via the given medium to the specified
    /// recipient. Messages originating from different transactions of the original block
    /// are kept in separate bundles. If the medium is a channel, does not verify that the
    /// recipient is actually subscribed to that channel.
    pub fn message_bundles_for<'a>(
        &'a self,
        medium: &'a Medium,
        recipient: ChainId,
    ) -> impl Iterator<Item = (Epoch, MessageBundle)> + 'a {
        let certificate_hash = self.hash();
        self.inner()
            .executed_block()
            .into_iter()
            .flat_map(move |executed_block| {
                executed_block.message_bundles_for(medium, recipient, certificate_hash)
            })
    }

    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.inner()
            .executed_block()
            .is_some_and(|executed_block| executed_block.requires_blob(blob_id))
    }

    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        let Some(executed_block) = self.inner().executed_block() else {
            return 0;
        };
        executed_block.messages().iter().map(Vec::len).sum()
    }
}

/// Verifies certificate signatures.
pub(crate) fn check_signatures(
    value_hash: CryptoHash,
    round: Round,
    signatures: &[(ValidatorName, Signature)],
    committee: &Committee,
) -> Result<(), ChainError> {
    // Check the quorum.
    let mut weight = 0;
    let mut used_validators = HashSet::new();
    for (validator, _) in signatures {
        // Check that each validator only appears once.
        ensure!(
            !used_validators.contains(validator),
            ChainError::CertificateValidatorReuse
        );
        used_validators.insert(*validator);
        // Update weight.
        let voting_rights = committee.weight(validator);
        ensure!(voting_rights > 0, ChainError::InvalidSigner);
        weight += voting_rights;
    }
    ensure!(
        weight >= committee.quorum_threshold(),
        ChainError::CertificateRequiresQuorum
    );
    // All that is left is checking signatures!
    let hash_and_round = ValueHashAndRound(value_hash, round);
    Signature::verify_batch(&hash_and_round, signatures.iter().map(|(v, s)| (&v.0, s)))?;
    Ok(())
}

impl BcsSignable for ProposalContent {}

impl BcsSignable for ValueHashAndRound {}

impl BcsHashable for CertificateValue {}

doc_scalar!(
    MessageAction,
    "Whether an incoming message is accepted or rejected."
);
doc_scalar!(
    ChannelFullName,
    "A channel name together with its application ID."
);
doc_scalar!(
    Medium,
    "The origin of a message coming from a particular chain. Used to identify each inbox."
);
doc_scalar!(
    Origin,
    "The origin of a message, relative to a particular application. Used to identify each inbox."
);
doc_scalar!(
    Target,
    "The target of a message, relative to a particular application. Used to identify each outbox."
);
