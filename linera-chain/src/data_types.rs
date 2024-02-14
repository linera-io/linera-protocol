// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ChainError;
use async_graphql::{Object, SimpleObject};
use linera_base::{
    crypto::{BcsHashable, BcsSignable, CryptoHash, KeyPair, Signature},
    data_types::{Amount, BlockHeight, Round, Timestamp},
    doc_scalar, ensure,
    identifiers::{Account, ChainId, ChannelName, Destination, MessageId, Owner},
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    BytecodeLocation, GenericApplicationId, Message, MessageKind, Operation,
};
use serde::{de::Deserializer, Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
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
    pub incoming_messages: Vec<IncomingMessage>,
    /// The operations to execute.
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
    pub authenticated_signer: Option<Owner>,
    /// Certified hash (see `Certificate` below) of the previous block in the
    /// chain, if any.
    pub previous_block_hash: Option<CryptoHash>,
}

impl Block {
    /// Returns all bytecode locations referred to in this block's incoming messages, with the
    /// sender chain ID.
    pub fn bytecode_locations(&self) -> HashMap<BytecodeLocation, ChainId> {
        let mut locations = HashMap::new();
        for message in &self.incoming_messages {
            if let Message::System(sys_message) = &message.event.message {
                locations.extend(
                    sys_message
                        .bytecode_locations(message.event.certificate_hash)
                        .map(|location| (location, message.origin.sender)),
                );
            }
        }
        locations
    }

    /// Returns whether the block contains only rejected incoming messages, which
    /// makes it admissible even on closed chains.
    pub fn has_only_rejected_messages(&self) -> bool {
        self.operations.is_empty()
            && self
                .incoming_messages
                .iter()
                .all(|message| message.action == MessageAction::Reject)
    }
}

/// A chain ID with a block height.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, SimpleObject)]
pub struct ChainAndHeight {
    pub chain_id: ChainId,
    pub height: BlockHeight,
}

/// A block with a round number.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct BlockAndRound {
    pub block: Block,
    pub round: Round,
}

/// A message received from a block of another chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct IncomingMessage {
    /// The origin of the message (chain and channel if any).
    pub origin: Origin,
    /// The content of the message to be delivered to the inbox identified by
    /// `origin`.
    pub event: Event,
    /// What to do with the message.
    pub action: MessageAction,
}

/// What to do with a message picked from the inbox.
#[derive(Copy, Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum MessageAction {
    /// Execute the incoming message.
    Accept,
    /// Do not execute the incoming message.
    Reject,
}

impl IncomingMessage {
    /// Returns the ID identifying this message.
    pub fn id(&self) -> MessageId {
        MessageId {
            chain_id: self.origin.sender,
            height: self.event.height,
            index: self.event.index,
        }
    }
}

/// A message together with non replayable information to ensure uniqueness in a
/// particular inbox.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The hash of the certificate that created the event.
    pub certificate_hash: CryptoHash,
    /// The height of the block that created the event.
    pub height: BlockHeight,
    /// The index of the message.
    pub index: u32,
    /// The authenticated signer for the operation that created the event, if any.
    pub authenticated_signer: Option<Owner>,
    /// A grant to pay for the message execution.
    pub grant: Amount,
    /// Where to send a refund for the unused part of the grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// The kind of event being delivered.
    pub kind: MessageKind,
    /// The timestamp of the block that caused the message.
    pub timestamp: Timestamp,
    /// The message of the event (i.e. the actual payload of a message).
    pub message: Message,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct MessageBundle {
    /// The block height.
    pub height: BlockHeight,
    /// The block's epoch.
    pub epoch: Epoch,
    /// The block's timestamp.
    pub timestamp: Timestamp,
    /// The confirmed block certificate hash.
    pub hash: CryptoHash,
    /// The relevant messages, with their index.
    pub messages: Vec<(u32, OutgoingMessage)>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
/// A channel name together with its application id.
pub struct ChannelFullName {
    /// The application owning the channel.
    pub application_id: GenericApplicationId,
    /// The name of the channel.
    pub name: ChannelName,
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct BlockProposal {
    pub content: BlockAndRound,
    pub owner: Owner,
    pub signature: Signature,
    pub blobs: Vec<HashedValue>,
    pub validated: Option<Certificate>,
}

/// A message together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OutgoingMessage {
    /// The destination of the message.
    pub destination: Destination,
    /// The user authentication carried by the message, if any.
    pub authenticated_signer: Option<Owner>,
    /// A grant to pay for the message execution.
    pub grant: Amount,
    /// Where to send a refund for the unused part of the grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// The kind of event being sent.
    pub kind: MessageKind,
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
}

/// A block, together with the messages and the state hash resulting from its execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct ExecutedBlock {
    pub block: Block,
    pub messages: Vec<OutgoingMessage>,
    /// For each transaction, the cumulative number of messages created by this and all previous
    /// transactions, i.e. `message_counts[i]` is the index of the first message created by
    /// transaction `i + 1` or later.
    pub message_counts: Vec<u32>,
    pub state_hash: CryptoHash,
}

/// The messages and the state hash resulting from a block's execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockExecutionOutcome {
    pub messages: Vec<OutgoingMessage>,
    /// For each transaction, the cumulative number of messages created by this and all previous
    /// transactions, i.e. `message_counts[i]` is the index of the first message created by
    /// transaction `i + 1` or later.
    pub message_counts: Vec<u32>,
    pub state_hash: CryptoHash,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum CertificateValue {
    ValidatedBlock {
        executed_block: ExecutedBlock,
    },
    ConfirmedBlock {
        executed_block: ExecutedBlock,
    },
    LeaderTimeout {
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
    },
}

#[Object]
impl CertificateValue {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> Option<ExecutedBlock> {
        self.executed_block().cloned()
    }

    async fn status(&self) -> String {
        match self {
            CertificateValue::ValidatedBlock { .. } => "validated".to_string(),
            CertificateValue::ConfirmedBlock { .. } => "confirmed".to_string(),
            CertificateValue::LeaderTimeout { .. } => "timeout".to_string(),
        }
    }
}

/// A statement to be certified by the validators, with its hash.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct HashedValue {
    value: CertificateValue,
    /// Hash of the value (used as key for storage).
    hash: CryptoHash,
}

#[Object]
impl HashedValue {
    #[graphql(derived(name = "hash"))]
    async fn _hash(&self) -> CryptoHash {
        self.hash
    }

    #[graphql(derived(name = "value"))]
    async fn _value(&self) -> CertificateValue {
        self.value.clone()
    }
}

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
    pub value: HashedValue,
    pub round: Round,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: HashedValue, round: Round, key_pair: &KeyPair) -> Self {
        let hash_and_round = ValueHashAndRound(value.hash, round);
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct LiteVote {
    pub value: LiteValue,
    pub round: Round,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl LiteVote {
    /// Returns the full vote, with the value, if it matches.
    pub fn with_value(self, value: HashedValue) -> Option<Vote> {
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
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
        if !is_strictly_ordered(&signatures) {
            // Not enforcing no duplicates, check the documentation for is_strictly_ordered
            // It's the responsibility of the caller to make sure signatures has no duplicates
            signatures.sort_by_key(|&(validator_name, _)| validator_name)
        }

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
    pub fn check(self, committee: &Committee) -> Result<LiteValue, ChainError> {
        check_signatures(&self.value, self.round, &self.signatures, committee)?;
        Ok(self.value)
    }

    /// Returns the `Certificate` with the specified value, if it matches.
    pub fn with_value(self, value: HashedValue) -> Option<Certificate> {
        if self.value.chain_id != value.inner().chain_id() || self.value.value_hash != value.hash()
        {
            return None;
        }
        Some(Certificate {
            value,
            round: self.round,
            signatures: self.signatures.into_owned(),
        })
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
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: HashedValue,
    /// The round in which the value was certified.
    pub round: Round,
    /// Signatures on the value.
    signatures: Vec<(ValidatorName, Signature)>,
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

impl Serialize for HashedValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for HashedValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        Ok(CertificateValue::deserialize(deserializer)?.into())
    }
}

impl From<CertificateValue> for HashedValue {
    fn from(value: CertificateValue) -> HashedValue {
        let hash = CryptoHash::new(&value);
        HashedValue { value, hash }
    }
}

impl From<HashedValue> for CertificateValue {
    fn from(hv: HashedValue) -> CertificateValue {
        hv.value
    }
}

impl CertificateValue {
    pub fn chain_id(&self) -> ChainId {
        match self {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => {
                executed_block.block.chain_id
            }
            CertificateValue::LeaderTimeout { chain_id, .. } => *chain_id,
        }
    }

    pub fn height(&self) -> BlockHeight {
        match self {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => {
                executed_block.block.height
            }
            CertificateValue::LeaderTimeout { height, .. } => *height,
        }
    }

    pub fn epoch(&self) -> Epoch {
        match self {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => executed_block.block.epoch,
            CertificateValue::LeaderTimeout { epoch, .. } => *epoch,
        }
    }

    /// Creates a `HashedValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> HashedValue {
        HashedValue { value: self, hash }
    }

    /// Returns whether this value contains the message with the specified ID.
    pub fn has_message(&self, message_id: &MessageId) -> bool {
        let Some(executed_block) = self.executed_block() else {
            return false;
        };
        let Ok(index) = usize::try_from(message_id.index) else {
            return false;
        };
        self.height() == message_id.height
            && self.chain_id() == message_id.chain_id
            && executed_block.messages.len() > index
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self, CertificateValue::ConfirmedBlock { .. })
    }

    pub fn is_validated(&self) -> bool {
        matches!(self, CertificateValue::ValidatedBlock { .. })
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, CertificateValue::LeaderTimeout { .. })
    }

    #[cfg(any(test, feature = "test"))]
    pub fn messages(&self) -> Option<&Vec<OutgoingMessage>> {
        Some(&self.executed_block()?.messages)
    }

    pub fn executed_block(&self) -> Option<&ExecutedBlock> {
        match self {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => Some(executed_block),
            CertificateValue::LeaderTimeout { .. } => None,
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
            CertificateValue::LeaderTimeout { .. } => "leader_timeout",
        }
    }
}

impl Event {
    pub fn is_skippable(&self) -> bool {
        use MessageKind::*;
        match self.kind {
            Protected | Tracked => false,
            Simple | Bouncing => self.grant == Amount::ZERO,
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
    /// Returns the `message_index`th outgoing message created by the `operation_index`th operation,
    /// or `None` if there is no such operation or message.
    pub fn message_id_for_operation(
        &self,
        operation_index: usize,
        message_index: u32,
    ) -> Option<MessageId> {
        let block = &self.block;
        let transaction_index = block.incoming_messages.len().checked_add(operation_index)?;
        let first_message_index = match transaction_index.checked_sub(1) {
            None => 0,
            Some(index) => *self.message_counts.get(index)?,
        };
        let index = first_message_index.checked_add(message_index)?;
        let next_transaction_index = *self.message_counts.get(transaction_index)?;
        if index < next_transaction_index {
            Some(self.message_id(index))
        } else {
            None
        }
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
        self.messages.get(usize::try_from(*index).ok()?)
    }

    /// Returns the message ID belonging to the `index`th outgoing message in this block.
    fn message_id(&self, index: u32) -> MessageId {
        MessageId {
            chain_id: self.block.chain_id,
            height: self.block.height,
            index,
        }
    }
}

impl BlockExecutionOutcome {
    pub fn with(self, block: Block) -> ExecutedBlock {
        let BlockExecutionOutcome {
            messages,
            message_counts,
            state_hash,
        } = self;
        ExecutedBlock {
            block,
            messages,
            message_counts,
            state_hash,
        }
    }
}

impl HashedValue {
    /// Creates a `ConfirmedBlock` value.
    pub fn new_confirmed(executed_block: ExecutedBlock) -> HashedValue {
        CertificateValue::ConfirmedBlock { executed_block }.into()
    }

    /// Creates a new `ValidatedBlock` value.
    pub fn new_validated(executed_block: ExecutedBlock) -> HashedValue {
        CertificateValue::ValidatedBlock { executed_block }.into()
    }

    /// Creates a new `LeaderTimeout` value.
    pub fn new_leader_timeout(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> HashedValue {
        CertificateValue::LeaderTimeout {
            chain_id,
            height,
            epoch,
        }
        .into()
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.value.chain_id(),
        }
    }

    pub fn into_confirmed(self) -> Option<HashedValue> {
        match self.value {
            value @ CertificateValue::ConfirmedBlock { .. } => Some(HashedValue {
                hash: self.hash,
                value,
            }),
            CertificateValue::ValidatedBlock { executed_block } => {
                Some(CertificateValue::ConfirmedBlock { executed_block }.into())
            }
            CertificateValue::LeaderTimeout { .. } => None,
        }
    }

    pub fn inner(&self) -> &CertificateValue {
        &self.value
    }

    pub fn into_inner(self) -> CertificateValue {
        self.value
    }
}

impl BlockProposal {
    pub fn new(
        content: BlockAndRound,
        secret: &KeyPair,
        blobs: Vec<HashedValue>,
        validated: Option<Certificate>,
    ) -> Self {
        let signature = Signature::new(&content, secret);
        Self {
            content,
            owner: secret.public().into(),
            signature,
            blobs,
            validated,
        }
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
    pub fn new(value: HashedValue, round: Round, committee: &'a Committee) -> Self {
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
            partial: Certificate {
                value,
                round,
                signatures: Vec::new(),
            },
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
fn is_strictly_ordered(values: &[(ValidatorName, Signature)]) -> bool {
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
            value: HashedValue,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }

        let helper: CertificateHelper = Deserialize::deserialize(deserializer)?;
        if !is_strictly_ordered(&helper.signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self {
                value: helper.value,
                round: helper.round,
                signatures: helper.signatures,
            })
        }
    }
}

impl Certificate {
    pub fn new(
        value: HashedValue,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        if !is_strictly_ordered(&signatures) {
            // Not enforcing no duplicates, check the documentation for is_strictly_ordered
            // It's the responsibility of the caller to make sure signatures has no duplicates
            signatures.sort_by_key(|&(validator_name, _)| validator_name)
        }

        Self {
            value,
            round,
            signatures,
        }
    }

    pub fn signatures(&self) -> &Vec<(ValidatorName, Signature)> {
        &self.signatures
    }

    // Adds a signature to the certificate's list of signatures
    // It's the responsibility of the caller to not insert duplicates
    pub fn add_signature(
        &mut self,
        signature: (ValidatorName, Signature),
    ) -> &Vec<(ValidatorName, Signature)> {
        let index = self
            .signatures
            .binary_search_by(|(name, _)| name.cmp(&signature.0))
            .unwrap_or_else(std::convert::identity);
        self.signatures.insert(index, signature);
        &self.signatures
    }

    /// Verifies the certificate.
    pub fn check<'a>(&'a self, committee: &Committee) -> Result<&'a HashedValue, ChainError> {
        check_signatures(&self.lite_value(), self.round, &self.signatures, committee)?;
        Ok(&self.value)
    }

    /// Returns the certificate without the full value.
    pub fn lite_certificate(&self) -> LiteCertificate {
        LiteCertificate {
            value: self.lite_value(),
            round: self.round,
            signatures: Cow::Borrowed(&self.signatures),
        }
    }

    /// Returns the `LiteValue` corresponding to the certified value.
    pub fn lite_value(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.value().chain_id(),
        }
    }

    /// Returns the certified value.
    pub fn value(&self) -> &CertificateValue {
        &self.value.value
    }

    /// Returns the certified value's hash.
    pub fn hash(&self) -> CryptoHash {
        self.value.hash
    }

    /// Returns whether the validator is among the signatories of this certificate.
    pub fn is_signed_by(&self, validator_name: &ValidatorName) -> bool {
        self.signatures
            .binary_search_by(|(name, _)| name.cmp(validator_name))
            .is_ok()
    }

    /// Returns the bundle of messages sent via the given medium to the specified
    /// recipient. If the medium is a channel, does not verify that the recipient is
    /// actually subscribed to that channel.
    pub fn message_bundle_for(&self, medium: &Medium, recipient: ChainId) -> Option<MessageBundle> {
        let executed_block = self.value().executed_block()?;
        let messages = (0u32..)
            .zip(&executed_block.messages)
            .filter(|(_, message)| message.has_destination(medium, recipient))
            .map(|(idx, message)| (idx, message.clone()))
            .collect();
        Some(MessageBundle {
            height: executed_block.block.height,
            epoch: executed_block.block.epoch,
            timestamp: executed_block.block.timestamp,
            hash: self.hash(),
            messages,
        })
    }
}

/// Verifies certificate signatures.
fn check_signatures(
    value: &LiteValue,
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
    let hash_and_round = ValueHashAndRound(value.value_hash, round);
    Signature::verify_batch(&hash_and_round, signatures.iter().map(|(v, s)| (&v.0, s)))?;
    Ok(())
}

impl BcsSignable for BlockAndRound {}

impl BcsSignable for ValueHashAndRound {}

impl BcsHashable for CertificateValue {}

doc_scalar!(
    MessageAction,
    "Whether an incoming message is accepted or rejected"
);
doc_scalar!(
    ChannelFullName,
    "A channel name together with its application id"
);
doc_scalar!(
    Event,
    "A message together with non replayable information to ensure uniqueness in a particular inbox"
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
