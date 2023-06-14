// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ChainError;
use async_graphql::SimpleObject;
use linera_base::{
    crypto::{BcsHashable, BcsSignable, CryptoHash, KeyPair, Signature},
    data_types::{BlockHeight, RoundNumber, Timestamp},
    doc_scalar, ensure,
    identifiers::{ChainId, ChannelName, Destination, MessageId, Owner},
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    ApplicationId, BytecodeLocation, Message, Operation,
};
use serde::{Deserialize, Serialize};
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
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
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
    /// The user signing for the operations in the block. (Currently, this must be the `owner`
    /// in the block proposal, or no one.)
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
    pub round: RoundNumber,
}

/// A message received from a block of another chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct IncomingMessage {
    /// The origin of the message (chain and channel if any).
    pub origin: Origin,
    /// The content of the message to be delivered to the inbox identified by
    /// `origin`.
    pub event: Event,
}

/// A message together with non replayable information to ensure uniqueness in a
/// particular inbox.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The hash of the certificate that created the event
    pub certificate_hash: CryptoHash,
    /// The height of the block that created the event.
    pub height: BlockHeight,
    /// The index of the message.
    pub index: u32,
    /// The authenticated signer for the operation that created the event, if any
    pub authenticated_signer: Option<Owner>,
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
/// A channel name together with its application id.
pub struct ChannelFullName {
    /// The application owning the channel.
    pub application_id: ApplicationId,
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
}

/// A message together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub destination: Destination,
    pub authenticated_signer: Option<Owner>,
    pub message: Message,
}

/// A block, together with the messages and the state hash resulting from its execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct ExecutedBlock {
    pub block: Block,
    pub messages: Vec<OutgoingMessage>,
    pub state_hash: CryptoHash,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum CertificateValue {
    ValidatedBlock { executed_block: ExecutedBlock },
    ConfirmedBlock { executed_block: ExecutedBlock },
}

/// A statement to be certified by the validators, with its hash.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct HashedValue {
    value: CertificateValue,
    /// Hash of the value (used as key for storage).
    hash: CryptoHash,
}

/// The hash and chain ID of a `CertificateValue`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct LiteValue {
    pub value_hash: CryptoHash,
    pub chain_id: ChainId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
struct ValueHashAndRound(CryptoHash, RoundNumber);

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vote {
    pub value: HashedValue,
    pub round: RoundNumber,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: HashedValue, round: RoundNumber, key_pair: &KeyPair) -> Self {
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
    pub round: RoundNumber,
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
    pub round: RoundNumber,
    /// Signatures on the value.
    pub signatures: Cow<'a, [(ValidatorName, Signature)]>,
}

impl<'a> LiteCertificate<'a> {
    pub fn new(
        value: LiteValue,
        round: RoundNumber,
        signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        let signatures = Cow::Owned(signatures);
        Self {
            value,
            round,
            signatures,
        }
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: HashedValue,
    /// The round in which the value was certified.
    pub round: RoundNumber,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
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
        D: serde::de::Deserializer<'a>,
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
        self.executed_block().block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.executed_block().block.height
    }

    pub fn epoch(&self) -> Epoch {
        self.executed_block().block.epoch
    }

    /// Creates a `HashedValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> HashedValue {
        HashedValue { value: self, hash }
    }

    /// Returns whether this value contains the message with the specified ID.
    pub fn has_message(&self, message_id: &MessageId) -> bool {
        self.height() == message_id.height
            && self.chain_id() == message_id.chain_id
            && self.executed_block().messages.len()
                > usize::try_from(message_id.index).unwrap_or(usize::MAX)
    }

    /// Skip `n-1` messages from the end of the block and return the message ID, if any.
    pub fn nth_last_message_id(&self, n: u32) -> Option<MessageId> {
        if n == 0 {
            return None;
        }
        let message_count = self.executed_block().messages.len();
        Some(MessageId {
            chain_id: self.chain_id(),
            height: self.height(),
            index: u32::try_from(message_count).ok()?.checked_sub(n)?,
        })
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self, CertificateValue::ConfirmedBlock { .. })
    }

    pub fn is_validated(&self) -> bool {
        matches!(self, CertificateValue::ValidatedBlock { .. })
    }

    #[cfg(any(test, feature = "test"))]
    pub fn messages(&self) -> &Vec<OutgoingMessage> {
        &self.executed_block().messages
    }

    fn executed_block(&self) -> &ExecutedBlock {
        match self {
            CertificateValue::ConfirmedBlock { executed_block, .. }
            | CertificateValue::ValidatedBlock { executed_block, .. } => executed_block,
        }
    }
}

impl HashedValue {
    /// Creates a `ConfirmedBlock` with round 0.
    #[cfg(any(test, feature = "test"))]
    pub fn new_confirmed(executed_block: ExecutedBlock) -> HashedValue {
        CertificateValue::ConfirmedBlock { executed_block }.into()
    }

    pub fn new_validated(executed_block: ExecutedBlock) -> HashedValue {
        CertificateValue::ValidatedBlock { executed_block }.into()
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

    pub fn into_confirmed(self) -> HashedValue {
        match self.value {
            value @ CertificateValue::ConfirmedBlock { .. } => HashedValue {
                hash: self.hash,
                value,
            },
            CertificateValue::ValidatedBlock { executed_block } => {
                CertificateValue::ConfirmedBlock { executed_block }.into()
            }
        }
    }

    pub fn inner(&self) -> &CertificateValue {
        &self.value
    }

    pub fn into_inner(self) -> CertificateValue {
        self.value
    }

    /// Skip `n-1` messages from the end of the block and return the message ID, if any.
    pub fn nth_last_message_id(&self, n: u32) -> Option<MessageId> {
        self.value.nth_last_message_id(n)
    }
}

impl BlockProposal {
    pub fn new(content: BlockAndRound, secret: &KeyPair, blobs: Vec<HashedValue>) -> Self {
        let signature = Signature::new(&content, secret);
        Self {
            content,
            owner: secret.public().into(),
            signature,
            blobs,
        }
    }
}

impl LiteVote {
    /// Uses the signing key to create a signed object.
    pub fn new(value: LiteValue, round: RoundNumber, key_pair: &KeyPair) -> Self {
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
    pub fn new(value: HashedValue, round: RoundNumber, committee: &'a Committee) -> Self {
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
        self.partial.signatures.push((validator, signature));

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

impl Certificate {
    pub fn new(
        value: HashedValue,
        round: RoundNumber,
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
            .iter()
            .any(|(name, _)| name == validator_name)
    }
}

/// Verifies certificate signatures.
fn check_signatures(
    value: &LiteValue,
    round: RoundNumber,
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
