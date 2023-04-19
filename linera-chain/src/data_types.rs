// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ChainError;
use async_graphql::SimpleObject;
use linera_base::{
    bcs_scalar,
    crypto::{BcsHashable, BcsSignable, CryptoHash, KeyPair, Signature},
    data_types::{BlockHeight, RoundNumber, Timestamp},
    doc_scalar, ensure,
    identifiers::{ChainId, ChannelName, Destination, EffectId, Owner},
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    ApplicationId, BytecodeLocation, Effect, Operation,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub incoming_messages: Vec<Message>,
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
            if let Effect::System(sys_effect) = &message.event.effect {
                locations.extend(
                    sys_effect
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
pub struct Message {
    /// The origin of the message (chain and channel if any).
    pub origin: Origin,
    /// The content of the message to be delivered to the inbox identified by
    /// `origin`.
    pub event: Event,
}

/// An effect together with non replayable information to ensure uniqueness in a
/// particular inbox.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The hash of the certificate that created the event
    pub certificate_hash: CryptoHash,
    /// The height of the block that created the event.
    pub height: BlockHeight,
    /// The index of the effect.
    pub index: u32,
    /// The authenticated signer for the operation that created the event, if any
    pub authenticated_signer: Option<Owner>,
    /// The timestamp of the block that caused the effect.
    pub timestamp: Timestamp,
    /// The effect of the event (i.e. the actual payload of a message).
    pub effect: Effect,
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

/// An effect together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OutgoingEffect {
    pub destination: Destination,
    pub authenticated_signer: Option<Owner>,
    pub effect: Effect,
}

/// The type of a value: whether it's a validated block in a particular round, or already
/// confirmed.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ValueKind {
    /// The block was validated but confirmation will require additional steps.
    ValidatedBlock { round: RoundNumber },
    /// The block is validated and confirmed (i.e. ready to be published).
    ConfirmedBlock,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct Value {
    pub block: Block,
    pub effects: Vec<OutgoingEffect>,
    pub state_hash: CryptoHash,
    pub kind: ValueKind,
}

/// A statement to be certified by the validators, with its hash.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct HashedValue {
    value: Value,
    /// Hash of the value (used as key for storage).
    hash: CryptoHash,
}

/// The hash and chain ID of a `Value`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct LiteValue {
    pub value_hash: CryptoHash,
    pub chain_id: ChainId,
}

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vote {
    pub value: HashedValue,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: HashedValue, key_pair: &KeyPair) -> Self {
        let signature = Signature::new(&value.lite(), key_pair);
        Self {
            value,
            validator: ValidatorName(key_pair.public()),
            signature,
        }
    }

    /// Returns the vote, with a `LiteValue` instead of the full value.
    pub fn lite(&self) -> LiteVote {
        LiteVote {
            value: self.value.lite(),
            validator: self.validator,
            signature: self.signature,
        }
    }
}

/// A vote on a statement from a validator, represented as a `LiteValue`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct LiteVote {
    pub value: LiteValue,
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
            validator: self.validator,
            signature: self.signature,
        })
    }
}

/// A certified statement from the committee, without the value.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct LiteCertificate {
    /// Hash and chain ID of the certified value (used as key for storage).
    pub value: LiteValue,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
}

impl LiteCertificate {
    pub fn new(value: LiteValue, signatures: Vec<(ValidatorName, Signature)>) -> Self {
        Self { value, signatures }
    }

    /// Verify the certificate.
    pub fn check(self, committee: &Committee) -> Result<LiteValue, ChainError> {
        check_signatures(&self.value, &self.signatures, committee)?;
        Ok(self.value)
    }

    /// Returns the `Certificate` with the specified value, if it matches.
    pub fn with_value(self, value: HashedValue) -> Option<Certificate> {
        if self.value.chain_id != value.chain_id() || self.value.value_hash != value.hash() {
            return None;
        }
        Some(Certificate {
            value,
            signatures: self.signatures,
        })
    }
}

/// A certified statement from the committee.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: HashedValue,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
}

/// A certificate, together with others required for its execution.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct CertificateWithDependencies {
    /// Certificate that may require blobs (e.g. bytecode) for execution.
    pub certificate: Certificate,
    /// Values containing blobs (e.g. bytecode) that the other one depends on.
    pub blobs: Vec<HashedValue>,
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
        Ok(Value::deserialize(deserializer)?.into())
    }
}

impl From<Value> for HashedValue {
    fn from(value: Value) -> HashedValue {
        let hash = CryptoHash::new(&value);
        HashedValue { value, hash }
    }
}

impl From<HashedValue> for Value {
    fn from(hv: HashedValue) -> Value {
        hv.value
    }
}

impl Value {
    /// Creates a `HashedValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> HashedValue {
        HashedValue { value: self, hash }
    }

    /// Returns whether this value contains the effect with the specified ID.
    pub fn has_effect(&self, effect_id: &EffectId) -> bool {
        self.block.height == effect_id.height
            && self.block.chain_id == effect_id.chain_id
            && self.effects.len() > usize::try_from(effect_id.index).unwrap_or(usize::MAX)
    }
}

impl HashedValue {
    pub fn new_confirmed(
        block: Block,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
    ) -> HashedValue {
        Value {
            block,
            effects,
            state_hash,
            kind: ValueKind::ConfirmedBlock,
        }
        .into()
    }
    pub fn new_validated(
        block: Block,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
        round: RoundNumber,
    ) -> HashedValue {
        Value {
            block,
            effects,
            state_hash,
            kind: ValueKind::ValidatedBlock { round },
        }
        .into()
    }

    pub fn chain_id(&self) -> ChainId {
        self.value.block.chain_id
    }

    pub fn block(&self) -> &Block {
        &self.value.block
    }

    pub fn state_hash(&self) -> CryptoHash {
        self.value.state_hash
    }

    pub fn effects(&self) -> &Vec<OutgoingEffect> {
        &self.value.effects
    }

    pub fn kind(&self) -> ValueKind {
        self.value.kind
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self.value.kind, ValueKind::ConfirmedBlock)
    }

    pub fn is_validated(&self) -> bool {
        matches!(self.value.kind, ValueKind::ValidatedBlock { .. })
    }

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.chain_id(),
        }
    }

    pub fn into_confirmed(self) -> HashedValue {
        Self::new_confirmed(self.value.block, self.value.effects, self.value.state_hash)
    }

    /// Returns whether this value contains the effect with the specified ID.
    pub fn has_effect(&self, effect_id: &EffectId) -> bool {
        self.value.has_effect(effect_id)
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
    /// Use signing key to create a signed object.
    pub fn new(value: LiteValue, key_pair: &KeyPair) -> Self {
        let signature = Signature::new(&value, key_pair);
        Self {
            value,
            validator: ValidatorName(key_pair.public()),
            signature,
        }
    }

    /// Verify the signature in the vote.
    pub fn check(&self) -> Result<(), ChainError> {
        Ok(self.signature.check(&self.value, self.validator.0)?)
    }
}

pub struct SignatureAggregator<'a> {
    committee: &'a Committee,
    weight: u64,
    used_validators: HashSet<ValidatorName>,
    partial: Certificate,
}

impl<'a> SignatureAggregator<'a> {
    /// Start aggregating signatures for the given value into a certificate.
    pub fn new(value: HashedValue, committee: &'a Committee) -> Self {
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
            partial: Certificate {
                value,
                signatures: Vec::new(),
            },
        }
    }

    /// Try to append a signature to a (partial) certificate. Returns Some(certificate) if a quorum was reached.
    /// The resulting final certificate is guaranteed to be valid in the sense of `check` below.
    /// Returns an error if the signed value cannot be aggregated.
    pub fn append(
        &mut self,
        validator: ValidatorName,
        signature: Signature,
    ) -> Result<Option<Certificate>, ChainError> {
        signature.check(&self.partial.value.lite(), validator.0)?;
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

impl Certificate {
    pub fn new(value: HashedValue, signatures: Vec<(ValidatorName, Signature)>) -> Self {
        Self { value, signatures }
    }

    /// Verify the certificate.
    pub fn check<'a>(&'a self, committee: &Committee) -> Result<&'a HashedValue, ChainError> {
        check_signatures(&self.lite(), &self.signatures, committee)?;
        Ok(&self.value)
    }

    /// Returns the certificate without the full value.
    pub fn without_value(&self) -> LiteCertificate {
        LiteCertificate {
            value: self.lite(),
            signatures: self.signatures.clone(),
        }
    }

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: self.value.hash(),
            chain_id: self.value.chain_id(),
        }
    }

    pub fn split(self) -> (LiteCertificate, HashedValue) {
        (
            LiteCertificate {
                value: self.lite(),
                signatures: self.signatures,
            },
            self.value,
        )
    }
}

/// Verifies certificate signatures.
fn check_signatures(
    value: &LiteValue,
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
    Signature::verify_batch(value, signatures.iter().map(|(v, s)| (&v.0, s)))?;
    Ok(())
}

impl BcsSignable for BlockAndRound {}

impl BcsHashable for Value {}

impl BcsSignable for LiteValue {}

bcs_scalar!(Certificate, "A certified statement from the committee");
doc_scalar!(
    ChannelFullName,
    "A channel name together with its application id"
);
doc_scalar!(
    Event,
    "An effect together with non replayable information to ensure uniqueness in a particular inbox"
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
