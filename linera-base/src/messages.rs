// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::Committee,
    crypto::*,
    ensure,
    error::Error,
    execution::{Balance, Effect, ExecutionState, Operation},
    manager::ChainManager,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, str::FromStr};

#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

#[cfg(test)]
#[path = "unit_tests/messages_tests.rs"]
mod messages_tests;

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct BlockHeight(pub u64);

/// A number to identify successive attempts to decide a value in a consensus protocol.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct RoundNumber(pub u64);

/// The identity of a validator.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct ValidatorName(pub PublicKey);

/// The owner of a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct Owner(pub PublicKey);

/// A number identifying the configuration of the chain (aka the committee).
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Epoch(pub u64);

/// How to create a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub enum ChainDescription {
    /// The chain was created by the genesis configuration.
    Root(usize),
    /// The chain was created by an effect from another chain.
    Child(EffectId),
}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct ChainId(pub HashValue);

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

/// A block containing operations to apply on a given chain, as well as the
/// acknowledgment of a number of incoming messages from other chains.
/// * Incoming messages must be selected in the order they were
///   produced by the sending chain, without skipping messages.
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
    pub incoming_messages: Vec<MessageGroup>,
    /// The operations to execute.
    pub operations: Vec<Operation>,
    /// The block height.
    pub height: BlockHeight,
    /// Certified hash (see `Certificate` below) of the previous block in the
    /// chain, if any.
    pub previous_block_hash: Option<HashValue>,
}

/// A block with a round number.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct BlockAndRound {
    pub block: Block,
    pub round: RoundNumber,
}

/// The identifier of a channel.
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct ChannelId {
    pub chain_id: ChainId,
    pub name: String,
}

/// The origin of a message. Used to identify each inbox.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Origin {
    /// The message is a direct message.
    Chain(ChainId),
    /// The message is a channel broadcast.
    Channel(ChannelId),
}

impl Origin {
    pub fn sender(&self) -> ChainId {
        match self {
            Origin::Chain(id) | Origin::Channel(ChannelId { chain_id: id, .. }) => *id,
        }
    }
}

/// A selection of messages sent by a block to another chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct MessageGroup {
    pub origin: Origin,
    pub height: BlockHeight,
    pub effects: Vec<(usize, Effect)>,
}

/// An authenticated proposal for a new block.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct BlockProposal {
    pub content: BlockAndRound,
    pub owner: Owner,
    pub signature: Signature,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    /// The block was validated but confirmation will require additional steps.
    ValidatedBlock {
        block: Block,
        round: RoundNumber,
        effects: Vec<Effect>,
        state_hash: HashValue,
    },
    /// The block is validated and confirmed (i.e. ready to be published).
    ConfirmedBlock {
        block: Block,
        effects: Vec<Effect>,
        state_hash: HashValue,
    },
}

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Vote {
    pub value: Value,
    pub validator: ValidatorName,
    pub signature: Signature,
}

/// A certified statement from the committee.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: Value,
    /// Hash of the certified value (used as key for storage).
    #[serde(skip_serializing)]
    pub hash: HashValue,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
}

/// A range of block heights as used in ChainInfoQuery.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Eq, PartialEq))]
pub struct BlockHeightRange {
    /// Starting point
    pub start: BlockHeight,
    /// Optional limit on the number of elements.
    pub limit: Option<usize>,
}

/// Message to obtain information on a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Eq, PartialEq))]
pub struct ChainInfoQuery {
    /// The chain id
    pub chain_id: ChainId,
    /// Optionally test that the block height is the one expected.
    pub test_next_block_height: Option<BlockHeight>,
    /// Query the full execution state (may not supported by all validators).
    pub request_execution_state: bool,
    /// Query the received messages that are waiting be picked in the next block.
    pub request_pending_messages: bool,
    /// Query a range of certificates sent from the chain.
    pub request_sent_certificates_in_range: Option<BlockHeightRange>,
    /// Query new certificates received from the chain.
    pub request_received_certificates_excluding_first_nth: Option<usize>,
}

impl ChainInfoQuery {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_execution_state: false,
            request_pending_messages: false,
            request_sent_certificates_in_range: None,
            request_received_certificates_excluding_first_nth: None,
        }
    }

    pub fn test_next_block_height(mut self, height: BlockHeight) -> Self {
        self.test_next_block_height = Some(height);
        self
    }

    pub fn with_execution_state(mut self) -> Self {
        self.request_execution_state = true;
        self
    }

    pub fn with_pending_messages(mut self) -> Self {
        self.request_pending_messages = true;
        self
    }

    pub fn with_sent_certificates_in_range(mut self, range: BlockHeightRange) -> Self {
        self.request_sent_certificates_in_range = Some(range);
        self
    }

    pub fn with_received_certificates_excluding_first_nth(mut self, n: usize) -> Self {
        self.request_received_certificates_excluding_first_nth = Some(n);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainInfo {
    /// The chain id.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Option<Epoch>,
    /// The chain description.
    pub description: Option<ChainDescription>,
    /// The state of the chain authentication.
    pub manager: ChainManager,
    /// The current balance.
    pub balance: Balance,
    /// The last block hash, if any.
    pub block_hash: Option<HashValue>,
    /// The height after the latest block in the chain.
    pub next_block_height: BlockHeight,
    /// The hash of the current execution state.
    pub state_hash: HashValue,
    /// The full execution state.
    pub requested_execution_state: Option<ExecutionState>,
    /// The received messages that are waiting be picked in the next block (if requested).
    pub requested_pending_messages: Vec<MessageGroup>,
    /// The response to `request_sent_certificates_in_range`
    pub requested_sent_certificates: Vec<Certificate>,
    /// The current number of received certificates (useful for `request_received_certificates_excluding_first_nth`)
    pub count_received_certificates: usize,
    /// The response to `request_received_certificates_excluding_first_nth`
    pub requested_received_certificates: Vec<Certificate>,
}

/// The response to an `ChainInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainInfoResponse {
    pub info: ChainInfo,
    pub signature: Option<Signature>,
}

/// An internal message between chains within a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
#[allow(clippy::large_enum_variant)]
pub enum CrossChainRequest {
    /// Communicate a number of confirmed blocks from the sender to the recipient.
    /// Blocks must be given by increasing heights.
    UpdateRecipient {
        origin: Origin,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    },
    /// Acknowledge the height of the highest confirmed block communicated with `UpdateRecipient`.
    ConfirmUpdatedRecipient {
        origin: Origin,
        recipient: ChainId,
        height: BlockHeight,
    },
}

impl CrossChainRequest {
    /// Where to send the cross-chain request.
    pub fn target_chain_id(&self) -> ChainId {
        use CrossChainRequest::*;
        match self {
            UpdateRecipient { recipient, .. } => *recipient,
            ConfirmUpdatedRecipient { origin, .. } => origin.sender(),
        }
    }
}

impl Value {
    pub fn chain_id(&self) -> ChainId {
        match self {
            Value::ConfirmedBlock { block, .. } => block.chain_id,
            Value::ValidatedBlock { block, .. } => block.chain_id,
        }
    }

    pub fn block(&self) -> &Block {
        match self {
            Value::ConfirmedBlock { block, .. } => block,
            Value::ValidatedBlock { block, .. } => block,
        }
    }

    pub fn state_hash(&self) -> HashValue {
        match self {
            Value::ConfirmedBlock { state_hash, .. } => *state_hash,
            Value::ValidatedBlock { state_hash, .. } => *state_hash,
        }
    }

    pub fn effects_and_state_hash(&self) -> (Vec<Effect>, HashValue) {
        match self {
            Value::ConfirmedBlock {
                effects,
                state_hash,
                ..
            }
            | Value::ValidatedBlock {
                effects,
                state_hash,
                ..
            } => (effects.clone(), *state_hash),
        }
    }

    pub fn confirmed_block(&self) -> Option<&Block> {
        match self {
            Value::ConfirmedBlock { block, .. } => Some(block),
            _ => None,
        }
    }

    pub fn validated_block(&self) -> Option<&Block> {
        match self {
            Value::ValidatedBlock { block, .. } => Some(block),
            _ => None,
        }
    }
}

impl BlockProposal {
    pub fn new(content: BlockAndRound, secret: &KeyPair) -> Self {
        let signature = Signature::new(&content, secret);
        Self {
            content,
            owner: Owner(secret.public()),
            signature,
        }
    }
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: Value, key_pair: &KeyPair) -> Self {
        let signature = Signature::new(&value, key_pair);
        Self {
            value,
            validator: ValidatorName(key_pair.public()),
            signature,
        }
    }

    /// Verify the signature in the vote.
    pub fn check(&self, name: ValidatorName) -> Result<(), Error> {
        self.signature.check(&self.value, name.0)
    }
}

impl ChainInfoResponse {
    pub fn new(info: ChainInfo, key_pair: Option<&KeyPair>) -> Self {
        let signature = key_pair.map(|kp| Signature::new(&info, kp));
        Self { info, signature }
    }

    pub fn check(&self, name: ValidatorName) -> Result<(), Error> {
        match self.signature {
            Some(sig) => sig.check(&self.info, name.0),
            None => Err(Error::InvalidChainInfoResponse),
        }
    }
}

pub struct SignatureAggregator<'a> {
    committee: &'a Committee,
    weight: usize,
    used_validators: HashSet<ValidatorName>,
    partial: Certificate,
}

impl<'a> SignatureAggregator<'a> {
    /// Start aggregating signatures for the given value into a certificate.
    pub fn new(value: Value, committee: &'a Committee) -> Self {
        let hash = HashValue::new(&value);
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
            partial: Certificate {
                hash,
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
    ) -> Result<Option<Certificate>, Error> {
        signature.check(&self.partial.value, validator.0)?;
        // Check that each validator only appears once.
        ensure!(
            !self.used_validators.contains(&validator),
            Error::CertificateValidatorReuse
        );
        self.used_validators.insert(validator);
        // Update weight.
        let voting_rights = self.committee.weight(&validator);
        ensure!(voting_rights > 0, Error::InvalidSigner);
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

impl<'a> Deserialize<'a> for Certificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'a>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Certificate")]
        struct NetworkCertificate {
            value: Value,
            signatures: Vec<(ValidatorName, Signature)>,
        }

        let cert = NetworkCertificate::deserialize(deserializer)?;
        Ok(Certificate::new(cert.value, cert.signatures))
    }
}

impl Certificate {
    pub fn new(value: Value, signatures: Vec<(ValidatorName, Signature)>) -> Self {
        let hash = HashValue::new(&value);
        Self {
            value,
            hash,
            signatures,
        }
    }

    /// Verify the certificate.
    pub fn check<'a>(&'a self, committee: &Committee) -> Result<&'a Value, Error> {
        // Check the quorum.
        let mut weight = 0;
        let mut used_validators = HashSet::new();
        for (validator, _) in self.signatures.iter() {
            // Check that each validator only appears once.
            ensure!(
                !used_validators.contains(validator),
                Error::CertificateValidatorReuse
            );
            used_validators.insert(*validator);
            // Update weight.
            let voting_rights = committee.weight(validator);
            ensure!(voting_rights > 0, Error::InvalidSigner);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            Error::CertificateRequiresQuorum
        );
        // All what is left is checking signatures!
        Signature::verify_batch(&self.value, self.signatures.iter().map(|(v, s)| (&v.0, s)))?;
        Ok(&self.value)
    }
}

impl std::fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for ValidatorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ValidatorName {
    type Err = PublicKeyFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ValidatorName(PublicKey::from_str(s)?))
    }
}

impl BlockHeight {
    #[inline]
    pub fn max() -> Self {
        BlockHeight(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<BlockHeight, Error> {
        let val = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<BlockHeight, Error> {
        let val = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(())
    }
}

impl RoundNumber {
    #[inline]
    pub fn max() -> Self {
        RoundNumber(0x7fff_ffff_ffff_ffff)
    }

    #[inline]
    pub fn try_add_one(self) -> Result<RoundNumber, Error> {
        let val = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub_one(self) -> Result<RoundNumber, Error> {
        let val = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(1).ok_or(Error::SequenceUnderflow)?;
        Ok(())
    }
}

impl Epoch {
    #[inline]
    pub fn try_add_one(self) -> Result<Self, Error> {
        let val = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), Error> {
        self.0 = self.0.checked_add(1).ok_or(Error::SequenceOverflow)?;
        Ok(())
    }
}

impl From<BlockHeight> for u64 {
    fn from(val: BlockHeight) -> Self {
        val.0
    }
}

impl From<u64> for BlockHeight {
    fn from(value: u64) -> Self {
        BlockHeight(value)
    }
}

impl From<BlockHeight> for usize {
    fn from(value: BlockHeight) -> Self {
        value.0 as usize
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Epoch(value)
    }
}

impl From<PublicKey> for ValidatorName {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl From<PublicKey> for Owner {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl std::str::FromStr for Owner {
    type Err = PublicKeyFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Owner(PublicKey::from_str(s)?))
    }
}

impl std::fmt::Display for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ChainId {
    type Err = HashFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ChainId(HashValue::from_str(s)?))
    }
}

impl std::fmt::Debug for ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

impl From<ChainDescription> for ChainId {
    fn from(description: ChainDescription) -> Self {
        Self(HashValue::new(&description))
    }
}

impl ChainId {
    pub fn root(index: usize) -> Self {
        Self(HashValue::new(&ChainDescription::Root(index)))
    }

    pub fn child(id: EffectId) -> Self {
        Self(HashValue::new(&ChainDescription::Child(id)))
    }
}

impl BcsSignable for ChainDescription {}
impl BcsSignable for ChainInfo {}
impl BcsSignable for BlockAndRound {}
impl BcsSignable for Value {}
