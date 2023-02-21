// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ChainError;
use async_graphql::SimpleObject;
use linera_base::{
    committee::Committee,
    crypto::{BcsHashable, BcsSignable, CryptoHash, KeyPair, Signature},
    data_types::{BlockHeight, ChainId, Epoch, Owner, RoundNumber, Timestamp, ValidatorName},
    ensure,
};
use linera_execution::{
    ApplicationId, ApplicationRegistryView, BytecodeId, BytecodeLocation, ChannelName, Destination,
    Effect, ExecutionError, Operation, SystemEffect, SystemOperation, UserApplicationDescription,
    UserApplicationId,
};
use linera_views::{common::Context, views::ViewError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

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
    pub operations: Vec<(ApplicationId, Operation)>,
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
    /// Returns the list of all bytecode locations required to execute this block.
    ///
    /// This is similar to `ApplicationRegistryView::find_dependencies`, but starts out from a
    /// whole block and includes applications that are not in the registry yet and only will get
    /// registered or created in this block.
    pub async fn required_bytecode<C>(
        &self,
        registry: &ApplicationRegistryView<C>,
    ) -> Result<BTreeMap<BytecodeId, BytecodeLocation>, ExecutionError>
    where
        C: Context + Clone + Send + Sync + 'static,
        ViewError: From<C::Error>,
    {
        // The bytecode locations required for apps created in this block.
        let created_app_locations = registry
            .bytecode_locations_for(self.created_application_bytecode_ids().cloned())
            .await?;
        // The applications that get registered in this block.
        let registered_apps = self
            .registered_applications()
            .map(|app| (UserApplicationId::from(app), app.clone()))
            .collect();
        // The bytecode locations for applications required for operations or incoming messages.
        let required_locations = registry
            .describe_applications_with_dependencies(
                self.required_application_ids().cloned().collect(),
                &registered_apps,
            )
            .await?
            .into_iter()
            .map(|app| (app.bytecode_id, app.bytecode_location));
        Ok(required_locations.chain(created_app_locations).collect())
    }

    fn registered_applications(&self) -> impl Iterator<Item = &UserApplicationDescription> {
        self.incoming_messages
            .iter()
            .filter_map(|message| {
                if let Effect::System(SystemEffect::RegisterApplications { applications }) =
                    &message.event.effect
                {
                    Some(applications)
                } else {
                    None
                }
            })
            .flatten()
    }

    fn required_application_ids(&self) -> impl Iterator<Item = &UserApplicationId> + '_ {
        self.operations
            .iter()
            .flat_map(|(app_id, op)| {
                app_id.user_application_id().into_iter().chain(
                    if let Operation::System(SystemOperation::CreateApplication {
                        required_application_ids,
                        ..
                    }) = op
                    {
                        required_application_ids.iter()
                    } else {
                        [].iter()
                    },
                )
            })
            .chain(
                self.incoming_messages
                    .iter()
                    .filter_map(|message| message.application_id.user_application_id()),
            )
    }

    fn created_application_bytecode_ids(&self) -> impl Iterator<Item = &BytecodeId> + '_ {
        self.operations.iter().filter_map(|(_, op)| {
            if let Operation::System(SystemOperation::CreateApplication { bytecode_id, .. }) = op {
                Some(bytecode_id)
            } else {
                None
            }
        })
    }
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
    /// The application on both ends of the message.
    pub application_id: ApplicationId,
    /// The origin of the message (chain and channel if any).
    pub origin: Origin,
    /// The content of the message to be delivered to the inbox identified by
    /// `(application_id, origin)`.
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
    pub index: usize,
    /// The authenticated signer for the operation that created the event, if any
    pub authenticated_signer: Option<Owner>,
    /// The timestamp of the block that caused the effect.
    pub timestamp: Timestamp,
    /// The effect of the event (i.e. the actual payload of a message).
    pub effect: Effect,
}

/// The origin of a message, relative to a particular application. Used to identify each inbox.
#[derive(
    Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize, SimpleObject,
)]
pub struct Origin {
    /// The chain ID of the sender.
    pub sender: ChainId,
    /// The medium.
    pub medium: Medium,
}

/// The target of a message, relative to a particular application. Used to identify each outbox.
#[derive(
    Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize, SimpleObject,
)]
pub struct Target {
    /// The chain ID of the recipient.
    pub recipient: ChainId,
    /// The medium.
    pub medium: Medium,
}

/// The origin of a message coming from a particular chain. Used to identify each inbox.
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Serialize, Deserialize)]
pub enum Medium {
    /// The message is a direct message.
    Direct,
    /// The message is a channel broadcast.
    Channel(ChannelName),
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
}

/// An effect together with routing information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OutgoingEffect {
    pub application_id: ApplicationId,
    pub destination: Destination,
    pub authenticated_signer: Option<Owner>,
    pub effect: Effect,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    /// The block was validated but confirmation will require additional steps.
    ValidatedBlock {
        block: Block,
        round: RoundNumber,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
    },
    /// The block is validated and confirmed (i.e. ready to be published).
    ConfirmedBlock {
        block: Block,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
    },
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
    pub value: Value,
    pub validator: ValidatorName,
    pub signature: Signature,
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: Value, key_pair: &KeyPair) -> Self {
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
    pub fn with_value(self, value: Value) -> Option<Vote> {
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
    pub fn with_value(self, value: Value) -> Option<Certificate> {
        if self.value.chain_id != value.chain_id()
            || self.value.value_hash != CryptoHash::new(&value)
        {
            return None;
        }
        Some(Certificate {
            value,
            hash: self.value.value_hash,
            signatures: self.signatures,
        })
    }
}

/// A certified statement from the committee.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: Value,
    /// Hash of the certified value (used as key for storage).
    #[serde(skip_serializing)]
    pub hash: CryptoHash,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
}

/// A certificate, together with others required for its execution.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct CertificateWithDependencies {
    /// Certificate that may require blobs (e.g. bytecode) for execution.
    pub certificate: Certificate,
    /// Certificates containing blobs (e.g. bytecode) that the other one depends on.
    pub blob_certificates: Vec<Certificate>,
}

impl Origin {
    pub fn chain(sender: ChainId) -> Self {
        Self {
            sender,
            medium: Medium::Direct,
        }
    }

    pub fn channel(sender: ChainId, name: ChannelName) -> Self {
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

    pub fn channel(recipient: ChainId, name: ChannelName) -> Self {
        Self {
            recipient,
            medium: Medium::Channel(name),
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

    pub fn state_hash(&self) -> CryptoHash {
        match self {
            Value::ConfirmedBlock { state_hash, .. } => *state_hash,
            Value::ValidatedBlock { state_hash, .. } => *state_hash,
        }
    }

    pub fn effects(&self) -> &Vec<OutgoingEffect> {
        match self {
            Value::ConfirmedBlock { effects, .. } | Value::ValidatedBlock { effects, .. } => {
                effects
            }
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

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: CryptoHash::new(self),
            chain_id: self.chain_id(),
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
    pub fn new(value: Value, committee: &'a Committee) -> Self {
        let hash = CryptoHash::new(&value);
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
        let hash = CryptoHash::new(&value);
        Self {
            value,
            hash,
            signatures,
        }
    }

    /// Verify the certificate.
    pub fn check<'a>(&'a self, committee: &Committee) -> Result<&'a Value, ChainError> {
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
            value_hash: self.hash,
            chain_id: self.value.chain_id(),
        }
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
