// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};

use async_graphql::SimpleObject;
use custom_debug_derive::Debug;
use linera_base::{
    bcs,
    crypto::{
        AccountSignature, BcsHashable, BcsSignable, CryptoError, CryptoHash, Signer,
        ValidatorPublicKey, ValidatorSecretKey, ValidatorSignature,
    },
    data_types::{Amount, Blob, BlockHeight, Epoch, Event, OracleResponse, Round, Timestamp},
    doc_scalar, ensure, hex_debug,
    identifiers::{Account, AccountOwner, BlobId, ChainId, MessageId, StreamId},
};
use linera_execution::{committee::Committee, Message, MessageKind, Operation, OutgoingMessage};
use serde::{Deserialize, Serialize};

use crate::{
    block::{Block, ValidatedBlock},
    types::{
        CertificateKind, CertificateValue, GenericCertificate, LiteCertificate,
        ValidatedBlockCertificate,
    },
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
pub struct ProposedBlock {
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
    pub authenticated_signer: Option<AccountOwner>,
    /// Certified hash (see `Certificate` below) of the previous block in the
    /// chain, if any.
    pub previous_block_hash: Option<CryptoHash>,
}

impl ProposedBlock {
    /// Returns all the published blob IDs in this block's operations.
    pub fn published_blob_ids(&self) -> BTreeSet<BlobId> {
        self.operations
            .iter()
            .flat_map(Operation::published_blob_ids)
            .collect()
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

    /// Returns an iterator over all transactions.
    ///
    /// First incoming bundles, then operations.
    pub fn transactions(&self) -> impl Iterator<Item = Transaction<'_>> {
        let bundles = self
            .incoming_bundles
            .iter()
            .map(Transaction::ReceiveMessages);
        let operations = self.operations.iter().map(Transaction::ExecuteOperation);
        bundles.chain(operations)
    }

    pub fn check_proposal_size(&self, maximum_block_proposal_size: u64) -> Result<(), ChainError> {
        let size = bcs::serialized_size(self)?;
        ensure!(
            size <= usize::try_from(maximum_block_proposal_size).unwrap_or(usize::MAX),
            ChainError::BlockProposalTooLarge(size)
        );
        Ok(())
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
    /// The origin of the messages.
    pub origin: ChainId,
    /// The messages to be delivered to the inbox identified by `origin`.
    pub bundle: MessageBundle,
    /// What to do with the message.
    pub action: MessageAction,
}

impl IncomingBundle {
    /// Returns an iterator over all posted messages in this bundle, together with their ID.
    pub fn messages_and_ids(&self) -> impl Iterator<Item = (MessageId, &PostedMessage)> {
        let chain_and_height = ChainAndHeight {
            chain_id: self.origin,
            height: self.bundle.height,
        };
        let messages = self.bundle.messages.iter();
        messages.map(move |posted_message| {
            let message_id = chain_and_height.to_message_id(posted_message.index);
            (message_id, posted_message)
        })
    }
}

impl BcsHashable<'_> for IncomingBundle {}

/// What to do with a message picked from the inbox.
#[derive(Copy, Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum MessageAction {
    /// Execute the incoming message.
    Accept,
    /// Do not execute the incoming message.
    Reject,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
/// An earlier proposal that is being retried.
pub enum OriginalProposal {
    /// A proposal in the fast round.
    Fast(AccountSignature),
    /// A validated block certificate from an earlier round.
    Regular {
        certificate: LiteCertificate<'static>,
    },
}

/// An authenticated proposal for a new block.
// TODO(#456): the signature of the block owner is currently lost but it would be useful
// to have it for auditing purposes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct BlockProposal {
    pub content: ProposalContent,
    pub signature: AccountSignature,
    #[debug(skip_if = Option::is_none)]
    pub original_proposal: Option<OriginalProposal>,
}

/// A message together with kind, authentication and grant information.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct PostedMessage {
    /// The user authentication carried by the message, if any.
    #[debug(skip_if = Option::is_none)]
    pub authenticated_signer: Option<AccountOwner>,
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

pub trait OutgoingMessageExt {
    /// Returns the posted message, i.e. the outgoing message without the destination.
    fn into_posted(self, index: u32) -> PostedMessage;
}

impl OutgoingMessageExt for OutgoingMessage {
    /// Returns the posted message, i.e. the outgoing message without the destination.
    fn into_posted(self, index: u32) -> PostedMessage {
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

/// The execution result of a single operation.
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OperationResult(
    #[debug(with = "hex_debug")]
    #[serde(with = "serde_bytes")]
    pub Vec<u8>,
);

impl BcsHashable<'_> for OperationResult {}

doc_scalar!(
    OperationResult,
    "The execution result of a single operation."
);

/// The messages and the state hash resulting from a [`ProposedBlock`]'s execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
#[cfg_attr(with_testing, derive(Default))]
pub struct BlockExecutionOutcome {
    /// The list of outgoing messages for each transaction.
    pub messages: Vec<Vec<OutgoingMessage>>,
    /// The hashes and heights of previous blocks that sent messages to the same recipients.
    pub previous_message_blocks: BTreeMap<ChainId, (CryptoHash, BlockHeight)>,
    /// The hashes and heights of previous blocks that published events to the same channels.
    pub previous_event_blocks: BTreeMap<StreamId, (CryptoHash, BlockHeight)>,
    /// The hash of the chain's execution state after this block.
    pub state_hash: CryptoHash,
    /// The record of oracle responses for each transaction.
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    /// The list of events produced by each transaction.
    pub events: Vec<Vec<Event>>,
    /// The list of blobs created by each transaction.
    pub blobs: Vec<Vec<Blob>>,
    /// The execution result for each operation.
    pub operation_results: Vec<OperationResult>,
}

/// The hash and chain ID of a `CertificateValue`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct LiteValue {
    pub value_hash: CryptoHash,
    pub chain_id: ChainId,
    pub kind: CertificateKind,
}

impl LiteValue {
    pub fn new<T: CertificateValue>(value: &T) -> Self {
        LiteValue {
            value_hash: value.hash(),
            chain_id: value.chain_id(),
            kind: T::KIND,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
struct VoteValue(CryptoHash, Round, CertificateKind);

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub struct Vote<T> {
    pub value: T,
    pub round: Round,
    pub public_key: ValidatorPublicKey,
    pub signature: ValidatorSignature,
}

impl<T> Vote<T> {
    /// Use signing key to create a signed object.
    pub fn new(value: T, round: Round, key_pair: &ValidatorSecretKey) -> Self
    where
        T: CertificateValue,
    {
        let hash_and_round = VoteValue(value.hash(), round, T::KIND);
        let signature = ValidatorSignature::new(&hash_and_round, key_pair);
        Self {
            value,
            round,
            public_key: key_pair.public(),
            signature,
        }
    }

    /// Returns the vote, with a `LiteValue` instead of the full value.
    pub fn lite(&self) -> LiteVote
    where
        T: CertificateValue,
    {
        LiteVote {
            value: LiteValue::new(&self.value),
            round: self.round,
            public_key: self.public_key,
            signature: self.signature,
        }
    }

    /// Returns the value this vote is for.
    pub fn value(&self) -> &T {
        &self.value
    }
}

/// A vote on a statement from a validator, represented as a `LiteValue`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LiteVote {
    pub value: LiteValue,
    pub round: Round,
    pub public_key: ValidatorPublicKey,
    pub signature: ValidatorSignature,
}

impl LiteVote {
    /// Returns the full vote, with the value, if it matches.
    #[cfg(any(feature = "benchmark", with_testing))]
    pub fn with_value<T: CertificateValue>(self, value: T) -> Option<Vote<T>> {
        if self.value.value_hash != value.hash() {
            return None;
        }
        Some(Vote {
            value,
            round: self.round,
            public_key: self.public_key,
            signature: self.signature,
        })
    }

    pub fn kind(&self) -> CertificateKind {
        self.value.kind
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

impl BlockExecutionOutcome {
    pub fn with(self, block: ProposedBlock) -> Block {
        Block::new(block, self)
    }

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

    pub fn iter_created_blobs_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.blobs.iter().flatten().map(|blob| blob.id())
    }

    pub fn created_blobs_ids(&self) -> HashSet<BlobId> {
        self.iter_created_blobs_ids().collect()
    }
}

/// The data a block proposer signs.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProposalContent {
    /// The proposed block.
    pub block: ProposedBlock,
    /// The consensus round in which this proposal is made.
    pub round: Round,
    /// If this is a retry from an earlier round, the execution outcome.
    #[debug(skip_if = Option::is_none)]
    pub outcome: Option<BlockExecutionOutcome>,
}

impl BlockProposal {
    pub async fn new_initial<S: Signer + ?Sized>(
        owner: AccountOwner,
        round: Round,
        block: ProposedBlock,
        signer: &S,
    ) -> Result<Self, S::Error> {
        let content = ProposalContent {
            round,
            block,
            outcome: None,
        };
        let signature = signer.sign(&owner, &CryptoHash::new(&content)).await?;

        Ok(Self {
            content,
            signature,
            original_proposal: None,
        })
    }

    pub async fn new_retry_fast<S: Signer + ?Sized>(
        owner: AccountOwner,
        round: Round,
        old_proposal: BlockProposal,
        signer: &S,
    ) -> Result<Self, S::Error> {
        let content = ProposalContent {
            round,
            block: old_proposal.content.block,
            outcome: None,
        };
        let signature = signer.sign(&owner, &CryptoHash::new(&content)).await?;

        Ok(Self {
            content,
            signature,
            original_proposal: Some(OriginalProposal::Fast(old_proposal.signature)),
        })
    }

    pub async fn new_retry_regular<S: Signer>(
        owner: AccountOwner,
        round: Round,
        validated_block_certificate: ValidatedBlockCertificate,
        signer: &S,
    ) -> Result<Self, S::Error> {
        let certificate = validated_block_certificate.lite_certificate().cloned();
        let block = validated_block_certificate.into_inner().into_inner();
        let (block, outcome) = block.into_proposal();
        let content = ProposalContent {
            block,
            round,
            outcome: Some(outcome),
        };
        let signature = signer.sign(&owner, &CryptoHash::new(&content)).await?;

        Ok(Self {
            content,
            signature,
            original_proposal: Some(OriginalProposal::Regular { certificate }),
        })
    }

    /// Returns the `AccountOwner` that proposed the block.
    pub fn owner(&self) -> AccountOwner {
        match self.signature {
            AccountSignature::Ed25519 { public_key, .. } => public_key.into(),
            AccountSignature::Secp256k1 { public_key, .. } => public_key.into(),
            AccountSignature::EvmSecp256k1 { address, .. } => AccountOwner::Address20(address),
        }
    }

    pub fn check_signature(&self) -> Result<(), CryptoError> {
        self.signature.verify(&self.content)
    }

    pub fn required_blob_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.content.block.published_blob_ids().into_iter().chain(
            self.content
                .outcome
                .iter()
                .flat_map(|outcome| outcome.oracle_blob_ids()),
        )
    }

    pub fn expected_blob_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.content.block.published_blob_ids().into_iter().chain(
            self.content.outcome.iter().flat_map(|outcome| {
                outcome
                    .oracle_blob_ids()
                    .into_iter()
                    .chain(outcome.iter_created_blobs_ids())
            }),
        )
    }

    /// Checks that the original proposal, if present, matches the new one and has a higher round.
    pub fn check_invariants(&self) -> Result<(), &'static str> {
        match (&self.original_proposal, &self.content.outcome) {
            (None, None) => {}
            (Some(OriginalProposal::Fast(_)), None) => ensure!(
                self.content.round > Round::Fast,
                "The new proposal's round must be greater than the original's"
            ),
            (None, Some(_))
            | (Some(OriginalProposal::Fast(_)), Some(_))
            | (Some(OriginalProposal::Regular { .. }), None) => {
                return Err("Must contain a validation certificate if and only if \
                     it contains the execution outcome from a previous round");
            }
            (Some(OriginalProposal::Regular { certificate }), Some(outcome)) => {
                ensure!(
                    self.content.round > certificate.round,
                    "The new proposal's round must be greater than the original's"
                );
                let block = outcome.clone().with(self.content.block.clone());
                let value = ValidatedBlock::new(block);
                ensure!(
                    certificate.check_value(&value),
                    "Lite certificate must match the given block and execution outcome"
                );
            }
        }
        Ok(())
    }
}

impl LiteVote {
    /// Uses the signing key to create a signed object.
    pub fn new(value: LiteValue, round: Round, secret_key: &ValidatorSecretKey) -> Self {
        let hash_and_round = VoteValue(value.value_hash, round, value.kind);
        let signature = ValidatorSignature::new(&hash_and_round, secret_key);
        Self {
            value,
            round,
            public_key: secret_key.public(),
            signature,
        }
    }

    /// Verifies the signature in the vote.
    pub fn check(&self) -> Result<(), ChainError> {
        let hash_and_round = VoteValue(self.value.value_hash, self.round, self.value.kind);
        Ok(self.signature.check(&hash_and_round, self.public_key)?)
    }
}

pub struct SignatureAggregator<'a, T: CertificateValue> {
    committee: &'a Committee,
    weight: u64,
    used_validators: HashSet<ValidatorPublicKey>,
    partial: GenericCertificate<T>,
}

impl<'a, T: CertificateValue> SignatureAggregator<'a, T> {
    /// Starts aggregating signatures for the given value into a certificate.
    pub fn new(value: T, round: Round, committee: &'a Committee) -> Self {
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
            partial: GenericCertificate::new(value, round, Vec::new()),
        }
    }

    /// Tries to append a signature to a (partial) certificate. Returns Some(certificate) if a
    /// quorum was reached. The resulting final certificate is guaranteed to be valid in the sense
    /// of `check` below. Returns an error if the signed value cannot be aggregated.
    pub fn append(
        &mut self,
        public_key: ValidatorPublicKey,
        signature: ValidatorSignature,
    ) -> Result<Option<GenericCertificate<T>>, ChainError>
    where
        T: CertificateValue,
    {
        let hash_and_round = VoteValue(self.partial.hash(), self.partial.round, T::KIND);
        signature.check(&hash_and_round, public_key)?;
        // Check that each validator only appears once.
        ensure!(
            !self.used_validators.contains(&public_key),
            ChainError::CertificateValidatorReuse
        );
        self.used_validators.insert(public_key);
        // Update weight.
        let voting_rights = self.committee.weight(&public_key);
        ensure!(voting_rights > 0, ChainError::InvalidSigner);
        self.weight += voting_rights;
        // Update certificate.
        self.partial.add_signature((public_key, signature));

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
pub(crate) fn is_strictly_ordered(values: &[(ValidatorPublicKey, ValidatorSignature)]) -> bool {
    values.windows(2).all(|pair| pair[0].0 < pair[1].0)
}

/// Verifies certificate signatures.
pub(crate) fn check_signatures(
    value_hash: CryptoHash,
    certificate_kind: CertificateKind,
    round: Round,
    signatures: &[(ValidatorPublicKey, ValidatorSignature)],
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
    let hash_and_round = VoteValue(value_hash, round, certificate_kind);
    ValidatorSignature::verify_batch(&hash_and_round, signatures.iter())?;
    Ok(())
}

impl BcsSignable<'_> for ProposalContent {}

impl BcsSignable<'_> for VoteValue {}

doc_scalar!(
    MessageAction,
    "Whether an incoming message is accepted or rejected."
);

#[cfg(test)]
mod signing {
    use linera_base::{
        crypto::{AccountSecretKey, AccountSignature, CryptoHash, EvmSignature, TestString},
        data_types::{BlockHeight, Epoch, Round},
        identifiers::ChainId,
    };

    use crate::data_types::{BlockProposal, ProposalContent, ProposedBlock};

    #[test]
    fn proposal_content_signing() {
        use std::str::FromStr;

        // Generated in MetaMask.
        let secret_key = linera_base::crypto::EvmSecretKey::from_str(
            "f77a21701522a03b01c111ad2d2cdaf2b8403b47507ee0aec3c2e52b765d7a66",
        )
        .unwrap();
        let address = secret_key.address();

        let signer: AccountSecretKey = AccountSecretKey::EvmSecp256k1(secret_key);
        let public_key = signer.public();

        let proposed_block = ProposedBlock {
            chain_id: ChainId(CryptoHash::new(&TestString::new("ChainId"))),
            epoch: Epoch(11),
            incoming_bundles: vec![],
            operations: vec![],
            height: BlockHeight(11),
            timestamp: 190000000u64.into(),
            authenticated_signer: None,
            previous_block_hash: None,
        };

        let proposal = ProposalContent {
            block: proposed_block,
            round: Round::SingleLeader(11),
            outcome: None,
        };

        // personal_sign of the `proposal_hash` done via MetaMask.
        // Wrap with proper variant so that bytes match (include the enum variant tag).
        let signature = EvmSignature::from_str(
            "f2d8afcd51d0f947f5c5e31ac1db73ec5306163af7949b3bb265ba53d03374b0\
            4b1e909007b555caf098da1aded29c600bee391c6ee8b4d0962a29044555796d1b",
        )
        .unwrap();
        let metamask_signature = AccountSignature::EvmSecp256k1 {
            signature,
            address: address.0 .0,
        };

        let signature = signer.sign(&proposal);
        assert_eq!(signature, metamask_signature);

        assert_eq!(signature.owner(), public_key.into());

        let block_proposal = BlockProposal {
            content: proposal,
            signature,
            original_proposal: None,
        };
        assert_eq!(block_proposal.owner(), public_key.into(),);
    }
}
