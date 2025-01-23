// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities

use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, BlockHeight, Round, Timestamp},
    hashed::Hashed,
    identifiers::{ChainId, Owner},
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorState},
    system::Recipient,
    Message, MessageKind, Operation, ResourceControlPolicy, SystemOperation,
};

use crate::{
    block::ConfirmedBlock,
    data_types::{
        BlockProposal, IncomingBundle, PostedMessage, ProposedBlock, SignatureAggregator, Vote,
    },
    types::{CertificateValue, GenericCertificate},
};

/// Creates a new child of the given block, with the same timestamp.
pub fn make_child_block(parent: &Hashed<ConfirmedBlock>) -> ProposedBlock {
    let parent_value = parent.inner();
    let parent_header = &parent_value.block().header;
    ProposedBlock {
        epoch: parent_header.epoch,
        chain_id: parent_header.chain_id,
        incoming_bundles: vec![],
        operations: vec![],
        previous_block_hash: Some(parent.hash()),
        height: parent_header.height.try_add_one().unwrap(),
        authenticated_signer: parent_header.authenticated_signer,
        timestamp: parent_header.timestamp,
    }
}

/// Creates a block at height 0 for a new chain.
pub fn make_first_block(chain_id: ChainId) -> ProposedBlock {
    ProposedBlock {
        epoch: Epoch::ZERO,
        chain_id,
        incoming_bundles: vec![],
        operations: vec![],
        previous_block_hash: None,
        height: BlockHeight::ZERO,
        authenticated_signer: None,
        timestamp: Timestamp::default(),
    }
}

/// A helper trait to simplify constructing blocks for tests.
pub trait BlockTestExt: Sized {
    /// Returns the block with the given authenticated signer.
    fn with_authenticated_signer(self, authenticated_signer: Option<Owner>) -> Self;

    /// Returns the block with the given operation appended at the end.
    fn with_operation(self, operation: impl Into<Operation>) -> Self;

    /// Returns the block with a transfer operation appended at the end.
    fn with_transfer(self, owner: Option<Owner>, recipient: Recipient, amount: Amount) -> Self;

    /// Returns the block with a simple transfer operation appended at the end.
    fn with_simple_transfer(self, chain_id: ChainId, amount: Amount) -> Self;

    /// Returns the block with the given message appended at the end.
    fn with_incoming_bundle(self, incoming_bundle: IncomingBundle) -> Self;

    /// Returns the block with the specified timestamp.
    fn with_timestamp(self, timestamp: impl Into<Timestamp>) -> Self;

    /// Returns the block with the specified epoch.
    fn with_epoch(self, epoch: impl Into<Epoch>) -> Self;

    /// Returns a block proposal in the first round in a default ownership configuration
    /// (`Round::MultiLeader(0)`) without any hashed certificate values or validated block.
    fn into_first_proposal(self, key_pair: &KeyPair) -> BlockProposal {
        self.into_proposal_with_round(key_pair, Round::MultiLeader(0))
    }

    /// Returns a block proposal without any hashed certificate values or validated block.
    fn into_proposal_with_round(self, key_pair: &KeyPair, round: Round) -> BlockProposal;
}

impl BlockTestExt for ProposedBlock {
    fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }

    fn with_operation(mut self, operation: impl Into<Operation>) -> Self {
        self.operations.push(operation.into());
        self
    }

    fn with_transfer(self, owner: Option<Owner>, recipient: Recipient, amount: Amount) -> Self {
        self.with_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        })
    }

    fn with_simple_transfer(self, chain_id: ChainId, amount: Amount) -> Self {
        self.with_transfer(None, Recipient::chain(chain_id), amount)
    }

    fn with_incoming_bundle(mut self, incoming_bundle: IncomingBundle) -> Self {
        self.incoming_bundles.push(incoming_bundle);
        self
    }

    fn with_timestamp(mut self, timestamp: impl Into<Timestamp>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    fn with_epoch(mut self, epoch: impl Into<Epoch>) -> Self {
        self.epoch = epoch.into();
        self
    }

    fn into_proposal_with_round(self, key_pair: &KeyPair, round: Round) -> BlockProposal {
        BlockProposal::new_initial(round, self, key_pair, vec![])
    }
}

pub trait VoteTestExt<T>: Sized {
    /// Returns a certificate for a committee consisting only of this validator.
    fn into_certificate(self) -> GenericCertificate<T>;
}

impl<T: CertificateValue> VoteTestExt<T> for Vote<T> {
    fn into_certificate(self) -> GenericCertificate<T> {
        let state = ValidatorState {
            network_address: "".to_string(),
            votes: 100,
        };
        let committee = Committee::new(
            vec![(self.validator, state)].into_iter().collect(),
            ResourceControlPolicy::only_fuel(),
        );
        SignatureAggregator::new(self.value, self.round, &committee)
            .append(self.validator, self.signature)
            .unwrap()
            .unwrap()
    }
}

/// Helper trait to simplify constructing messages for tests.
pub trait MessageTestExt: Sized {
    fn to_posted(self, index: u32, kind: MessageKind) -> PostedMessage;
}

impl<T: Into<Message>> MessageTestExt for T {
    fn to_posted(self, index: u32, kind: MessageKind) -> PostedMessage {
        PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind,
            index,
            message: self.into(),
        }
    }
}
