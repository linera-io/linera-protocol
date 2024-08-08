// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities

use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{Amount, BlockHeight, Round, Timestamp},
    identifiers::{ChainId, Owner},
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorState},
    system::Recipient,
    Message, MessageKind, Operation, ResourceControlPolicy, SystemOperation,
};

use crate::data_types::{
    Block, BlockProposal, Certificate, HashedCertificateValue, IncomingBundle, MessageAction,
    MessageBundle, Origin, PostedMessage, SignatureAggregator, Vote,
};

/// Creates a new child of the given block, with the same timestamp.
pub fn make_child_block(parent: &HashedCertificateValue) -> Block {
    let parent_value = parent.inner();
    let parent_block = parent_value.block().unwrap();
    Block {
        epoch: parent_value.epoch(),
        chain_id: parent_value.chain_id(),
        incoming_bundles: vec![],
        operations: vec![],
        previous_block_hash: Some(parent.hash()),
        height: parent_value.height().try_add_one().unwrap(),
        authenticated_signer: None,
        timestamp: parent_block.timestamp,
    }
}

/// Creates a block at height 0 for a new chain.
pub fn make_first_block(chain_id: ChainId) -> Block {
    Block {
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

    /// Returns a block proposal in `Round::Fast` without any hashed certificate values or validated block.
    fn into_fast_proposal(self, key_pair: &KeyPair) -> BlockProposal {
        self.into_proposal_with_round(key_pair, Round::Fast)
    }

    /// Returns a block proposal without any hashed certificate values or validated block.
    fn into_proposal_with_round(self, key_pair: &KeyPair, round: Round) -> BlockProposal;
}

impl BlockTestExt for Block {
    fn with_operation(mut self, operation: impl Into<Operation>) -> Self {
        self.operations.push(operation.into());
        self
    }

    fn with_transfer(self, owner: Option<Owner>, recipient: Recipient, amount: Amount) -> Self {
        self.with_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
            user_data: Default::default(),
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
        BlockProposal::new_initial(round, self, key_pair, vec![], vec![])
    }
}

pub trait VoteTestExt: Sized {
    /// Returns a certificate for a committee consisting only of this validator.
    fn into_certificate(self) -> Certificate;
}

impl VoteTestExt for Vote {
    fn into_certificate(self) -> Certificate {
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
    fn to_simple_incoming(self, sender: ChainId, height: BlockHeight) -> IncomingBundle;

    fn to_posted(self, index: u32, kind: MessageKind) -> PostedMessage;
}

impl<T: Into<Message>> MessageTestExt for T {
    fn to_simple_incoming(self, sender: ChainId, height: BlockHeight) -> IncomingBundle {
        let posted_message = PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind: MessageKind::Protected,
            index: 0,
            message: self.into(),
        };
        let bundle = MessageBundle {
            certificate_hash: CryptoHash::test_hash("certificate"),
            height,
            transaction_index: 0,
            timestamp: Timestamp::from(0),
            messages: vec![posted_message],
        };
        IncomingBundle {
            origin: Origin::chain(sender),
            bundle,
            action: MessageAction::Accept,
        }
    }

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
