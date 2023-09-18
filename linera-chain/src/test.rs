// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities

use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, BlockHeight, RoundNumber, Timestamp},
    identifiers::ChainId,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorState},
    pricing::Pricing,
    system::Recipient,
    Operation, SystemOperation,
};

use crate::data_types::{
    Block, BlockAndRound, BlockProposal, Certificate, HashedValue, IncomingMessage,
    SignatureAggregator, Vote,
};

/// Creates a new child of the given block, with the same timestamp.
pub fn make_child_block(parent: &HashedValue) -> Block {
    let parent_value = parent.inner();
    let parent_block = parent_value.block().unwrap();
    Block {
        epoch: parent_value.epoch(),
        chain_id: parent_value.chain_id(),
        incoming_messages: vec![],
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
        incoming_messages: vec![],
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
    fn with_simple_transfer(self, recipient: Recipient, amount: Amount) -> Self;

    /// Returns the block with the given message appended at the end.
    fn with_incoming_message(self, incoming_message: IncomingMessage) -> Self;

    /// Returns the block with the specified timestamp.
    fn with_timestamp(self, timestamp: impl Into<Timestamp>) -> Self;

    /// Returns the block with the specified epoch.
    fn with_epoch(self, epoch: impl Into<Epoch>) -> Self;

    /// Returns a block proposal without any blobs or validated block.
    fn into_simple_proposal(
        self,
        key_pair: &KeyPair,
        round: impl Into<RoundNumber>,
    ) -> BlockProposal;
}

impl BlockTestExt for Block {
    fn with_operation(mut self, operation: impl Into<Operation>) -> Self {
        self.operations.push(operation.into());
        self
    }

    fn with_simple_transfer(self, recipient: Recipient, amount: Amount) -> Self {
        self.with_operation(SystemOperation::Transfer {
            owner: None,
            recipient,
            amount,
            user_data: Default::default(),
        })
    }

    fn with_incoming_message(mut self, incoming_message: IncomingMessage) -> Self {
        self.incoming_messages.push(incoming_message);
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

    fn into_simple_proposal(
        self,
        key_pair: &KeyPair,
        round: impl Into<RoundNumber>,
    ) -> BlockProposal {
        let content = BlockAndRound {
            block: self,
            round: round.into(),
        };
        BlockProposal::new(content, key_pair, vec![], None)
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
            Pricing::only_fuel(),
        );
        SignatureAggregator::new(self.value, self.round, &committee)
            .append(self.validator, self.signature)
            .unwrap()
            .unwrap()
    }
}
