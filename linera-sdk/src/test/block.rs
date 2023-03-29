// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use super::TestValidator;
use linera_base::{
    data_types::Timestamp,
    identifiers::{ChainId, Owner},
};
use linera_chain::data_types::{Block, Certificate, HashedValue, LiteVote, SignatureAggregator};

/// A helper type to build [`Block`]s using the builder pattern, and then signing them into
/// [`Certificate`]s using a [`TestValidator`].
pub struct BlockBuilder {
    block: Block,
    validator: TestValidator,
}

impl BlockBuilder {
    /// Creates a new [`BlockBuilder`], initializing the block so that it belongs to a micro-chain.
    ///
    /// Initializes the block so that it belongs to the micro-chain identified by `chain_id` and
    /// owned by `owner`. It becomes the block after the specified `previous_block`, or the genesis
    /// block if [`None`] is specified.
    ///
    /// # Notes
    ///
    /// This is an internal method, because the [`BlockBuilder`] instance should be built by an
    /// [`ActiveChain`]. External users should only be able to add operations and messages to the
    /// block.
    pub(crate) fn new(
        chain_id: ChainId,
        owner: Owner,
        previous_block: Option<&Certificate>,
        validator: TestValidator,
    ) -> Self {
        let previous_block_hash = previous_block.map(|certificate| certificate.value.hash());
        let height = previous_block
            .map(|certificate| {
                certificate
                    .value
                    .block()
                    .height
                    .try_add_one()
                    .expect("Block height limit reached")
            })
            .unwrap_or_default();

        BlockBuilder {
            block: Block {
                epoch: 0.into(),
                chain_id,
                incoming_messages: vec![],
                operations: vec![],
                previous_block_hash,
                height,
                authenticated_signer: Some(owner),
                timestamp: Timestamp::from(0),
            },
            validator,
        }
    }

    /// Signs the prepared [`Block`] with the [`TestValidator`]'s keys and returns the resulting
    /// [`Certificate`].
    pub(crate) async fn sign(self) -> Certificate {
        let (effects, info) = self
            .validator
            .worker()
            .await
            .stage_block_execution(&self.block)
            .await
            .expect("Failed to execute block");
        let state_hash = info.info.state_hash.expect("Missing execution state hash");

        let value = HashedValue::new_confirmed(self.block, effects, state_hash);
        let vote = LiteVote::new(value.lite(), self.validator.key_pair());
        let mut builder = SignatureAggregator::new(value, self.validator.committee());
        builder
            .append(vote.validator, vote.signature)
            .expect("Failed to sign block")
            .expect("Committee has more than one test validator")
    }
}
