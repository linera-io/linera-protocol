// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use linera_base::{
    data_types::Timestamp,
    identifiers::{ChainId, Owner},
};
use linera_chain::data_types::{Block, Certificate};

/// A helper type to build [`Block`]s using the builder pattern.
pub struct BlockBuilder {
    block: Block,
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
        }
    }
}
