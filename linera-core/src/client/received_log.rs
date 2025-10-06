// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use linera_base::{crypto::ValidatorPublicKey, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::ChainAndHeight;

/// Struct keeping track of the blocks sending messages to a particular chain.
pub(super) struct ReceivedLogs(BTreeMap<ChainId, BTreeSet<BlockHeight>>);

impl ReceivedLogs {
    /// Creates a new instance of `ReceivedLogs`.
    pub(super) fn new() -> Self {
        ReceivedLogs(BTreeMap::new())
    }

    /// Converts a set of logs received from validators into a single log.
    pub(super) fn from_received_result(
        result: Vec<(ValidatorPublicKey, Vec<ChainAndHeight>)>,
    ) -> Self {
        Self::from_iterator(
            result
                .into_iter()
                .flat_map(|(_, received_log)| received_log),
        )
    }

    /// Returns a map that assigns to each chain ID the set of heights. The returned map contains
    /// no empty values.
    pub(super) fn heights_per_chain(&self) -> &BTreeMap<ChainId, BTreeSet<BlockHeight>> {
        &self.0
    }

    /// Returns the number of chains that sent messages according to this log.
    pub(super) fn num_chains(&self) -> usize {
        self.0.len()
    }

    /// Returns the total number of certificates recorded in this log.
    pub(super) fn num_certs(&self) -> usize {
        self.0.values().map(|heights| heights.len()).sum()
    }

    /// Splits this `ReceivedLogs` into batches of size `batch_size`. Batches are sorted
    /// by chain ID and height.
    pub(super) fn into_batches(self, batch_size: usize) -> impl Iterator<Item = ReceivedLogs> {
        LazyBatch {
            iterator: self.0.into_iter().flat_map(|(chain_id, heights)| {
                heights
                    .into_iter()
                    .map(move |height| ChainAndHeight { chain_id, height })
            }),
            batch_size,
        }
    }

    fn from_iterator<I: IntoIterator<Item = ChainAndHeight>>(iterator: I) -> Self {
        iterator
            .into_iter()
            .fold(Self::new(), |mut acc, chain_and_height| {
                acc.0
                    .entry(chain_and_height.chain_id)
                    .or_default()
                    .insert(chain_and_height.height);
                acc
            })
    }
}

pub(super) struct LazyBatch<I: Iterator<Item = ChainAndHeight>> {
    iterator: I,
    batch_size: usize,
}

impl<I: Iterator<Item = ChainAndHeight>> Iterator for LazyBatch<I> {
    type Item = ReceivedLogs;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|next_item| {
            ReceivedLogs::from_iterator(
                std::iter::once(next_item)
                    .chain((&mut self.iterator).take(self.batch_size.saturating_sub(1))),
            )
        })
    }
}
