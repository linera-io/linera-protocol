// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use linera_base::{crypto::ValidatorPublicKey, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::ChainAndHeight;

/// Struct keeping track of the blocks sending messages to some chain, from chains identified by
/// the keys in the map.
pub(super) struct ReceivedLogs(
    BTreeMap<ChainId, BTreeMap<BlockHeight, BTreeSet<ValidatorPublicKey>>>,
);

impl ReceivedLogs {
    /// Converts a set of logs received from validators into a single log.
    pub(super) fn from_received_result(
        result: Vec<(ValidatorPublicKey, Vec<ChainAndHeight>)>,
    ) -> Self {
        Self::from_iterator(result.into_iter().flat_map(|(validator, received_log)| {
            received_log
                .into_iter()
                .map(move |chain_and_height| (chain_and_height, validator))
        }))
    }

    /// Returns a map that assigns to each chain ID the set of heights. The returned map contains
    /// no empty values.
    pub(super) fn heights_per_chain(&self) -> BTreeMap<ChainId, BTreeSet<BlockHeight>> {
        self.0
            .iter()
            .map(|(chain_id, heights)| (*chain_id, heights.keys().cloned().collect()))
            .collect()
    }

    /// Returns whether a given validator should have a block at the given chain and
    /// height, according to this log.
    pub(super) fn validator_has_block(
        &self,
        validator: &ValidatorPublicKey,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> bool {
        self.0
            .get(&chain_id)
            .and_then(|heights| heights.get(&height))
            .is_some_and(|validators| validators.contains(validator))
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
        BatchingHelper::new(self.0, batch_size)
    }

    fn from_iterator<I: IntoIterator<Item = (ChainAndHeight, ValidatorPublicKey)>>(
        iterator: I,
    ) -> Self {
        iterator.into_iter().fold(
            Self(BTreeMap::new()),
            |mut acc, (chain_and_height, validator)| {
                acc.0
                    .entry(chain_and_height.chain_id)
                    .or_default()
                    .entry(chain_and_height.height)
                    .or_default()
                    .insert(validator);
                acc
            },
        )
    }
}

/// Iterator adapter lazily yielding batches of size `self.batch_size` from
/// `self.iterator`.
struct BatchingHelper {
    keys: Vec<ChainId>,
    heights: BTreeMap<ChainId, BTreeMap<BlockHeight, BTreeSet<ValidatorPublicKey>>>,
    batch_size: usize,
    current_chain: usize,
}

impl BatchingHelper {
    fn new(
        heights: BTreeMap<ChainId, BTreeMap<BlockHeight, BTreeSet<ValidatorPublicKey>>>,
        batch_size: usize,
    ) -> Self {
        let keys = heights.keys().copied().collect();
        Self {
            keys,
            heights,
            batch_size,
            current_chain: 0,
        }
    }

    fn next_chain_and_height(&mut self) -> Option<(ChainAndHeight, BTreeSet<ValidatorPublicKey>)> {
        if self.keys.is_empty() {
            return None;
        }

        let (chain_id, maybe_heights) = loop {
            let chain_id = self.keys[self.current_chain];
            if self.heights[&chain_id].is_empty() {
                self.heights.remove(&chain_id);
                self.keys.remove(self.current_chain);
                if self.current_chain >= self.keys.len() {
                    self.current_chain = 0;
                }
                if self.keys.is_empty() {
                    return None;
                }
            } else {
                break (chain_id, self.heights.get_mut(&chain_id));
            }
        };

        if self.current_chain < self.keys.len() - 1 {
            self.current_chain += 1;
        } else {
            self.current_chain = 0;
        }

        maybe_heights
            .and_then(|heights| heights.pop_first())
            .map(|(height, validators)| (ChainAndHeight { chain_id, height }, validators))
    }
}

impl Iterator for BatchingHelper {
    type Item = ReceivedLogs;

    fn next(&mut self) -> Option<Self::Item> {
        let batch_size = self.batch_size;
        let result = ReceivedLogs::from_iterator(
            std::iter::from_fn(|| self.next_chain_and_height())
                .take(batch_size)
                .flat_map(|(chain_and_height, validators)| {
                    validators
                        .into_iter()
                        .map(move |validator| (chain_and_height, validator))
                }),
        );
        (result.num_chains() > 0).then_some(result)
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, ValidatorKeypair},
        identifiers::ChainId,
    };
    use linera_chain::data_types::ChainAndHeight;

    use super::ReceivedLogs;

    #[test]
    fn test_received_log_batching() {
        let (chain1, chain2) = {
            // make sure that chain1 is lexicographically earlier than chain2
            let chain_a = ChainId(CryptoHash::test_hash("chain_a"));
            let chain_b = ChainId(CryptoHash::test_hash("chain_b"));
            if chain_a < chain_b {
                (chain_a, chain_b)
            } else {
                (chain_b, chain_a)
            }
        };
        let validator = ValidatorKeypair::generate().public_key;
        let test_log = ReceivedLogs::from_iterator(
            vec![
                (chain1, 1),
                (chain1, 2),
                (chain2, 1),
                (chain1, 3),
                (chain2, 2),
                (chain2, 3),
                (chain1, 4),
                (chain1, 5),
                (chain2, 4),
            ]
            .into_iter()
            .map(|(chain_id, height)| {
                (
                    ChainAndHeight {
                        chain_id,
                        height: height.into(),
                    },
                    validator,
                )
            }),
        );

        let batches = test_log.into_batches(2).collect::<Vec<_>>();

        assert_eq!(batches.len(), 5);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.num_chains())
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 2, 1]
        );

        let chains_heights = batches
            .into_iter()
            .map(|batch| {
                batch
                    .heights_per_chain()
                    .into_iter()
                    .flat_map(|(chain_id, heights)| {
                        heights.into_iter().map(move |height| (chain_id, height.0))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            chains_heights,
            vec![
                vec![(chain1, 1), (chain2, 1)],
                vec![(chain1, 2), (chain2, 2)],
                vec![(chain1, 3), (chain2, 3)],
                vec![(chain1, 4), (chain2, 4)],
                vec![(chain1, 5)]
            ]
        );
    }

    #[test]
    fn test_received_log_batching_unbalanced() {
        let (chain1, chain2) = {
            // make sure that chain1 is lexicographically earlier than chain2
            let chain_a = ChainId(CryptoHash::test_hash("chain_a"));
            let chain_b = ChainId(CryptoHash::test_hash("chain_b"));
            if chain_a < chain_b {
                (chain_a, chain_b)
            } else {
                (chain_b, chain_a)
            }
        };
        let validator = ValidatorKeypair::generate().public_key;
        let test_log = ReceivedLogs::from_iterator(
            vec![
                (chain1, 1),
                (chain1, 2),
                (chain2, 1),
                (chain1, 3),
                (chain2, 2),
                (chain1, 4),
                (chain1, 5),
                (chain1, 6),
                (chain1, 7),
            ]
            .into_iter()
            .map(|(chain_id, height)| {
                (
                    ChainAndHeight {
                        chain_id,
                        height: height.into(),
                    },
                    validator,
                )
            }),
        );

        let batches = test_log.into_batches(2).collect::<Vec<_>>();

        assert_eq!(batches.len(), 5);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.num_chains())
                .collect::<Vec<_>>(),
            vec![2, 2, 1, 1, 1]
        );

        let chains_heights = batches
            .into_iter()
            .map(|batch| {
                batch
                    .heights_per_chain()
                    .into_iter()
                    .flat_map(|(chain_id, heights)| {
                        heights.into_iter().map(move |height| (chain_id, height.0))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            chains_heights,
            vec![
                vec![(chain1, 1), (chain2, 1)],
                vec![(chain1, 2), (chain2, 2)],
                vec![(chain1, 3), (chain1, 4)],
                vec![(chain1, 5), (chain1, 6)],
                vec![(chain1, 7)]
            ]
        );
    }
}
