// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap, VecDeque};

use linera_base::{crypto::ValidatorPublicKey, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::ChainAndHeight;

use super::received_log::ReceivedLogs;

/// Keeps multiple `ValidatorTracker`s for multiple validators.
pub(super) struct ValidatorTrackers(BTreeMap<ValidatorPublicKey, ValidatorTracker>);

impl ValidatorTrackers {
    /// Creates a new `ValidatorTrackers`.
    pub(super) fn new(
        received_logs: Vec<(ValidatorPublicKey, Vec<ChainAndHeight>)>,
        trackers: &HashMap<ValidatorPublicKey, u64>,
    ) -> Self {
        Self(
            received_logs
                .into_iter()
                .map(|(validator, log)| {
                    (
                        validator,
                        ValidatorTracker::new(*trackers.get(&validator).unwrap_or(&0), log),
                    )
                })
                .collect(),
        )
    }

    /// Updates all the trackers with the information that a particular certificate has
    /// been downloaded and processed.
    pub(super) fn downloaded_cert(&mut self, chain_and_height: ChainAndHeight) {
        for tracker in self.0.values_mut() {
            tracker.downloaded_cert(chain_and_height);
        }
    }

    /// Converts the `ValidatorTrackers` into a map of per-validator tracker values
    /// (indices into the validators' received logs).
    pub(super) fn to_map(&self) -> BTreeMap<ValidatorPublicKey, u64> {
        self.0
            .iter()
            .map(|(validator, tracker)| (*validator, tracker.current_tracker_value))
            .collect()
    }

    /// Compares validators' received logs of sender chains with local node information and returns
    /// a per-chain list of block heights that sent us messages we didn't see yet. Updates
    /// the trackers accordingly.
    pub(super) fn filter_out_already_known(
        &mut self,
        received_logs: &mut ReceivedLogs,
        local_next_heights: BTreeMap<ChainId, BlockHeight>,
    ) {
        for (sender_chain_id, local_highest) in &local_next_heights {
            if let Some(remote_heights) = received_logs.get_chain_mut(sender_chain_id) {
                remote_heights.retain(|height, _| {
                    if height < local_highest {
                        // we consider all of the heights below our local next height
                        // to have been already downloaded, so we will increase the
                        // validators' trackers accordingly
                        self.downloaded_cert(ChainAndHeight {
                            chain_id: *sender_chain_id,
                            height: *height,
                        });
                        false
                    } else {
                        true
                    }
                });
            }
        }
    }
}

/// Manages a "tracker"  of a single validator.
/// The received log is the list of chains and heights of blocks sending messages to a
/// particular receiver chain. The tracker is the index of the first entry in that log
/// that corresponds to a block that hasn't been processed yet.
/// In order to keep the tracker value up to date, we keep the part of the log starting
/// with the first entry corresponding to a not-yet-processed block, and a record of which
/// certificates we have already downloaded. Whenever we download a certificate corresponding to
/// the first block in the log, we increase the tracker and pop the blocks off the log,
/// until we hit one we haven't downloaded yet.
struct ValidatorTracker {
    current_tracker_value: u64,
    to_be_downloaded: VecDeque<ChainAndHeight>,
    highest_downloaded: BTreeMap<ChainId, BlockHeight>,
}

impl ValidatorTracker {
    /// Creates a new `ValidatorTracker`.
    fn new(tracker: u64, validator_log: Vec<ChainAndHeight>) -> Self {
        Self {
            current_tracker_value: tracker,
            to_be_downloaded: validator_log.into_iter().collect(),
            highest_downloaded: BTreeMap::new(),
        }
    }

    /// Marks a certificate at a particular height in a particular chain as downloaded,
    /// and updates the tracker accordingly.
    fn downloaded_cert(&mut self, chain_and_height: ChainAndHeight) {
        let current_highest = self
            .highest_downloaded
            .entry(chain_and_height.chain_id)
            .or_insert(BlockHeight(0));
        *current_highest = chain_and_height.height.max(*current_highest);
        self.maximize_tracker();
    }

    /// Increases the tracker value to the first index that hasn't been downloaded yet.
    fn maximize_tracker(&mut self) {
        while self.to_be_downloaded.front().is_some_and(|first_cert| {
            self.highest_downloaded
                .get(&first_cert.chain_id)
                .is_some_and(|max_downloaded_height| *max_downloaded_height >= first_cert.height)
        }) {
            let _first_cert = self.to_be_downloaded.pop_front().unwrap();
            self.current_tracker_value += 1;
        }
    }
}

#[cfg(test)]
mod test {
    use linera_base::{
        crypto::{CryptoHash, ValidatorKeypair},
        data_types::BlockHeight,
        identifiers::ChainId,
    };
    use linera_chain::data_types::ChainAndHeight;

    use super::{super::received_log::ReceivedLogs, ValidatorTracker, ValidatorTrackers};

    #[test]
    fn test_validator_tracker() {
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        let mut tracker = ValidatorTracker::new(
            0,
            vec![(chain1, 0), (chain2, 0), (chain1, 1)]
                .into_iter()
                .map(|(chain_id, height)| ChainAndHeight {
                    chain_id,
                    height: height.into(),
                })
                .collect(),
        );

        tracker.downloaded_cert(ChainAndHeight {
            chain_id: chain1,
            height: 0.into(),
        });
        assert_eq!(tracker.current_tracker_value, 1);

        tracker.downloaded_cert(ChainAndHeight {
            chain_id: chain1,
            height: 1.into(),
        });
        assert_eq!(tracker.current_tracker_value, 1);

        tracker.downloaded_cert(ChainAndHeight {
            chain_id: chain2,
            height: 0.into(),
        });
        assert_eq!(tracker.current_tracker_value, 3);
    }

    #[test]
    fn test_filter_out_already_known() {
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));

        let validator = ValidatorKeypair::generate().public_key;

        let log: Vec<_> = vec![
            (chain1, 0),
            (chain2, 0),
            (chain1, 1),
            (chain1, 2),
            (chain2, 1),
            (chain1, 3),
            (chain2, 2),
        ]
        .into_iter()
        .map(|(chain_id, height)| ChainAndHeight {
            chain_id,
            height: height.into(),
        })
        .collect();

        let mut received_log = ReceivedLogs::from_received_result(vec![(validator, log.clone())]);

        assert_eq!(received_log.num_chains(), 2);
        assert_eq!(received_log.num_certs(), 7);

        let mut tracker = ValidatorTrackers::new(
            vec![(validator, log)],
            &vec![(validator, 0)].into_iter().collect(),
        );

        let local_heights = vec![(chain1, BlockHeight(3)), (chain2, BlockHeight(3))]
            .into_iter()
            .collect();

        tracker.filter_out_already_known(&mut received_log, local_heights);

        assert_eq!(received_log.num_chains(), 2); // we do not remove empty chains
        assert_eq!(received_log.num_certs(), 1);

        // tracker should have shifted to point to (chain1, 3)
        assert_eq!(tracker.0[&validator].current_tracker_value, 5);
    }
}
