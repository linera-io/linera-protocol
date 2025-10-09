// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use linera_base::{crypto::ValidatorPublicKey, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::ChainAndHeight;

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
    already_downloaded: BTreeSet<ChainAndHeight>,
}

impl ValidatorTracker {
    /// Creates a new `ValidatorTracker`.
    fn new(tracker: u64, validator_log: Vec<ChainAndHeight>) -> Self {
        Self {
            current_tracker_value: tracker,
            to_be_downloaded: validator_log.into_iter().collect(),
            already_downloaded: BTreeSet::new(),
        }
    }

    /// Marks a certificate at a particular height in a particular chain as downloaded,
    /// and updates the tracker accordingly.
    fn downloaded_cert(&mut self, chain_and_height: ChainAndHeight) {
        self.already_downloaded.insert(chain_and_height);
        self.maximize_tracker();
    }

    /// Matches the downloaded certificates with the log and increases the tracker value
    /// to the first index that hasn't been downloaded yet.
    fn maximize_tracker(&mut self) {
        while self
            .to_be_downloaded
            .front()
            .is_some_and(|first_cert| self.already_downloaded.contains(first_cert))
        {
            let first_cert = self.to_be_downloaded.pop_front().unwrap();
            self.already_downloaded.remove(&first_cert);
            self.current_tracker_value += 1;
        }
    }
}

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
    pub(super) fn filter_heights_to_download_and_update_trackers(
        &mut self,
        mut remote_heights: BTreeMap<ChainId, BTreeSet<BlockHeight>>,
        local_next_heights: BTreeMap<ChainId, BlockHeight>,
    ) -> BTreeMap<ChainId, BTreeSet<BlockHeight>> {
        for (sender_chain_id, remote_heights) in remote_heights.iter_mut() {
            let local_next = *local_next_heights
                .get(sender_chain_id)
                .unwrap_or(&BlockHeight(0));
            for height in &*remote_heights {
                if *height < local_next {
                    // we consider all of the heights below our local next height
                    // to have been already downloaded, so we will increase the
                    // validators' trackers accordingly
                    self.downloaded_cert(ChainAndHeight {
                        chain_id: *sender_chain_id,
                        height: *height,
                    });
                }
            }
            remote_heights.retain(|h| *h >= local_next);
        }

        remote_heights
    }
}
