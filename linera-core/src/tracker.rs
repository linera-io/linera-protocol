// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::{Notification, Reason};
use linera_base::{data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::Origin;
use std::collections::HashMap;

/// A structure which tracks the latest block heights seen for a given ChainId.
#[derive(Default)]
pub struct NotificationTracker {
    new_block: HashMap<ChainId, BlockHeight>,
    new_message: HashMap<(ChainId, Origin), BlockHeight>,
}

impl NotificationTracker {
    /// Returns whether the `Notification` has a higher `BlockHeight` than any previously
    /// seen `Notification`.
    pub fn is_new(&mut self, notification: &Notification) -> bool {
        match &notification.reason {
            Reason::NewBlock { height, .. } => self
                .new_block
                .get(&notification.chain_id)
                .map_or(true, |prev_height| height > prev_height),
            Reason::NewIncomingMessage { height, origin } => self
                .new_message
                .get(&(notification.chain_id, origin.clone()))
                .map_or(true, |prev_height| height > prev_height),
        }
    }

    /// Adds a `Notification` to the `Tracker`.
    ///
    /// If the `Notification` has a higher `BlockHeight` than any previously seen `Notification`
    /// we return true, otherwise we return false.
    pub fn insert(&mut self, notification: Notification) -> bool {
        match notification.reason {
            Reason::NewBlock { height, .. } => self.insert_new_block(notification.chain_id, height),
            Reason::NewIncomingMessage { height, origin } => {
                self.insert_new_message(notification.chain_id, origin, height)
            }
        }
    }

    fn insert_new_block(&mut self, chain_id: ChainId, height: BlockHeight) -> bool {
        match self.new_block.get(&chain_id) {
            None => {
                self.new_block.insert(chain_id, height);
                true
            }
            Some(prev_height) => {
                if height > *prev_height {
                    self.new_block.insert(chain_id, height);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn insert_new_message(
        &mut self,
        chain_id: ChainId,
        origin: Origin,
        height: BlockHeight,
    ) -> bool {
        let key = (chain_id, origin);
        match self.new_message.get(&key) {
            None => {
                self.new_message.insert(key, height);
                true
            }
            Some(prev_height) => {
                if height > *prev_height {
                    self.new_message.insert(key, height);
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use linera_base::crypto::{BcsHashable, CryptoHash};

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Foo;

    impl BcsHashable for Foo {}

    #[test]
    fn test_empty() {
        let notification = Notification {
            chain_id: ChainId::root(0),
            reason: Reason::NewBlock {
                height: BlockHeight(0),
                hash: CryptoHash::new(&Foo),
            },
        };

        let mut tracker = NotificationTracker::default();

        assert!(tracker.insert(notification.clone()));
        assert!(!tracker.insert(notification))
    }

    #[test]
    fn test_new_blocks() {
        let reason_0 = Reason::NewBlock {
            height: BlockHeight(0),
            hash: CryptoHash::new(&Foo),
        };
        let reason_1 = Reason::NewBlock {
            height: BlockHeight(1),
            hash: CryptoHash::new(&Foo),
        };
        let chain_0 = ChainId::root(0);
        let chain_1 = ChainId::root(1);

        let notification_0_0 = Notification {
            chain_id: chain_0,
            reason: reason_0.clone(),
        };

        let notification_1_0 = Notification {
            chain_id: chain_1,
            reason: reason_0,
        };

        let notification_0_1 = Notification {
            chain_id: chain_0,
            reason: reason_1.clone(),
        };

        let notification_1_1 = Notification {
            chain_id: chain_1,
            reason: reason_1,
        };

        let mut tracker = NotificationTracker::default();

        assert!(tracker.insert(notification_0_0.clone()));
        assert!(tracker.insert(notification_0_1.clone()));
        assert!(!tracker.insert(notification_0_0));
        assert!(!tracker.insert(notification_0_1));
        assert!(tracker.insert(notification_1_0));
        assert!(tracker.insert(notification_1_1));
    }

    #[test]
    fn test_application_origin() {
        let reason_0 = Reason::NewIncomingMessage {
            origin: Origin::chain(ChainId::root(0)),
            height: BlockHeight::from(0),
        };
        let reason_1 = Reason::NewIncomingMessage {
            origin: Origin::chain(ChainId::root(0)),
            height: BlockHeight::from(1),
        };

        let chain_0 = ChainId::root(0);
        let chain_1 = ChainId::root(1);

        let notification_0_0 = Notification {
            chain_id: chain_0,
            reason: reason_0.clone(),
        };

        let notification_1_0 = Notification {
            chain_id: chain_1,
            reason: reason_0,
        };

        let notification_0_1 = Notification {
            chain_id: chain_0,
            reason: reason_1.clone(),
        };

        let notification_1_1 = Notification {
            chain_id: chain_1,
            reason: reason_1,
        };

        let mut tracker = NotificationTracker::default();

        assert!(tracker.insert(notification_0_0.clone()));
        assert!(tracker.insert(notification_0_1.clone()));
        assert!(!tracker.insert(notification_0_0));
        assert!(!tracker.insert(notification_0_1));
        assert!(tracker.insert(notification_1_0));
        assert!(tracker.insert(notification_1_1));
    }
}
