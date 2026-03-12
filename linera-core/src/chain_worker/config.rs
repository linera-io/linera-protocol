// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::{collections::HashSet, sync::Arc};

use linera_base::{crypto::ValidatorSecretKey, identifiers::ChainId, time::Duration};

use crate::CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES;

/// Configuration parameters for the [`ChainWorkerState`][`super::state::ChainWorkerState`].
#[derive(Clone)]
pub struct ChainWorkerConfig {
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    pub key_pair: Option<Arc<ValidatorSecretKey>>,
    /// Whether inactive chains are allowed in storage.
    pub allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    pub allow_messages_from_deprecated_epochs: bool,
    /// Whether the user application services should be long-lived.
    pub long_lived_services: bool,
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    pub block_time_grace_period: Duration,
    /// Idle chain workers free their memory after that duration without requests.
    pub ttl: Duration,
    /// TTL for sender chains.
    // We don't want them to keep in memory forever since usually they're short-lived.
    pub sender_chain_ttl: Duration,
    /// The size to truncate receive log entries in chain info responses.
    pub chain_info_max_received_log_entries: usize,
    /// Chain IDs whose incoming bundles should be processed first.
    pub priority_bundle_origins: HashSet<ChainId>,
}

impl ChainWorkerConfig {
    /// Configures the `key_pair` in this [`ChainWorkerConfig`].
    pub fn with_key_pair(mut self, key_pair: Option<ValidatorSecretKey>) -> Self {
        self.key_pair = key_pair.map(Arc::new);
        self
    }

    /// Gets a reference to the [`ValidatorSecretKey`], if available.
    pub fn key_pair(&self) -> Option<&ValidatorSecretKey> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }
}

impl Default for ChainWorkerConfig {
    fn default() -> Self {
        Self {
            key_pair: None,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            long_lived_services: false,
            block_time_grace_period: Default::default(),
            ttl: Default::default(),
            sender_chain_ttl: Default::default(),
            chain_info_max_received_log_entries: CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
            priority_bundle_origins: HashSet::new(),
        }
    }
}
