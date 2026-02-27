// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::sync::Arc;

use linera_base::{crypto::ValidatorSecretKey, time::Duration};

use crate::CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES;

/// Configuration parameters for the chain worker and its owning
/// [`WorkerState`][`crate::worker::WorkerState`].
#[derive(Clone)]
pub struct ChainWorkerConfig {
    /// A name used for logging.
    pub nickname: String,
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
    /// Maximum number of entries in the block cache.
    pub block_cache_size: usize,
    /// Maximum number of entries in the execution state cache.
    pub execution_state_cache_size: usize,
}

impl ChainWorkerConfig {
    /// Configures the `key_pair` in this [`ChainWorkerConfig`].
    pub fn with_key_pair(mut self, key_pair: Option<ValidatorSecretKey>) -> Self {
        match key_pair {
            Some(validator_secret) => {
                self.key_pair = Some(Arc::new(validator_secret));
            }
            None => {
                self.key_pair = None;
            }
        }
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
            nickname: String::new(),
            key_pair: None,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            long_lived_services: false,
            block_time_grace_period: Default::default(),
            ttl: Default::default(),
            sender_chain_ttl: Duration::from_secs(1),
            chain_info_max_received_log_entries: CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
            block_cache_size: 5000,
            execution_state_cache_size: 10_000,
        }
    }
}
