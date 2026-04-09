// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::{collections::HashSet, sync::Arc};

use linera_base::{crypto::ValidatorSecretKey, identifiers::ChainId, time::Duration};

use super::DynamicTtl;
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
    /// Idle chain workers free their memory after this duration without requests.
    /// `None` means no expiry (handle lives forever). The TTL can be dynamically
    /// reduced at runtime (e.g. by a memory monitor) via [`DynamicTtl::set`].
    pub ttl: Option<Arc<DynamicTtl>>,
    /// TTL for sender chains. `None` means no expiry.
    pub sender_chain_ttl: Option<Arc<DynamicTtl>>,
    /// The size to truncate receive log entries in chain info responses.
    pub chain_info_max_received_log_entries: usize,
    /// Maximum number of entries in the block cache.
    pub block_cache_size: usize,
    /// Maximum number of entries in the execution state cache.
    pub execution_state_cache_size: usize,
    /// Chain IDs whose incoming bundles should be processed first.
    pub priority_bundle_origins: HashSet<ChainId>,
    /// Maximum estimated serialized size of bundles in a single `UpdateRecipient`
    /// cross-chain message. When exceeded, the bundles are split into multiple requests.
    /// Defaults to `usize::MAX` (no chunking).
    pub cross_chain_message_chunk_limit: usize,
    /// Whether to attempt recovery via `RevertConfirm` when an inbox gap is detected.
    pub allow_revert_confirm: bool,
    /// If set, reset the chain state and re-execute all blocks when an
    /// `IncorrectOutcome` error is encountered — but only if the given duration has
    /// elapsed since block 0 was last executed (to prevent reset loops).
    pub reset_on_incorrect_outcome: Option<Duration>,
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
            nickname: String::new(),
            key_pair: None,
            allow_inactive_chains: false,
            allow_messages_from_deprecated_epochs: false,
            long_lived_services: false,
            block_time_grace_period: Default::default(),
            ttl: None,
            sender_chain_ttl: Some(Arc::new(DynamicTtl::new(Duration::from_secs(1)))),
            chain_info_max_received_log_entries: CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
            block_cache_size: 5000,
            execution_state_cache_size: 10_000,
            priority_bundle_origins: HashSet::new(),
            cross_chain_message_chunk_limit: usize::MAX,
            allow_revert_confirm: false,
            reset_on_incorrect_outcome: None,
        }
    }
}
