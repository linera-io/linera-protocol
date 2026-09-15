// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::{collections::HashSet, sync::Arc};

use linera_base::{crypto::ValidatorSecretKey, identifiers::ChainId, time::Duration};

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
    /// `None` means no expiry (handle lives forever).
    pub ttl: Option<Duration>,
    /// TTL for sender chains. `None` means no expiry.
    pub sender_chain_ttl: Option<Duration>,
    /// The size to truncate receive log entries in chain info responses.
    pub chain_info_max_received_log_entries: usize,
    /// Maximum number of entries in the block cache.
    pub block_cache_size: usize,
    /// Maximum number of entries in the execution state cache.
    pub execution_state_cache_size: usize,
    /// Maximum estimated serialized size of bundles in a single `UpdateRecipient`
    /// cross-chain message. When exceeded, the bundles are split into multiple requests.
    /// Defaults to `usize::MAX` (no chunking).
    pub cross_chain_message_chunk_limit: usize,
    /// How often, at most, export progress is folded into the persisted `exported_heights` of an
    /// active chain — the fold rewrites the whole register.
    pub exported_heights_fold_interval: linera_base::time::Duration,
    /// Maximum number of cross-chain requests coalesced into a single batch by the
    /// per-chain driver. Smaller values bound the worst-case write-lock hold time at
    /// the cost of more lock acquisitions; larger values amortize lock and storage
    /// overhead better.
    pub cross_chain_batch_size_limit: usize,
    /// Whether to attempt recovery via `RevertConfirm` when an inbox gap is detected.
    pub allow_revert_confirm: bool,
    /// Whether to accept "super-sparse" sender catch-up: a proposer may push only the sender
    /// blocks its current proposal consumes, omitting message-bearing ancestors the recipient
    /// has *already consumed* by anticipation.
    ///
    /// Two relaxations are enabled together, and only together:
    ///
    /// * the sender's outbox will schedule a preprocessed block for a recipient even when the
    ///   recipient's outbox is not caught up to that block's declared predecessor, deferring the
    ///   decision to the recipient (which is where the consumption frontier lives);
    /// * the recipient accepts such an update when it has already consumed past the declared
    ///   predecessor, and drops the matching stale entries from its anticipation queue.
    ///
    /// Bundles the recipient has *not* consumed are still delivered contiguously, so ordering
    /// guarantees for unskippable bundles are unchanged.
    ///
    /// Requires [`Self::allow_revert_confirm`] to be sound: when the recipient reports a genuine
    /// gap, the resulting `RevertConfirm` is what repairs the sender's outbox high-water mark.
    pub allow_sparse_sender_catchup: bool,
    /// If set, reset the chain state and re-execute all blocks when the chain
    /// state is detected to be corrupted — but only if the given duration has
    /// elapsed since block 0 was last executed (to prevent reset loops).
    pub reset_on_corrupted_chain_state: Option<Duration>,
    /// Optional whitelist restricting which chains are eligible for the
    /// `allow_revert_confirm` and `reset_on_corrupted_chain_state` recovery
    /// mechanisms. If `None`, every chain is eligible (subject to the
    /// respective feature flag). If `Some`, only chains in the set are.
    pub recovery_whitelist: Option<HashSet<ChainId>>,
}

impl ChainWorkerConfig {
    /// Configures the `key_pair` in this [`ChainWorkerConfig`].
    #[cfg(with_testing)]
    pub fn with_key_pair(mut self, key_pair: Option<ValidatorSecretKey>) -> Self {
        self.key_pair = key_pair.map(Arc::new);
        self
    }

    /// Gets a reference to the [`ValidatorSecretKey`], if available.
    pub fn key_pair(&self) -> Option<&ValidatorSecretKey> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    /// Returns whether `chain_id` is allowed to attempt the `RevertConfirm` and
    /// corrupted-state-reset recovery mechanisms.
    pub(crate) fn recovery_allowed_for(&self, chain_id: &ChainId) -> bool {
        self.recovery_whitelist
            .as_ref()
            .is_none_or(|set| set.contains(chain_id))
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
            sender_chain_ttl: None,
            chain_info_max_received_log_entries: CHAIN_INFO_MAX_RECEIVED_LOG_ENTRIES,
            block_cache_size: crate::worker::DEFAULT_BLOCK_CACHE_SIZE,
            execution_state_cache_size: crate::worker::DEFAULT_EXECUTION_STATE_CACHE_SIZE,
            cross_chain_message_chunk_limit: usize::MAX,
            exported_heights_fold_interval: linera_base::time::Duration::from_secs(5),
            cross_chain_batch_size_limit: 1000,
            allow_revert_confirm: false,
            allow_sparse_sender_catchup: false,
            reset_on_corrupted_chain_state: None,
            recovery_whitelist: None,
        }
    }
}
