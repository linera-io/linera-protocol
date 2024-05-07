// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::sync::Arc;

use linera_base::crypto::KeyPair;

/// Configuration parameters for the [`ChainWorkerState`][`super::state::ChainWorkerState`].
#[derive(Clone, Default)]
pub struct ChainWorkerConfig {
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    pub key_pair: Option<Arc<KeyPair>>,
    /// Whether inactive chains are allowed in storage.
    pub allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    pub allow_messages_from_deprecated_epochs: bool,
}

impl ChainWorkerConfig {
    /// Configures the `key_pair` in this [`ChainWorkerConfig`].
    pub fn with_key_pair(mut self, key_pair: impl Into<Option<KeyPair>>) -> Self {
        self.key_pair = key_pair.into().map(Arc::new);
        self
    }

    /// Gets a reference to the [`KeyPair`], if available.
    pub fn key_pair(&self) -> Option<&KeyPair> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }
}
