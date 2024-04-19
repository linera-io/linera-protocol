// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

/// Configuration parameters for the [`ChainWorkerState`][`super::state::ChainWorkerState`].
#[derive(Clone, Default)]
pub struct ChainWorkerConfig {
    /// Whether inactive chains are allowed in storage.
    pub allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    pub allow_messages_from_deprecated_epochs: bool,
}
