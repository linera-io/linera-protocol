// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration parameters for the chain worker.

use std::sync::Arc;

use linera_base::{
    crypto::{AccountSecretKey, ValidatorSecretKey},
    time::Duration,
};

/// Configuration parameters for the [`ChainWorkerState`][`super::state::ChainWorkerState`].
#[derive(Clone, Default)]
pub struct ChainWorkerConfig {
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    pub key_pair: Option<Arc<ValidatorSecretKey>>,
    /// The account secret key of the validator. Can be used to sign blocks as chain owner.
    pub account_key: Option<Arc<AccountSecretKey>>,
    /// Whether inactive chains are allowed in storage.
    pub allow_inactive_chains: bool,
    /// Whether new messages from deprecated epochs are allowed.
    pub allow_messages_from_deprecated_epochs: bool,
    /// Whether the user application services should be long-lived.
    pub long_lived_services: bool,
    /// Blocks with a timestamp this far in the future will still be accepted, but the validator
    /// will wait until that timestamp before voting.
    pub grace_period: Duration,
}

impl ChainWorkerConfig {
    /// Configures the `key_pair` in this [`ChainWorkerConfig`].
    pub fn with_key_pair(
        mut self,
        key_pair: Option<(ValidatorSecretKey, AccountSecretKey)>,
    ) -> Self {
        match key_pair {
            Some((validator_secret, account_secret)) => {
                self.key_pair = Some(Arc::new(validator_secret));
                self.account_key = Some(Arc::new(account_secret));
            }
            None => {
                self.key_pair = None;
                self.account_key = None;
            }
        }
        self
    }

    /// Gets a reference to the [`ValidatorSecretKey`], if available.
    pub fn key_pair(&self) -> Option<&ValidatorSecretKey> {
        self.key_pair.as_ref().map(Arc::as_ref)
    }

    /// Gets a reference to the [`AccountSecretKey`], if available.
    pub fn account_key(&self) -> Option<&AccountSecretKey> {
        self.account_key.as_ref().map(Arc::as_ref)
    }
}
