// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A barrier that permanently stops a validator from signing votes in old epochs.

use std::sync::Arc;

use linera_base::data_types::Epoch;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use crate::worker::WorkerError;

/// Tracks the highest epoch whose signatures are frozen on this validator, and
/// acts as a barrier between vote creation and freezing an epoch: every signing
/// path holds a [`SigningGuard`] across its whole check-sign-record-save
/// sequence, and [`SignatureFreezer::freeze`] returns only once all guards taken
/// out before the freeze have been dropped. So after `freeze` returns, no vote in
/// the frozen epochs can be created anymore, and every vote created before the
/// call is fully persisted.
///
/// All chain workers of a validator process share one instance, via
/// [`ChainWorkerConfig`](super::ChainWorkerConfig).
#[derive(Clone, Debug, Default)]
pub struct SignatureFreezer {
    /// The highest frozen epoch; this epoch and all earlier ones are frozen.
    max_frozen: Arc<RwLock<Option<Epoch>>>,
}

impl SignatureFreezer {
    /// Checks that the given epoch is not frozen, and returns a guard that delays
    /// any freeze until it is dropped.
    pub(crate) async fn guard_signing(&self, epoch: Epoch) -> Result<SigningGuard, WorkerError> {
        let guard = self.max_frozen.clone().read_owned().await;
        match *guard {
            Some(max_frozen) if epoch <= max_frozen => Err(WorkerError::VoteInFrozenEpoch(epoch)),
            _ => Ok(SigningGuard { _guard: guard }),
        }
    }

    /// Freezes the given epoch and all earlier ones, waiting for signing in
    /// progress to complete.
    pub(crate) async fn freeze(&self, epoch: Epoch) {
        let mut max_frozen = self.max_frozen.write().await;
        if max_frozen.is_none_or(|max| max < epoch) {
            *max_frozen = Some(epoch);
        }
    }
}

/// Keeps signing open while alive: a freeze started while a `SigningGuard`
/// exists completes only after the guard is dropped.
pub(crate) struct SigningGuard {
    _guard: OwnedRwLockReadGuard<Option<Epoch>>,
}

#[cfg(test)]
mod tests {
    use futures::future::poll_immediate;

    use super::*;

    #[tokio::test]
    async fn test_signature_freezer() {
        let freezer = SignatureFreezer::default();
        let epoch = Epoch::from(2);
        let guard = freezer.guard_signing(epoch).await.unwrap();

        // While a signing guard is alive, the freeze waits for it.
        let mut freeze = Box::pin(freezer.freeze(epoch));
        assert!(poll_immediate(&mut freeze).await.is_none());
        drop(guard);
        freeze.await;

        // The frozen epoch and earlier ones are refused; later ones are open.
        for frozen_epoch in [Epoch::ZERO, Epoch::from(1), epoch] {
            assert!(matches!(
                freezer.guard_signing(frozen_epoch).await,
                Err(WorkerError::VoteInFrozenEpoch(_))
            ));
        }
        freezer.guard_signing(Epoch::from(3)).await.unwrap();

        // Freezing an earlier epoch does not lower the frozen boundary.
        freezer.freeze(Epoch::from(1)).await;
        assert!(freezer.guard_signing(epoch).await.is_err());
        freezer.guard_signing(Epoch::from(3)).await.unwrap();
    }
}
