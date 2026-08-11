// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What survives a crash, a restart, or a replay.
//!
//! From a chain's persisted state, three events are indistinguishable: a worker crashing and
//! restarting, a client retrying a request, and
//! `ChainWorkerState::reset_and_reexecute_chain` replaying history. Each re-runs a transition
//! from a persisted point. The property below is what makes all three safe, and it is the mirror
//! of [`DurablePersistence`]: that assumption says an effect is never *emitted* before it is
//! persisted, this lemma says an effect already persisted is never *lost*.
//!
//! [`DurablePersistence`]: linera_chain::manager::proof::model::DurablePersistence

use linera_chain::manager::proof::model::{SerializedChainState, StorageAtomicity};

/// **Lemma (Effects are a function of persisted state).** Everything a chain worker emits to
/// other chains is derivable from that chain's saved state, and re-emitting it is harmless. So a
/// worker that crashes between persisting a transition and dispatching its effects loses nothing:
/// on restart it re-derives them, and the recipients absorb the repeats.
///
/// *Proof.* Two halves, one per side of the delivery.
///
/// *The sender re-derives.* `ChainWorkerState::create_network_actions` does not read the
/// transition's result. It calls `reconcile_tracked_outboxes` and then `build_network_actions`,
/// which builds the pending cross-chain requests from the reconciled **outbox index** — part of
/// the chain's view, and therefore part of what `save()` wrote. By [`StorageAtomicity`] that view
/// is consistent after any crash, so the same set of actions is derivable again.
///
/// *The recipient absorbs repeats.* A redelivered bundle reaches `Inbox::add_bundle`, which
/// reconciles it against `added_bundles` and `removed_bundles` by [`Cursor`] and reports whether
/// it was new. A bundle already consumed by a block is in `removed_bundles` and is discarded; one
/// already queued is not queued twice. Delivery is therefore at-least-once with idempotent
/// effect. ∎
///
/// `reset_and_reexecute_chain` relies on exactly this from the other direction: having wiped and
/// replayed a chain, it returns a `CrossChainRequest::RevertConfirm` to every known sender,
/// asking them to re-derive and resend anything the replay dropped from the inbox.
///
/// **What this does not cover.** The four crash windows have different mechanisms, and only two
/// are this lemma:
///
/// | crash window | what covers it |
/// |---|---|
/// | before `save()` | the transition is rolled back and the client retries; needs re-execution to reproduce the outcome |
/// | after `save()`, before dispatch | this lemma, sender half |
/// | during `save()` | [`StorageAtomicity`] |
/// | after dispatch, recipient restarts | this lemma, recipient half |
///
/// [`Cursor`]: linera_base::data_types::Cursor
/// [`StorageAtomicity`]: linera_chain::manager::proof::model::StorageAtomicity
pub trait EffectsSurviveRestart: StorageAtomicity + SerializedChainState {}
