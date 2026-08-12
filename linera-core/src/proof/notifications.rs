// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a notification tells a client, and what it does not.
//!
//! A [`Notification`] is a hint that shortens the wait between a change and a client noticing it.
//! From a correct validator it is sound — what it reports really happened — and from any validator
//! it is best effort, since it may never arrive and carries nothing that could be checked if it
//! did. The results here fix each half, and together they are the reason for the third: nothing in
//! this specification may depend on a notification.
//!
//! [`Notification`]: crate::worker::Notification

use linera_chain::manager::proof::model::{CorrectValidator, StorageAtomicity};

/// **Lemma (A correct validator's notification reports a change that is already persisted).** If a
/// client receives a [`Notification`] from a correct validator, the state change it names is in
/// that validator's storage. Nothing is claimed about a faulty validator's notifications, which by
/// [`CorrectValidator`] may report anything at all.
///
/// *Proof.* Notifications are not sent where they are built. Each is pushed onto the
/// `notifications` field of a [`NetworkActions`] value which the handler *returns*, and every
/// handler that changes state calls `save()` before returning it — a failed save propagates with
/// `?`, so the actions are dropped and nothing is dispatched. Site by site:
///
/// * `Reason::NewBlock` and `Reason::NewEvents` are pushed in
///   `ChainWorkerState::execute_contiguous_block` *before* its `self.save()?`, so a save that
///   fails discards them along with the actions that carry them.
/// * `Reason::NewEvents` is pushed again in `preprocess_certified_block`, there *after* the save
///   rather than before it, which reaches the same conclusion more directly.
/// * `Reason::NewRound` is built inside `create_network_actions` from
///   `ChainManager::current_round`, and travels out in the same returned value.
/// * `Reason::NewIncomingBundle` is pushed in `WorkerState::handle_cross_chain_request` only once
///   `process_cross_chain_update` has returned `CrossChainUpdateResult::Updated`, and that result
///   crosses a `chain_write` batch, which takes the lock and saves once before replying.
///
/// The caller dispatches through `Notifier::notify_chain` on the returned value alone, so no path
/// reaches a client without a completed save. By [`StorageAtomicity`] that save is all or nothing,
/// so a client is never told about a half-written change. ∎
///
/// **A notification carries no evidence.** [`Notification`] is a `chain_id` and a [`Reason`]: no
/// signature, no certificate, not even a field naming the sender. So the qualifier above is not a
/// technicality a client can discharge by inspection — a fabricated notification is
/// indistinguishable from a sound one, and the only way to learn whether the change really happened
/// is to ask. What a client may take from one is that it is worth querying now, and never what the
/// answer will be. That the named block is *the* block at that height, rather than one validator's
/// idea of it, is [`CommitAgreement`]'s and not this lemma's.
///
/// [`Notification`]: crate::worker::Notification
/// [`NetworkActions`]: crate::worker::NetworkActions
/// [`Reason`]: crate::worker::Reason
/// [`CorrectValidator`]: linera_chain::manager::proof::model::CorrectValidator
/// [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement
pub trait NotificationImpliesPersistedChange: CorrectValidator + StorageAtomicity {}

/// **Caveat (Notifications are best effort).** A change can be persisted and its notification
/// never reach any client. Three ways, none of them an error:
///
/// * *Nobody is listening.* `Notifier::notify_chain` looks the chain up in its sender map and
///   returns immediately when there is no entry.
/// * *The receiver went away.* A failed `sender.send` is ignored and the dead sender reaped; for
///   the delivery notifiers, `notifier.send(())` failing is logged at debug and dropped.
/// * *The process crashed in between.* [`NotificationImpliesPersistedChange`] orders the save
///   before the dispatch, which leaves exactly this window: saved, not yet notified, gone.
///
/// Delivery is therefore neither retried nor acknowledged, and no notification is durable.
pub trait NotificationsAreBestEffort {}

/// **Invariant (No proof depends on a notification arriving).** No statement in this specification
/// has the delivery of a [`Notification`] among its premises.
///
/// *Proof.* By inspection of the whole specification: no statement names a notification in its
/// dependencies, and the progress argument reaches its conclusions without one. What drives a
/// chain forward is [`ActiveCorrectDriver`] — a client that polls and retries — together with the
/// `linera_core::updater` loops, which learn what a validator is missing from the *error it
/// returns*, never from a notification. [`MissingDependenciesAreRecoverable`] is that argument, and
/// nothing in it consults a notification. ∎
///
/// This is what makes both ways a notification can fail harmless. A lost one costs latency, never
/// correctness and never progress. A *fabricated* one — which a faulty validator can send freely,
/// notifications being unauthenticated — costs at most a wasted query, since the client acts on
/// what it then reads and not on what it was told. It also fixes their role: notifications are an
/// optimization over polling, and a client that ignores them entirely is slower but no less
/// correct.
///
/// **Re-established, not preserved.** Like any exhaustive-search argument this one holds only for
/// the statements that exist. A future statement that assumed a client learns of a change promptly
/// would break it, and would have to introduce a delivery assumption to say so.
///
/// [`Notification`]: crate::worker::Notification
/// [`ActiveCorrectDriver`]: super::assumptions::ActiveCorrectDriver
/// [`MissingDependenciesAreRecoverable`]: super::availability::MissingDependenciesAreRecoverable
pub trait NoProofDependsOnNotifications: NotificationsAreBestEffort {}
