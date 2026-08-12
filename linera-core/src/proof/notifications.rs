// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a notification tells a client, and what it does not.
//!
//! A [`Notification`] is a hint that shortens the wait between a change and a client noticing it.
//! It is sound — what it reports really happened — and it is best effort — it may never arrive.
//! The results here fix both halves, and the second is the reason for the third: nothing in this
//! specification may depend on a notification.
//!
//! [`Notification`]: crate::worker::Notification

use linera_chain::manager::proof::model::{CorrectValidator, StorageAtomicity};

/// **Lemma (A notification reports a change that is already persisted).** If a client receives a
/// [`Notification`], the state change it names is in the validator's storage.
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
/// **Not a claim about interpretation.** The lemma says the named height, hash, round or stream
/// really was written by the validator that sent it. A client trusting a *single* validator's
/// notification is trusting that validator; agreement is what makes the report meaningful across
/// the committee, and that comes from [`CommitAgreement`], not from here.
///
/// [`Notification`]: crate::worker::Notification
/// [`NetworkActions`]: crate::worker::NetworkActions
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
/// This is what makes [`NotificationsAreBestEffort`] harmless: a lost notification costs latency,
/// never correctness and never progress. It also fixes their role — notifications are an
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
