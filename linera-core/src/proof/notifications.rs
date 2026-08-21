// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a notification tells a client, and what it does not.
//!
//! A [`Notification`] tells a client that a chain has changed. From a correct validator it is sound
//! — what it reports really happened — and from any validator it is best effort, since it may never
//! arrive and carries nothing that could be checked if it did.
//!
//! That combination would be alarming if clients merely used notifications to go faster. They do
//! not: a `ChainListener` acts on them, processing inboxes and following new chains, so an
//! application's own liveness can rest on one arriving. What makes best-effort delivery tolerable
//! is not that nobody depends on it, but that the dependence is *self-repairing* — handlers bring a
//! chain up to date rather than applying the change they were told about, so any later notification
//! does the work of every lost one. The exception is the last.
//!
//! [`Notification`]: crate::worker::Notification

use linera_chain::manager::proof::model::{CorrectValidator, StorageAtomicity};

use super::availability::{BlockOutputsArePersisted, InboxHoldsOnlySentBundles};

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
/// signature, no certificate, not even a field naming the sender. Validators are not asked to sign
/// them.
///
/// Knowing *who* sent one is a deployment matter rather than a protocol one. Under
/// `NetworkProtocol::Grpc(TlsConfig::Tls)` the transport authenticates which validator the stream
/// came from, so a third party cannot inject notifications into it; `TlsConfig::ClearText` is an
/// equally valid configuration, so the specification cannot assume even that. And where it does
/// hold it authenticates the *origin*, never the content, and only to the client holding the
/// connection — a client cannot show anyone else what a validator told it. A notification is
/// therefore never evidence in the sense the [accountability results] use: none is convictable, and
/// none can be forwarded as proof of anything.
///
/// So the qualifier above is not a technicality a client can discharge by inspection. A fabricated
/// notification is indistinguishable *by content* from a sound one, and the only way to learn
/// whether the change happened is to ask. What a client may take from one is that it is worth
/// querying now, and never what the answer will be. That the named block is *the* block at that
/// height, rather than one validator's idea of it, is [`CommitAgreement`]'s and not this lemma's.
///
/// [accountability results]: linera_chain::justification::proof
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
/// Delivery is therefore neither retried nor acknowledged, and no notification is durable. What
/// bounds the damage is [`LostNotificationsCoalesce`], not any property of delivery itself.
pub trait NotificationsAreBestEffort {}

/// **Lemma (A lost notification is repaired by the next one).** If a client misses a notification
/// for a chain but receives any later one for that same chain, it ends in the state it would have
/// reached had none been lost. Only a loss with no successor has lasting effect.
///
/// *Proof.* No handler applies the change it was told about; each brings the chain up to date.
/// In `linera_client::chain_listener`, `Reason::NewIncomingBundle` and `Reason::NewEvents` both
/// reduce to `maybe_notify_inbox_processing`, which pokes a per-chain [`Notify`]; the waiting loop
/// then runs `ChainClient::process_inbox_without_prepare`, which drains *everything* pending rather
/// than the one bundle named. `Reason::NewBlock` calls `update_wallet` for the chain and re-derives
/// its event subscriptions. `Reason::NewRound` calls `update_validators`.
///
/// Each handler is therefore idempotent and its effect depends only on the chain's current state,
/// not on which notification triggered it. Running it once after `k` notifications achieves what
/// running it `k` times would. Two mechanisms make the coalescing safe rather than lossy:
/// `Notify::notify_one` stores a permit when no task is waiting, so a notification arriving *during*
/// processing is not dropped but starts another pass; and the pass itself reads the inbox, so work
/// that arrived while it ran is picked up regardless. ∎
///
/// **The last notification is the one that matters.** `ChainListener::next_action` selects over the
/// notification streams, the cancellation token and its command channel — and nothing else. There
/// is no timer and no periodic poll. So if the final notification for a chain is lost, no later one
/// repairs it and the listener waits indefinitely, with pending bundles unprocessed and subscribed
/// events unread. The window this lemma closes is bounded by the *next* notification; when there is
/// no next one, it does not close.
///
/// This is a real exposure for applications rather than a theoretical one, and it is the reason
/// [`NotificationsAreBestEffort`] must not be read as "notifications do not matter". They do; what
/// is true is narrower — losing one in the middle of a stream is free.
///
/// [`Notify`]: https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html
pub trait LostNotificationsCoalesce: NotificationsAreBestEffort {}

/// **Lemma (Chain progress does not depend on notifications).** A chain's block height grows without
/// bound under the liveness assumptions whether or not any notification is delivered.
///
/// This is the boundary worth drawing, because it is *not* true of everything a client does. What
/// is independent of notifications is the protocol: a validator never waits for one — it has none to
/// wait for, since notifications travel only outward to clients — and the progress argument reaches
/// its conclusions from [`ActiveCorrectDriver`], a client that polls and retries, together with the
/// `linera_core::updater` loops, which learn what a validator is missing from the *error it returns*
/// rather than from any notification ([`MissingDependenciesAreRecoverable`]).
///
/// *Proof.* By inspection of the closure of `UnboundedProgress`: no statement in it names a
/// notification, and none of the transitions it depends on reads one. `Notifier::notify_chain` has
/// no caller inside the consensus path; its only effect is to write to client subscriptions. ∎
///
/// **What does depend on them is application liveness.** A `ChainListener` processes an inbox
/// because it was told to, so a chain whose owner is a `ChainListener` advances only when
/// notifications arrive — the client is the driver [`ActiveCorrectDriver`] assumes, and
/// notifications are what wakes it. So the split is: consensus cannot be stalled by losing
/// notifications, an application can. [`LostNotificationsCoalesce`] bounds that to the case where no
/// further notification arrives.
///
/// [`ActiveCorrectDriver`]: super::assumptions::ActiveCorrectDriver
/// [`MissingDependenciesAreRecoverable`]: super::availability::MissingDependenciesAreRecoverable
/// [`UnboundedProgress`]: super::liveness::UnboundedProgress
pub trait ChainProgressIsIndependentOfNotifications: LostNotificationsCoalesce {}

/// **Lemma (A notification is backed by a certificate the validator can serve — except for a new
/// round).** For every notification a correct validator emits other than `Reason::NewRound`, that
/// validator holds a quorum-signed certificate establishing what the notification reports, and
/// will hand it to anyone who asks. A recipient can therefore not merely learn that something
/// happened but verify it, and carry the evidence to other validators.
///
/// *Proof.* Site by site, for the reasons a validator emits.
///
/// * `Reason::NewBlock` and `Reason::NewEvents` are reachable only through
///   `ChainWorkerState::process_confirmed_block`, which verifies the [`ConfirmedBlockCertificate`]
///   with `certificate.check` and writes it with `write_blobs_and_certificate` *before* dispatching
///   to the path that emits them ([`BlockOutputsArePersisted`]). The certificate is in storage
///   before the notification exists.
/// * `Reason::NewIncomingBundle` is emitted once `process_cross_chain_update` reports
///   `CrossChainUpdateResult::Updated`. By [`InboxHoldsOnlySentBundles`] that bundle reached the
///   inbox from another worker of the *same validator*, built from its persisted outbox for a block
///   that validator had processed — so the sending block's certificate is in this validator's
///   storage too. Note it certifies a block of the *sending* chain, not of the chain the
///   notification names.
///
/// Serving them is the ordinary node surface: `download_certificate`, `download_certificates` and
/// `download_certificates_by_heights`. A node that receives one of these notifications can fetch
/// the certificate, verify it against the committee for its epoch, and push it onward —
/// `send_confirmed_certificate`
/// is exactly that path. ∎
///
/// This is the precise sense in which a notification is worth acting on despite carrying no
/// evidence itself. The message is unsigned and, at best, authenticated only to its recipient by
/// the transport; what it points at is quorum-signed and transferable to anyone. The hint is
/// non-transferable, the thing it hints at is not.
///
/// **`Reason::NewRound` has no such backing, and cannot.** `ChainManager::update_current_round`
/// takes a maximum over four inputs, and only two are certificates: a [`TimeoutCertificate`], or a
/// locking block, which is a [`ValidatedBlockCertificate`]. The other two are `proposed` and
/// `signed_proposal` — one owner's signature, not a quorum's. A round raised by a proposal in a
/// higher multi-leader round therefore has nothing portable behind it, which is
/// [`MultiLeaderRoundsAreLocal`] seen from the notification side: a recipient that wants to reach
/// that round must be sent the proposal itself, because no compact proof of it exists to send.
///
/// **`Reason::BlockExecuted` is out of scope here.** It is emitted by `linera_core::client`, not by
/// a validator, so it is a client telling itself something rather than a claim one node makes to
/// another.
///
/// [`ConfirmedBlockCertificate`]: linera_chain::types::ConfirmedBlockCertificate
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`TimeoutCertificate`]: linera_chain::types::TimeoutCertificate
/// [`MultiLeaderRoundsAreLocal`]: linera_chain::manager::proof::timeouts::MultiLeaderRoundsAreLocal
pub trait NotificationIsCertificateBacked:
    NotificationImpliesPersistedChange + BlockOutputsArePersisted + InboxHoldsOnlySentBundles
{
}
