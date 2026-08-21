// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a notification tells a client, and what it does not.
//!
//! A [`Notification`] tells a client that a chain has changed. From a correct validator it is sound
//! — what it reports really happened — but from any validator it may simply never arrive, and it
//! carries nothing that could be checked if it did.
//!
//! The channel is **lossy by model**, not merely unreliable in practice, so nothing here may assume
//! a notification arrives. That would be alarming if clients used notifications merely to go
//! faster. They do not: a `ChainListener` acts on them, processing inboxes and following new
//! chains, so an application's own liveness can rest on one arriving.
//!
//! What makes a lossy channel tolerable is that the dependence is *self-repairing*, in three ways
//! that are independent of each other: a client subscribes to every validator, so one silence is
//! covered by the rest; establishing a stream resynchronizes chain state, so a gap is closed rather
//! than replayed; and handlers bring a chain up to date rather than applying the change they were
//! told about, so any later notification does the work of every lost one. All three need the client
//! to be connected to somebody.
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

/// **Definition (The notification channel is lossy).** The channel carrying [`Notification`]s from
/// a validator to a client may drop any message, without notice to either side. Delivery is never
/// retried, never acknowledged, and never durable.
///
/// *Where this sits in the fault model.* Network loss needs no separate treatment — a notification
/// is a message between participants, so [`EventualSynchrony`] already permits it to be dropped
/// before GST and forbids that after. What this definition adds are three *local* ways to lose one,
/// which the network model does not describe: `Notifier::notify_chain` returns immediately when the
/// chain has no entry in its sender map; a failed `sender.send` is ignored and the dead sender
/// reaped; and [`NotificationImpliesPersistedChange`] orders the save before the dispatch, leaving a
/// window in which a change is persisted and the process dies before anything is sent.
///
/// All three behave like a crash rather than like corruption, and are modelled the same way
/// [`CorrectValidator`] models one: permitted freely before GST, and after GST not persisting long
/// enough to prevent progress. So no result may assume a *particular* notification arrives, and a
/// result may assume that a client which stays connected to a reachable correct validator
/// eventually learns.
///
/// This is why the alternative statement — "no result depends on notification delivery" — is not
/// worth making. In a model where the channel is lossy it cannot fail to hold; it is a property of
/// the specification rather than of the system.
///
/// What repairs an individual loss is [`LostNotificationsAreRepaired`].
///
/// [`Notification`]: crate::worker::Notification
/// [`EventualSynchrony`]: super::assumptions::EventualSynchrony
pub trait NotificationChannelIsLossy {}

/// **Lemma (A lost notification is repaired).** A client that misses a notification still reaches
/// the state it would have reached, provided it remains connected to at least one correct validator
/// that has the change. Three independent mechanisms do this, and none of them replays the lost
/// message.
///
/// *Redundancy.* `ChainClient::listen` opens one subscription per validator in the committee, via
/// `update_notification_streams`, and processes them concurrently. Every validator that processes a
/// change notifies about it, so a client loses a notification only when *every* validator it is
/// subscribed to fails to deliver that one — not when any single one does.
///
/// *Resynchronization on (re)subscribe.* Establishing a stream is not just an attachment point: the
/// same future calls `Client::synchronize_chain_state_from` against that validator before yielding
/// the stream, precisely because, in the code's own words, "we may have missed notifications since
/// the last time we synchronized". Since `update_notification_streams` is re-run on every
/// `Reason::NewBlock`, and a dropped connection is re-established through the same path, a gap in
/// the stream is closed by *state synchronization* rather than by recovering the messages that fell
/// in it.
///
/// *Coalescing.* No handler applies the change it was told about; each brings the chain up to date.
/// In `linera_client::chain_listener`, `Reason::NewIncomingBundle` and `Reason::NewEvents` both
/// reduce to `maybe_notify_inbox_processing`, whose waiting loop runs
/// `ChainClient::process_inbox_without_prepare` — draining *everything* pending, not the one bundle
/// named. `Reason::NewBlock` calls `update_wallet` and re-derives event subscriptions. Handlers are
/// therefore idempotent and depend only on current state, so one run after `k` notifications
/// achieves what `k` runs would. `Notify::notify_one` stores a permit when no task is waiting, so a
/// notification arriving *during* a pass starts another rather than being swallowed. ∎
///
/// **What is not repaired.** The mechanisms are conditional on the client having a live subscription
/// to a validator that holds the change. A client subscribed to nobody, or whose validators are all
/// unreachable, learns nothing and is not told that it is learning nothing — there is no timer in
/// `ChainListener::next_action`, which selects over the notification streams, the cancellation token
/// and its command channel and nothing else. By [`NotificationChannelIsLossy`] that condition is a
/// pre-GST one, so the model does not admit it persisting; a deployment can still sit in it
/// indefinitely, with pending bundles unprocessed and subscribed events unread, and nothing in the
/// implementation escalates.
pub trait LostNotificationsAreRepaired: NotificationChannelIsLossy {}

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
