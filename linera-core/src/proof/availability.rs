// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a certified block guarantees to everyone else, and what a crash costs.
//!
//! Agreement ([`CommitAgreement`]) says at most one block is certified per height. It says nothing
//! about anyone being *able to act on* that block. The results here supply the other half: once a
//! quorum has certified a block, any node — a validator that was down, a validator that did not
//! exist yet, a client — can obtain it and everything needed to execute it.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement

use linera_chain::manager::proof::{
    commit::{CommittedBlock, IncomingBundlesAreSelfDerived},
    model::{CorrectValidator, SerializedChainState, StorageAtomicity},
};

use super::assumptions::{
    BlobRetention, BoundedRecovery, CorrectValidatorAvailability, EventualSynchrony,
};

/// **Lemma (A block's outputs are persisted before it counts as processed).** When a correct
/// validator's [`ChainTipState::next_block_height`] passes a height, the outputs of the block at
/// that height are already in storage: the blobs it publishes, the events it emits, and the
/// certificate itself. A crash before that point costs nothing but repeated work — the tip is what
/// marks a block processed, so the block is handled again on restart and the writes are redone.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | `ChainWorkerState::process_confirmed_block` |
/// | writes | `write_blobs_and_certificate`, then `write_events`, then `maybe_write_blob_states`, and only then the tip |
/// | re-entry guard | `tip.next_block_height > height`, returning `BlockOutcome::Skipped` |
///
/// *Proof.* Three parts.
///
/// *Ordering.* `process_confirmed_block` issues the three storage writes and only afterwards
/// dispatches to `execute_contiguous_block` (or `execute_block_with_checkpoint_restore`), which is
/// where `tip_state` is set and `save()` runs. The writes are three separate awaited calls, not
/// one batch, so a crash can land between them; what the argument needs is only that all of them
/// precede the tip.
///
/// *The tip is the guard.* On restart the certificate is offered again, and the early return
/// `if !in_trust_set && tip.next_block_height > height` decides whether the block is skipped. That
/// test reads persisted chain state, which by [`StorageAtomicity`] is consistent, so a crash
/// before `save()` leaves the block unprocessed and every write is reissued.
///
/// *The repeats are byte-identical.* The events come from `block.body.events` and the blobs from
/// `get_required_blobs` over the block's `required_blob_ids` and `created_blobs` — all fields of an
/// already-certified block rather than products of execution. Nothing is recomputed, so the
/// argument needs no appeal to [`DeterministicExecution`]. ∎
///
/// **Preprocessing persists outputs with no tip to record them.** In `Preprocess` mode, or `Auto`
/// with an unbridgeable gap, `preprocess_certified_block` updates outboxes and event streams and
/// deliberately does not advance the tip. The outputs of such a block are in storage, but the
/// guard above will not short-circuit a later offer of the same certificate, so the writes are
/// simply redone.
///
/// **What this does not cover.** It places a block's outputs in the *producing* validator's
/// storage. That the resulting bundles reach the recipient chain's inbox is
/// [`EffectsSurviveRestart`]; that the outbox is ever drained is stated nowhere.
///
/// [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
/// [`DeterministicExecution`]: linera_chain::manager::proof::model::DeterministicExecution
pub trait BlockOutputsArePersisted: CommittedBlock + StorageAtomicity + CorrectValidator {}

/// **Lemma (A certified block and its dependencies are retrievable).** Once a block is a
/// [`CommittedBlock`], any node that can reach a quorum can obtain the certificate, the ancestors
/// it needs, and every blob and event the block requires, and can then execute it.
///
/// This is what a *uniform* agreement statement needs beyond agreement itself: not merely that
/// correct validators do not disagree, but that a node which took no part in the decision — one
/// that was crashed throughout, or joined afterwards — reaches the same state.
///
/// *Proof.* The dependencies are of three kinds, each retrievable from any validator holding the
/// block.
///
/// * *The certificate and its ancestors.* `Client::download_certificates` fetches from the
///   validator set up to a target height, and
///   `receive_certificate_with_checked_signatures` re-verifies before applying, so retrieval
///   requires trusting no individual source.
/// * *Blobs.* `Client::update_local_node_with_blobs_from` fetches by [`BlobId`] across validators,
///   hedged. Content addressing supplies *integrity* for free — a wrong blob is detectable by
///   hashing, so one honest source suffices — but says nothing about *availability*, which is the
///   property actually needed here and which rests on [`BlobRetention`].
/// * *Events.* Read across chains as `OracleResponse::Event`, so they are recorded in the block
///   itself; a validator missing the *publishing* chain's state answers `EventsNotFound`, and the
///   updater's response is to push the admin chain (`update_admin_chain`) or the publishing
///   chain's certificates.
///
/// Each is served by every validator that has processed the block, and a quorum has by definition
/// voted for it, so under [`CorrectValidatorAvailability`] and [`EventualSynchrony`] a reachable
/// quorum yields all three. ∎
///
/// **What this does not bound.** *That* the dependencies are retrievable does not say how long
/// retrieval takes; see [`BoundedCatchUp`].
///
/// [`CommittedBlock`]: linera_chain::manager::proof::commit::CommittedBlock
/// [`BlobId`]: linera_base::identifiers::BlobId
/// [`BlobRetention`]: super::assumptions::BlobRetention
pub trait CertifiedBlockIsAvailable:
    BlockOutputsArePersisted + CorrectValidatorAvailability + EventualSynchrony + BlobRetention
{
}

/// **Caveat (Catch-up is not time-bounded).** The work a node must do to reach a chain's tip is
/// proportional to the number of blocks it must replay, which is the height above the chain's
/// latest checkpoint — and nothing in the protocol bounds that distance.
///
/// The mechanism to bound it exists. A block whose sole transaction is
/// `SystemOperation::Checkpoint` publishes the chain's execution state as a blob;
/// `Client::bootstrap_chain_from_checkpoint` installs it and resumes downloading from that height,
/// so the blocks below are never replayed. `ChainWorkerState::reset_and_reexecute_chain` uses the
/// same shortcut, replaying only from `latest_checkpoint_height`.
///
/// **Nothing schedules it.** `ChainClient::checkpoint` is invoked from one place in the workspace,
/// the `linera` CLI. There is no policy, no interval, and no protocol rule requiring a chain to
/// checkpoint — so on a chain that never does, catch-up is linear in the chain's whole history and
/// [`BoundedRecovery`] cannot be discharged for a node that has fallen far behind.
///
/// This is the sharp edge of an otherwise-clean property: [`CertifiedBlockIsAvailable`] says a
/// recovering or joining node *can* reach the tip; making that *quick* is a deployment obligation
/// resting on checkpoint frequency that the protocol does not enforce.
///
/// [`BoundedRecovery`]: super::assumptions::BoundedRecovery
pub trait BoundedCatchUp: CertifiedBlockIsAvailable + BoundedRecovery {}

/// **Lemma (Missing dependencies are recoverable).** A validator needs data in hand for either of
/// two operations: *accepting a block proposal*, which means executing it, and *executing a
/// certified block*. When it lacks that data, what is missing falls into a closed set of classes,
/// each with a route by which it arrives.
///
/// This is what makes the retry loops in `linera_core::updater` converge rather than spin, and it
/// is the substance behind [`ValidationQuorumForms`]'s claim that a step completes in `2Δ`: a
/// straggler is not waited out, it is *supplied*.
///
/// *Proof.* The classes are exactly the errors those loops match, and the recovery route differs
/// by where the data originates:
///
/// | class | blocks | what is missing | route |
/// |---|---|---|---|
/// | `BlobsNotFound` | both | a blob the block publishes or reads | pushed by the requester: `send_pending_blobs` when accepting a proposal, `upload_blobs` from local storage when executing a certified block |
/// | `EventsNotFound` | both | an event the block read | the **publishing** chain's certificates; the admin chain is special-cased for epoch events |
/// | `BlocksNotFound` | execution | ancestor block bytes a checkpoint trust-marked | pushed from the requester's storage |
/// | `MissingCrossChainUpdate` | acceptance | an incoming bundle the block consumes | the **sending** chain's certificates |
/// | `InactiveChain` | acceptance | the chain does not exist at that validator | pushed with the chain's creation |
/// | `WrongRound`, `UnexpectedBlockHeight` — validator behind | acceptance | consensus state for this chain | pushed by `send_chain_information` |
/// | `WrongRound`, `UnexpectedBlockHeight` — *requester* behind | acceptance | nothing; the requester is wrong | pulled: `sync_remote_if_needed` reports [`LocalNodeLagging`] and the client synchronizes |
///
/// All but `MissingCrossChainUpdate` and `EventsNotFound` are *self-suppliable*: the requester
/// holds the data by construction, because it built the block or already verified the certificate.
/// Those classes cannot stall. ∎
///
/// **Those two are not, and that is where general liveness is weakest.** Their data originates
/// on a *third* chain. A client that does not follow the sending or publishing chain cannot push
/// what the validator is missing, and the push simply fails.
///
/// There is a second, independent route for them, which is why this is a weakness rather than a
/// hole: the validator's own worker for the sending chain populates the inbox as it processes that
/// chain ([`IncomingBundlesAreSelfDerived`]), and events likewise arrive as the publishing chain
/// is processed. So the data reaches the validator either because someone pushes it or because the
/// validator catches up on the originating chain — and progress on *this* chain waits on whichever
/// happens first.
///
/// Neither route is bounded by anything the specification currently states.
/// [`ValidationQuorumForms`] assumes the proposal is accepted once every correct validator is in
/// the round; for a block consuming a message from a chain that some validator has not yet
/// processed, that is an additional condition, and no assumption in
/// [`super::assumptions`] supplies it.
///
/// **Why the pushes terminate.** Each class carries a well-founded measure.
/// `send_confirmed_certificate` latches `sent_admin_chain` / `sent_blobs` / `sent_blocks`, so each
/// class is attempted once. `send_block_proposal` drains its `blob_ids` with `mem::take` and
/// records `publisher_chain_ids_sent` per publishing chain. `MissingCrossChainUpdate` is the one
/// that is not a latch: it retries per origin while the reported height strictly increases, which
/// terminates because those heights are bounded by the sender's tip and a block has finitely many
/// origins. A validator reporting the same class with no progress is not retried, so a dependency
/// that nobody can supply surfaces as an error rather than looping.
///
/// [`LocalNodeLagging`]: crate::client::chain_client::Error::LocalNodeLagging
/// [`IncomingBundlesAreSelfDerived`]: linera_chain::manager::proof::commit::IncomingBundlesAreSelfDerived
/// [`ValidationQuorumForms`]: super::progress::ValidationQuorumForms
pub trait MissingDependenciesAreRecoverable:
    CorrectValidatorAvailability + EventualSynchrony
{
}

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
/// *The recipient absorbs repeats.* A redelivered bundle is filtered out before it reaches the
/// inbox: `ChainWorkerState::select_message_bundles` drops every bundle whose height is below the
/// inbox's `next_block_height_to_receive`, logging them as repeated. `Inbox::add_bundle` would not
/// absorb one in any case — it requires the [`Cursor`] to be at least `next_cursor_to_add` and
/// rejects anything lower with `InboxError::IncorrectOrder`. Its two reconciliation branches cover
/// different situations: `removed_bundles` a bundle this chain consumed *by anticipation* before
/// delivery, and `restored_cursor` a bundle whose effects a checkpoint restore has already baked
/// into the state. Delivery is therefore at-least-once with idempotent effect. ∎
///
/// `reset_and_reexecute_chain` relies on exactly this from the other direction: having wiped and
/// replayed a chain, it returns a `CrossChainRequest::RevertConfirm` to every known sender,
/// asking them to re-derive and resend anything the replay dropped from the inbox.
///
/// **The crash windows.** [`CorrectValidator`] admits a crash at any point, and the four windows
/// have different mechanisms — only two are this lemma:
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
/// [`CorrectValidator`]: linera_chain::manager::proof::model::CorrectValidator
pub trait EffectsSurviveRestart:
    StorageAtomicity + SerializedChainState + CorrectValidator
{
}

/// **Lemma (An inbox holds only bundles its origin really sent).** Every [`MessageBundle`] in a
/// correct validator's inbox for an origin was produced by a block of that origin which the *same
/// validator* has processed.
///
/// *Proof.* Bundles enter an inbox at exactly one place, `Inbox::add_bundle`, reached only from
/// `ChainWorkerState::process_cross_chain_update`. That handler serves a
/// `CrossChainRequest::UpdateRecipient`, and cross-chain requests are internal to one validator:
/// `linera_rpc` routes each to the shard owning the target chain, so the request comes from
/// another worker of the same validator, which built it in `build_network_actions` from its own
/// persisted outbox for a block it had processed ([`EffectsSurviveRestart`], sender half). No
/// other validator's word enters, and by [`SerializedChainState`] no other process writes this
/// chain's inboxes. `select_message_bundles` additionally drops bundles whose epoch has been
/// revoked, unless they were already anticipated. ∎
///
/// This is the premise [`IncomingBundlesAreSelfDerived`] leaves open: that lemma proves a voter
/// matches consumed bundles against its own inbox, which is worth exactly as much as the inbox's
/// own provenance.
///
/// [`MessageBundle`]: linera_chain::data_types::MessageBundle
pub trait InboxHoldsOnlySentBundles: CorrectValidator + SerializedChainState {}

/// **Lemma (A bundle is consumed at most once).** No two blocks of a chain consume the same
/// [`MessageBundle`] from the same origin, even though delivery is at-least-once.
///
/// Together with [`EffectsSurviveRestart`]'s at-least-once delivery this is the exactly-once
/// property for *consumption*. It is not exactly-once *delivery*: the same bundle may arrive any
/// number of times, and nothing here says it arrives at all.
///
/// *Proof.* Four filters, one per way a repeat can present itself:
///
/// * *Redelivery.* `select_message_bundles` drops bundles below the inbox's
///   `next_block_height_to_receive`, which has advanced past every height already received.
/// * *Order.* Should one slip through, `Inbox::add_bundle` requires the [`Cursor`] to be at least
///   `next_cursor_to_add` — set to the previous cursor plus one on every successful add — and
///   fails with `InboxError::IncorrectOrder` otherwise.
/// * *Anticipation.* A bundle consumed before it arrived sits in `removed_bundles`; on arrival it
///   is matched by cursor, checked for equality and deleted rather than queued, so it is never
///   offered for consumption a second time.
/// * *Checkpoint restore.* A bundle below `restored_cursor` is dropped, its effects being already
///   part of the restored state.
///
/// Consumption itself removes the bundle: `remove_bundles_from_inboxes` pops it from
/// `added_bundles`, and by [`IncomingBundlesAreSelfDerived`] a correct validator does not vote for
/// a block consuming a bundle that is not there. ∎
///
/// **Scoped to one validator.** Every clause above is about one validator's own inboxes. That all
/// correct validators consume the same bundles in the same blocks follows from agreement on the
/// block sequence ([`UniqueChain`]), not from anything here.
///
/// [`MessageBundle`]: linera_chain::data_types::MessageBundle
/// [`Cursor`]: linera_base::data_types::Cursor
/// [`UniqueChain`]: linera_chain::manager::proof::safety::UniqueChain
pub trait BundleConsumedAtMostOnce:
    InboxHoldsOnlySentBundles + EffectsSurviveRestart + IncomingBundlesAreSelfDerived
{
}

/// **Lemma (Unpaid blob storage is bounded).** A validator's storage of blobs that no certified
/// block references is bounded, so pushing data at a validator buys neither storage nor
/// availability.
///
/// This is the companion to [`BlobRetention`]. Retention says a *certified* block's blobs stay
/// available; without a matching bound on *uncertified* ones, "blobs are available" would be an
/// invitation to store anything for free.
///
/// *Proof.* Blobs arrive ahead of certification only through
/// `ChainWorkerState::handle_pending_blob`, which admits one only if it is *expected*: it must
/// belong to a pending proposal or validated block for this chain, or the call fails with
/// `WorkerError::UnexpectedBlob`. Admission is then bounded twice over by the committee's
/// [`ResourceControlPolicy`] — in count, `ensure!(count < policy.maximum_published_blobs)` with
/// `WorkerError::TooManyPublishedBlobs`, and in size, by `check_blob_size` against
/// `maximum_blob_size`. The staging areas themselves are per-chain view state
/// (`pending_proposed_blobs`, `pending_validated_blobs`) and are cleared when the chain manager is
/// reset for the next height.
///
/// Publication that does survive is charged: the policy prices it at `blob_published` per blob and
/// `blob_byte_published` per byte, paid by the block that publishes it. ∎
///
/// **The economic side is out of scope.** That those prices *cover* the cost of keeping a blob for
/// as long as [`BlobRetention`] requires is an economic question this specification does not
/// address; it treats the fee schedule as given.
///
/// [`ResourceControlPolicy`]: linera_execution::ResourceControlPolicy
/// [`BlobRetention`]: super::assumptions::BlobRetention
pub trait BlobAdmissionIsBounded: CorrectValidator {}
