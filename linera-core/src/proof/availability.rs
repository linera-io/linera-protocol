// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a certified block guarantees to everyone else, and what a crash costs.
//!
//! Agreement ([`CommitAgreement`]) says at most one block is certified per height. It says nothing
//! about anyone being *able to act on* that block. The results here supply the other half: once a
//! quorum has certified a block, any node — a validator that was down, a validator that did not
//! exist yet, a client — can obtain it and everything needed to execute it.
//!
//! This is deliberately framed as availability rather than durability. Durability is a property of
//! one node's storage; what the protocol actually needs is that the certified block and its
//! dependencies are *retrievable from the network*, which is what makes a crashed validator's
//! recovery and a new validator's bootstrap the same operation.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement

use linera_chain::manager::proof::{
    commit::CommittedBlock,
    model::{CorrectValidator, SerializedChainState, StorageAtomicity},
};

use super::assumptions::{
    BlobRetention, BoundedRecovery, CorrectValidatorAvailability, EventualSynchrony,
};

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
    CommittedBlock + CorrectValidatorAvailability + EventualSynchrony + BlobRetention
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

/// **Lemma (Missing dependencies are recoverable).** When a validator cannot act on a proposal or
/// a certificate because it lacks data, the missing data falls into a closed set of classes, and
/// each has a route by which it arrives.
///
/// This is what makes the retry loops in `linera_core::updater` converge rather than spin, and it
/// is the substance behind [`ValidationQuorumForms`]'s claim that a step completes in `2Δ`: a
/// straggler is not waited out, it is *supplied*.
///
/// *Proof.* The classes are exactly the errors those loops match, and the recovery route differs
/// by where the data originates:
///
/// | class | what is missing | route |
/// |---|---|---|
/// | `BlobsNotFound` | a blob the block publishes or reads | pushed by the requester: `send_pending_blobs` on the proposal path, `upload_blobs` from local storage on the certificate path |
/// | `BlocksNotFound` | ancestor block bytes a checkpoint trust-marked | pushed from the requester's storage |
/// | `InactiveChain` | the chain does not exist at that validator | pushed with the chain's creation |
/// | `WrongRound`, `UnexpectedBlockHeight` — validator behind | consensus state for this chain | pushed by `send_chain_information` |
/// | `WrongRound`, `UnexpectedBlockHeight` — *requester* behind | nothing; the requester is wrong | pulled: `sync_remote_if_needed` reports [`LocalNodeLagging`] and the client synchronizes |
/// | `MissingCrossChainUpdate` | an incoming bundle the block consumes | the **sending** chain's certificates |
/// | `EventsNotFound` | an event the block read | the **publishing** chain's certificates; the admin chain is special-cased for epoch events |
///
/// The first five are *self-suppliable*: the requester holds the data by construction, because it
/// built the block or already verified the certificate. Those classes cannot stall. ∎
///
/// **The last two are not, and that is where general liveness is weakest.** Their data originates
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
