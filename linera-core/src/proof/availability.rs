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

use super::assumptions::{BoundedRecovery, CorrectValidatorAvailability, EventualSynchrony};

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
///   hedged. Content addressing means a blob needs no trust either: a wrong one is detectable by
///   hashing, so a single honest source suffices.
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
pub trait CertifiedBlockIsAvailable:
    CommittedBlock + CorrectValidatorAvailability + EventualSynchrony
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

/// **Lemma (A proposer can supply its block's dependencies).** A correct client holding a block's
/// blobs can get a proposal accepted even by validators that hold none of them, without waiting
/// for those dependencies to reach the validators by any other route.
///
/// Without this, block creation would be gated on dependency propagation: a chain could not
/// publish a blob and use it, or act on a freshly received message, until every validator had
/// independently caught up.
///
/// *Proof.* Each dependency kind has a push path, taken on demand when a validator reports it
/// missing.
///
/// * *Blobs.* `RemoteNodeUpdater::send_block_proposal` loops; its `BlobsNotFound | InactiveChain`
///   arm sends the proposal's published blobs with `send_pending_blobs` and re-submits to that
///   validator alone. The worker holds them in `pending_proposed_blobs` until the vote is cast.
/// * *Incoming messages.* A validator that has not yet received a bundle the block consumes
///   rejects with [`MissingCrossChainUpdate`] rather than voting — the strict half of
///   [`IncomingBundlesAreSelfDerived`] — and the updater responds by pushing the sending chain's
///   certificates, after which the bundle is derivable locally.
/// * *Events.* `EventsNotFound` is answered by pushing the publishing chain, the admin chain
///   included as a special case for epoch events.
///
/// Each push is per-validator and re-submits only to that validator, so a straggler costs neither
/// a new round nor the other validators' votes ([`ValidationQuorumForms`]). ∎
///
/// **Where this is bounded.** The pushes terminate because each is guarded by a "already sent"
/// flag or by draining a fixed set: `send_block_proposal` takes its `blob_ids` with `mem::take`,
/// and the certificate paths use `sent_blobs` / `sent_admin_chain` / `sent_blocks` latches. A
/// validator that reports the same class of dependency missing twice is not retried again.
///
/// [`MissingCrossChainUpdate`]: linera_chain::ChainError::MissingCrossChainUpdate
/// [`IncomingBundlesAreSelfDerived`]: linera_chain::manager::proof::commit::IncomingBundlesAreSelfDerived
/// [`ValidationQuorumForms`]: super::progress::ValidationQuorumForms
pub trait ProposalDependenciesAreSuppliable:
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
