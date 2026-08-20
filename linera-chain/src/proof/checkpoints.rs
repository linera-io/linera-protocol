// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Event streams across a checkpoint boundary.

use crate::manager::proof::model::SerializedChainState;

/// **Invariant (A stream's readable floor is its first index since the last checkpoint).** For
/// every event stream of a chain, [`StreamCounts::first_index`] is the index of the first event
/// published to that stream since the most recent checkpoint, and the indices from there up to
/// [`StreamCounts::next_index`] are contiguous. Events below the floor may have been pruned and
/// are unavailable to a node that bootstrapped from that checkpoint; events at or above it are
/// untouched by checkpointing.
///
/// This is the sense in which pre-checkpoint events are *summarized* rather than merely dropped:
/// what a checkpoint discards is bounded, and the boundary is a published number rather than
/// something a reader has to infer.
///
/// *Proof.* [`ChainStateView`]'s `process_emitted_events` maintains both fields whenever a block is
/// processed — executed or merely preprocessed — and branches on whether the block records a
/// predecessor for the stream in [`previous_event_blocks`], the per-stream map holding the hash
/// and height of the last block that emitted to it. That map is built during execution and is
/// covered by the block hash through [`previous_event_blocks_hash`], so every node branches the
/// same way on the same block.
///
/// * *A predecessor is recorded.* The stream has published since the last checkpoint, so its
///   events must continue contiguously. If they do not — `lo != counts.next_index` — the tracker
///   is left untouched, so a gap never advances `next_index` over a missing index and never moves
///   the floor.
/// * *No predecessor, and `lo >= first_index`.* This block is the first to emit to the stream
///   since a checkpoint. `lo` becomes the new floor and `next_index` advances to `max(next_index,
///   hi + 1)`.
/// * *No predecessor, and `lo < first_index`.* An earlier checkpoint era, already superseded by a
///   later one this chain has recorded. It is ignored, so the floor does not move backwards. ∎
///
/// **The floor only ever rises**, which is what makes it safe to publish. The `>=` guard in the
/// second branch exists for checkpoints seen out of order — an earlier one preprocessed after a
/// later one — and without it a stale checkpoint would lower a floor that readers had already
/// relied on.
///
/// **What a reader may conclude.** A cross-chain read of an event through
/// [`OracleResponse::Event`] is guaranteed to find it only at an index at or above that stream's
/// floor. Below the floor, availability depends on some node not having pruned, which no statement
/// here provides.
///
/// [`StreamCounts::first_index`]: crate::StreamCounts::first_index
/// [`StreamCounts::next_index`]: crate::StreamCounts::next_index
/// [`ChainStateView`]: crate::ChainStateView
/// [`previous_event_blocks`]: crate::block::BlockBody::previous_event_blocks
/// [`previous_event_blocks_hash`]: crate::block::BlockHeader::previous_event_blocks_hash
/// [`OracleResponse::Event`]: linera_base::data_types::OracleResponse::Event
pub trait EventFloorTracksCheckpoints: SerializedChainState {}

/// **Lemma (A checkpoint summarizes every user stream that published since the previous one).**
/// At a checkpoint, each application holding an event stream that has published since the previous
/// checkpoint is given the chance to replace that stream's history with a summary, and no system
/// stream is ever in that position.
///
/// *Proof.* `ExecutionStateActor::summarize_events_at_checkpoint` takes as its work list exactly
/// the user streams in [`previous_event_blocks`]. That map is cleared by every checkpoint, so an
/// entry means the stream has published since the previous one. Each owning application is run
/// through `UserAction::SummarizeEvents` with a `StreamUpdate` whose `previous_index` is `0` and
/// whose `first_index` and `next_index` are both the stream's current count: a summary is an
/// absolute-state snapshot, so the application is handed no incremental range to fold in. The
/// summary it emits lands at `next_index`, which is a fresh index with no predecessor recorded, so
/// by [`EventFloorTracksCheckpoints`] it becomes the stream's readable floor.
///
/// The map is then cleared, dropping every pre-checkpoint anchor, so no later block links back to
/// blocks whose events are no longer guaranteed to be readable.
///
/// Only user streams can appear. A chain that has *published* to a system stream cannot checkpoint
/// at all — `ExecutionStateView::prepare_checkpoint` scans [`previous_event_blocks`] and refuses,
/// because system streams have no application to summarize them — and a chain that has *consumed*
/// system events is refused separately by `ChainStateView`'s `check_checkpoint_preconditions`,
/// which scans the reader-side trackers and fails with
/// [`ChainError::CheckpointPreconditionFailed`]. The admin chain's epoch streams are the case both
/// guards exist for. ∎
///
/// **A silent application closes its stream.** Summarization is an opportunity, not an obligation.
/// A stream whose application emits nothing when summarized loses its anchor along with every
/// other, and is not summarized again unless it publishes something new. Nothing distinguishes a
/// stream deliberately closed from one whose application neglected to summarize, and in both cases
/// the events below the floor are gone.
///
/// [`previous_event_blocks`]: crate::block::BlockBody::previous_event_blocks
/// [`ChainError::CheckpointPreconditionFailed`]: crate::ChainError::CheckpointPreconditionFailed
pub trait CheckpointSummarizesUserStreams: EventFloorTracksCheckpoints {}

/// **Lemma (A checkpoint moves the consumption boundary and nothing else about messages).**
/// Restoring a chain from a checkpoint changes which incoming bundles are still *retained*, never
/// which have been consumed. Bundles below the restored cursor are already reflected in the
/// restored execution state and are ignored on arrival and on consumption; bundles at or above it
/// are queued, delivered and consumed exactly as they would have been with no checkpoint at all.
///
/// *Proof.* Three parts — what the checkpoint records, what a restore does, and what the guards
/// then absorb.
///
/// *Recorded.* `PreparedCheckpoint::inbox_cursors` carries the cursor of **every** inbox with a
/// non-default `next_cursor_to_remove`, not only those the checkpoint acknowledges. A bootstrapping
/// node therefore learns the consumption position of every origin the chain had consumed from,
/// including origins it will never hear from again.
///
/// *Restored.* `Inbox::restore_from_checkpoint` sets `restored_cursor` to that cursor, raises
/// `next_cursor_to_add` to it if it lagged, sets `next_cursor_to_remove` to it, drops
/// `added_bundles` below it, and clears `removed_bundles` — those anticipated removals came from
/// pre-restore blocks the rollback has invalidated. It refuses to move backwards: restoring at a
/// cursor below the current `restored_cursor` is an error, so a checkpoint dispatched out of order
/// cannot undo a later one.
///
/// *Absorbed.* Below `restored_cursor`, `Inbox::add_bundle` returns without queueing and
/// `Inbox::remove_bundle` returns immediately, reporting the bundle as already known and
/// deliberately *not* recording it in `removed_bundles` — otherwise that queue would fill with
/// anticipations no sender will ever satisfy. So a sender that has not yet seen the matching
/// acknowledgement and re-pushes an already-consumed bundle causes a silent no-op, not a duplicate
/// consumption. At or above the cursor the guards are the ordinary ones, and
/// `linera_core::proof::availability::BundleConsumedAtMostOnce` applies unchanged. ∎
///
/// **The acknowledgement is what lets a sender forget.** A checkpoint emits
/// `SystemMessage::CheckpointAck` to each origin in `PreparedCheckpoint::origin_cursors`, carrying
/// the position past the last bundle from that origin this chain has consumed. The recipients are
/// `pending_checkpoint_ack_targets`: the chains that have sent this one a message which was not
/// itself a `CheckpointAck`, so an acknowledgement never obliges an acknowledgement in return.
/// What the origin then does with it, and why dropping those bundles is safe, is
/// [`AcknowledgedMessagesMayBeForgotten`].
///
/// **What the outboxes still reference is certified, not merely named.**
/// `PreparedCheckpoint::outbox_block_hashes` lists every block this chain's outboxes still refer
/// to, captured before the checkpoint block runs, and travels in the checkpoint's oracle response.
/// The checkpoint block's certificate therefore transitively certifies those older blocks, so a
/// bootstrapping node can rely on them without replaying the chain.
pub trait CheckpointPreservesConsumptionBoundary: SerializedChainState {}

/// **Lemma (A checkpoint leaves blob availability unchanged).** Checkpointing neither strands a
/// blob the chain can still reach nor silently requires one a bootstrapping node cannot obtain.
///
/// *Proof.* Two directions.
///
/// *Nothing is stranded.* Blobs live in storage shared across chains, addressed by content and
/// owned by no chain's view. What a checkpoint prunes is this chain's *history* — its older blocks
/// and the events they published — which is not where blobs are kept. So no blob becomes
/// unreachable by checkpointing, and the retention obligation is exactly what it was before.
///
/// *Nothing is silently required.* `ExecutionStateView::apply_checkpoint` records an
/// [`OracleResponse::Checkpoint`] carrying `used_blobs`, read from the system state's `used_blobs`
/// set: every blob the chain references at that moment. An oracle response is part of the block's
/// outcome and covered by the block hash, so the list is certified rather than advisory, and a
/// bootstrapping node knows precisely which blobs it must hold in shared storage before applying
/// the checkpoint — otherwise a later operation could read blob content it does not have. ∎
///
/// **The state dump is itself a blob, on the ordinary terms.** The execution state is split at the
/// current epoch's `maximum_blob_size` and published through `add_created_blob`, so a checkpoint's
/// snapshot is published by its block exactly as any other blob is: priced by the block that
/// publishes it, bounded in count and size, and retained on the same footing. A checkpoint buys a
/// node the right to skip replaying history; it does not buy free storage.
///
/// [`OracleResponse::Checkpoint`]: linera_base::data_types::OracleResponse::Checkpoint
pub trait CheckpointPreservesBlobAvailability: SerializedChainState {}

/// **Lemma (A checkpoint restores exactly the execution state it captured).** Applying a
/// checkpoint's blobs reproduces the chain's execution state as it stood immediately before the
/// checkpoint block, in full.
///
/// *Proof.* Four parts.
///
/// *The dump is total.* `ExecutionStateView` has exactly one field: an inner view holding the
/// system state, the user applications' key-value stores, and the two previous-block maps.
/// Everything the outer view exposes is reached by dereferencing into it, and `dump_content`
/// serializes that inner view's persisted content whole. No part of the execution state can be
/// left out of a checkpoint by oversight — totality here is structural, not an inventory someone
/// has to keep current as fields are added.
///
/// *The dump is quiescent.* `dump_content` reads from storage and refuses to run while the view
/// holds pending in-memory changes, failing with `ViewError::HasPendingChanges`.
/// `prepare_checkpoint` is therefore a *pre-block* operation, run before block-level setup mutates
/// the chain. The captured bytes are the committed pre-block state — exactly what a bootstrapping
/// node restores before re-applying the certified checkpoint block.
///
/// *The bytes are pinned by the certificate.* The dump is chunked at the epoch's
/// `maximum_blob_size` and published as created blobs of the checkpoint block, their ids listed in
/// that block's `OracleResponse::Checkpoint` as `execution_state_blobs`. Blobs are content
/// addressed, so a node fetching them cannot be handed different bytes; and the id list is part of
/// the certified outcome, so it cannot be pointed at a different dump. Integrity here is free, in
/// the way integrity of any blob is free — availability is the separate question, and is
/// `linera_core::proof::availability::CertifiedBlockIsAvailable`'s.
///
/// *The hash agrees.* `ExecutionStateView::crypto_hash_mut` derives the state hash from the inner
/// view's historical hash, and `restore_from_content` records the hash of the restored bytes as the
/// new stored hash. A node that restores and then re-applies the certified checkpoint block
/// computes the `state_hash` that block certifies, so a restore that went wrong does not go
/// unnoticed. ∎
///
/// **Residual obligation.** `restore_from_content` leaves the in-memory view stale: its
/// documentation requires the caller to reload afterwards, and nothing in the type enforces it. A
/// caller that skipped the reload would continue against a view that no longer describes storage.
/// This is the same shape as [`SafetyStateRecovery`] — a correctness condition discharged by
/// convention at the call site rather than by construction.
///
/// [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
pub trait CheckpointRestoresExecutionState: SerializedChainState {}

/// **Lemma (A sender may forget messages its recipient has checkpointed).** A chain's checkpoint
/// dump names only those of its blocks that still carry outgoing bundles no recipient has
/// acknowledged consuming. Acknowledged bundles are dropped, and no future incarnation of any
/// recipient can ask for them again.
///
/// Without this a chain could never forget anything it had ever sent: `outbox_block_hashes` would
/// name every block with an outgoing message for the life of the chain, and each checkpoint would
/// be larger than the last. The acknowledgement is what makes checkpointing a chain with busy
/// outboxes sustainable rather than merely possible.
///
/// *Proof.* Four steps, alternating between the two chains.
///
/// *The sender tracks what is outstanding.* `unfinalized_message_blocks` maps each recipient to the
/// cursors of outgoing bundles not yet acknowledged. It lives in the *system execution state*
/// rather than in the local off-chain outbox, which is what makes it identical across validators
/// and therefore fit to feed a certified oracle response: `PreparedCheckpoint::outbox_block_hashes`
/// is the set of unique heights across those cursors. Cursors rather than heights, so that an
/// acknowledgement landing mid-block can still evict an entry entirely — which is what a
/// high-fanout chain needs, whose recipients each interact with it once.
///
/// *The recipient acknowledges on its own checkpoint.* `collect_inbox_cursors` snapshots
/// `(origin, next_cursor_to_remove)` for each chain in `pending_checkpoint_ack_targets`, and
/// `apply_checkpoint` emits a `SystemMessage::CheckpointAck` carrying that cursor to each.
///
/// *The sender trims.* Handling `CheckpointAck { latest_received_cursor }`, the system state calls
/// `split_off` on that recipient's cursor set, retaining those at or above the cursor and dropping
/// the strict prefix below it. A recipient that has consumed everything ever sent to it leaves an
/// empty set, and the entry is removed altogether.
///
/// *Forgetting is safe.* The acknowledged cursor is the recipient's `next_cursor_to_remove`, so it
/// has consumed everything below it; and the same checkpoint records that position in
/// `inbox_cursors`, so a node bootstrapping the recipient starts with `restored_cursor` at least
/// that high. By [`CheckpointPreservesConsumptionBoundary`] anything below `restored_cursor` is
/// dropped on arrival and is a no-op on consumption. There is therefore no incarnation of the
/// recipient, present or future, that can ask for a bundle the sender has dropped. ∎
///
/// **Why the exchange terminates.** An acknowledgement is itself a message, so the protocol has to
/// avoid two chains acknowledging each other forever. Two exclusions do it, at different points. A
/// received `CheckpointAck` does not put its origin into `pending_checkpoint_ack_targets`, so it
/// creates no debt to answer; and a bundle whose only messages to a recipient were `CheckpointAck`
/// is kept out of `unfinalized_message_blocks`, so it never becomes something to acknowledge in the
/// first place. `apply_checkpoint` then clears `pending_checkpoint_ack_targets`, so only a fresh
/// real message re-enters a chain into the next round.
///
/// **A sender can forget only as fast as its recipients checkpoint.** Nothing obliges a recipient
/// to checkpoint, and until it does it sends no acknowledgement, so the sender keeps naming those
/// blocks. A chain whose recipients never checkpoint therefore has an ever-growing dump however
/// often it checkpoints itself — its own frequency does not help. This is the outbox-side face of
/// [issue #6693](https://github.com/linera-io/linera-protocol/issues/6693): with nothing scheduling
/// checkpoints anywhere, the bound this lemma provides is conditional on behaviour no rule
/// currently requires.
pub trait AcknowledgedMessagesMayBeForgotten: CheckpointPreservesConsumptionBoundary {}
