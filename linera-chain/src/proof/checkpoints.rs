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
/// system events is refused separately by [`ChainStateView`]'s `check_checkpoint_preconditions`,
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
/// [`ChainStateView`]: crate::ChainStateView
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
/// *The dump is total.* [`ExecutionStateView`] has exactly one field: an inner view holding the
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
/// **This is the execution state, not the chain.** The two totality arguments point opposite ways
/// and it is worth being exact about which applies. [`ExecutionStateView`] has one field, so the
/// dump covers all of it. [`ChainStateView`] has sixteen, of which the blob covers exactly one —
/// `execution_state`. Inboxes, outboxes, the tip, the chain manager, the block-hash index and the
/// event trackers are all outside it.
///
/// Two of those are restored by named mechanisms rather than by the blob, and a checkpoint would be
/// unusable without them:
///
/// * *Inboxes.* Each inbox's `restored_cursor` is seeded from `PreparedCheckpoint::inbox_cursors`,
///   carried in the certified oracle response rather than the dump
///   ([`CheckpointPreservesConsumptionBoundary`]).
/// * *Outboxes.* `outboxes`, `outbox_counters` and `nonempty_outboxes` are rebuilt by
///   [`ChainStateView`]'s `restore_outboxes_from_unfinalized`, run once after
///   `restore_from_content`, from the on-chain `unfinalized_message_blocks`
///   ([`AcknowledgedMessagesMayBeForgotten`]). Off-chain outbox state is not certified, so without
///   this a bootstrapped node would go quiet on cross-chain delivery while looking healthy.
///
/// So a checkpoint is not a snapshot of a chain. It is a certified snapshot of the chain's
/// *execution state*, plus enough certified bookkeeping to reconstruct the message-passing state
/// around it. What the remaining chain-state fields hold after a bootstrap is outside this lemma.
///
/// **Residual obligation.** `restore_from_content` leaves the in-memory view stale: its
/// documentation requires the caller to reload afterwards, and nothing in the type enforces it. A
/// caller that skipped the reload would continue against a view that no longer describes storage.
/// This is the same shape as [`SafetyStateRecovery`] — a correctness condition discharged by
/// convention at the call site rather than by construction.
///
/// [`ChainStateView`]: crate::ChainStateView
/// [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
/// [`ExecutionStateView`]: linera_execution::ExecutionStateView
pub trait CheckpointRestoresExecutionState: SerializedChainState {}

/// **Lemma (A sender may forget messages its recipient has checkpointed).** A chain's checkpoint
/// dump names only those of its blocks that still carry outgoing bundles no recipient has
/// acknowledged consuming. Acknowledged bundles are dropped, and no future incarnation of any
/// recipient can ask for them again.
///
/// Without this a chain could never forget anything it had ever sent: `outbox_block_hashes` would
/// name every block with an outgoing message for the life of the chain, and each checkpoint would
/// be larger than the last.
///
/// *Code correspondence.* Three transitions, alternating between the two chains.
///
/// **The sender records an outstanding bundle.**
///
/// | | |
/// |---|---|
/// | transition | the per-recipient loop in `ChainStateView::execute_block_inner` |
/// | reads | `BlockTracker::non_checkpoint_ack_tx_indices`, and the current entry for that recipient |
/// | writes | `system.unfinalized_message_blocks[recipient]`, adding `Cursor { height, index }` for each kept transaction index |
/// | precondition | the recipient has at least one message in this block that is not a `CheckpointAck`; otherwise the entry is not touched |
///
/// **The recipient acknowledges, at its own checkpoint.**
///
/// | | |
/// |---|---|
/// | transition | `ExecutionStateView::apply_checkpoint`, over `PreparedCheckpoint::origin_cursors` built before the block by `ChainStateView::collect_inbox_cursors` |
/// | reads | `system.pending_checkpoint_ack_targets`, and `next_cursor_to_remove` of each named inbox |
/// | writes | one `SystemMessage::CheckpointAck { latest_received_cursor }` per target through `TransactionTracker::add_outgoing_message`, then clears `pending_checkpoint_ack_targets` |
/// | precondition | `prepare_checkpoint` and `check_checkpoint_preconditions` both passed ([`CheckpointSummarizesUserStreams`]) |
///
/// **The sender trims.**
///
/// | | |
/// |---|---|
/// | transition | the `CheckpointAck` arm of `SystemExecutionStateView::execute_message` |
/// | reads | `system.unfinalized_message_blocks[context.origin]` |
/// | writes | the same entry, replaced by `cursors.split_off(&latest_received_cursor)`, or removed when that is empty |
/// | precondition | none — an acknowledgement naming an origin with no entry is a no-op |
///
/// *Proof.* Four steps.
///
/// *What gets recorded.* `non_checkpoint_ack_tx_indices` walks the block's outgoing messages and
/// keeps, per destination, the indices of transactions holding at least one message for which
/// `Message::is_checkpoint_ack` is false. `execute_block_inner` inserts a [`Cursor`] for each. So a
/// block whose only traffic to a recipient is an acknowledgement adds nothing to track — which is
/// the first of the two exclusions below.
///
/// *What the dump names.* `unfinalized_message_blocks` is held in the *system execution state*, not
/// in the off-chain outbox, so it is identical across validators and may feed a certified oracle
/// response: `PreparedCheckpoint::outbox_block_hashes` is the unique heights across all its
/// cursors, resolved to hashes through `block_hashes` by the pre-block hook — the cursors are
/// written mid-execution, when the block's own hash is not yet known. Cursors rather than bare
/// heights is what lets an acknowledgement landing part-way through a block evict a recipient
/// outright, which matters for a high-fanout chain whose recipients each interact with it once.
///
/// *What clears an entry.* On `CheckpointAck { latest_received_cursor }`, `split_off` retains the
/// cursors at or above it and discards the strict prefix below; an emptied set removes the
/// recipient entirely. Since the dump is derived from what remains, those blocks stop being named.
///
/// *Why that is safe.* `latest_received_cursor` is the recipient's `next_cursor_to_remove` at its
/// checkpoint, so it has consumed everything below it. The same checkpoint records that position in
/// `PreparedCheckpoint::inbox_cursors`, so any node bootstrapping the recipient seeds
/// `restored_cursor` at least that high; and by [`CheckpointPreservesConsumptionBoundary`] a bundle
/// below `restored_cursor` is dropped by `Inbox::add_bundle` on arrival and reported as already
/// known by `Inbox::remove_bundle` on consumption. No incarnation of the recipient, present or
/// future, can therefore ask for a bundle the sender has dropped. ∎
///
/// **What is kept is exactly what a bootstrapped node needs.** The same map has a second reader:
/// `ChainStateView::restore_outboxes_from_unfinalized`, called once after
/// `restore_from_content` when a node bootstraps from a checkpoint. Off-chain outbox state —
/// `outboxes`, `outbox_counters`, `nonempty_outboxes` — is not part of the certified blob, so
/// without this rebuild a bootstrapped node would silently stop pushing pending messages onward.
/// Retention and resumption are therefore the same set: a sender keeps a block precisely while some
/// recipient might still need it delivered.
///
/// **Why the exchange terminates.** An acknowledgement is itself a message, so two exclusions stop
/// two chains acknowledging each other forever, and they act at different points. A bundle whose
/// only messages to a recipient were `CheckpointAck` never enters `unfinalized_message_blocks`
/// (`non_checkpoint_ack_tx_indices`), so it never becomes something to acknowledge; and a received
/// `CheckpointAck` never enters its origin into `pending_checkpoint_ack_targets` (the
/// `!posted_message.message.is_checkpoint_ack()` guard in `BlockTracker`), so it creates no debt to
/// answer. `apply_checkpoint` then clears that set, so only a fresh non-acknowledgement message
/// re-enters a chain for the next round.
///
/// **A sender can forget only as fast as its recipients checkpoint.** Nothing obliges a recipient
/// to checkpoint, and until it does it sends no acknowledgement, so the sender keeps naming those
/// blocks. A chain whose recipients never checkpoint therefore has an ever-growing dump however
/// often it checkpoints itself — its own frequency does not help. This is the outbox-side face of
/// [issue #6693](https://github.com/linera-io/linera-protocol/issues/6693): with nothing scheduling
/// checkpoints anywhere, the bound this lemma provides rests on behaviour no rule requires.
///
/// [`Cursor`]: linera_base::data_types::Cursor
pub trait AcknowledgedMessagesMayBeForgotten: CheckpointPreservesConsumptionBoundary {}
