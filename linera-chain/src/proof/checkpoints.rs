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
