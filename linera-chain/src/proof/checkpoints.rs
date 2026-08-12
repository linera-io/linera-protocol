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
