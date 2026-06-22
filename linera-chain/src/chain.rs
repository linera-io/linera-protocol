// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use allocative::Allocative;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ApplicationPermissions, ArithmeticError, Blob, BlockHeight, Cursor,
        Epoch, NonCanonicalBTreeMap, NonCanonicalBTreeSet, OracleResponse, Timestamp,
    },
    ensure,
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, BlobType, ChainId, GenericApplicationId, StreamId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
};
use linera_execution::{
    committee::Committee, ExecutionRuntimeContext, ExecutionStateView, Message, Operation,
    OutgoingMessage, PreparedCheckpoint, Query, QueryContext, QueryOutcome, ResourceController,
    ResourceTracker, ServiceRuntimeEndpoint, TransactionTracker,
};
use linera_views::{
    context::Context,
    log_view::LogView,
    map_view::{CustomMapView, MapView},
    reentrant_collection_view::{ReadGuardedView, ReentrantCollectionView},
    register_view::RegisterView,
    set_view::SetView,
    views::{ClonableView, RootView, View},
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};

use crate::{
    block::{Block, ConfirmedBlock},
    block_tracker::BlockExecutionTracker,
    data_types::{
        BlockExecutionOutcome, BundleExecutionPolicy, BundleFailurePolicy, ChainAndHeight,
        IncomingBundle, MessageAction, MessageBundle, ProposedBlock, Transaction,
    },
    inbox::{InboxError, InboxStateView},
    manager::ChainManager,
    outbox::OutboxStateView,
    pending_blobs::PendingBlobsView,
    ChainError, ChainExecutionContext, ExecutionError, ExecutionResultExt,
};

#[cfg(test)]
#[path = "unit_tests/chain_tests.rs"]
mod chain_tests;

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency;

#[cfg(with_metrics)]
pub(crate) mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, register_histogram_vec, register_int_counter_vec,
    };
    use linera_execution::ResourceTracker;
    use prometheus::{HistogramVec, IntCounterVec};

    pub static NUM_BLOCKS_EXECUTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec("num_blocks_executed", "Number of blocks executed", &[])
    });

    pub static BLOCK_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "block_execution_latency",
            "Block execution latency",
            &[],
            exponential_bucket_interval(50.0_f64, 10_000_000.0),
        )
    });

    #[cfg(with_metrics)]
    pub static MESSAGE_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "message_execution_latency",
            "Message execution latency",
            &[],
            exponential_bucket_interval(0.1_f64, 50_000.0),
        )
    });

    pub static OPERATION_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "operation_execution_latency",
            "Operation execution latency",
            &[],
            exponential_bucket_interval(0.1_f64, 50_000.0),
        )
    });

    pub static WASM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "wasm_fuel_used_per_block",
            "Wasm fuel used per block",
            &[],
            exponential_bucket_interval(10.0, 100_000_000.0),
        )
    });

    pub static EVM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "evm_fuel_used_per_block",
            "EVM fuel used per block",
            &[],
            exponential_bucket_interval(10.0, 100_000_000.0),
        )
    });

    pub static VM_NUM_READS_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "vm_num_reads_per_block",
            "VM number of reads per block",
            &[],
            exponential_bucket_interval(0.1, 100.0),
        )
    });

    pub static VM_BYTES_READ_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "vm_bytes_read_per_block",
            "VM number of bytes read per block",
            &[],
            exponential_bucket_interval(0.1, 10_000_000.0),
        )
    });

    pub static VM_BYTES_WRITTEN_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "vm_bytes_written_per_block",
            "VM number of bytes written per block",
            &[],
            exponential_bucket_interval(0.1, 10_000_000.0),
        )
    });

    pub static STATE_HASH_COMPUTATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "state_hash_computation_latency",
            "Time to recompute the state hash, in microseconds",
            &[],
            exponential_bucket_interval(1.0, 2_000_000.0),
        )
    });

    pub static NUM_OUTBOXES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_outboxes",
            "Number of outboxes",
            &[],
            exponential_bucket_interval(1.0, 1_000_000.0),
        )
    });

    pub static OUTBOX_COUNTERS_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "outbox_counters_size",
            "Number of entries in the outbox_counters map (in-flight message heights)",
            &[],
            exponential_bucket_interval(1.0, 1_000_000.0),
        )
    });

    /// Tracks block execution metrics in Prometheus.
    pub(crate) fn track_block_metrics(tracker: &ResourceTracker) {
        NUM_BLOCKS_EXECUTED.with_label_values(&[]).inc();
        WASM_FUEL_USED_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.wasm_fuel as f64);
        EVM_FUEL_USED_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.evm_fuel as f64);
        VM_NUM_READS_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.read_operations as f64);
        VM_BYTES_READ_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.bytes_read as f64);
        VM_BYTES_WRITTEN_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.bytes_written as f64);
    }
}

/// The BCS-serialized size of an empty [`Block`].
pub(crate) const EMPTY_BLOCK_SIZE: usize = 94;

/// A set of fully-tracked chains. Wrapped in [`Hashed`] (as `Hashed<ChainIdSet>`) so the hash that
/// identifies the set — stored in [`ChainStateView::outbox_index_tracked_hash`] to detect when the
/// outbox indices must be reconciled — is computed once when the tracked set changes rather than on
/// every cross-chain operation. The hash is order-independent because `BTreeSet` iterates in sorted
/// order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainIdSet(pub BTreeSet<ChainId>);

impl linera_base::crypto::BcsHashable<'_> for ChainIdSet {}

impl std::ops::Deref for ChainIdSet {
    type Target = BTreeSet<ChainId>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The event indices we track for a stream, maintained whenever a block is processed
/// (executed or merely preprocessed).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Allocative)]
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
pub struct StreamCounts {
    /// The lowest event index still guaranteed to be readable (if it exists): the index of the
    /// first event published to this stream since the most recent checkpoint. Earlier events may
    /// have been pruned by a checkpoint and are not available to nodes that bootstrapped from it.
    /// When there are no events yet, this equals [`next_index`](Self::next_index).
    pub first_index: u32,
    /// The next event index we expect to see, i.e. the lowest for which no event is known yet.
    /// May be ahead of the last executed block on sparse chains.
    pub next_index: u32,
}

/// A view accessing the state of a chain.
#[cfg_attr(
    with_graphql,
    derive(async_graphql::SimpleObject),
    graphql(cache_control(no_cache))
)]
#[derive(Debug, RootView, ClonableView, Allocative)]
#[allocative(bound = "C")]
pub struct ChainStateView<C>
where
    C: Clone + Context + 'static,
{
    /// Execution state, including system and user applications.
    pub execution_state: ExecutionStateView<C>,

    /// Block-chaining state.
    pub tip_state: RegisterView<C, ChainTipState>,

    /// Consensus state.
    pub manager: ChainManager<C>,
    /// Pending validated block that is still missing blobs.
    /// The incomplete set of blobs for the pending validated block.
    pub pending_validated_blobs: PendingBlobsView<C>,
    /// The incomplete sets of blobs for upcoming proposals.
    pub pending_proposed_blobs: ReentrantCollectionView<C, AccountOwner, PendingBlobsView<C>>,

    /// Hashes of all known blocks in this chain, indexed by their height. A block at
    /// `height < next_block_height` is executed; a block at `height >= next_block_height`
    /// is preprocessed (verified but not yet executed) and may not be contiguous.
    pub block_hashes: CustomMapView<C, BlockHeight, CryptoHash>,
    /// Sender chain and height of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, ChainAndHeight>,
    /// The number of `received_log` entries we have synchronized, for each validator.
    pub received_certificate_trackers: RegisterView<C, HashMap<ValidatorPublicKey, u64>>,

    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: ReentrantCollectionView<C, ChainId, InboxStateView<C>>,
    /// Mailboxes used to send messages, indexed by their target.
    pub outboxes: ReentrantCollectionView<C, ChainId, OutboxStateView<C>>,
    /// The event indices we track per stream: the next expected index (could be ahead of the
    /// last executed block in sparse chains) and the lowest readable index since the most
    /// recent checkpoint.
    pub next_expected_events: MapView<C, StreamId, StreamCounts>,
    /// Number of outgoing messages in flight for each block height.
    /// We use a `RegisterView` to prioritize speed for small maps.
    pub outbox_counters: RegisterView<C, NonCanonicalBTreeMap<BlockHeight, u32>>,
    /// Outboxes with at least one pending message. This allows us to avoid loading all outboxes.
    pub nonempty_outboxes: RegisterView<C, NonCanonicalBTreeSet<ChainId>>,

    /// Inboxes with at least one pending added bundle. This allows us to avoid loading all inboxes.
    pub nonempty_inboxes: RegisterView<C, NonCanonicalBTreeSet<ChainId>>,

    /// The local wall-clock time when this chain's state was last established — by
    /// executing block 0, or by installing a checkpoint snapshot (on bootstrap or reset).
    /// Used to prevent reset-on-incorrect-outcome from looping: if not enough time has
    /// elapsed since the last reset, the error is returned instead.
    pub chain_initialized_at: RegisterView<C, Timestamp>,

    /// The height at which the next block can be preprocessed: one past the highest
    /// height in `block_hashes` (executed or preprocessed), or `next_block_height` if
    /// `block_hashes` is empty.
    ///
    /// Maintained as an O(1) shortcut for `next_height_to_preprocess`, since
    /// `CustomMapView` does not yet expose a `last_index` lookup. Once
    /// `linera-views` gains efficient first/last key support, this field can be
    /// removed in favor of `block_hashes.last_index()`.
    pub next_height_to_preprocess: RegisterView<C, BlockHeight>,

    /// The height of the most recent checkpoint block applied to this chain, if any.
    /// Maintained by `apply_confirmed_block` whenever a block starting with
    /// `SystemOperation::Checkpoint` is executed.
    pub latest_checkpoint_height: RegisterView<C, Option<BlockHeight>>,

    /// Hashes of pre-checkpoint sender blocks the chain has seen a checkpoint cert
    /// vouch for via `outbox_block_hashes`, but whose actual cert bytes are not yet
    /// in storage. The worker errors a checkpoint push with
    /// `BlocksNotFound` when this set is non-empty, then accepts each
    /// referenced cert (regardless of its own — possibly revoked — epoch) and
    /// removes the entry. Once the set is empty, the checkpoint restoration can run
    /// end-to-end.
    pub pre_checkpoint_block_trust: SetView<C, CryptoHash>,

    /// The hash of the set of fully-tracked chains that `nonempty_outboxes` and
    /// `outbox_counters` were last reconciled against. On a client these two indices only hold
    /// entries for tracked targets; when the tracked set changes this hash stops matching and the
    /// indices are reconciled (`reconcile_outbox_index`). `None` means
    /// they have never been filtered — a pre-existing database entry (migration), or a validator
    /// that tracks all chains and never filters.
    pub outbox_index_tracked_hash: RegisterView<C, Option<CryptoHash>>,
}

/// Block-chaining state.
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize, Allocative)]
pub struct ChainTipState {
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<CryptoHash>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,
    /// Number of incoming message bundles.
    pub num_incoming_bundles: u32,
    /// Number of operations.
    pub num_operations: u32,
    /// Number of outgoing messages.
    pub num_outgoing_messages: u32,
}

impl ChainTipState {
    /// Checks that the proposed block is suitable, i.e. at the expected height and with the
    /// expected parent.
    pub fn verify_block_chaining(&self, new_block: &ProposedBlock) -> Result<(), ChainError> {
        ensure!(
            new_block.height == self.next_block_height,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: self.next_block_height,
                found_block_height: new_block.height
            }
        );
        ensure!(
            new_block.previous_block_hash == self.block_hash,
            ChainError::UnexpectedPreviousBlockHash
        );
        Ok(())
    }

    /// Returns `true` if the validated block's height is below the tip height. Returns an error if
    /// it is higher than the tip.
    pub fn already_validated_block(&self, height: BlockHeight) -> Result<bool, ChainError> {
        ensure!(
            self.next_block_height >= height,
            ChainError::MissingEarlierBlocks {
                current_block_height: self.next_block_height,
            }
        );
        Ok(self.next_block_height > height)
    }

    /// Checks if the measurement counters would be valid.
    pub fn update_counters(
        &mut self,
        transactions: &[Transaction],
        messages: &[Vec<OutgoingMessage>],
    ) -> Result<(), ChainError> {
        let mut num_incoming_bundles = 0u32;
        let mut num_operations = 0u32;

        for transaction in transactions {
            match transaction {
                Transaction::ReceiveMessages(_) => {
                    num_incoming_bundles = num_incoming_bundles
                        .checked_add(1)
                        .ok_or(ArithmeticError::Overflow)?;
                }
                Transaction::ExecuteOperation(_) => {
                    num_operations = num_operations
                        .checked_add(1)
                        .ok_or(ArithmeticError::Overflow)?;
                }
            }
        }

        self.num_incoming_bundles = self
            .num_incoming_bundles
            .checked_add(num_incoming_bundles)
            .ok_or(ArithmeticError::Overflow)?;

        self.num_operations = self
            .num_operations
            .checked_add(num_operations)
            .ok_or(ArithmeticError::Overflow)?;

        let num_outgoing_messages = u32::try_from(messages.iter().map(Vec::len).sum::<usize>())
            .map_err(|_| ArithmeticError::Overflow)?;
        self.num_outgoing_messages = self
            .num_outgoing_messages
            .checked_add(num_outgoing_messages)
            .ok_or(ArithmeticError::Overflow)?;

        Ok(())
    }
}

impl<C> ChainStateView<C>
where
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Returns the [`ChainId`] of the chain this [`ChainStateView`] represents.
    pub fn chain_id(&self) -> ChainId {
        self.context().extra().chain_id()
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
    ))]
    /// Executes the given query against an application on this chain.
    pub async fn query_application(
        &mut self,
        local_time: Timestamp,
        query: Query,
        service_runtime_endpoint: Option<&mut ServiceRuntimeEndpoint>,
    ) -> Result<QueryOutcome, ChainError> {
        let context = QueryContext {
            chain_id: self.chain_id(),
            next_block_height: self.tip_state.get().next_block_height,
            local_time,
        };
        self.execution_state
            .query_application(context, query, service_runtime_endpoint)
            .await
            .with_execution_context(ChainExecutionContext::Query)
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        application_id = %application_id
    ))]
    /// Returns the description of the application with the given ID.
    pub async fn describe_application(
        &mut self,
        application_id: ApplicationId,
    ) -> Result<ApplicationDescription, ChainError> {
        self.execution_state
            .system
            .describe_application(application_id, &mut TransactionTracker::default())
            .await
            .with_execution_context(ChainExecutionContext::DescribeApplication)
    }

    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        target = %target,
        height = %height
    ))]
    /// Marks all messages sent to `target` up to the given height as received, returning whether
    /// the outbox changed.
    pub async fn mark_messages_as_received(
        &mut self,
        target: &ChainId,
        height: BlockHeight,
        tracked: Option<&ChainIdSet>,
    ) -> Result<bool, ChainError> {
        let mut outbox = self.outboxes.try_load_entry_mut(target).await?;
        let updates = outbox.mark_messages_as_received(height).await?;
        if updates.is_empty() {
            return Ok(false);
        }
        // `outbox_counters` is keyed by block height and shared across all recipients of that
        // block, but only counts targets we index: every chain on a validator (`tracked == None`),
        // or tracked targets on a client. An untracked target was never counted, so confirming it
        // must NOT touch the counters at all — a present `counter[height]` belongs to a tracked
        // sibling recipient of the same block and must be left intact. We only drain the queue
        // (done above) for such a target.
        if tracked.is_none_or(|tracked| tracked.contains(target)) {
            for update in updates {
                let counter = self
                    .outbox_counters
                    .get_mut()
                    .get_mut(&update)
                    .ok_or_else(|| {
                        ChainError::CorruptedChainState("message counter should be present".into())
                    })?;
                *counter = counter.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
                if *counter == 0 {
                    // Important for the test in `all_messages_delivered_up_to`.
                    self.outbox_counters.get_mut().remove(&update);
                }
            }
        }
        if outbox.queue.count() == 0 {
            self.nonempty_outboxes.get_mut().remove(target);
            // If the outbox is empty and not ahead of the executed blocks, remove it.
            if *outbox.next_height_to_schedule.get() <= self.tip_state.get().next_block_height {
                self.outboxes.remove_entry(target)?;
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_OUTBOXES
            .with_label_values(&[])
            .observe(self.nonempty_outboxes.get().len() as f64);
        #[cfg(with_metrics)]
        metrics::OUTBOX_COUNTERS_SIZE
            .with_label_values(&[])
            .observe(self.outbox_counters.get().len() as f64);
        Ok(true)
    }

    /// Returns true if there are no more outgoing messages in flight up to the given
    /// block height.
    pub fn all_messages_delivered_up_to(&self, height: BlockHeight) -> bool {
        tracing::debug!(
            "Messages left in {:.8}'s outbox: {:?}",
            self.chain_id(),
            self.outbox_counters.get()
        );
        if let Some((key, _)) = self.outbox_counters.get().first_key_value() {
            key > &height
        } else {
            true
        }
    }

    /// Invariant for the states of active chains.
    pub async fn is_active(&self) -> Result<bool, ChainError> {
        Ok(self.execution_state.system.is_active().await?)
    }

    /// Initializes the chain if it is not active yet.
    pub async fn initialize_if_needed(&mut self, local_time: Timestamp) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        // Initialize ourselves.
        if self
            .execution_state
            .system
            .initialize_chain(chain_id)
            .await
            .with_execution_context(ChainExecutionContext::Block)?
        {
            // The chain was already initialized.
            return Ok(());
        }
        let maybe_committee = self
            .execution_state
            .system
            .current_committee()
            .await
            .with_execution_context(ChainExecutionContext::Block)?;
        // Last, reset the consensus state based on the current ownership.
        self.manager.reset(
            self.execution_state.system.ownership.get().await?.clone(),
            BlockHeight(0),
            local_time,
            maybe_committee
                .iter()
                .flat_map(|(_, committee)| committee.account_keys_and_weights()),
        )?;
        Ok(())
    }

    /// Inserts `(height, hash)` into `block_hashes` and updates the
    /// `next_height_to_preprocess` register accordingly. Every write to
    /// `block_hashes` must go through this helper so the register stays in sync.
    fn insert_block_hash(
        &mut self,
        height: BlockHeight,
        hash: CryptoHash,
    ) -> Result<(), ChainError> {
        self.block_hashes.insert(&height, hash)?;
        let next = self.next_height_to_preprocess.get_mut();
        if *next <= height {
            *next = height.try_add_one()?;
        }
        Ok(())
    }

    /// Attempts to process a new `bundle` of messages from the given `origin`. Returns an
    /// internal error if the bundle doesn't appear to be new, based on the sender's
    /// height. The value `local_time` is specific to each validator and only used for
    /// round timeouts.
    ///
    /// Returns `true` if incoming `Subscribe` messages created new outbox entries.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        origin = %origin,
        bundle_height = %bundle.height
    ))]
    pub async fn receive_message_bundle_with_inbox(
        &mut self,
        inbox: &mut InboxStateView<C>,
        origin: &ChainId,
        bundle: MessageBundle,
        local_time: Timestamp,
        add_to_received_log: bool,
    ) -> Result<(), ChainError> {
        assert!(!bundle.messages.is_empty());
        let chain_id = self.chain_id();
        tracing::trace!(
            "Processing new messages from {origin} at height {}",
            bundle.height,
        );
        let chain_and_height = ChainAndHeight {
            chain_id: *origin,
            height: bundle.height,
        };

        match self.initialize_if_needed(local_time).await {
            Ok(_) => (),
            // if the only issue was that we couldn't initialize the chain because of a
            // missing chain description blob, we might still want to update the inbox
            Err(ChainError::ExecutionError(exec_err, _))
                if matches!(*exec_err, ExecutionError::BlobsNotFound(ref blobs)
                if blobs.iter().all(|blob_id| {
                    blob_id.blob_type == BlobType::ChainDescription && blob_id.hash == chain_id.0
                })) => {}
            err => {
                return err;
            }
        }

        // Process the inbox bundle and update the inbox state.
        let newly_added = inbox
            .add_bundle(bundle)
            .await
            .map_err(|error| match error {
                InboxError::ViewError(error) => ChainError::ViewError(error),
                error => ChainError::CorruptedChainState(format!(
                    "while processing messages in certified block: {error}"
                )),
            })?;
        if newly_added {
            self.nonempty_inboxes.get_mut().insert(*origin);
        }

        // Remember the certificate for future validator/client synchronizations.
        if add_to_received_log {
            self.received_log.push(chain_and_height);
        }
        Ok(())
    }

    /// Updates the `received_log` trackers.
    pub fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorPublicKey, u64>,
    ) {
        for (name, tracker) in new_trackers {
            self.received_certificate_trackers
                .get_mut()
                .entry(name)
                .and_modify(|t| {
                    // Because several synchronizations could happen in parallel, we need to make
                    // sure to never go backward.
                    if tracker > *t {
                        *t = tracker;
                    }
                })
                .or_insert(tracker);
        }
    }

    /// Returns the current epoch and committee of this chain.
    pub async fn current_committee(&self) -> Result<(Epoch, Arc<Committee>), ChainError> {
        let chain_id = self.chain_id();
        self.execution_state
            .system
            .current_committee()
            .await
            .with_execution_context(ChainExecutionContext::Block)?
            .ok_or(ChainError::InactiveChain(chain_id))
    }

    /// Returns the ownership configuration of this chain.
    pub async fn ownership(&self) -> Result<&ChainOwnership, ChainError> {
        Ok(self.execution_state.system.ownership.get().await?)
    }

    /// Removes the incoming message bundles in the block from the inboxes.
    ///
    /// If `must_be_present` is `true`, an error is returned if any of the bundles have not been
    /// added to the inbox yet. So this should be `true` if the bundles are in a block _proposal_,
    /// and `false` if the block is already confirmed.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
    ))]
    pub async fn remove_bundles_from_inboxes(
        &mut self,
        timestamp: Timestamp,
        must_be_present: bool,
        incoming_bundles: impl IntoIterator<Item = &IncomingBundle>,
    ) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let mut bundles_by_origin: BTreeMap<_, Vec<&MessageBundle>> = Default::default();
        for IncomingBundle { bundle, origin, .. } in incoming_bundles {
            ensure!(
                bundle.timestamp <= timestamp,
                ChainError::IncorrectBundleTimestamp {
                    chain_id,
                    bundle_timestamp: bundle.timestamp,
                    block_timestamp: timestamp,
                }
            );
            let bundles = bundles_by_origin.entry(*origin).or_default();
            bundles.push(bundle);
        }
        let origins = bundles_by_origin.keys().copied().collect::<Vec<_>>();
        let inboxes = self.inboxes.try_load_entries_mut(&origins).await?;
        for ((origin, bundles), mut inbox) in bundles_by_origin.into_iter().zip(inboxes) {
            tracing::trace!(
                "Removing [{}] from inbox for {origin}",
                bundles
                    .iter()
                    .map(|bundle| bundle.height.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for bundle in bundles {
                // Mark the message as processed in the inbox.
                let was_present = inbox
                    .remove_bundle(bundle)
                    .await
                    .map_err(|error| (chain_id, origin, error))?;
                if must_be_present {
                    ensure!(
                        was_present,
                        ChainError::MissingCrossChainUpdate {
                            chain_id,
                            origin,
                            height: bundle.height,
                        }
                    );
                }
            }
            inbox.observe_size_metric();
            if inbox.added_bundles.count() == 0 {
                self.nonempty_inboxes.get_mut().remove(&origin);
            }
        }
        Ok(())
    }

    /// Returns the chain IDs of all recipients for which a message is waiting in the outbox.
    pub fn nonempty_outbox_chain_ids(&self) -> Vec<ChainId> {
        self.nonempty_outboxes.get().iter().copied().collect()
    }

    /// Returns the outboxes for the given targets, or an error if any of them are missing.
    pub async fn load_outboxes(
        &self,
        targets: &[ChainId],
    ) -> Result<Vec<ReadGuardedView<OutboxStateView<C>>>, ChainError> {
        let vec_of_options = self.outboxes.try_load_entries(targets).await?;
        let optional_vec = vec_of_options.into_iter().collect::<Option<Vec<_>>>();
        optional_vec.ok_or_else(|| ChainError::CorruptedChainState("Missing outboxes".into()))
    }

    /// Reconciles the `nonempty_outboxes` and `outbox_counters` indices from the retained outbox
    /// queues, rebuilding only when the tracked set changed since the last call (i.e. the stored
    /// [`Self::outbox_index_tracked_hash`] no longer matches). The per-target outbox *queues* in
    /// `outboxes` are always kept; only these indices are filtered. Returns whether a rebuild
    /// actually happened, so a read-only caller can skip persisting when nothing changed.
    pub async fn reconcile_outbox_index(
        &mut self,
        tracked: Option<&Hashed<ChainIdSet>>,
    ) -> Result<bool, ChainError> {
        if *self.outbox_index_tracked_hash.get() == tracked.map(|tracked| tracked.hash()) {
            return Ok(false);
        }
        self.rebuild_outbox_index(tracked).await?;
        Ok(true)
    }

    /// Rebuilds `nonempty_outboxes` and `outbox_counters` from the retained outbox queues for the
    /// tracked set (every retained queue on a validator, `tracked == None`), and stamps
    /// [`Self::outbox_index_tracked_hash`]. Unlike [`Self::reconcile_outbox_index`] this always
    /// rebuilds, so callers that have just rewritten the queues can refresh the indices regardless
    /// of the stored stamp.
    async fn rebuild_outbox_index(
        &mut self,
        tracked: Option<&Hashed<ChainIdSet>>,
    ) -> Result<(), ChainError> {
        self.nonempty_outboxes.get_mut().clear();
        self.outbox_counters.get_mut().clear();
        // In full mode (`None`) there is no tracked subset to iterate, so re-index from the keys of
        // every retained outbox queue.
        let targets = match tracked {
            Some(tracked) => tracked.inner().iter().copied().collect::<Vec<_>>(),
            None => self.outboxes.indices().await?,
        };
        for target in &targets {
            let heights = {
                let Some(outbox) = self.outboxes.try_load_entry(target).await? else {
                    continue;
                };
                outbox.queue.elements().await?
            };
            if heights.is_empty() {
                continue;
            }
            for height in heights {
                *self.outbox_counters.get_mut().entry(height).or_default() += 1;
            }
            self.nonempty_outboxes.get_mut().insert(*target);
        }
        self.outbox_index_tracked_hash
            .set(tracked.map(|tracked| tracked.hash()));
        Ok(())
    }

    /// Returns whether the outbox index is already reconciled to `tracked` (the stored hash
    /// matches), so the read-only network-actions path can read it without a write-lock rebuild.
    pub fn outbox_index_is_reconciled(&self, tracked: Option<&Hashed<ChainIdSet>>) -> bool {
        *self.outbox_index_tracked_hash.get() == tracked.map(|tracked| tracked.hash())
    }

    /// Executes a block with a specified policy for handling bundle failures.
    #[expect(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(
        chain_id = %block.chain_id,
        block_height = %block.height
    ))]
    async fn execute_block_inner(
        chain: &mut ExecutionStateView<C>,
        block_hashes: &CustomMapView<C, BlockHeight, CryptoHash>,
        block: &mut ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
        exec_policy: BundleExecutionPolicy,
        checkpoint_origin_cursors: Vec<(ChainId, Cursor)>,
        checkpoint_inbox_cursors: Vec<(ChainId, Cursor)>,
        checkpoint_outbox_block_hashes: Vec<CryptoHash>,
    ) -> Result<(BlockExecutionOutcome, ResourceTracker, HashSet<ChainId>), ChainError> {
        // AutoRetry is incompatible with replaying oracle responses because discarding or
        // rejecting bundles would change which transactions execute.
        if !matches!(&exec_policy.on_failure, BundleFailurePolicy::Abort) {
            assert!(
                replaying_oracle_responses.is_none(),
                "Cannot use AutoRetry policy when replaying oracle responses"
            );
        }

        #[cfg(with_metrics)]
        let _execution_latency = metrics::BLOCK_EXECUTION_LATENCY.measure_latency_us();

        // Resolve the current epoch's resource policy first: `prepare_checkpoint` needs
        // `maximum_blob_size` to chunk the dump, and `current_committee` is a pure read
        // so it can run before the dump without tainting the inner view's pending-changes
        // set.
        let committee_policy = chain
            .system
            .current_committee()
            .await
            .with_execution_context(ChainExecutionContext::Block)?
            .ok_or_else(|| ChainError::InactiveChain(block.chain_id))?
            .1
            .policy()
            .clone();

        // Pre-block hook: if this block contains a `SystemOperation::Checkpoint`, dump the
        // execution state from storage *now*, before any block-level mutation taints the
        // inner view's pending-changes set. The matching operation handler will publish
        // the resulting blobs without re-dumping; subsequent fees and other state changes
        // accumulate normally and end up persisted with the override hash on save. The
        // inbox snapshot was already taken by the outer `execute_block` and passed in as
        // `checkpoint_origin_cursors`; we bundle the two halves into a
        // [`PreparedCheckpoint`] for the handler.
        let prepared_checkpoint = if block.starts_with_checkpoint() {
            let blobs = chain
                .prepare_checkpoint(committee_policy.maximum_blob_size)
                .await
                .with_execution_context(ChainExecutionContext::Block)?;
            Some(PreparedCheckpoint {
                blobs,
                origin_cursors: checkpoint_origin_cursors,
                inbox_cursors: checkpoint_inbox_cursors,
                outbox_block_hashes: checkpoint_outbox_block_hashes,
            })
        } else {
            None
        };

        chain.system.timestamp.set(block.timestamp);

        let mut resource_controller = ResourceController::new(
            Arc::new(committee_policy),
            ResourceTracker::default(),
            block.authenticated_owner,
        );

        for blob in published_blobs {
            let blob_id = blob.id();
            resource_controller
                .policy()
                .check_blob_size(blob.content())
                .with_execution_context(ChainExecutionContext::Block)?;
            chain.system.used_blobs.insert(&blob_id)?;
        }

        let mut block_execution_tracker = BlockExecutionTracker::new(
            &mut resource_controller,
            published_blobs
                .iter()
                .map(|blob| (blob.id(), blob))
                .collect(),
            local_time,
            replaying_oracle_responses,
            block,
        )?;
        if let Some(prepared) = prepared_checkpoint {
            block_execution_tracker.set_prepared_checkpoint(prepared);
        }

        // Extract failure-policy parameters from exec_policy.
        let (max_failures, never_reject_application_ids) = match &exec_policy.on_failure {
            BundleFailurePolicy::Abort => (0, Arc::new(HashSet::new())),
            BundleFailurePolicy::AutoRetry {
                max_failures,
                never_reject_application_ids,
            } => (*max_failures, never_reject_application_ids.clone()),
        };
        let auto_retry = !matches!(exec_policy.on_failure, BundleFailurePolicy::Abort);
        let mut failure_count = 0u32;
        let mut never_reject_discarded_origins = HashSet::new();

        let time_budget = exec_policy.time_budget;
        let mut cumulative_bundle_time = Duration::ZERO;

        let mut i = 0;
        while i < block.transactions.len() {
            let transaction = &mut block.transactions[i];
            let is_bundle = matches!(transaction, Transaction::ReceiveMessages(_));
            let is_stream_update = transaction.is_update_stream();

            // If we have a time budget and it's been exceeded, discard remaining bundles.
            if is_bundle && time_budget.is_some_and(|budget| cumulative_bundle_time >= budget) {
                info!(
                    ?cumulative_bundle_time,
                    ?time_budget,
                    "Time budget for bundle staging exceeded, discarding remaining bundles"
                );
                Self::discard_remaining_bundles(block, i, None);
                continue;
            }

            let checkpoint = if auto_retry && (is_bundle || is_stream_update) {
                Some((
                    chain.clone_unchecked()?,
                    block_execution_tracker.create_checkpoint(),
                ))
            } else {
                None
            };

            let bundle_start = if is_bundle && time_budget.is_some() {
                Some(Instant::now())
            } else {
                None
            };

            let result = block_execution_tracker
                .execute_transaction(&*transaction, round, chain)
                .await;

            if let Some(start) = bundle_start {
                cumulative_bundle_time += start.elapsed();
            }

            // If the transaction executed successfully, we move on to the next one.
            // On transient errors (e.g. missing blobs) we fail, so it can be retried after
            // syncing. In auto-retry mode, we can discard or reject message bundles that failed
            // with non-transient errors.
            match (result, transaction, checkpoint) {
                (Ok(()), _, _) => {
                    i += 1;
                }
                (
                    Err(ChainError::ExecutionError(error, _context)),
                    Transaction::ReceiveMessages(incoming_bundle),
                    Some((saved_chain, saved_tracker)),
                ) if !error.is_transient_error() && error.is_limit_error() && i > 0 => {
                    // Restore checkpoint.
                    *chain = saved_chain;
                    block_execution_tracker.restore_checkpoint(&saved_tracker);
                    failure_count += 1;
                    // If we've exceeded max failures, discard all remaining message bundles.
                    let maybe_sender = if failure_count > max_failures {
                        info!(
                            failure_count,
                            max_failures,
                            "Exceeded max bundle failures, discarding all remaining message \
                            bundles and stream updates"
                        );
                        Self::discard_remaining_stream_updates(block, i);
                        None
                    } else {
                        // Not the first - discard it and same-sender subsequent bundles.
                        info!(
                            %error,
                            index = i,
                            origin = %incoming_bundle.origin,
                            "Message bundle exceeded block limits and will be discarded for \
                            retry in a later block"
                        );
                        Some(incoming_bundle.origin)
                    };
                    Self::discard_remaining_bundles(block, i, maybe_sender);
                    // Do not increment i - the next transaction is now at i.
                }
                (
                    Err(ChainError::ExecutionError(error, context)),
                    Transaction::ReceiveMessages(incoming_bundle),
                    Some((saved_chain, saved_tracker)),
                ) if !error.is_transient_error() => {
                    // Restore checkpoint.
                    *chain = saved_chain;
                    block_execution_tracker.restore_checkpoint(&saved_tracker);

                    let all_messages_never_reject = !never_reject_application_ids.is_empty()
                        && incoming_bundle.messages().all(|posted_msg| {
                            never_reject_application_ids
                                .contains(&posted_msg.message.application_id())
                        });
                    if (all_messages_never_reject || incoming_bundle.bundle.is_protected())
                        && incoming_bundle.action != MessageAction::Reject
                    {
                        let origin = incoming_bundle.origin;
                        never_reject_discarded_origins.insert(origin);
                        warn!(
                            %error,
                            index = i,
                            %origin,
                            "Message bundle cannot be rejected (protected or never-reject); \
                            discarding the bundle (and same-sender subsequent bundles) for retry \
                            in a later block"
                        );
                        Self::discard_remaining_bundles(block, i, Some(origin));
                    } else if incoming_bundle.action == MessageAction::Reject {
                        // Failed rejected bundles fail the block.
                        return Err(ChainError::ExecutionError(error, context));
                    } else {
                        // Reject the bundle: either a non-limit error, or the first bundle
                        // exceeded limits (and is inherently too large for any block).
                        info!(
                            %error,
                            index = i,
                            origin = %incoming_bundle.origin,
                            "Message bundle failed to execute and will be rejected"
                        );
                        incoming_bundle.action = MessageAction::Reject;
                    }
                    // Do not increment i - retry the transaction after modification.
                }
                (
                    Err(ChainError::ExecutionError(error, _context)),
                    transaction,
                    Some((saved_chain, saved_tracker)),
                ) if transaction.is_update_stream()
                    && !error.is_transient_error()
                    && error.is_limit_error()
                    && i > 0 =>
                {
                    // Restore checkpoint.
                    *chain = saved_chain;
                    block_execution_tracker.restore_checkpoint(&saved_tracker);
                    failure_count += 1;
                    if failure_count > max_failures {
                        info!(
                            failure_count,
                            max_failures,
                            "Exceeded max failures, discarding all remaining stream updates and \
                            message bundles"
                        );
                        Self::discard_remaining_bundles(block, i, None);
                        Self::discard_remaining_stream_updates(block, i);
                    } else {
                        info!(
                            %error,
                            index = i,
                            "UpdateStream exceeded block limits, discarding for retry"
                        );
                        block.transactions.remove(i);
                    }
                    // Do not increment i - the next transaction is now at i.
                }
                (Err(e), _, _) => return Err(e),
            };
        }

        // This can only happen if all transactions were incoming bundles that all got discarded
        // due to resource limit errors. This is unlikely in practice but theoretically possible.
        ensure!(!block.transactions.is_empty(), ChainError::EmptyBlock);

        let recipients = block_execution_tracker.recipients();
        let non_ack_tx_indices = block_execution_tracker.non_checkpoint_ack_tx_indices();
        let mut recipient_heights = Vec::new();
        for (recipient, height) in chain
            .previous_message_blocks
            .multi_get_pairs(recipients)
            .await?
        {
            // Only `CheckpointAck`-only blocks are excluded from the chain-level
            // tracking. Otherwise the recipient never acknowledges (a
            // `CheckpointAck` doesn't trigger a return `CheckpointAck`), so the
            // entry would never get trimmed. Off-chain outbox bookkeeping further
            // down still queues these for delivery; only the
            // `previous_message_blocks` / `unfinalized_message_blocks` chain skips
            // them.
            if let Some(tx_indices) = non_ack_tx_indices.get(&recipient) {
                chain
                    .previous_message_blocks
                    .insert(&recipient, block.height)?;
                // Track each non-CheckpointAck bundle's cursor as pending
                // acknowledgement from the recipient. We don't know this block's
                // hash yet (we're mid-execution); the checkpoint pre-block hook
                // resolves heights to hashes via `block_hashes` when it builds the
                // oracle response.
                let mut cursors = chain
                    .system
                    .unfinalized_message_blocks
                    .get(&recipient)
                    .await?
                    .unwrap_or_default();
                for index in tx_indices {
                    cursors.insert(Cursor {
                        height: block.height,
                        index: *index,
                    });
                }
                chain
                    .system
                    .unfinalized_message_blocks
                    .insert(&recipient, cursors)?;
            }
            if let Some(height) = height {
                recipient_heights.push((recipient, height));
            }
        }
        let hashes = block_hashes
            .multi_get(recipient_heights.iter().map(|(_, height)| height))
            .await?;
        let mut previous_message_blocks = BTreeMap::new();
        for (hash, (recipient, height)) in hashes.into_iter().zip(recipient_heights) {
            let hash = hash.ok_or_else(|| {
                ChainError::CorruptedChainState("missing entry in block_hashes".into())
            })?;
            previous_message_blocks.insert(recipient, (hash, height));
        }

        let streams = block_execution_tracker.event_streams();
        let mut stream_heights = Vec::new();
        for (stream, height) in chain.previous_event_blocks.multi_get_pairs(streams).await? {
            chain.previous_event_blocks.insert(&stream, block.height)?;
            if let Some(height) = height {
                stream_heights.push((stream, height));
            }
        }
        let hashes = block_hashes
            .multi_get(stream_heights.iter().map(|(_, height)| height))
            .await?;
        let mut previous_event_blocks = BTreeMap::new();
        for (hash, (stream, height)) in hashes.into_iter().zip(stream_heights) {
            let hash = hash.ok_or_else(|| {
                ChainError::CorruptedChainState("missing entry in block_hashes".into())
            })?;
            previous_event_blocks.insert(stream, (hash, height));
        }

        let state_hash = {
            #[cfg(with_metrics)]
            let _hash_latency = metrics::STATE_HASH_COMPUTATION_LATENCY.measure_latency_us();
            chain.crypto_hash_mut().await?
        };

        let (messages, oracle_responses, events, blobs, operation_results, resource_tracker) =
            block_execution_tracker.finalize(block.transactions.len());

        Ok((
            BlockExecutionOutcome {
                messages,
                previous_message_blocks,
                previous_event_blocks,
                state_hash,
                oracle_responses,
                events,
                blobs,
                operation_results,
            },
            resource_tracker,
            never_reject_discarded_origins,
        ))
    }

    fn discard_remaining_stream_updates(block: &mut ProposedBlock, mut index: usize) {
        while index < block.transactions.len() {
            if block.transactions[index].is_update_stream() {
                block.transactions.remove(index);
            } else {
                index += 1;
            }
        }
    }

    fn discard_remaining_bundles(
        block: &mut ProposedBlock,
        mut index: usize,
        maybe_origin: Option<ChainId>,
    ) {
        while index < block.transactions.len() {
            if matches!(
                &block.transactions[index],
                Transaction::ReceiveMessages(bundle)
                if maybe_origin.is_none_or(|origin| bundle.origin == origin)
            ) {
                block.transactions.remove(index);
            } else {
                index += 1;
            }
        }
    }

    /// Executes a block with a specified policy for handling bundle failures.
    ///
    /// This method supports automatic retry with checkpointing when bundles fail:
    /// - For limit errors (block too large, fuel exceeded, etc.): the bundle is discarded
    ///   so it can be retried in a later block, unless it's the first transaction
    ///   (which gets rejected as inherently too large).
    /// - For non-limit errors: the bundle is rejected (triggering bounced messages).
    /// - After `max_failures` failed bundles, all remaining message bundles are discarded.
    ///
    /// The block may be modified to reflect the actual executed transactions.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.height
    ))]
    pub async fn execute_block(
        &mut self,
        mut block: ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
        policy: BundleExecutionPolicy,
    ) -> Result<
        (
            ProposedBlock,
            BlockExecutionOutcome,
            ResourceTracker,
            HashSet<ChainId>,
        ),
        ChainError,
    > {
        assert_eq!(
            block.chain_id,
            self.execution_state.context().extra().chain_id()
        );

        self.initialize_if_needed(local_time).await?;

        let chain_timestamp = *self.execution_state.system.timestamp.get();
        ensure!(
            chain_timestamp <= block.timestamp,
            ChainError::InvalidBlockTimestamp {
                parent: chain_timestamp,
                new: block.timestamp
            }
        );
        ensure!(!block.transactions.is_empty(), ChainError::EmptyBlock);

        ensure!(
            block.published_blob_ids()
                == published_blobs
                    .iter()
                    .map(|blob| blob.id())
                    .collect::<BTreeSet<_>>(),
            ChainError::InternalError("published_blobs mismatch".to_string())
        );

        if *self.execution_state.system.closed.get() {
            ensure!(block.has_only_rejected_messages(), ChainError::ClosedChain);
        }

        Self::check_app_permissions(
            self.execution_state
                .system
                .application_permissions
                .get()
                .await?,
            &block,
        )?;

        ensure!(
            !block
                .transactions
                .iter()
                .skip(1)
                .any(Transaction::is_checkpoint),
            ChainError::CheckpointPreconditionFailed(
                "Checkpoint must be the first transaction in its block",
            )
        );
        let (origin_cursors, inbox_cursors, outbox_block_hashes) = if block.starts_with_checkpoint()
        {
            self.check_checkpoint_preconditions().await?;
            let origin_cursors = self.collect_inbox_cursors().await?;
            let inbox_cursors = self.collect_all_inbox_cursors().await?;
            let hashes = self.collect_unfinalized_block_hashes().await?;
            (origin_cursors, inbox_cursors, hashes)
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };

        Self::execute_block_inner(
            &mut self.execution_state,
            &self.block_hashes,
            &mut block,
            local_time,
            round,
            published_blobs,
            replaying_oracle_responses,
            policy,
            origin_cursors,
            inbox_cursors,
            outbox_block_hashes,
        )
        .await
        .map(|(outcome, tracker, never_reject_origins)| {
            (block, outcome, tracker, never_reject_origins)
        })
    }

    /// Snapshots `(origin, next_cursor_to_remove)` for each chain we've received a
    /// non-`Checkpoint` message from since our last own checkpoint. Used by the
    /// pre-block hook so the matching operation handler can emit a
    /// [`SystemMessage::CheckpointAck`] to each origin chain. Iterating
    /// `pending_checkpoint_ack_targets` (instead of every inbox) is what prevents the
    /// notification ping-pong: a chain that only ever sent us `Checkpoint`s is
    /// excluded here, so we never reply with a `Checkpoint` of our own.
    async fn collect_inbox_cursors(&self) -> Result<Vec<(ChainId, Cursor)>, ChainError> {
        let targets = self
            .execution_state
            .system
            .pending_checkpoint_ack_targets
            .indices()
            .await?;
        let mut cursors = Vec::with_capacity(targets.len());
        for origin in targets {
            let Some(inbox) = self.inboxes.try_load_entry(&origin).await? else {
                continue;
            };
            cursors.push((origin, *inbox.next_cursor_to_remove.get()));
        }
        Ok(cursors)
    }

    /// Snapshots `(origin, next_cursor_to_remove)` for every inbox with a non-default
    /// `next_cursor_to_remove`. Recorded in the checkpoint's oracle response so a
    /// bootstrapping node can seed each inbox's `restored_cursor` and silently drop
    /// any sender re-pushes whose effects are already in the restored execution state.
    async fn collect_all_inbox_cursors(&self) -> Result<Vec<(ChainId, Cursor)>, ChainError> {
        let origins = self.inboxes.indices().await?;
        let mut cursors = Vec::new();
        for origin in origins {
            let Some(inbox) = self.inboxes.try_load_entry(&origin).await? else {
                continue;
            };
            let cursor = *inbox.next_cursor_to_remove.get();
            if cursor != Cursor::default() {
                cursors.push((origin, cursor));
            }
        }
        Ok(cursors)
    }

    /// Re-populates `outboxes`, `outbox_counters`, and `nonempty_outboxes` from
    /// the on-chain `unfinalized_message_blocks` map after a checkpoint
    /// bootstrap. Called once after `execution_state.restore_from_content` so
    /// the freshly-restored chain can pick up cross-chain delivery for
    /// pre-checkpoint messages — the off-chain outbox state isn't part of the
    /// certified checkpoint blob, so without this a bootstrapped node would
    /// silently stop pushing pending messages forward.
    pub async fn restore_outboxes_from_unfinalized(
        &mut self,
        tracked: Option<&Hashed<ChainIdSet>>,
    ) -> Result<(), ChainError> {
        // A lagging validator hit by a checkpoint push may already have outbox
        // queues from blocks it processed before the gap. Clear them so the
        // rebuild from `unfinalized_message_blocks` doesn't append duplicate
        // heights onto stale queues (the counters/nonempty set below `set`
        // wholesale, but the queues are appended to per recipient).
        let prior_recipients = self.outboxes.indices().await?;
        for recipient in prior_recipients {
            let mut outbox = self.outboxes.try_load_entry_mut(&recipient).await?;
            outbox.queue.clear();
            outbox.next_height_to_schedule.set(BlockHeight::ZERO);
        }
        let entries = self
            .execution_state
            .system
            .unfinalized_message_blocks
            .index_values()
            .await?;
        for (recipient, cursors) in entries {
            if cursors.is_empty() {
                continue;
            }
            // Dedup by height: the outbox queue tracks block heights only (multiple
            // bundles at the same height share a single queue entry).
            let heights = cursors
                .into_iter()
                .map(|cursor| cursor.height)
                .collect::<BTreeSet<_>>();
            let mut outbox = self.outboxes.try_load_entry_mut(&recipient).await?;
            for height in &heights {
                outbox.queue.push_back(*height);
            }
            let max_height = *heights
                .last()
                .expect("the empty case was filtered out above");
            outbox
                .next_height_to_schedule
                .set(max_height.try_add_one()?);
        }
        // The queues are now authoritative; rebuild the tracked-only indices from them.
        self.rebuild_outbox_index(tracked).await?;
        Ok(())
    }

    /// Collects the hashes of every block on this chain still listed in the on-chain
    /// `unfinalized_message_blocks` map. The checkpoint pre-block hook calls this to
    /// build the oracle response's `outbox_block_hashes`, so the checkpoint
    /// certificate transitively re-certifies those older (possibly revoked-epoch)
    /// blocks.
    async fn collect_unfinalized_block_hashes(&self) -> Result<Vec<CryptoHash>, ChainError> {
        let heights = self.collect_unfinalized_heights().await?;
        let mut hashes = Vec::with_capacity(heights.len());
        for height in heights {
            let hash = self.block_hashes.get(&height).await?.ok_or_else(|| {
                ChainError::CorruptedChainState(format!(
                    "missing entry in block_hashes at height {height}"
                ))
            })?;
            hashes.push(hash);
        }
        Ok(hashes)
    }

    /// Returns the sorted, deduplicated set of block heights referenced by the on-chain
    /// `unfinalized_message_blocks` map (one entry per height even if multiple bundles
    /// at that height are still unfinalized). Used both when building the checkpoint
    /// oracle response (to resolve heights to hashes via `block_hashes`) and on the
    /// bootstrap path (to zip with the certified `outbox_block_hashes` from the
    /// response).
    pub async fn collect_unfinalized_heights(&self) -> Result<BTreeSet<BlockHeight>, ChainError> {
        let mut heights = BTreeSet::new();
        let entries = self
            .execution_state
            .system
            .unfinalized_message_blocks
            .index_values()
            .await?;
        for (_, per_recipient) in entries {
            heights.extend(per_recipient.into_iter().map(|cursor| cursor.height));
        }
        Ok(heights)
    }

    /// Applies an execution outcome to the chain, updating the outboxes, state hash and chain
    /// manager. This does not touch the execution state itself, which must be updated separately.
    /// Returns the set of event streams that were updated as a result of applying the block.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.inner().inner().header.height
    ))]
    pub async fn apply_confirmed_block(
        &mut self,
        block: &ConfirmedBlock,
        local_time: Timestamp,
        tracked: Option<&ChainIdSet>,
    ) -> Result<BTreeSet<StreamId>, ChainError> {
        let hash = block.inner().hash();
        let block = block.inner().inner();
        if block.header.height == BlockHeight::ZERO {
            self.chain_initialized_at.set(local_time);
        }
        let updated_streams = self.process_emitted_events(block).await?;
        self.process_outgoing_messages(block, tracked).await?;

        // Last, reset the consensus state based on the current ownership.
        self.reset_chain_manager(block.header.height.try_add_one()?, local_time)
            .await?;

        // Advance to next block height.
        let tip = self.tip_state.get_mut();
        tip.block_hash = Some(hash);
        tip.next_block_height.try_add_assign_one()?;
        tip.update_counters(&block.body.transactions, &block.body.messages)?;
        self.insert_block_hash(block.header.height, hash)?;
        if block.body.starts_with_checkpoint() {
            self.latest_checkpoint_height.set(Some(block.header.height));
        }
        Ok(updated_streams)
    }

    /// Adds a block to `block_hashes` as preprocessed, and updates the outboxes where possible.
    /// Returns the set of streams that were updated as a result of preprocessing the block.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.inner().inner().header.height
    ))]
    pub async fn preprocess_block(
        &mut self,
        block: &ConfirmedBlock,
        tracked: Option<&ChainIdSet>,
    ) -> Result<BTreeSet<StreamId>, ChainError> {
        let hash = block.inner().hash();
        let block = block.inner().inner();
        let height = block.header.height;
        if height < self.tip_state.get().next_block_height {
            return Ok(BTreeSet::new());
        }
        self.process_outgoing_messages(block, tracked).await?;
        let updated_streams = self.process_emitted_events(block).await?;
        self.insert_block_hash(height, hash)?;
        Ok(updated_streams)
    }

    /// Verifies that the block is valid according to the chain's application permission settings.
    #[instrument(skip_all, fields(
        block_height = %block.height,
        num_transactions = %block.transactions.len()
    ))]
    fn check_app_permissions(
        app_permissions: &ApplicationPermissions,
        block: &ProposedBlock,
    ) -> Result<(), ChainError> {
        let mut mandatory = app_permissions
            .mandatory_applications
            .iter()
            .copied()
            .collect::<HashSet<ApplicationId>>();
        for transaction in &block.transactions {
            match transaction {
                Transaction::ExecuteOperation(operation)
                    if operation.is_exempt_from_permissions() =>
                {
                    mandatory.clear()
                }
                Transaction::ExecuteOperation(operation) => {
                    ensure!(
                        app_permissions.can_execute_operations(&operation.application_id()),
                        ChainError::AuthorizedApplications(
                            app_permissions.execute_operations.clone().unwrap()
                        )
                    );
                    if let Operation::User { application_id, .. } = operation {
                        mandatory.remove(application_id);
                    }
                }
                Transaction::ReceiveMessages(incoming_bundle)
                    if incoming_bundle.action == MessageAction::Accept =>
                {
                    for pending in incoming_bundle.messages() {
                        if let Message::User { application_id, .. } = &pending.message {
                            mandatory.remove(application_id);
                        }
                    }
                }
                Transaction::ReceiveMessages(_) => {}
            }
        }
        ensure!(
            mandatory.is_empty(),
            ChainError::MissingMandatoryApplications(mandatory.into_iter().collect())
        );
        Ok(())
    }

    /// Validates the chain-state-level preconditions for a `SystemOperation::Checkpoint`:
    /// no *system* event stream tracker is set.
    ///
    /// The structural invariant that `Checkpoint` must be the *first* transaction in its
    /// block is enforced unconditionally in `execute_block`, independently of these
    /// preconditions. Sender-side event conditions are validated inside
    /// `ExecutionStateView::prepare_checkpoint`.
    async fn check_checkpoint_preconditions(&self) -> Result<(), ChainError> {
        let mut had_system_event_tracker = false;
        self.next_expected_events
            .for_each_index_while(|stream_id| {
                if matches!(stream_id.application_id, GenericApplicationId::System) {
                    had_system_event_tracker = true;
                    Ok(false)
                } else {
                    Ok(true)
                }
            })
            .await?;
        ensure!(
            !had_system_event_tracker,
            ChainError::CheckpointPreconditionFailed("chain has consumed system events")
        );

        Ok(())
    }

    /// Returns the hashes of all blocks we have at the given heights, in input order.
    /// Unknown heights are skipped.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        next_block_height = %self.tip_state.get().next_block_height,
    ))]
    pub async fn block_hashes_for_heights(
        &self,
        heights: impl IntoIterator<Item = BlockHeight>,
    ) -> Result<Vec<CryptoHash>, ChainError> {
        let heights = heights.into_iter().collect::<Vec<_>>();
        Ok(self
            .block_hashes
            .multi_get(&heights)
            .await?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Resets the chain manager for the next block height.
    async fn reset_chain_manager(
        &mut self,
        next_height: BlockHeight,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        let maybe_committee = self
            .execution_state
            .system
            .current_committee()
            .await
            .with_execution_context(ChainExecutionContext::Block)?;
        let ownership = self.execution_state.system.ownership.get().await?.clone();
        let fallback_owners = maybe_committee
            .iter()
            .flat_map(|(_, committee)| committee.account_keys_and_weights());
        self.pending_validated_blobs.clear();
        self.pending_proposed_blobs.clear();
        self.manager
            .reset(ownership, next_height, local_time, fallback_owners)
    }

    /// Updates the outboxes with the messages sent in the block.
    ///
    /// Returns the set of all recipients.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.header.height
    ))]
    async fn process_outgoing_messages(
        &mut self,
        block: &Block,
        tracked: Option<&ChainIdSet>,
    ) -> Result<Vec<ChainId>, ChainError> {
        // Record the messages of the execution. Messages are understood within an
        // application.
        let recipients = block.recipients();
        let block_height = block.header.height;
        let next_height = self.tip_state.get().next_block_height;

        // Update the outboxes. Every recipient's per-target outbox queue is updated, but the
        // `nonempty_outboxes` and `outbox_counters` indices are only populated for targets we track.
        let targets = recipients.into_iter().collect::<Vec<_>>();
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        let mut scheduled_tracked = Vec::new();
        for (mut outbox, target) in outboxes.into_iter().zip(&targets) {
            if block_height > next_height {
                // There may be a gap in the chain before this block. We can only add it to this
                // outbox if the previous message to the same recipient has already been added.
                if *outbox.next_height_to_schedule.get() > block_height {
                    continue; // We already added this recipient's messages to the outbox.
                }
                let maybe_prev_hash = match outbox.next_height_to_schedule.get().try_sub_one().ok()
                {
                    Some(height) => {
                        Some(self.block_hashes.get(&height).await?.ok_or_else(|| {
                            ChainError::CorruptedChainState("missing entry in block_hashes".into())
                        })?)
                    }
                    None => None, // No message to that sender was added yet.
                };
                // Only schedule if this block contains the next message for that recipient.
                match (
                    maybe_prev_hash,
                    block.body.previous_message_blocks.get(target),
                ) {
                    (None, None) => {
                        // No previous message block expected and none indicated by the outbox -
                        // all good
                    }
                    (Some(_), None) => {
                        // The outbox already has a previous height for this recipient,
                        // but this block's body recorded no predecessor — that means
                        // this is the first non-`CheckpointAck` send to this recipient,
                        // even though earlier `CheckpointAck`-only blocks have already
                        // been added to the off-chain outbox. We can still schedule:
                        // the bundle will carry `previous_height = None`, which the
                        // receiver accepts as "first ever".
                    }
                    (None, Some((_, prev_msg_block_height))) => {
                        // We have no previously processed block in the outbox, but we are
                        // expecting one - this could be due to an empty outbox having been pruned.
                        // Only process the outbox if the height of the previous message block is
                        // lower than the tip
                        if *prev_msg_block_height >= next_height {
                            continue;
                        }
                    }
                    (Some(ref prev_hash), Some((prev_msg_block_hash, _))) => {
                        // Only process the outbox if the hashes match. A mismatch can
                        // arise legitimately when intermediate `CheckpointAck`-only
                        // blocks sit in the off-chain outbox but are skipped from
                        // `body.previous_message_blocks`; same fallback as the
                        // `(Some, None)` arm above.
                        if prev_hash != prev_msg_block_hash {
                            continue;
                        }
                    }
                }
            }
            if outbox.schedule_message(block_height)?
                && tracked.is_none_or(|set| set.contains(target))
            {
                scheduled_tracked.push(*target);
            }
            #[cfg(with_metrics)]
            crate::outbox::metrics::OUTBOX_SIZE
                .with_label_values(&[])
                .observe(outbox.queue.count() as f64);
        }

        if !scheduled_tracked.is_empty() {
            // All scheduled messages are at `block_height`.
            let scheduled_count =
                u32::try_from(scheduled_tracked.len()).map_err(|_| ArithmeticError::Overflow)?;
            *self
                .outbox_counters
                .get_mut()
                .entry(block_height)
                .or_default() += scheduled_count;
            let nonempty_outboxes = self.nonempty_outboxes.get_mut();
            for target in &scheduled_tracked {
                nonempty_outboxes.insert(*target);
            }
        }

        #[cfg(with_metrics)]
        metrics::NUM_OUTBOXES
            .with_label_values(&[])
            .observe(self.nonempty_outboxes.get().len() as f64);
        #[cfg(with_metrics)]
        metrics::OUTBOX_COUNTERS_SIZE
            .with_label_values(&[])
            .observe(self.outbox_counters.get().len() as f64);
        Ok(targets)
    }

    /// Updates the per-stream event trackers from the events emitted by the block: advances
    /// `next_index` over contiguous new events (which might not be contiguous when preprocessing
    /// a block), and advances each stream's readable floor (`first_index`) when the block is the
    /// first to emit to it since a checkpoint. Returns the set of updated event streams.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id(),
        block_height = %block.header.height
    ))]
    async fn process_emitted_events(
        &mut self,
        block: &Block,
    ) -> Result<BTreeSet<StreamId>, ChainError> {
        // A stream's events within a single block are a contiguous run (each event takes the
        // next sequential index), so the lowest and highest index it emits here are all we need.
        let mut emitted_ranges = BTreeMap::<StreamId, (u32, u32)>::new();
        for event in block.body.events.iter().flatten() {
            emitted_ranges
                .entry(event.stream_id.clone())
                .and_modify(|(lo, hi)| {
                    *lo = (*lo).min(event.index);
                    *hi = (*hi).max(event.index);
                })
                .or_insert((event.index, event.index));
        }
        let mut stream_ids = Vec::new();
        let mut ranges = Vec::new();
        for (stream_id, range) in emitted_ranges {
            stream_ids.push(stream_id);
            ranges.push(range);
        }

        let mut updated_streams = BTreeSet::new();
        for ((stream_id, counts), (lo, hi)) in self
            .next_expected_events
            .multi_get_pairs(stream_ids)
            .await?
            .into_iter()
            .zip(ranges)
        {
            let mut counts = counts.unwrap_or_default();
            let next = hi.saturating_add(1);
            if block.body.previous_event_blocks.contains_key(&stream_id) {
                // The stream already published since the most recent checkpoint, so we expect its
                // events to continue contiguously. If they don't, we have a gap (missing events
                // since the checkpoint) and leave the tracker untouched; the floor is preserved.
                if lo != counts.next_index {
                    continue;
                }
                counts.next_index = next;
            } else if lo >= counts.first_index {
                // First block to emit to this stream since the most recent checkpoint (which
                // clears `previous_event_blocks`): `lo` is the new readable floor, everything
                // earlier was pruned. The `>=` guard keeps a checkpoint seen out of order — an
                // earlier one preprocessed after a later one — from lowering the floor.
                counts.first_index = lo;
                counts.next_index = counts.next_index.max(next);
            } else {
                // An earlier checkpoint era, already superseded by a later one we recorded.
                continue;
            }
            updated_streams.insert(stream_id.clone());
            self.next_expected_events.insert(&stream_id, counts)?;
        }
        Ok(updated_streams)
    }
}

#[test]
fn empty_block_size() {
    let size = bcs::serialized_size(&crate::block::Block::new(
        crate::test::make_first_block(
            linera_execution::test_utils::dummy_chain_description(0).id(),
        ),
        crate::data_types::BlockExecutionOutcome::default(),
    ))
    .unwrap();
    assert_eq!(size, EMPTY_BLOCK_SIZE);
}
