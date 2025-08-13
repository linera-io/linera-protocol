// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::RangeBounds,
    sync::Arc,
};

use futures::stream::{self, StreamExt, TryStreamExt};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{
        ApplicationDescription, ApplicationPermissions, ArithmeticError, Blob, BlockHeight,
        BlockHeightRangeBounds as _, Epoch, OracleResponse, Timestamp,
    },
    ensure,
    identifiers::{AccountOwner, ApplicationId, BlobType, ChainId, StreamId},
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::Committee, ExecutionRuntimeContext, ExecutionStateView, Message, Operation,
    OutgoingMessage, Query, QueryContext, QueryOutcome, ResourceController, ResourceTracker,
    ServiceRuntimeEndpoint, TransactionTracker,
};
use linera_views::{
    bucket_queue_view::BucketQueueView,
    context::Context,
    log_view::LogView,
    map_view::MapView,
    reentrant_collection_view::{ReadGuardedView, ReentrantCollectionView},
    register_view::RegisterView,
    set_view::SetView,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, CryptoHashView, RootView, View},
};
use serde::{Deserialize, Serialize};

use crate::{
    block::{Block, ConfirmedBlock},
    block_tracker::BlockExecutionTracker,
    data_types::{
        BlockExecutionOutcome, ChainAndHeight, IncomingBundle, MessageBundle, ProposedBlock,
        Transaction,
    },
    inbox::{Cursor, InboxError, InboxStateView},
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
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram_vec,
        register_int_counter_vec,
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
            exponential_bucket_latencies(1000.0),
        )
    });

    #[cfg(with_metrics)]
    pub static MESSAGE_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "message_execution_latency",
            "Message execution latency",
            &[],
            exponential_bucket_latencies(50.0),
        )
    });

    pub static OPERATION_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "operation_execution_latency",
            "Operation execution latency",
            &[],
            exponential_bucket_latencies(50.0),
        )
    });

    pub static WASM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "wasm_fuel_used_per_block",
            "Wasm fuel used per block",
            &[],
            exponential_bucket_interval(10.0, 1_000_000.0),
        )
    });

    pub static EVM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "evm_fuel_used_per_block",
            "EVM fuel used per block",
            &[],
            exponential_bucket_interval(10.0, 1_000_000.0),
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
            "Time to recompute the state hash",
            &[],
            exponential_bucket_latencies(500.0),
        )
    });

    pub static NUM_INBOXES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_inboxes",
            "Number of inboxes",
            &[],
            exponential_bucket_interval(1.0, 10_000.0),
        )
    });

    pub static NUM_OUTBOXES: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "num_outboxes",
            "Number of outboxes",
            &[],
            exponential_bucket_interval(1.0, 10_000.0),
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

/// An origin, cursor and timestamp of a unskippable bundle in our inbox.
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedBundleInInbox {
    /// The origin and cursor of the bundle.
    pub entry: BundleInInbox,
    /// The timestamp when the bundle was added to the inbox.
    pub seen: Timestamp,
}

/// An origin and cursor of a unskippable bundle that is no longer in our inbox.
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct BundleInInbox {
    /// The origin from which we received the bundle.
    pub origin: ChainId,
    /// The cursor of the bundle in the inbox.
    pub cursor: Cursor,
}

impl BundleInInbox {
    fn new(origin: ChainId, bundle: &MessageBundle) -> Self {
        BundleInInbox {
            cursor: Cursor::from(bundle),
            origin,
        }
    }
}

// The `TimestampedBundleInInbox` is a relatively small type, so a total
// of 100 seems reasonable for the storing of the data.
const TIMESTAMPBUNDLE_BUCKET_SIZE: usize = 100;

/// A view accessing the state of a chain.
#[cfg_attr(
    with_graphql,
    derive(async_graphql::SimpleObject),
    graphql(cache_control(no_cache))
)]
#[derive(Debug, RootView, ClonableView)]
pub struct ChainStateView<C>
where
    C: Clone + Context + Send + Sync + 'static,
{
    /// Execution state, including system and user applications.
    pub execution_state: ExecutionStateView<C>,
    /// Hash of the execution state.
    pub execution_state_hash: RegisterView<C, Option<CryptoHash>>,

    /// Block-chaining state.
    pub tip_state: RegisterView<C, ChainTipState>,

    /// Consensus state.
    pub manager: ChainManager<C>,
    /// Pending validated block that is still missing blobs.
    /// The incomplete set of blobs for the pending validated block.
    pub pending_validated_blobs: PendingBlobsView<C>,
    /// The incomplete sets of blobs for upcoming proposals.
    pub pending_proposed_blobs: ReentrantCollectionView<C, AccountOwner, PendingBlobsView<C>>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: LogView<C, CryptoHash>,
    /// Sender chain and height of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, ChainAndHeight>,
    /// The number of `received_log` entries we have synchronized, for each validator.
    pub received_certificate_trackers: RegisterView<C, HashMap<ValidatorPublicKey, u64>>,

    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: ReentrantCollectionView<C, ChainId, InboxStateView<C>>,
    /// A queue of unskippable bundles, with the timestamp when we added them to the inbox.
    pub unskippable_bundles:
        BucketQueueView<C, TimestampedBundleInInbox, TIMESTAMPBUNDLE_BUCKET_SIZE>,
    /// Unskippable bundles that have been removed but are still in the queue.
    pub removed_unskippable_bundles: SetView<C, BundleInInbox>,
    /// The heights of previous blocks that sent messages to the same recipients.
    pub previous_message_blocks: MapView<C, ChainId, BlockHeight>,
    /// The heights of previous blocks that published events to the same streams.
    pub previous_event_blocks: MapView<C, StreamId, BlockHeight>,
    /// Mailboxes used to send messages, indexed by their target.
    pub outboxes: ReentrantCollectionView<C, ChainId, OutboxStateView<C>>,
    /// Number of outgoing messages in flight for each block height.
    /// We use a `RegisterView` to prioritize speed for small maps.
    pub outbox_counters: RegisterView<C, BTreeMap<BlockHeight, u32>>,
    /// Outboxes with at least one pending message. This allows us to avoid loading all outboxes.
    pub nonempty_outboxes: RegisterView<C, BTreeSet<ChainId>>,

    /// Blocks that have been verified but not executed yet, and that may not be contiguous.
    pub preprocessed_blocks: MapView<C, BlockHeight, CryptoHash>,
}

/// Block-chaining state.
#[cfg_attr(with_graphql, derive(async_graphql::SimpleObject))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Returns the [`ChainId`] of the chain this [`ChainStateView`] represents.
    pub fn chain_id(&self) -> ChainId {
        self.context().extra().chain_id()
    }

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

    pub async fn mark_messages_as_received(
        &mut self,
        target: &ChainId,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let mut outbox = self.outboxes.try_load_entry_mut(target).await?;
        let updates = outbox.mark_messages_as_received(height).await?;
        if updates.is_empty() {
            return Ok(false);
        }
        for update in updates {
            let counter = self
                .outbox_counters
                .get_mut()
                .get_mut(&update)
                .ok_or_else(|| {
                    ChainError::InternalError("message counter should be present".into())
                })?;
            *counter = counter.checked_sub(1).ok_or(ArithmeticError::Underflow)?;
            if *counter == 0 {
                // Important for the test in `all_messages_delivered_up_to`.
                self.outbox_counters.get_mut().remove(&update);
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
            .observe(self.outboxes.count().await? as f64);
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
    pub fn is_active(&self) -> bool {
        self.execution_state.system.is_active()
    }

    /// Invariant for the states of active chains.
    pub async fn ensure_is_active(&mut self, local_time: Timestamp) -> Result<(), ChainError> {
        // Initialize ourselves.
        if self
            .execution_state
            .system
            .initialize_chain(self.chain_id())
            .await
            .with_execution_context(ChainExecutionContext::Block)?
        {
            // the chain was already initialized
            return Ok(());
        }
        // Recompute the state hash.
        let hash = self.execution_state.crypto_hash().await?;
        self.execution_state_hash.set(Some(hash));
        let maybe_committee = self.execution_state.system.current_committee().into_iter();
        // Last, reset the consensus state based on the current ownership.
        self.manager.reset(
            self.execution_state.system.ownership.get().clone(),
            BlockHeight(0),
            local_time,
            maybe_committee.flat_map(|(_, committee)| committee.account_keys_and_weights()),
        )?;
        Ok(())
    }

    /// Verifies that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub async fn validate_incoming_bundles(&self) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let pairs = self.inboxes.try_load_all_entries().await?;
        let max_stream_queries = self.context().store().max_stream_queries();
        let stream = stream::iter(pairs)
            .map(|(origin, inbox)| async move {
                if let Some(bundle) = inbox.removed_bundles.front().await? {
                    return Err(ChainError::MissingCrossChainUpdate {
                        chain_id,
                        origin,
                        height: bundle.height,
                    });
                }
                Ok::<(), ChainError>(())
            })
            .buffer_unordered(max_stream_queries);
        stream.try_collect::<Vec<_>>().await?;
        Ok(())
    }

    pub async fn next_block_height_to_receive(
        &self,
        origin: &ChainId,
    ) -> Result<BlockHeight, ChainError> {
        let inbox = self.inboxes.try_load_entry(origin).await?;
        match inbox {
            Some(inbox) => inbox.next_block_height_to_receive(),
            None => Ok(BlockHeight::ZERO),
        }
    }

    pub async fn last_anticipated_block_height(
        &self,
        origin: &ChainId,
    ) -> Result<Option<BlockHeight>, ChainError> {
        let inbox = self.inboxes.try_load_entry(origin).await?;
        match inbox {
            Some(inbox) => match inbox.removed_bundles.back().await? {
                Some(bundle) => Ok(Some(bundle.height)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Attempts to process a new `bundle` of messages from the given `origin`. Returns an
    /// internal error if the bundle doesn't appear to be new, based on the sender's
    /// height. The value `local_time` is specific to each validator and only used for
    /// round timeouts.
    ///
    /// Returns `true` if incoming `Subscribe` messages created new outbox entries.
    pub async fn receive_message_bundle(
        &mut self,
        origin: &ChainId,
        bundle: MessageBundle,
        local_time: Timestamp,
        add_to_received_log: bool,
    ) -> Result<(), ChainError> {
        assert!(!bundle.messages.is_empty());
        let chain_id = self.chain_id();
        tracing::trace!(
            "Processing new messages to {chain_id:.8} from {origin} at height {}",
            bundle.height,
        );
        let chain_and_height = ChainAndHeight {
            chain_id: *origin,
            height: bundle.height,
        };

        match self.ensure_is_active(local_time).await {
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
        let mut inbox = self.inboxes.try_load_entry_mut(origin).await?;
        #[cfg(with_metrics)]
        metrics::NUM_INBOXES
            .with_label_values(&[])
            .observe(self.inboxes.count().await? as f64);
        let entry = BundleInInbox::new(*origin, &bundle);
        let skippable = bundle.is_skippable();
        let newly_added = inbox
            .add_bundle(bundle)
            .await
            .map_err(|error| match error {
                InboxError::ViewError(error) => ChainError::ViewError(error),
                error => ChainError::InternalError(format!(
                    "while processing messages in certified block: {error}"
                )),
            })?;
        if newly_added && !skippable {
            let seen = local_time;
            self.unskippable_bundles
                .push_back(TimestampedBundleInInbox { entry, seen });
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

    pub fn current_committee(&self) -> Result<(Epoch, &Committee), ChainError> {
        self.execution_state
            .system
            .current_committee()
            .ok_or_else(|| ChainError::InactiveChain(self.chain_id()))
    }

    pub fn ownership(&self) -> &ChainOwnership {
        self.execution_state.system.ownership.get()
    }

    /// Removes the incoming message bundles in the block from the inboxes.
    pub async fn remove_bundles_from_inboxes(
        &mut self,
        timestamp: Timestamp,
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
        let mut removed_unskippable = HashSet::new();
        for ((origin, bundles), mut inbox) in bundles_by_origin.into_iter().zip(inboxes) {
            tracing::trace!(
                "Removing [{}] from {chain_id:.8}'s inbox for {origin:}",
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
                if was_present && !bundle.is_skippable() {
                    removed_unskippable.insert(BundleInInbox::new(origin, bundle));
                }
            }
        }
        if !removed_unskippable.is_empty() {
            // Delete all removed bundles from the front of the unskippable queue.
            let maybe_front = self.unskippable_bundles.front();
            if maybe_front.is_some_and(|ts_entry| removed_unskippable.remove(&ts_entry.entry)) {
                self.unskippable_bundles.delete_front().await?;
                while let Some(ts_entry) = self.unskippable_bundles.front() {
                    if !removed_unskippable.remove(&ts_entry.entry) {
                        if !self
                            .removed_unskippable_bundles
                            .contains(&ts_entry.entry)
                            .await?
                        {
                            break;
                        }
                        self.removed_unskippable_bundles.remove(&ts_entry.entry)?;
                    }
                    self.unskippable_bundles.delete_front().await?;
                }
            }
            for entry in removed_unskippable {
                self.removed_unskippable_bundles.insert(&entry)?;
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_INBOXES
            .with_label_values(&[])
            .observe(self.inboxes.count().await? as f64);
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
        optional_vec.ok_or_else(|| ChainError::InternalError("Missing outboxes".into()))
    }

    /// Executes a block: first the incoming messages, then the main operation.
    /// Does not update chain state other than the execution state.
    #[expect(clippy::too_many_arguments)]
    async fn execute_block_inner(
        chain: &mut ExecutionStateView<C>,
        confirmed_log: &LogView<C, CryptoHash>,
        previous_message_blocks_view: &MapView<C, ChainId, BlockHeight>,
        previous_event_blocks_view: &MapView<C, StreamId, BlockHeight>,
        block: &ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    ) -> Result<BlockExecutionOutcome, ChainError> {
        #[cfg(with_metrics)]
        let _execution_latency = metrics::BLOCK_EXECUTION_LATENCY.measure_latency();
        chain.system.timestamp.set(block.timestamp);

        let policy = chain
            .system
            .current_committee()
            .ok_or_else(|| ChainError::InactiveChain(block.chain_id))?
            .1
            .policy()
            .clone();

        let mut resource_controller = ResourceController::new(
            Arc::new(policy),
            ResourceTracker::default(),
            block.authenticated_signer,
        );

        for blob in published_blobs {
            let blob_id = blob.id();
            resource_controller
                .policy()
                .check_blob_size(blob.content())
                .with_execution_context(ChainExecutionContext::Block)?;
            chain.system.used_blobs.insert(&blob_id)?;
        }

        // Execute each incoming bundle as a transaction, then each operation.
        // Collect messages, events and oracle responses, each as one list per transaction.
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

        for transaction in block.transaction_refs() {
            block_execution_tracker
                .execute_transaction(transaction, round, chain)
                .await?;
        }

        let recipients = block_execution_tracker.recipients();
        let mut previous_message_blocks = BTreeMap::new();
        for recipient in recipients {
            if let Some(height) = previous_message_blocks_view.get(&recipient).await? {
                let hash = confirmed_log
                    .get(usize::try_from(height.0).map_err(|_| ArithmeticError::Overflow)?)
                    .await?
                    .ok_or_else(|| {
                        ChainError::InternalError("missing entry in confirmed_log".into())
                    })?;
                previous_message_blocks.insert(recipient, (hash, height));
            }
        }

        let streams = block_execution_tracker.event_streams();
        let mut previous_event_blocks = BTreeMap::new();
        for stream in streams {
            if let Some(height) = previous_event_blocks_view.get(&stream).await? {
                let hash = confirmed_log
                    .get(usize::try_from(height.0).map_err(|_| ArithmeticError::Overflow)?)
                    .await?
                    .ok_or_else(|| {
                        ChainError::InternalError("missing entry in confirmed_log".into())
                    })?;
                previous_event_blocks.insert(stream, (hash, height));
            }
        }

        let state_hash = {
            #[cfg(with_metrics)]
            let _hash_latency = metrics::STATE_HASH_COMPUTATION_LATENCY.measure_latency();
            chain.crypto_hash().await?
        };

        let (messages, oracle_responses, events, blobs, operation_results) =
            block_execution_tracker.finalize();

        Ok(BlockExecutionOutcome {
            messages,
            previous_message_blocks,
            previous_event_blocks,
            state_hash,
            oracle_responses,
            events,
            blobs,
            operation_results,
        })
    }

    /// Executes a block: first the incoming messages, then the main operation.
    /// Does not update chain state other than the execution state.
    pub async fn execute_block(
        &mut self,
        block: &ProposedBlock,
        local_time: Timestamp,
        round: Option<u32>,
        published_blobs: &[Blob],
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    ) -> Result<BlockExecutionOutcome, ChainError> {
        assert_eq!(
            block.chain_id,
            self.execution_state.context().extra().chain_id()
        );

        self.ensure_is_active(local_time).await?;

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
            self.execution_state.system.application_permissions.get(),
            block,
        )?;

        Self::execute_block_inner(
            &mut self.execution_state,
            &self.confirmed_log,
            &self.previous_message_blocks,
            &self.previous_event_blocks,
            block,
            local_time,
            round,
            published_blobs,
            replaying_oracle_responses,
        )
        .await
    }

    /// Applies an execution outcome to the chain, updating the outboxes, state hash and chain
    /// manager. This does not touch the execution state itself, which must be updated separately.
    pub async fn apply_confirmed_block(
        &mut self,
        block: &ConfirmedBlock,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        let hash = block.inner().hash();
        let block = block.inner().inner();
        self.execution_state_hash.set(Some(block.header.state_hash));
        let recipients = self.process_outgoing_messages(block).await?;

        for recipient in recipients {
            self.previous_message_blocks
                .insert(&recipient, block.header.height)?;
        }
        for event in block.body.events.iter().flatten() {
            self.previous_event_blocks
                .insert(&event.stream_id, block.header.height)?;
        }
        // Last, reset the consensus state based on the current ownership.
        self.reset_chain_manager(block.header.height.try_add_one()?, local_time)?;

        // Advance to next block height.
        let tip = self.tip_state.get_mut();
        tip.block_hash = Some(hash);
        tip.next_block_height.try_add_assign_one()?;
        tip.update_counters(&block.body.transactions, &block.body.messages)?;
        self.confirmed_log.push(hash);
        self.preprocessed_blocks.remove(&block.header.height)?;
        Ok(())
    }

    /// Adds a block to `preprocessed_blocks`, and updates the outboxes where possible.
    pub async fn preprocess_block(&mut self, block: &ConfirmedBlock) -> Result<(), ChainError> {
        let hash = block.inner().hash();
        let block = block.inner().inner();
        let height = block.header.height;
        if height < self.tip_state.get().next_block_height {
            return Ok(());
        }
        self.process_outgoing_messages(block).await?;
        self.preprocessed_blocks.insert(&height, hash)?;
        Ok(())
    }

    /// Returns whether this is a child chain.
    pub fn is_child(&self) -> bool {
        let Some(description) = self.execution_state.system.description.get() else {
            // Root chains are always initialized, so this must be a child chain.
            return true;
        };
        description.is_child()
    }

    /// Verifies that the block is valid according to the chain's application permission settings.
    fn check_app_permissions(
        app_permissions: &ApplicationPermissions,
        block: &ProposedBlock,
    ) -> Result<(), ChainError> {
        let mut mandatory = HashSet::<ApplicationId>::from_iter(
            app_permissions.mandatory_applications.iter().cloned(),
        );
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
                Transaction::ReceiveMessages(incoming_bundle) => {
                    for pending in incoming_bundle.messages() {
                        if let Message::User { application_id, .. } = &pending.message {
                            mandatory.remove(application_id);
                        }
                    }
                }
            }
        }
        ensure!(
            mandatory.is_empty(),
            ChainError::MissingMandatoryApplications(mandatory.into_iter().collect())
        );
        Ok(())
    }

    /// Returns the hashes of all blocks we have in the given range.
    pub async fn block_hashes(
        &self,
        range: impl RangeBounds<BlockHeight>,
    ) -> Result<Vec<CryptoHash>, ChainError> {
        let next_height = self.tip_state.get().next_block_height;
        // If the range is not empty, it can always be represented as start..=end.
        let Some((start, end)) = range.to_inclusive() else {
            return Ok(Vec::new());
        };
        // Everything up to (excluding) next_height is in confirmed_log.
        let mut hashes = if let Ok(last_height) = next_height.try_sub_one() {
            let usize_start = usize::try_from(start)?;
            let usize_end = usize::try_from(end.min(last_height))?;
            self.confirmed_log.read(usize_start..=usize_end).await?
        } else {
            Vec::new()
        };
        // Everything after (including) next_height in preprocessed_blocks if we have it.
        for height in start.max(next_height).0..=end.0 {
            if let Some(hash) = self.preprocessed_blocks.get(&BlockHeight(height)).await? {
                hashes.push(hash);
            }
        }
        Ok(hashes)
    }

    /// Resets the chain manager for the next block height.
    fn reset_chain_manager(
        &mut self,
        next_height: BlockHeight,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        let maybe_committee = self.execution_state.system.current_committee().into_iter();
        let ownership = self.execution_state.system.ownership.get().clone();
        let fallback_owners =
            maybe_committee.flat_map(|(_, committee)| committee.account_keys_and_weights());
        self.pending_validated_blobs.clear();
        self.pending_proposed_blobs.clear();
        self.manager
            .reset(ownership, next_height, local_time, fallback_owners)
    }

    /// Updates the outboxes with the messages sent in the block.
    ///
    /// Returns the set of all recipients.
    async fn process_outgoing_messages(
        &mut self,
        block: &Block,
    ) -> Result<Vec<ChainId>, ChainError> {
        // Record the messages of the execution. Messages are understood within an
        // application.
        let recipients = block.recipients();
        let block_height = block.header.height;
        let next_height = self.tip_state.get().next_block_height;

        // Update the outboxes.
        let outbox_counters = self.outbox_counters.get_mut();
        let nonempty_outboxes = self.nonempty_outboxes.get_mut();
        let targets = recipients.into_iter().collect::<Vec<_>>();
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        for (mut outbox, target) in outboxes.into_iter().zip(&targets) {
            if block_height > next_height {
                // There may be a gap in the chain before this block. We can only add it to this
                // outbox if the previous message to the same recipient has already been added.
                if *outbox.next_height_to_schedule.get() > block_height {
                    continue; // We already added this recipient's messages to the outbox.
                }
                let maybe_prev_hash = match outbox.next_height_to_schedule.get().try_sub_one().ok()
                {
                    // The block with the last added message has already been executed; look up its
                    // hash in the confirmed_log.
                    Some(height) if height < next_height => {
                        let index =
                            usize::try_from(height.0).map_err(|_| ArithmeticError::Overflow)?;
                        Some(self.confirmed_log.get(index).await?.ok_or_else(|| {
                            ChainError::InternalError("missing entry in confirmed_log".into())
                        })?)
                    }
                    // The block with last added message has not been executed yet. If we have it,
                    // it's in preprocessed_blocks.
                    Some(height) => Some(self.preprocessed_blocks.get(&height).await?.ok_or_else(
                        || ChainError::InternalError("missing entry in preprocessed_blocks".into()),
                    )?),
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
                        // Outbox indicates there was a previous message block, but
                        // previous_message_blocks has no idea about it - possible bug
                        return Err(ChainError::InternalError(
                            "block indicates no previous message block,\
                            but we have one in the outbox"
                                .into(),
                        ));
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
                        // Only process the outbox if the hashes match.
                        if prev_hash != prev_msg_block_hash {
                            continue;
                        }
                    }
                }
            }
            if outbox.schedule_message(block_height)? {
                *outbox_counters.entry(block_height).or_default() += 1;
                nonempty_outboxes.insert(*target);
            }
        }

        #[cfg(with_metrics)]
        metrics::NUM_OUTBOXES
            .with_label_values(&[])
            .observe(self.outboxes.count().await? as f64);
        Ok(targets)
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
