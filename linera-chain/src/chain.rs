// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use async_graphql::SimpleObject;
use futures::stream::{self, StreamExt, TryStreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ArithmeticError, Blob, BlockHeight, OracleResponse, Timestamp,
        UserApplicationDescription,
    },
    ensure,
    identifiers::{
        BlobId, ChainId, ChannelName, Destination, GenericApplicationId, MessageId, Owner,
        StreamId, UserApplicationId,
    },
};
use linera_execution::{
    committee::ValidatorName, system::OpenChainConfig, ExecutionOutcome, ExecutionRuntimeContext,
    ExecutionStateView, Message, MessageContext, Operation, OperationContext, Query, QueryContext,
    RawExecutionOutcome, RawOutgoingMessage, ResourceController, ResourceTracker, Response,
    ServiceRuntimeEndpoint, TransactionTracker,
};
use linera_views::{
    context::Context,
    log_view::LogView,
    map_view::MapView,
    queue_view::QueueView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    set_view::SetView,
    views::{ClonableView, CryptoHashView, RootView, View},
};
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{
        BlockExecutionOutcome, ChainAndHeight, ChannelFullName, EventRecord, IncomingBundle,
        MessageAction, MessageBundle, Origin, OutgoingMessage, PostedMessage, ProposedBlock,
        Target, Transaction,
    },
    inbox::{Cursor, InboxError, InboxStateView},
    manager::ChainManager,
    outbox::OutboxStateView,
    types::ValidatedBlockCertificate,
    ChainError, ChainExecutionContext, ExecutionResultExt,
};

#[cfg(test)]
#[path = "unit_tests/chain_tests.rs"]
mod chain_tests;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        bucket_interval, bucket_latencies, register_histogram_vec, register_int_counter_vec,
        MeasureLatency,
    },
    prometheus::{HistogramVec, IntCounterVec},
};

#[cfg(with_metrics)]
static NUM_BLOCKS_EXECUTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec("num_blocks_executed", "Number of blocks executed", &[])
});

#[cfg(with_metrics)]
static BLOCK_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "block_execution_latency",
        "Block execution latency",
        &[],
        bucket_latencies(50.0),
    )
});

#[cfg(with_metrics)]
static MESSAGE_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "message_execution_latency",
        "Message execution latency",
        &[],
        bucket_latencies(2.5),
    )
});

#[cfg(with_metrics)]
static OPERATION_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "operation_execution_latency",
        "Operation execution latency",
        &[],
        bucket_latencies(2.5),
    )
});

#[cfg(with_metrics)]
static WASM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "wasm_fuel_used_per_block",
        "Wasm fuel used per block",
        &[],
        bucket_interval(10.0, 500_000.0),
    )
});

#[cfg(with_metrics)]
static WASM_NUM_READS_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "wasm_num_reads_per_block",
        "Wasm number of reads per block",
        &[],
        bucket_interval(0.1, 100.0),
    )
});

#[cfg(with_metrics)]
static WASM_BYTES_READ_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "wasm_bytes_read_per_block",
        "Wasm number of bytes read per block",
        &[],
        bucket_interval(0.1, 10_000_000.0),
    )
});

#[cfg(with_metrics)]
static WASM_BYTES_WRITTEN_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "wasm_bytes_written_per_block",
        "Wasm number of bytes written per block",
        &[],
        bucket_interval(0.1, 10_000_000.0),
    )
});

#[cfg(with_metrics)]
static STATE_HASH_COMPUTATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "state_hash_computation_latency",
        "Time to recompute the state hash",
        &[],
        bucket_latencies(5.0),
    )
});

/// The BCS-serialized size of an empty [`Block`].
const EMPTY_BLOCK_SIZE: usize = 91;

/// An origin, cursor and timestamp of a unskippable bundle in our inbox.
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct TimestampedBundleInInbox {
    /// The origin and cursor of the bundle.
    pub entry: BundleInInbox,
    /// The timestamp when the bundle was added to the inbox.
    pub seen: Timestamp,
}

/// An origin and cursor of a unskippable bundle that is no longer in our inbox.
#[derive(
    Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject,
)]
pub struct BundleInInbox {
    /// The origin from which we received the bundle.
    pub origin: Origin,
    /// The cursor of the bundle in the inbox.
    pub cursor: Cursor,
}

impl BundleInInbox {
    fn new(origin: Origin, bundle: &MessageBundle) -> Self {
        BundleInInbox {
            cursor: Cursor::from(bundle),
            origin,
        }
    }
}

/// A view accessing the state of a chain.
#[derive(Debug, RootView, ClonableView, SimpleObject)]
#[graphql(cache_control(no_cache))]
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
    #[graphql(skip)]
    pub pending_validated_block: RegisterView<C, Option<ValidatedBlockCertificate>>,
    /// The incomplete set of blobs for the pending validated block.
    pub pending_validated_blobs: MapView<C, BlobId, Option<Blob>>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: LogView<C, CryptoHash>,
    /// Sender chain and height of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, ChainAndHeight>,
    /// The number of `received_log` entries we have synchronized, for each validator.
    pub received_certificate_trackers: RegisterView<C, HashMap<ValidatorName, u64>>,

    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: ReentrantCollectionView<C, Origin, InboxStateView<C>>,
    /// A queue of unskippable bundles, with the timestamp when we added them to the inbox.
    pub unskippable_bundles: QueueView<C, TimestampedBundleInInbox>,
    /// Unskippable bundles that have been removed but are still in the queue.
    pub removed_unskippable_bundles: SetView<C, BundleInInbox>,
    /// Mailboxes used to send messages, indexed by their target.
    pub outboxes: ReentrantCollectionView<C, Target, OutboxStateView<C>>,
    /// Number of outgoing messages in flight for each block height.
    /// We use a `RegisterView` to prioritize speed for small maps.
    pub outbox_counters: RegisterView<C, BTreeMap<BlockHeight, u32>>,
    /// Channels able to multicast messages to subscribers.
    pub channels: ReentrantCollectionView<C, ChannelFullName, ChannelStateView<C>>,
}

/// Block-chaining state.
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize, SimpleObject)]
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

    /// Returns `true` if the next block will be the first, i.e. the chain doesn't have any blocks.
    pub fn is_first_block(&self) -> bool {
        self.next_block_height == BlockHeight::ZERO
    }

    /// Checks if the measurement counters would be valid.
    pub fn verify_counters(
        &self,
        new_block: &ProposedBlock,
        outcome: &BlockExecutionOutcome,
    ) -> Result<(), ChainError> {
        let num_incoming_bundles = u32::try_from(new_block.incoming_bundles.len())
            .map_err(|_| ArithmeticError::Overflow)?;
        self.num_incoming_bundles
            .checked_add(num_incoming_bundles)
            .ok_or(ArithmeticError::Overflow)?;

        let num_operations =
            u32::try_from(new_block.operations.len()).map_err(|_| ArithmeticError::Overflow)?;
        self.num_operations
            .checked_add(num_operations)
            .ok_or(ArithmeticError::Overflow)?;

        let num_outgoing_messages =
            u32::try_from(outcome.messages.len()).map_err(|_| ArithmeticError::Overflow)?;
        self.num_outgoing_messages
            .checked_add(num_outgoing_messages)
            .ok_or(ArithmeticError::Overflow)?;

        Ok(())
    }
}

/// The state of a channel followed by subscribers.
#[derive(Debug, ClonableView, View, SimpleObject)]
pub struct ChannelStateView<C>
where
    C: Context + Send + Sync,
{
    /// The current subscribers.
    pub subscribers: SetView<C, ChainId>,
    /// The block heights so far, to be sent to future subscribers.
    pub block_heights: LogView<C, BlockHeight>,
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
    ) -> Result<Response, ChainError> {
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
        application_id: UserApplicationId,
    ) -> Result<UserApplicationDescription, ChainError> {
        self.execution_state
            .system
            .registry
            .describe_application(application_id)
            .await
            .with_execution_context(ChainExecutionContext::DescribeApplication)
    }

    pub async fn mark_messages_as_received(
        &mut self,
        target: &Target,
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
                .expect("message counter should be present");
            *counter = counter
                .checked_sub(1)
                .expect("message counter should not underflow");
            if *counter == 0 {
                // Important for the test in `all_messages_delivered_up_to`.
                self.outbox_counters.get_mut().remove(&update);
            }
        }
        if outbox.queue.count() == 0 {
            self.outboxes.remove_entry(target)?;
        }
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

    /// Returns whether this chain has been closed.
    pub fn is_closed(&self) -> bool {
        *self.execution_state.system.closed.get()
    }

    /// Invariant for the states of active chains.
    pub fn ensure_is_active(&self) -> Result<(), ChainError> {
        if self.is_active() {
            Ok(())
        } else {
            Err(ChainError::InactiveChain(self.chain_id()))
        }
    }

    /// Verifies that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub async fn validate_incoming_bundles(&self) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let pairs = self.inboxes.try_load_all_entries().await?;
        let max_stream_queries = self.context().max_stream_queries();
        let stream = stream::iter(pairs)
            .map(|(origin, inbox)| async move {
                if let Some(bundle) = inbox.removed_bundles.front().await? {
                    return Err(ChainError::MissingCrossChainUpdate {
                        chain_id,
                        origin: origin.into(),
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
        origin: &Origin,
    ) -> Result<BlockHeight, ChainError> {
        let inbox = self.inboxes.try_load_entry(origin).await?;
        match inbox {
            Some(inbox) => inbox.next_block_height_to_receive(),
            None => Ok(BlockHeight::from(0)),
        }
    }

    pub async fn last_anticipated_block_height(
        &self,
        origin: &Origin,
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
        origin: &Origin,
        bundle: MessageBundle,
        local_time: Timestamp,
        add_to_received_log: bool,
    ) -> Result<bool, ChainError> {
        assert!(!bundle.messages.is_empty());
        let chain_id = self.chain_id();
        tracing::trace!(
            "Processing new messages to {chain_id:.8} from {origin} at height {}",
            bundle.height,
        );
        let chain_and_height = ChainAndHeight {
            chain_id: origin.sender,
            height: bundle.height,
        };
        let mut subscribe_names_and_ids = Vec::new();
        let mut unsubscribe_names_and_ids = Vec::new();

        // Handle immediate messages.
        for posted_message in &bundle.messages {
            if let Some(config) = posted_message.message.matches_open_chain() {
                if self.execution_state.system.description.get().is_none() {
                    let message_id = chain_and_height.to_message_id(posted_message.index);
                    self.execute_init_message(message_id, config, bundle.timestamp, local_time)
                        .await?;
                }
            } else if let Some((id, subscription)) = posted_message.message.matches_subscribe() {
                subscribe_names_and_ids.push((subscription.name.clone(), *id));
            }
            if let Some((id, subscription)) = posted_message.message.matches_unsubscribe() {
                unsubscribe_names_and_ids.push((subscription.name.clone(), *id));
            }
        }
        self.process_unsubscribes(unsubscribe_names_and_ids, GenericApplicationId::System)
            .await?;
        let new_outbox_entries = self
            .process_subscribes(subscribe_names_and_ids, GenericApplicationId::System)
            .await?;

        if bundle.goes_to_inbox() {
            // Process the inbox bundle and update the inbox state.
            let mut inbox = self.inboxes.try_load_entry_mut(origin).await?;
            let entry = BundleInInbox::new(origin.clone(), &bundle);
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
        }

        // Remember the certificate for future validator/client synchronizations.
        if add_to_received_log {
            self.received_log.push(chain_and_height);
        }
        Ok(new_outbox_entries)
    }

    /// Updates the `received_log` trackers.
    pub fn update_received_certificate_trackers(
        &mut self,
        new_trackers: BTreeMap<ValidatorName, u64>,
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

    pub async fn execute_init_message(
        &mut self,
        message_id: MessageId,
        config: &OpenChainConfig,
        timestamp: Timestamp,
        local_time: Timestamp,
    ) -> Result<bool, ChainError> {
        // Initialize ourself.
        self.execution_state
            .system
            .initialize_chain(message_id, timestamp, config.clone());
        // Recompute the state hash.
        let hash = self.execution_state.crypto_hash().await?;
        self.execution_state_hash.set(Some(hash));
        let maybe_committee = self.execution_state.system.current_committee().into_iter();
        // Last, reset the consensus state based on the current ownership.
        self.manager.reset(
            self.execution_state.system.ownership.get().clone(),
            BlockHeight(0),
            local_time,
            maybe_committee.flat_map(|(_, committee)| committee.keys_and_weights()),
        )?;
        Ok(true)
    }

    /// Removes the incoming message bundles in the block from the inboxes.
    pub async fn remove_bundles_from_inboxes(
        &mut self,
        timestamp: Timestamp,
        incoming_bundles: &[IncomingBundle],
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
            let bundles = bundles_by_origin.entry(origin).or_default();
            bundles.push(bundle);
        }
        let origins = bundles_by_origin.keys().copied();
        let inboxes = self.inboxes.try_load_entries_mut(origins).await?;
        let mut removed_unskippable = HashSet::new();
        for ((origin, bundles), mut inbox) in bundles_by_origin.into_iter().zip(inboxes) {
            tracing::trace!(
                "Removing {:?} from {chain_id:.8}'s inbox for {origin:}",
                bundles
                    .iter()
                    .map(|bundle| bundle.height)
                    .collect::<Vec<_>>()
            );
            for bundle in bundles {
                // Mark the message as processed in the inbox.
                let was_present = inbox
                    .remove_bundle(bundle)
                    .await
                    .map_err(|error| ChainError::from((chain_id, origin.clone(), error)))?;
                if was_present && !bundle.is_skippable() {
                    removed_unskippable.insert(BundleInInbox::new(origin.clone(), bundle));
                }
            }
        }
        if !removed_unskippable.is_empty() {
            // Delete all removed bundles from the front of the unskippable queue.
            let maybe_front = self.unskippable_bundles.front().await?;
            if maybe_front.is_some_and(|ts_entry| removed_unskippable.remove(&ts_entry.entry)) {
                self.unskippable_bundles.delete_front();
                while let Some(ts_entry) = self.unskippable_bundles.front().await? {
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
                    self.unskippable_bundles.delete_front();
                }
            }
            for entry in removed_unskippable {
                self.removed_unskippable_bundles.insert(&entry)?;
            }
        }
        Ok(())
    }

    /// Executes a block: first the incoming messages, then the main operation.
    /// * Modifies the state of outboxes and channels, if needed.
    /// * As usual, in case of errors, `self` may not be consistent any more and should be thrown
    ///   away.
    /// * Returns the outcome of the execution.
    pub async fn execute_block(
        &mut self,
        block: &ProposedBlock,
        local_time: Timestamp,
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    ) -> Result<BlockExecutionOutcome, ChainError> {
        #[cfg(with_metrics)]
        let _execution_latency = BLOCK_EXECUTION_LATENCY.measure_latency();

        let chain_id = self.chain_id();
        assert_eq!(block.chain_id, chain_id);
        // The first incoming message of any child chain must be `OpenChain`. A root chain must
        // already be initialized
        if block.height == BlockHeight::ZERO
            && self
                .execution_state
                .system
                .description
                .get()
                .map_or(true, |description| description.is_child())
        {
            let (in_bundle, posted_message, config) = block
                .starts_with_open_chain_message()
                .ok_or_else(|| ChainError::InactiveChain(chain_id))?;
            if !self.is_active() {
                let message_id = MessageId {
                    chain_id: in_bundle.origin.sender,
                    height: in_bundle.bundle.height,
                    index: posted_message.index,
                };
                self.execute_init_message(message_id, config, block.timestamp, local_time)
                    .await?;
            }
        }

        ensure!(
            *self.execution_state.system.timestamp.get() <= block.timestamp,
            ChainError::InvalidBlockTimestamp
        );
        self.execution_state.system.timestamp.set(block.timestamp);
        let Some((_, committee)) = self.execution_state.system.current_committee() else {
            return Err(ChainError::InactiveChain(chain_id));
        };
        let mut resource_controller = ResourceController {
            policy: Arc::new(committee.policy().clone()),
            tracker: ResourceTracker::default(),
            account: block.authenticated_signer,
        };
        resource_controller
            .track_block_size(EMPTY_BLOCK_SIZE)
            .and_then(|()| {
                resource_controller
                    .track_executed_block_size_sequence_extension(0, block.incoming_bundles.len())
            })
            .and_then(|()| {
                resource_controller
                    .track_executed_block_size_sequence_extension(0, block.operations.len())
            })
            .with_execution_context(ChainExecutionContext::Block)?;

        if self.is_closed() {
            ensure!(
                !block.incoming_bundles.is_empty() && block.has_only_rejected_messages(),
                ChainError::ClosedChain
            );
        }
        let app_permissions = self.execution_state.system.application_permissions.get();
        let mut mandatory = HashSet::<UserApplicationId>::from_iter(
            app_permissions.mandatory_applications.iter().cloned(),
        );
        for operation in &block.operations {
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
        for pending in block.incoming_messages() {
            if mandatory.is_empty() {
                break;
            }
            if let Message::User { application_id, .. } = &pending.message {
                mandatory.remove(application_id);
            }
        }
        ensure!(
            mandatory.is_empty(),
            ChainError::MissingMandatoryApplications(mandatory.into_iter().collect())
        );

        // Execute each incoming bundle as a transaction, then each operation.
        // Collect messages, events and oracle responses, each as one list per transaction.
        let mut replaying_oracle_responses = replaying_oracle_responses.map(Vec::into_iter);
        let mut next_message_index = 0;
        let mut oracle_responses = Vec::new();
        let mut events = Vec::new();
        let mut messages = Vec::new();
        for (txn_index, transaction) in block.transactions() {
            let chain_execution_context = match transaction {
                Transaction::ReceiveMessages(_) => ChainExecutionContext::IncomingBundle(txn_index),
                Transaction::ExecuteOperation(_) => ChainExecutionContext::Operation(txn_index),
            };
            let maybe_responses = match replaying_oracle_responses.as_mut().map(Iterator::next) {
                Some(Some(responses)) => Some(responses),
                Some(None) => return Err(ChainError::MissingOracleResponseList),
                None => None,
            };
            let mut txn_tracker = TransactionTracker::new(next_message_index, maybe_responses);
            match transaction {
                Transaction::ReceiveMessages(incoming_bundle) => {
                    resource_controller
                        .track_block_size_of(&incoming_bundle)
                        .with_execution_context(chain_execution_context)?;
                    for (message_id, posted_message) in incoming_bundle.messages_and_ids() {
                        Box::pin(self.execute_message_in_block(
                            message_id,
                            posted_message,
                            incoming_bundle,
                            block,
                            txn_index,
                            local_time,
                            &mut txn_tracker,
                            &mut resource_controller,
                        ))
                        .await?;
                    }
                }
                Transaction::ExecuteOperation(operation) => {
                    resource_controller
                        .track_block_size_of(&operation)
                        .with_execution_context(chain_execution_context)?;
                    #[cfg(with_metrics)]
                    let _operation_latency = OPERATION_EXECUTION_LATENCY.measure_latency();
                    let context = OperationContext {
                        chain_id,
                        height: block.height,
                        index: Some(txn_index),
                        authenticated_signer: block.authenticated_signer,
                        authenticated_caller_id: None,
                    };
                    Box::pin(self.execution_state.execute_operation(
                        context,
                        local_time,
                        operation.clone(),
                        &mut txn_tracker,
                        &mut resource_controller,
                    ))
                    .await
                    .with_execution_context(chain_execution_context)?;
                    resource_controller
                        .with_state(&mut self.execution_state)
                        .await?
                        .track_operation(operation)
                        .with_execution_context(chain_execution_context)?;
                }
            }

            self.execution_state
                .update_execution_outcomes_with_app_registrations(&mut txn_tracker)
                .await
                .with_execution_context(chain_execution_context)?;
            let (txn_outcomes, txn_oracle_responses, new_next_message_index) = txn_tracker
                .destructure()
                .with_execution_context(chain_execution_context)?;
            next_message_index = new_next_message_index;
            let (txn_messages, txn_events) = self
                .process_execution_outcomes(block.height, txn_outcomes)
                .await?;
            if matches!(
                transaction,
                Transaction::ExecuteOperation(_)
                    | Transaction::ReceiveMessages(IncomingBundle {
                        action: MessageAction::Accept,
                        ..
                    })
            ) {
                for message_out in &txn_messages {
                    resource_controller
                        .with_state(&mut self.execution_state)
                        .await?
                        .track_message(&message_out.message)
                        .with_execution_context(chain_execution_context)?;
                }
            }
            resource_controller
                .track_block_size_of(&(&txn_oracle_responses, &txn_messages, &txn_events))
                .with_execution_context(chain_execution_context)?;
            resource_controller
                .track_executed_block_size_sequence_extension(oracle_responses.len(), 1)
                .with_execution_context(chain_execution_context)?;
            resource_controller
                .track_executed_block_size_sequence_extension(messages.len(), 1)
                .with_execution_context(chain_execution_context)?;
            resource_controller
                .track_executed_block_size_sequence_extension(events.len(), 1)
                .with_execution_context(chain_execution_context)?;
            oracle_responses.push(txn_oracle_responses);
            messages.push(txn_messages);
            events.push(txn_events);
        }

        // Finally, charge for the block fee, except if the chain is closed. Closed chains should
        // always be able to reject incoming messages.
        if !self.is_closed() {
            resource_controller
                .with_state(&mut self.execution_state)
                .await?
                .track_block()
                .with_execution_context(ChainExecutionContext::Block)?;
        }

        // Recompute the state hash.
        let state_hash = {
            #[cfg(with_metrics)]
            let _hash_latency = STATE_HASH_COMPUTATION_LATENCY.measure_latency();
            self.execution_state.crypto_hash().await?
        };
        self.execution_state_hash.set(Some(state_hash));
        // Last, reset the consensus state based on the current ownership.
        let maybe_committee = self.execution_state.system.current_committee().into_iter();

        self.pending_validated_blobs.clear();
        self.pending_validated_block.set(None);
        self.manager.reset(
            self.execution_state.system.ownership.get().clone(),
            block.height.try_add_one()?,
            local_time,
            maybe_committee.flat_map(|(_, committee)| committee.keys_and_weights()),
        )?;

        #[cfg(with_metrics)]
        {
            // Log Prometheus metrics
            NUM_BLOCKS_EXECUTED.with_label_values(&[]).inc();
            WASM_FUEL_USED_PER_BLOCK
                .with_label_values(&[])
                .observe(resource_controller.tracker.fuel as f64);
            WASM_NUM_READS_PER_BLOCK
                .with_label_values(&[])
                .observe(resource_controller.tracker.read_operations as f64);
            WASM_BYTES_READ_PER_BLOCK
                .with_label_values(&[])
                .observe(resource_controller.tracker.bytes_read as f64);
            WASM_BYTES_WRITTEN_PER_BLOCK
                .with_label_values(&[])
                .observe(resource_controller.tracker.bytes_written as f64);
        }

        assert_eq!(
            messages.len(),
            block.incoming_bundles.len() + block.operations.len()
        );
        let outcome = BlockExecutionOutcome {
            messages,
            state_hash,
            oracle_responses,
            events,
        };
        Ok(outcome)
    }

    /// Executes a message as part of an incoming bundle in a block.
    #[expect(clippy::too_many_arguments)]
    async fn execute_message_in_block(
        &mut self,
        message_id: MessageId,
        posted_message: &PostedMessage,
        incoming_bundle: &IncomingBundle,
        block: &ProposedBlock,
        txn_index: u32,
        local_time: Timestamp,
        txn_tracker: &mut TransactionTracker,
        resource_controller: &mut ResourceController<Option<Owner>>,
    ) -> Result<(), ChainError> {
        #[cfg(with_metrics)]
        let _message_latency = MESSAGE_EXECUTION_LATENCY.measure_latency();
        let context = MessageContext {
            chain_id: block.chain_id,
            is_bouncing: posted_message.is_bouncing(),
            height: block.height,
            certificate_hash: incoming_bundle.bundle.certificate_hash,
            message_id,
            authenticated_signer: posted_message.authenticated_signer,
            refund_grant_to: posted_message.refund_grant_to,
        };
        let mut grant = posted_message.grant;
        match incoming_bundle.action {
            MessageAction::Accept => {
                // Once a chain is closed, accepting incoming messages is not allowed.
                ensure!(!self.is_closed(), ChainError::ClosedChain);

                Box::pin(self.execution_state.execute_message(
                    context,
                    local_time,
                    posted_message.message.clone(),
                    (grant > Amount::ZERO).then_some(&mut grant),
                    txn_tracker,
                    resource_controller,
                ))
                .await
                .with_execution_context(ChainExecutionContext::IncomingBundle(txn_index))?;
                if grant > Amount::ZERO {
                    if let Some(refund_grant_to) = posted_message.refund_grant_to {
                        self.execution_state
                            .send_refund(context, grant, refund_grant_to, txn_tracker)
                            .await
                            .with_execution_context(ChainExecutionContext::IncomingBundle(
                                txn_index,
                            ))?;
                    }
                }
            }
            MessageAction::Reject => {
                // If rejecting a message fails, the entire block proposal should be
                // scrapped.
                ensure!(
                    !posted_message.is_protected() || self.is_closed(),
                    ChainError::CannotRejectMessage {
                        chain_id: block.chain_id,
                        origin: Box::new(incoming_bundle.origin.clone()),
                        posted_message: Box::new(posted_message.clone()),
                    }
                );
                if posted_message.is_tracked() {
                    // Bounce the message.
                    self.execution_state
                        .bounce_message(context, grant, posted_message.message.clone(), txn_tracker)
                        .await
                        .with_execution_context(ChainExecutionContext::Block)?;
                } else if grant > Amount::ZERO {
                    // Nothing to do except maybe refund the grant.
                    let Some(refund_grant_to) = posted_message.refund_grant_to else {
                        // See OperationContext::refund_grant_to()
                        return Err(ChainError::InternalError(
                            "Messages with grants should have a non-empty `refund_grant_to`".into(),
                        ));
                    };
                    // Refund grant.
                    self.execution_state
                        .send_refund(context, posted_message.grant, refund_grant_to, txn_tracker)
                        .await
                        .with_execution_context(ChainExecutionContext::Block)?;
                }
            }
        }
        Ok(())
    }

    async fn process_execution_outcomes(
        &mut self,
        height: BlockHeight,
        results: Vec<ExecutionOutcome>,
    ) -> Result<(Vec<OutgoingMessage>, Vec<EventRecord>), ChainError> {
        let mut messages = Vec::new();
        let mut events = Vec::new();
        for result in results {
            match result {
                ExecutionOutcome::System(result) => {
                    self.process_raw_execution_outcome(
                        GenericApplicationId::System,
                        Message::System,
                        &mut messages,
                        &mut events,
                        height,
                        result,
                    )
                    .await?;
                }
                ExecutionOutcome::User(application_id, result) => {
                    self.process_raw_execution_outcome(
                        GenericApplicationId::User(application_id),
                        |bytes| Message::User {
                            application_id,
                            bytes,
                        },
                        &mut messages,
                        &mut events,
                        height,
                        result,
                    )
                    .await?;
                }
            }
        }
        Ok((messages, events))
    }

    async fn process_raw_execution_outcome<E, F>(
        &mut self,
        application_id: GenericApplicationId,
        lift: F,
        messages: &mut Vec<OutgoingMessage>,
        events: &mut Vec<EventRecord>,
        height: BlockHeight,
        raw_outcome: RawExecutionOutcome<E, Amount>,
    ) -> Result<(), ChainError>
    where
        F: Fn(E) -> Message,
    {
        events.extend(
            raw_outcome
                .events
                .into_iter()
                .map(|(stream_name, key, value)| EventRecord {
                    stream_id: StreamId {
                        application_id,
                        stream_name,
                    },
                    key,
                    value,
                }),
        );
        let max_stream_queries = self.context().max_stream_queries();
        // Record the messages of the execution. Messages are understood within an
        // application.
        let mut recipients = HashSet::new();
        let mut channel_broadcasts = HashSet::new();
        for RawOutgoingMessage {
            destination,
            authenticated,
            grant,
            kind,
            message,
        } in raw_outcome.messages
        {
            match &destination {
                Destination::Recipient(id) => {
                    recipients.insert(*id);
                }
                Destination::Subscribers(name) => {
                    ensure!(grant == Amount::ZERO, ChainError::GrantUseOnBroadcast);
                    channel_broadcasts.insert(name.clone());
                }
            }
            let authenticated_signer = raw_outcome.authenticated_signer.filter(|_| authenticated);
            let refund_grant_to = raw_outcome.refund_grant_to.filter(|_| grant > Amount::ZERO);
            messages.push(OutgoingMessage {
                destination,
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                message: lift(message),
            });
        }

        // Update the (regular) outboxes.
        let outbox_counters = self.outbox_counters.get_mut();
        let targets = recipients
            .into_iter()
            .map(Target::chain)
            .collect::<Vec<_>>();
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        for mut outbox in outboxes {
            if outbox.schedule_message(height)? {
                *outbox_counters.entry(height).or_default() += 1;
            }
        }

        // Update the channels.
        self.process_unsubscribes(raw_outcome.unsubscribe, application_id)
            .await?;

        let full_names = channel_broadcasts
            .into_iter()
            .map(|name| ChannelFullName {
                application_id,
                name,
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        let stream = full_names.into_iter().zip(channels);
        let stream = stream::iter(stream)
            .map(|(full_name, mut channel)| async move {
                let recipients = channel.subscribers.indices().await?;
                channel.block_heights.push(height);
                let targets = recipients
                    .into_iter()
                    .map(|recipient| Target::channel(recipient, full_name.clone()))
                    .collect::<Vec<_>>();
                Ok::<_, ChainError>(targets)
            })
            .buffer_unordered(max_stream_queries);
        let infos = stream.try_collect::<Vec<_>>().await?;
        let targets = infos.into_iter().flatten().collect::<Vec<_>>();
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        let outbox_counters = self.outbox_counters.get_mut();
        for mut outbox in outboxes {
            if outbox.schedule_message(height)? {
                *outbox_counters.entry(height).or_default() += 1;
            }
        }

        self.process_subscribes(raw_outcome.subscribe, application_id)
            .await?;
        Ok(())
    }

    /// Processes new subscriptions. Returns `true` if at least one new subscriber was added for
    /// which we have outgoing messages.
    async fn process_subscribes(
        &mut self,
        names_and_ids: Vec<(ChannelName, ChainId)>,
        application_id: GenericApplicationId,
    ) -> Result<bool, ChainError> {
        if names_and_ids.is_empty() {
            return Ok(false);
        }
        let full_names = names_and_ids
            .iter()
            .map(|(name, _)| ChannelFullName {
                application_id,
                name: name.clone(),
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        let subscribe_channels = names_and_ids.into_iter().zip(channels);
        let max_stream_queries = self.context().max_stream_queries();
        let stream = stream::iter(subscribe_channels)
            .map(|((name, id), mut channel)| async move {
                if channel.subscribers.contains(&id).await? {
                    return Ok(None); // Was already a subscriber.
                }
                let full_name = ChannelFullName {
                    application_id,
                    name,
                };
                tracing::trace!("Adding subscriber {id:.8} for {full_name:}");
                channel.subscribers.insert(&id)?;
                // Send all messages.
                let heights = channel.block_heights.read(..).await?;
                if heights.is_empty() {
                    return Ok(None); // No messages on this channel yet.
                }
                let target = Target::channel(id, full_name.clone());
                Ok::<_, ChainError>(Some((target, heights)))
            })
            .buffer_unordered(max_stream_queries);
        let infos = stream.try_collect::<Vec<_>>().await?;
        let (targets, heights): (Vec<_>, Vec<_>) = infos.into_iter().flatten().unzip();
        let mut new_outbox_entries = false;
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        let outbox_counters = self.outbox_counters.get_mut();
        for (heights, mut outbox) in heights.into_iter().zip(outboxes) {
            for height in heights {
                if outbox.schedule_message(height)? {
                    *outbox_counters.entry(height).or_default() += 1;
                    new_outbox_entries = true;
                }
            }
        }
        Ok(new_outbox_entries)
    }

    async fn process_unsubscribes(
        &mut self,
        names_and_ids: Vec<(ChannelName, ChainId)>,
        application_id: GenericApplicationId,
    ) -> Result<(), ChainError> {
        if names_and_ids.is_empty() {
            return Ok(());
        }
        let full_names = names_and_ids
            .iter()
            .map(|(name, _)| ChannelFullName {
                application_id,
                name: name.clone(),
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        for ((_name, id), mut channel) in names_and_ids.into_iter().zip(channels) {
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id)?;
        }
        Ok(())
    }
}

#[test]
fn empty_block_size() {
    let executed_block = crate::data_types::ExecutedBlock {
        block: crate::test::make_first_block(ChainId::root(0)),
        outcome: crate::data_types::BlockExecutionOutcome::default(),
    };
    let size = bcs::serialized_size(&crate::block::Block::new(
        executed_block.block,
        executed_block.outcome,
    ))
    .unwrap();
    assert_eq!(size, EMPTY_BLOCK_SIZE);
}
