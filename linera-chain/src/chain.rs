// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
};

use async_graphql::SimpleObject;
use futures::stream::{self, StreamExt, TryStreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ArithmeticError, BlockHeight, OracleResponse, Timestamp},
    ensure,
    identifiers::{ChainId, Destination, GenericApplicationId, MessageId, StreamId},
};
use linera_execution::{
    system::SystemMessage, ExecutionOutcome, ExecutionRequest, ExecutionRuntimeContext,
    ExecutionStateView, Message, MessageContext, Operation, OperationContext, Query, QueryContext,
    RawExecutionOutcome, RawOutgoingMessage, ResourceController, ResourceTracker, Response,
    ServiceRuntimeRequest, UserApplicationDescription, UserApplicationId,
};
use linera_views::{
    common::Context,
    log_view::LogView,
    queue_view::QueueView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    set_view::SetView,
    views::{ClonableView, CryptoHashView, RootView, View, ViewError},
};
use serde::{Deserialize, Serialize};
#[cfg(with_testing)]
use {linera_base::identifiers::BytecodeId, linera_execution::BytecodeLocation};

use crate::{
    data_types::{
        Block, BlockExecutionOutcome, ChainAndHeight, ChannelFullName, EventRecord, IncomingBundle,
        MessageAction, MessageBundle, Origin, OutgoingMessage, Target,
    },
    inbox::{Cursor, InboxError, InboxStateView},
    manager::ChainManager,
    outbox::OutboxStateView,
    ChainError, ChainExecutionContext,
};

#[cfg(test)]
#[path = "unit_tests/chain_tests.rs"]
mod chain_tests;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    prometheus::{HistogramVec, IntCounterVec},
};

#[cfg(with_metrics)]
static NUM_BLOCKS_EXECUTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "num_blocks_executed",
        "Number of blocks executed",
        &[],
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static BLOCK_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "block_execution_latency",
        "Block execution latency",
        &[],
        Some(vec![
            0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
            1.0, 2.5, 5.0, 10.0, 25.0, 50.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static MESSAGE_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "message_execution_latency",
        "Message execution latency",
        &[],
        Some(vec![
            0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
            1.0, 2.5,
        ]),
    )
    .expect("Histogram creation should not fail")
});

#[cfg(with_metrics)]
static OPERATION_EXECUTION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "operation_execution_latency",
        "Operation execution latency",
        &[],
        Some(vec![
            0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
            1.0, 2.5,
        ]),
    )
    .expect("Histogram creation should not fail")
});

#[cfg(with_metrics)]
static WASM_FUEL_USED_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "wasm_fuel_used_per_block",
        "Wasm fuel used per block",
        &[],
        Some(vec![
            50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10_000.0, 25_000.0, 50_000.0,
            100_000.0, 250_000.0, 500_000.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static WASM_NUM_READS_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "wasm_num_reads_per_block",
        "Wasm number of reads per block",
        &[],
        Some(vec![0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 50.0, 100.0]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static WASM_BYTES_READ_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "wasm_bytes_read_per_block",
        "Wasm number of bytes read per block",
        &[],
        Some(vec![
            0.5,
            1.0,
            10.0,
            100.0,
            256.0,
            512.0,
            1024.0,
            2048.0,
            4096.0,
            8192.0,
            16384.0,
            65_536.0,
            524_288.0,
            1_048_576.0,
            8_388_608.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static WASM_BYTES_WRITTEN_PER_BLOCK: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "wasm_bytes_written_per_block",
        "Wasm number of bytes written per block",
        &[],
        Some(vec![
            0.5,
            1.0,
            10.0,
            100.0,
            256.0,
            512.0,
            1024.0,
            2048.0,
            4096.0,
            8192.0,
            16384.0,
            65_536.0,
            524_288.0,
            1_048_576.0,
            8_388_608.0,
        ]),
    )
    .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
static STATE_HASH_COMPUTATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "state_hash_computation_latency",
        "Time to recompute the state hash",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

/// An origin, cursor and timestamp of a unskippable message in our inbox.
#[derive(Debug, Clone, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct TimestampedInboxEntry {
    /// The origin and cursor of the message.
    pub entry: InboxEntry,
    /// The timestamp when the message was added to the inbox.
    pub seen: Timestamp,
}

/// An origin and cursor of a unskippable message that is no longer in our inbox.
#[derive(
    Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject,
)]
pub struct InboxEntry {
    /// The origin from which we received the message.
    pub origin: Origin,
    /// The cursor of the message in the inbox.
    pub cursor: Cursor,
}

impl InboxEntry {
    fn new(origin: Origin, bundle: &MessageBundle) -> Self {
        InboxEntry {
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
    ViewError: From<C::Error>,
{
    /// Execution state, including system and user applications.
    pub execution_state: ExecutionStateView<C>,
    /// Hash of the execution state.
    pub execution_state_hash: RegisterView<C, Option<CryptoHash>>,

    /// Block-chaining state.
    pub tip_state: RegisterView<C, ChainTipState>,

    /// Consensus state.
    pub manager: RegisterView<C, ChainManager>,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: LogView<C, CryptoHash>,
    /// Sender chain and height of all certified blocks known as a receiver (local ordering).
    pub received_log: LogView<C, ChainAndHeight>,

    /// Mailboxes used to receive messages indexed by their origin.
    pub inboxes: ReentrantCollectionView<C, Origin, InboxStateView<C>>,
    /// A queue of unskippable bundles, with the timestamp when we added them to the inbox.
    pub unskippable: QueueView<C, TimestampedInboxEntry>,
    /// Non-skippable bundles that have been removed but are still in the queue.
    pub removed_unskippable: SetView<C, InboxEntry>,
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
    /// Number of incoming messages.
    pub num_incoming_bundles: u32,
    /// Number of operations.
    pub num_operations: u32,
    /// Number of outgoing messages.
    pub num_outgoing_messages: u32,
}

impl ChainTipState {
    /// Checks that the proposed block is suitable, i.e. at the expected height and with the
    /// expected parent.
    pub fn verify_block_chaining(&self, new_block: &Block) -> Result<(), ChainError> {
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
        new_block: &Block,
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
    ViewError: From<C::Error>,
{
    /// The current subscribers.
    pub subscribers: SetView<C, ChainId>,
    /// The latest block height, if any, to be sent to future subscribers.
    pub block_height: RegisterView<C, Option<BlockHeight>>,
}

impl<C> ChainStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
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
        incoming_execution_requests: &mut futures::channel::mpsc::UnboundedReceiver<
            ExecutionRequest,
        >,
        runtime_request_sender: &mut std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) -> Result<Response, ChainError> {
        let context = QueryContext {
            chain_id: self.chain_id(),
            next_block_height: self.tip_state.get().next_block_height,
            local_time,
        };
        let response = self
            .execution_state
            .query_application(
                context,
                query,
                incoming_execution_requests,
                runtime_request_sender,
            )
            .await
            .map_err(|error| ChainError::ExecutionError(error, ChainExecutionContext::Query))?;
        Ok(response)
    }

    /// Reads the [`BytecodeLocation`] for the requested [`BytecodeId`], if it is registered in
    /// this chain's [`ApplicationRegistryView`][`linera_execution::ApplicationRegistryView`].
    #[cfg(with_testing)]
    pub async fn read_bytecode_location(
        &mut self,
        bytecode_id: BytecodeId,
    ) -> Result<Option<BytecodeLocation>, ChainError> {
        self.execution_state
            .system
            .registry
            .bytecode_location_for(&bytecode_id)
            .await
            .map_err(|err| {
                ChainError::ExecutionError(err.into(), ChainExecutionContext::ReadBytecodeLocation)
            })
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
            .map_err(|err| {
                ChainError::ExecutionError(err.into(), ChainExecutionContext::DescribeApplication)
            })
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
    pub fn all_messages_delivered_up_to(&mut self, height: BlockHeight) -> bool {
        tracing::debug!(
            "Messages left in {:?}'s outbox: {:?}",
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
    pub async fn receive_message_bundle(
        &mut self,
        origin: &Origin,
        bundle: MessageBundle,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        tracing::trace!(
            "Processing new messages to {:?} from {:?} at height {}",
            chain_id,
            origin,
            bundle.height,
        );
        let chain_and_height = ChainAndHeight {
            chain_id: origin.sender,
            height: bundle.height,
        };
        if self.execution_state.system.description.get().is_none() {
            // The chain isn't initialized yet. Process the OpenChain message.
            if let Some(posted_message) = bundle.messages.first() {
                let message_id = chain_and_height.to_message_id(posted_message.index);
                let message = &posted_message.message;
                self.execute_init_message(message_id, message, bundle.timestamp, local_time)
                    .await?;
            }
        }
        // Process the inbox bundle and update the inbox state.
        let mut inbox = self.inboxes.try_load_entry_mut(origin).await?;
        let entry = InboxEntry::new(origin.clone(), &bundle);
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
            self.unskippable
                .push_back(TimestampedInboxEntry { entry, seen });
        }
        // Remember the certificate for future validator/client synchronizations.
        self.received_log.push(chain_and_height);
        Ok(())
    }

    pub async fn execute_init_message(
        &mut self,
        message_id: MessageId,
        message: &Message,
        timestamp: Timestamp,
        local_time: Timestamp,
    ) -> Result<bool, ChainError> {
        let Message::System(SystemMessage::OpenChain(config)) = message else {
            return Ok(false);
        };
        // Initialize ourself.
        self.execution_state
            .system
            .initialize_chain(message_id, timestamp, config.clone());
        // Recompute the state hash.
        let hash = self.execution_state.crypto_hash().await?;
        self.execution_state_hash.set(Some(hash));
        let maybe_committee = self.execution_state.system.current_committee().into_iter();
        // Last, reset the consensus state based on the current ownership.
        self.manager.get_mut().reset(
            self.execution_state.system.ownership.get(),
            BlockHeight(0),
            local_time,
            maybe_committee.flat_map(|(_, committee)| committee.keys_and_weights()),
        )?;
        Ok(true)
    }

    /// Removes the incoming messages in the block from the inboxes.
    pub async fn remove_bundles_from_inboxes(&mut self, block: &Block) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let mut bundles_by_origin: BTreeMap<_, Vec<&MessageBundle>> = Default::default();
        for IncomingBundle { bundle, origin, .. } in &block.incoming_bundles {
            ensure!(
                bundle.timestamp <= block.timestamp,
                ChainError::IncorrectBundleTimestamp {
                    chain_id,
                    bundle_timestamp: bundle.timestamp,
                    block_timestamp: block.timestamp,
                }
            );
            let bundles = bundles_by_origin.entry(origin).or_default();
            bundles.push(bundle);
        }
        let origins = bundles_by_origin.keys().copied();
        let inboxes = self.inboxes.try_load_entries_mut(origins).await?;
        let mut removed_unskippable = HashSet::new();
        for ((origin, bundles), mut inbox) in bundles_by_origin.into_iter().zip(inboxes) {
            tracing::trace!("Updating inbox {:?} in chain {:?}", origin, chain_id);
            for bundle in bundles {
                // Mark the message as processed in the inbox.
                let was_present = inbox
                    .remove_bundle(bundle)
                    .await
                    .map_err(|error| ChainError::from((chain_id, origin.clone(), error)))?;
                if was_present && !bundle.is_skippable() {
                    removed_unskippable.insert(InboxEntry::new(origin.clone(), bundle));
                }
            }
        }
        if !removed_unskippable.is_empty() {
            // Delete all removed bundles from the front of the unskippable queue.
            let maybe_front = self.unskippable.front().await?;
            if maybe_front.is_some_and(|ts_entry| removed_unskippable.remove(&ts_entry.entry)) {
                self.unskippable.delete_front();
                while let Some(ts_entry) = self.unskippable.front().await? {
                    if !removed_unskippable.remove(&ts_entry.entry) {
                        if !self.removed_unskippable.contains(&ts_entry.entry).await? {
                            break;
                        }
                        self.removed_unskippable.remove(&ts_entry.entry)?;
                    }
                    self.unskippable.delete_front();
                }
            }
            for entry in removed_unskippable {
                self.removed_unskippable.insert(&entry)?;
            }
        }
        Ok(())
    }

    /// Executes a block: first the incoming messages, then the main operation.
    /// * Modifies the state of inboxes, outboxes, and channels, if needed.
    /// * As usual, in case of errors, `self` may not be consistent any more and should be thrown
    ///   away.
    /// * Returns the list of messages caused by the block being executed.
    pub async fn execute_block(
        &mut self,
        block: &Block,
        local_time: Timestamp,
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    ) -> Result<BlockExecutionOutcome, ChainError> {
        #[cfg(with_metrics)]
        let _execution_latency = BLOCK_EXECUTION_LATENCY.measure_latency();

        let chain_id = self.chain_id();
        assert_eq!(block.chain_id, chain_id);
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

        if self.is_closed() {
            ensure!(
                !block.incoming_bundles.is_empty() && block.has_only_rejected_messages(),
                ChainError::ClosedChain
            );
        }

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
            ensure!(
                block.starts_with_open_chain(),
                ChainError::InactiveChain(self.chain_id())
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
        let mut oracle_responses = Vec::new();
        let mut events = Vec::new();
        let mut messages = Vec::new();
        let mut next_message_index = 0;
        for (bundle_index, incoming_bundle) in (0..).zip(&block.incoming_bundles) {
            let mut txn_oracle_responses = Vec::new();
            let mut txn_messages = Vec::new();
            let mut txn_events = Vec::new();
            let txn_replaying_oracle_responses = match &mut replaying_oracle_responses {
                Some(responses) => Some(Arc::new(Mutex::new(
                    responses
                        .next()
                        .ok_or_else(|| ChainError::MissingOracleResponseList)?
                        .into_iter(),
                ))),
                None => None,
            };
            for (message_id, posted_message) in incoming_bundle.messages_with_id() {
                #[cfg(with_metrics)]
                let _message_latency = MESSAGE_EXECUTION_LATENCY.measure_latency();
                let chain_execution_context = ChainExecutionContext::IncomingBundle(bundle_index);
                let context = MessageContext {
                    chain_id,
                    is_bouncing: posted_message.is_bouncing(),
                    height: block.height,
                    certificate_hash: incoming_bundle.bundle.certificate_hash,
                    message_id,
                    authenticated_signer: posted_message.authenticated_signer,
                    refund_grant_to: posted_message.refund_grant_to,
                    next_message_index,
                };
                let (outcomes, new_oracle_responses) = match incoming_bundle.action {
                    MessageAction::Accept => {
                        // Once a chain is closed, accepting incoming messages is not allowed.
                        ensure!(!self.is_closed(), ChainError::ClosedChain);

                        let mut grant = posted_message.grant;
                        let (mut outcomes, oracle_responses) = self
                            .execution_state
                            .execute_message(
                                context,
                                local_time,
                                posted_message.message.clone(),
                                (grant > Amount::ZERO).then_some(&mut grant),
                                txn_replaying_oracle_responses.clone(),
                                &mut resource_controller,
                            )
                            .await
                            .map_err(|err| {
                                ChainError::ExecutionError(err, chain_execution_context)
                            })?;
                        if grant > Amount::ZERO {
                            if let Some(refund_grant_to) = posted_message.refund_grant_to {
                                let outcome = self
                                    .execution_state
                                    .send_refund(context, grant, refund_grant_to)
                                    .await
                                    .map_err(|err| {
                                        ChainError::ExecutionError(err, chain_execution_context)
                                    })?;
                                outcomes.push(outcome);
                            }
                        }
                        (outcomes, oracle_responses)
                    }
                    MessageAction::Reject => {
                        // If rejecting a message fails, the entire block proposal should be
                        // scrapped.
                        let chain_execution_context = ChainExecutionContext::Block;
                        ensure!(
                            !posted_message.is_protected() || self.is_closed(),
                            ChainError::CannotRejectMessage {
                                chain_id,
                                origin: Box::new(incoming_bundle.origin.clone()),
                                posted_message: posted_message.clone(),
                            }
                        );
                        let outcomes = if posted_message.is_tracked() {
                            // Bounce the message.
                            self.execution_state
                                .bounce_message(
                                    context,
                                    posted_message.grant,
                                    posted_message.refund_grant_to,
                                    posted_message.message.clone(),
                                )
                                .await
                                .map_err(|err| {
                                    ChainError::ExecutionError(err, chain_execution_context)
                                })?
                        } else if posted_message.grant > Amount::ZERO {
                            // Nothing to do except maybe refund the grant.
                            let Some(refund_grant_to) = posted_message.refund_grant_to else {
                                // See OperationContext::refund_grant_to()
                                return Err(ChainError::InternalError(
                                "Messages with grants should have a non-empty `refund_grant_to`"
                                    .into(),
                            ));
                            };
                            // Refund grant.
                            let outcome = self
                                .execution_state
                                .send_refund(context, posted_message.grant, refund_grant_to)
                                .await
                                .map_err(|err| {
                                    ChainError::ExecutionError(err, chain_execution_context)
                                })?;
                            vec![outcome]
                        } else {
                            Vec::new()
                        };
                        (outcomes, Vec::new())
                    }
                };
                txn_oracle_responses.extend(new_oracle_responses);
                let (new_messages, new_events) = self
                    .process_execution_outcomes(context.height, outcomes)
                    .await?;
                if let MessageAction::Accept = incoming_bundle.action {
                    for message_out in &new_messages {
                        resource_controller
                            .with_state(&mut self.execution_state)
                            .await?
                            .track_message(&message_out.message)
                            .map_err(|err| {
                                ChainError::ExecutionError(err, chain_execution_context)
                            })?;
                    }
                }
                next_message_index +=
                    u32::try_from(new_messages.len()).map_err(|_| ArithmeticError::Overflow)?;
                txn_messages.extend(new_messages);
                txn_events.extend(new_events);
            }
            oracle_responses.push(txn_oracle_responses);
            messages.push(txn_messages);
            events.push(txn_events);
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, operation) in (0..).zip(&block.operations) {
            #[cfg(with_metrics)]
            let _operation_latency = OPERATION_EXECUTION_LATENCY.measure_latency();
            let chain_execution_context = ChainExecutionContext::Operation(index);
            let context = OperationContext {
                chain_id,
                height: block.height,
                index: Some(index),
                authenticated_signer: block.authenticated_signer,
                authenticated_caller_id: None,
                next_message_index,
            };
            let (outcomes, txn_oracle_responses) = self
                .execution_state
                .execute_operation(
                    context,
                    local_time,
                    operation.clone(),
                    match &mut replaying_oracle_responses {
                        Some(responses) => Some(
                            responses
                                .next()
                                .ok_or_else(|| ChainError::MissingOracleResponseList)?,
                        ),
                        None => None,
                    },
                    &mut resource_controller,
                )
                .await
                .map_err(|err| ChainError::ExecutionError(err, chain_execution_context))?;
            oracle_responses.push(txn_oracle_responses);
            let (messages_out, new_events) = self
                .process_execution_outcomes(context.height, outcomes)
                .await?;
            resource_controller
                .with_state(&mut self.execution_state)
                .await?
                .track_operation(operation)
                .map_err(|err| ChainError::ExecutionError(err, chain_execution_context))?;
            for message_out in &messages_out {
                resource_controller
                    .with_state(&mut self.execution_state)
                    .await?
                    .track_message(&message_out.message)
                    .map_err(|err| ChainError::ExecutionError(err, chain_execution_context))?;
            }
            next_message_index +=
                u32::try_from(messages_out.len()).map_err(|_| ArithmeticError::Overflow)?;
            messages.push(messages_out);
            events.push(new_events);
        }

        // Finally, charge for the block fee, except if the chain is closed. Closed chains should
        // always be able to reject incoming messages.
        if !self.is_closed() {
            resource_controller
                .with_state(&mut self.execution_state)
                .await?
                .track_block()
                .map_err(|err| ChainError::ExecutionError(err, ChainExecutionContext::Block))?;
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
        self.manager.get_mut().reset(
            self.execution_state.system.ownership.get(),
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
        Ok(BlockExecutionOutcome {
            messages,
            state_hash,
            oracle_responses,
            events,
        })
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
        let full_names = raw_outcome
            .unsubscribe
            .clone()
            .into_iter()
            .map(|(name, _id)| ChannelFullName {
                application_id,
                name,
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        for ((_name, id), mut channel) in raw_outcome.unsubscribe.into_iter().zip(channels) {
            // Remove subscriber. Do not remove the channel outbox yet.
            channel.subscribers.remove(&id)?;
        }
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
                channel.block_height.set(Some(height));
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
        for mut outbox in outboxes {
            if outbox.schedule_message(height)? {
                *outbox_counters.entry(height).or_default() += 1;
            }
        }
        let full_names = raw_outcome
            .subscribe
            .clone()
            .into_iter()
            .map(|(name, _id)| ChannelFullName {
                application_id,
                name,
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        let stream = raw_outcome.subscribe.into_iter().zip(channels);
        let stream = stream::iter(stream)
            .map(|((name, id), mut channel)| async move {
                let mut result = None;
                let full_name = ChannelFullName {
                    application_id,
                    name,
                };
                // Add subscriber.
                if !channel.subscribers.contains(&id).await? {
                    // Send the latest message if any.
                    if let Some(latest_height) = channel.block_height.get() {
                        let target = Target::channel(id, full_name.clone());
                        result = Some((target, *latest_height));
                    }
                    channel.subscribers.insert(&id)?;
                }
                Ok::<_, ChainError>(result)
            })
            .buffer_unordered(max_stream_queries);
        let infos = stream.try_collect::<Vec<_>>().await?;
        let (targets, heights): (Vec<_>, Vec<_>) = infos.into_iter().flatten().unzip();
        let outboxes = self.outboxes.try_load_entries_mut(&targets).await?;
        for (height, mut outbox) in heights.into_iter().zip(outboxes) {
            if outbox.schedule_message(height)? {
                *outbox_counters.entry(height).or_default() += 1;
            }
        }
        Ok(())
    }
}
