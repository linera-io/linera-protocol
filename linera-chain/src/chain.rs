// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{
        Block, BlockExecutionOutcome, ChainAndHeight, ChannelFullName, Event, IncomingMessage,
        Medium, MessageAction, Origin, OutgoingMessage, Target,
    },
    inbox::{InboxError, InboxStateView},
    outbox::OutboxStateView,
    ChainError, ChainExecutionContext, ChainManager,
};
use async_graphql::SimpleObject;
use futures::stream::{self, StreamExt, TryStreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ArithmeticError, BlockHeight, Timestamp},
    ensure,
    identifiers::{ChainId, Destination, MessageId},
    sync::Lazy,
};
use linera_execution::{
    system::{SystemExecutionError, SystemMessage},
    ExecutionError, ExecutionResult, ExecutionRuntimeContext, ExecutionStateView,
    GenericApplicationId, Message, MessageContext, OperationContext, Query, QueryContext,
    RawExecutionResult, RawOutgoingMessage, ResourceTracker, Response, UserApplicationDescription,
    UserApplicationId,
};
use linera_views::{
    common::Context,
    log_view::LogView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    set_view::SetView,
    views::{CryptoHashView, RootView, View, ViewError},
};
use prometheus::{register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    time::Instant,
};

pub static NUM_BLOCKS_EXECUTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("num_blocks_executed", "Number of blocks executed", &[])
        .expect("Counter creation should not fail")
});

pub static BLOCK_EXECUTION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!("block_execution_latency", "Block execution latency", &[])
        .expect("Counter creation should not fail")
});

pub static WASM_FUEL_USED_PER_BLOCK: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!("wasm_fuel_used_per_block", "Wasm fuel used per block", &[])
        .expect("Counter creation should not fail")
});

pub static WASM_NUM_READS_PER_BLOCK: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "wasm_num_reads_per_block",
        "Wasm number of reads per block",
        &[]
    )
    .expect("Counter can be created")
});

pub static WASM_BYTES_READ_PER_BLOCK: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "wasm_bytes_read_per_block",
        "Wasm number of bytes read per block",
        &[]
    )
    .expect("Counter can be created")
});

pub static WASM_BYTES_WRITTEN_PER_BLOCK: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "wasm_bytes_written_per_block",
        "Wasm number of bytes written per block",
        &[]
    )
    .expect("Counter can be created")
});

/// A view accessing the state of a chain.
#[derive(Debug, RootView, SimpleObject)]
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
    pub num_incoming_messages: u32,
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
        let num_incoming_messages = u32::try_from(new_block.incoming_messages.len())
            .map_err(|_| ArithmeticError::Overflow)?;
        self.num_incoming_messages
            .checked_add(num_incoming_messages)
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
#[derive(Debug, View, SimpleObject)]
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
    /// Substracts an amount from a balance and reports an error if that is impossible
    fn sub_assign_fees(
        balance: &mut Amount,
        fees: Amount,
        chain_execution_context: ChainExecutionContext,
    ) -> Result<(), ChainError> {
        balance.try_sub_assign(fees).map_err(|_| {
            ChainError::ExecutionError(
                ExecutionError::SystemError(SystemExecutionError::InsufficientFunding {
                    current_balance: *balance,
                }),
                chain_execution_context,
            )
        })
    }

    pub fn chain_id(&self) -> ChainId {
        self.context().extra().chain_id()
    }

    pub async fn query_application(&mut self, query: Query) -> Result<Response, ChainError> {
        let context = QueryContext {
            chain_id: self.chain_id(),
        };
        let response = self
            .execution_state
            .query_application(context, query)
            .await
            .map_err(|error| ChainError::ExecutionError(error, ChainExecutionContext::Query))?;
        Ok(response)
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
        target: Target,
        height: BlockHeight,
    ) -> Result<bool, ChainError> {
        let mut outbox = self.outboxes.try_load_entry_mut(&target).await?;
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
            self.outboxes.remove_entry(&target)?;
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
    pub async fn validate_incoming_messages(&mut self) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let origins = self.inboxes.indices().await?;
        let inboxes = self.inboxes.try_load_entries(&origins).await?;
        let stream = origins.into_iter().zip(inboxes);
        let max_stream_queries = self.context().max_stream_queries();
        let stream = stream::iter(stream)
            .map(|(origin, inbox)| async move {
                if let Some(event) = inbox.removed_events.front().await? {
                    return Err(ChainError::MissingCrossChainUpdate {
                        chain_id,
                        origin: origin.into(),
                        height: event.height,
                    });
                }
                Ok::<(), ChainError>(())
            })
            .buffer_unordered(max_stream_queries);
        stream.try_collect::<Vec<_>>().await?;
        Ok(())
    }

    pub async fn next_block_height_to_receive(
        &mut self,
        origin: &Origin,
    ) -> Result<BlockHeight, ChainError> {
        let inbox = self.inboxes.try_load_entry(origin).await?;
        inbox.next_block_height_to_receive()
    }

    pub async fn last_anticipated_block_height(
        &mut self,
        origin: &Origin,
    ) -> Result<Option<BlockHeight>, ChainError> {
        let inbox = self.inboxes.try_load_entry(origin).await?;
        match inbox.removed_events.back().await? {
            Some(event) => Ok(Some(event.height)),
            None => Ok(None),
        }
    }

    /// Schedules operations to be executed as a recipient, unless this block was already
    /// processed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights and indices.
    pub async fn receive_block(
        &mut self,
        origin: &Origin,
        height: BlockHeight,
        timestamp: Timestamp,
        messages: Vec<OutgoingMessage>,
        certificate_hash: CryptoHash,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        ensure!(
            height >= self.next_block_height_to_receive(origin).await?,
            ChainError::InternalError("Trying to receive blocks in the wrong order".to_string())
        );
        tracing::trace!(
            "Processing new messages to {:?} from {:?} at height {}",
            chain_id,
            origin,
            height
        );
        // Process immediate messages and create inbox events.
        let mut events = Vec::new();
        for (index, outgoing_message) in messages.into_iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| ArithmeticError::Overflow)?;
            let OutgoingMessage {
                destination,
                authenticated_signer,
                is_protected,
                message,
            } = outgoing_message;
            // Skip events that do not belong to this origin OR have no effect on this
            // recipient.
            match destination {
                Destination::Recipient(id) => {
                    if origin.medium != Medium::Direct || id != chain_id {
                        continue;
                    }
                }
                Destination::Subscribers(name) => {
                    let expected_medium = Medium::Channel(ChannelFullName {
                        application_id: message.application_id(),
                        name,
                    });
                    if origin.medium != expected_medium {
                        continue;
                    }
                }
            }
            if let Message::System(_) = message {
                if self.execution_state.system.description.get().is_none() {
                    // Handle special messages to be executed immediately.
                    let message_id = MessageId {
                        chain_id: origin.sender,
                        height,
                        index,
                    };
                    self.execute_immediate_message(message_id, &message, timestamp, local_time)
                        .await?;
                }
            }
            // Record the inbox event to process it below.
            events.push(Event {
                certificate_hash,
                height,
                index,
                authenticated_signer,
                is_protected,
                timestamp,
                message,
            });
        }
        // There should be inbox events. Otherwise, this means the cross-chain request was
        // not routed correctly.
        ensure!(
            !events.is_empty(),
            ChainError::InternalError(format!(
                "The block received by {:?} from {:?} at height {:?} was entirely ignored. \
                This should not happen",
                chain_id, origin, height
            ))
        );
        // Process the inbox events and update the inbox state.
        let mut inbox = self.inboxes.try_load_entry_mut(origin).await?;
        for event in events {
            inbox.add_event(event).await.map_err(|error| match error {
                InboxError::ViewError(error) => ChainError::ViewError(error),
                error => ChainError::InternalError(format!(
                    "while processing messages in certified block: {error}"
                )),
            })?;
        }
        // Remember the certificate for future validator/client synchronizations.
        self.received_log.push(ChainAndHeight {
            chain_id: origin.sender,
            height,
        });
        Ok(())
    }

    pub async fn execute_immediate_message(
        &mut self,
        message_id: MessageId,
        message: &Message,
        timestamp: Timestamp,
        local_time: Timestamp,
    ) -> Result<(), ChainError> {
        if let Message::System(SystemMessage::OpenChain {
            ownership,
            epoch,
            committees,
            admin_id,
            balance,
        }) = message
        {
            // Initialize ourself.
            self.execution_state.system.open_chain(
                message_id,
                ownership.clone(),
                *epoch,
                committees.clone(),
                *admin_id,
                timestamp,
                *balance,
            );
            // Recompute the state hash.
            let hash = self.execution_state.crypto_hash().await?;
            self.execution_state_hash.set(Some(hash));
            // Last, reset the consensus state based on the current ownership.
            self.manager.get_mut().reset(
                self.execution_state.system.ownership.get(),
                BlockHeight(0),
                local_time,
            )?;
        }
        Ok(())
    }

    /// Removes the incoming messages in the block from the inboxes.
    pub async fn remove_events_from_inboxes(&mut self, block: &Block) -> Result<(), ChainError> {
        let chain_id = self.chain_id();
        let mut events_by_origin: BTreeMap<_, Vec<&Event>> = Default::default();
        for IncomingMessage { event, origin, .. } in &block.incoming_messages {
            ensure!(
                event.timestamp <= block.timestamp,
                ChainError::IncorrectEventTimestamp {
                    chain_id,
                    message_timestamp: event.timestamp,
                    block_timestamp: block.timestamp,
                }
            );
            let events = events_by_origin.entry(origin).or_default();
            events.push(event);
        }
        let origins = events_by_origin.keys().copied();
        let inboxes = self.inboxes.try_load_entries_mut(origins).await?;
        for ((origin, events), mut inbox) in events_by_origin.into_iter().zip(inboxes) {
            tracing::trace!("Updating inbox {:?} in chain {:?}", origin, chain_id);
            for event in events {
                // Mark the message as processed in the inbox.
                inbox
                    .remove_event(event)
                    .await
                    .map_err(|error| ChainError::from((chain_id, origin.clone(), error)))?;
            }
        }
        Ok(())
    }

    /// Executes a new block: first the incoming messages, then the main operation.
    /// * Modifies the state of inboxes, outboxes, and channels, if needed.
    /// * As usual, in case of errors, `self` may not be consistent any more and should be thrown
    ///   away.
    /// * Returns the list of messages caused by the block being executed.
    pub async fn execute_block(
        &mut self,
        block: &Block,
        local_time: Timestamp,
    ) -> Result<BlockExecutionOutcome, ChainError> {
        let start_time = Instant::now();

        assert_eq!(block.chain_id, self.chain_id());
        let chain_id = self.chain_id();
        ensure!(
            *self.execution_state.system.timestamp.get() <= block.timestamp,
            ChainError::InvalidBlockTimestamp
        );
        self.execution_state.system.timestamp.set(block.timestamp);
        let Some((_, committee)) = self.execution_state.system.current_committee() else {
            return Err(ChainError::InactiveChain(chain_id));
        };

        let policy = committee.policy().clone();
        let mut messages = Vec::new();
        let mut message_counts = Vec::new();
        let maximum_bytes_left_to_read = policy.maximum_bytes_read_per_block;
        let maximum_bytes_left_to_write = policy.maximum_bytes_written_per_block;
        let mut tracker = ResourceTracker {
            used_fuel: 0,
            num_reads: 0,
            bytes_read: 0,
            bytes_written: 0,
            maximum_bytes_left_to_read,
            maximum_bytes_left_to_write,
            stored_size_delta: 0,
        };
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
                matches!(
                    block.incoming_messages.first(),
                    Some(IncomingMessage {
                        event: Event {
                            message: Message::System(SystemMessage::OpenChain { .. }),
                            ..
                        },
                        action: MessageAction::Accept,
                        ..
                    })
                ),
                ChainError::InactiveChain(self.chain_id())
            );
        }
        for (index, message) in block.incoming_messages.iter().enumerate() {
            if let MessageAction::Reject = message.action {
                ensure!(
                    !message.event.is_protected,
                    ChainError::CannotRejectMessage {
                        chain_id,
                        origin: Box::new(message.origin.clone()),
                        event: message.event.clone(),
                    }
                );
                // Skip execution of rejected message.
                continue;
            }
            let index = u32::try_from(index).map_err(|_| ArithmeticError::Overflow)?;
            let chain_execution_context = ChainExecutionContext::IncomingMessage(index);
            // Execute the received message.
            let context = MessageContext {
                chain_id,
                height: block.height,
                certificate_hash: message.event.certificate_hash,
                message_id: MessageId {
                    chain_id: message.origin.sender,
                    height: message.event.height,
                    index: message.event.index,
                },
                authenticated_signer: message.event.authenticated_signer,
            };
            let results = self
                .execution_state
                .execute_message(
                    context,
                    message.event.message.clone(),
                    &policy,
                    &mut tracker,
                )
                .await
                .map_err(|err| ChainError::ExecutionError(err, chain_execution_context))?;
            let mut messages_out = self
                .process_execution_results(context.height, results)
                .await?;
            let balance = self.execution_state.system.balance.get_mut();
            Self::sub_assign_fees(
                balance,
                policy.storage_bytes_written_price_raw(&message)?,
                chain_execution_context,
            )?;
            Self::sub_assign_fees(
                balance,
                policy.messages_price(&messages_out)?,
                chain_execution_context,
            )?;
            messages.append(&mut messages_out);
            message_counts
                .push(u32::try_from(messages.len()).map_err(|_| ArithmeticError::Overflow)?);
        }
        // Second, execute the operations in the block and remember the recipients to notify.
        for (index, operation) in block.operations.iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| ArithmeticError::Overflow)?;
            let chain_execution_context = ChainExecutionContext::Operation(index);
            let next_message_index =
                u32::try_from(messages.len()).map_err(|_| ArithmeticError::Overflow)?;
            let context = OperationContext {
                chain_id,
                height: block.height,
                index,
                authenticated_signer: block.authenticated_signer,
                next_message_index,
            };
            let results = self
                .execution_state
                .execute_operation(context, operation.clone(), &policy, &mut tracker)
                .await
                .map_err(|err| ChainError::ExecutionError(err, chain_execution_context))?;
            let mut messages_out = self
                .process_execution_results(context.height, results)
                .await?;
            let balance = self.execution_state.system.balance.get_mut();
            Self::sub_assign_fees(
                balance,
                policy.storage_bytes_written_price_raw(&operation)?,
                chain_execution_context,
            )?;
            Self::sub_assign_fees(
                balance,
                policy.messages_price(&messages_out)?,
                chain_execution_context,
            )?;
            messages.append(&mut messages_out);
            message_counts
                .push(u32::try_from(messages.len()).map_err(|_| ArithmeticError::Overflow)?);
        }
        let balance = self.execution_state.system.balance.get_mut();
        Self::sub_assign_fees(
            balance,
            policy.certificate_price(),
            ChainExecutionContext::Block,
        )?;

        // Recompute the state hash.
        let state_hash = self.execution_state.crypto_hash().await?;
        self.execution_state_hash.set(Some(state_hash));
        // Last, reset the consensus state based on the current ownership.
        self.manager.get_mut().reset(
            self.execution_state.system.ownership.get(),
            block.height.try_add_one()?,
            local_time,
        )?;

        // Log Prometheus metrics
        NUM_BLOCKS_EXECUTED.with_label_values(&[]).inc();
        BLOCK_EXECUTION_LATENCY
            .with_label_values(&[])
            .observe(start_time.elapsed().as_secs_f64());
        WASM_FUEL_USED_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.used_fuel as f64);
        WASM_NUM_READS_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.num_reads as f64);
        WASM_BYTES_READ_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.bytes_read as f64);
        WASM_BYTES_WRITTEN_PER_BLOCK
            .with_label_values(&[])
            .observe(tracker.bytes_written as f64);
        Ok(BlockExecutionOutcome {
            messages,
            message_counts,
            state_hash,
        })
    }

    async fn process_execution_results(
        &mut self,
        height: BlockHeight,
        results: Vec<ExecutionResult>,
    ) -> Result<Vec<OutgoingMessage>, ChainError> {
        let mut messages = Vec::new();
        for result in results {
            match result {
                ExecutionResult::System(result) => {
                    self.process_raw_execution_result(
                        GenericApplicationId::System,
                        Message::System,
                        &mut messages,
                        height,
                        result,
                    )
                    .await?;
                }
                ExecutionResult::User(application_id, result) => {
                    self.process_raw_execution_result(
                        GenericApplicationId::User(application_id),
                        |bytes| Message::User {
                            application_id,
                            bytes,
                        },
                        &mut messages,
                        height,
                        result,
                    )
                    .await?;
                }
            }
        }
        Ok(messages)
    }

    async fn process_raw_execution_result<E, F>(
        &mut self,
        application_id: GenericApplicationId,
        lift: F,
        messages: &mut Vec<OutgoingMessage>,
        height: BlockHeight,
        raw_result: RawExecutionResult<E>,
    ) -> Result<(), ChainError>
    where
        F: Fn(E) -> Message,
    {
        let max_stream_queries = self.context().max_stream_queries();
        // Record the messages of the execution. Messages are understood within an
        // application.
        let mut recipients = HashSet::new();
        let mut channel_broadcasts = HashSet::new();
        for RawOutgoingMessage {
            destination,
            authenticated,
            is_protected,
            message,
        } in raw_result.messages
        {
            match &destination {
                Destination::Recipient(id) => {
                    recipients.insert(*id);
                }
                Destination::Subscribers(name) => {
                    channel_broadcasts.insert(name.clone());
                }
            }
            let authenticated_signer = if authenticated {
                raw_result.authenticated_signer
            } else {
                None
            };
            messages.push(OutgoingMessage {
                destination,
                authenticated_signer,
                is_protected,
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
        let full_names = raw_result
            .unsubscribe
            .clone()
            .into_iter()
            .map(|(name, _id)| ChannelFullName {
                application_id,
                name,
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        for ((_name, id), mut channel) in raw_result.unsubscribe.into_iter().zip(channels) {
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
        let full_names = raw_result
            .subscribe
            .clone()
            .into_iter()
            .map(|(name, _id)| ChannelFullName {
                application_id,
                name,
            })
            .collect::<Vec<_>>();
        let channels = self.channels.try_load_entries_mut(&full_names).await?;
        let stream = raw_result.subscribe.into_iter().zip(channels);
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
