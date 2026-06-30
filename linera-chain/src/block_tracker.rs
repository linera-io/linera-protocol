// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use custom_debug_derive::Debug;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency;
use linera_base::{
    data_types::{Amount, ArithmeticError, Blob, BlockHeight, Event, OracleResponse, Timestamp},
    ensure,
    identifiers::{AccountOwner, BlobId, ChainId, StreamId},
};
use linera_execution::{
    execution_state_actor::ExecutionStateActor, ExecutionRuntimeContext, ExecutionStateView,
    Message, MessageContext, MessageKind, OperationContext, OutgoingMessage, PreparedCheckpoint,
    ResourceController, ResourceTracker, SystemExecutionStateView, TransactionOutcome,
    TransactionTracker,
};
use linera_views::context::Context;
use tracing::{debug, instrument};

#[cfg(with_metrics)]
use crate::chain::metrics;
use crate::{
    chain::{BlockExecutionPhase, EMPTY_BLOCK_SIZE},
    data_types::{
        IncomingBundle, MessageAction, OperationResult, PostedMessage, ProposedBlock, Transaction,
    },
    ChainError, ChainExecutionContext, ExecutionResultExt,
};

/// Tracks execution of transactions within a block.
/// Captures the resource policy, produced messages, oracle responses and events.
#[derive(Debug)]
pub struct BlockExecutionTracker<'resources, 'blobs> {
    chain_id: ChainId,
    /// The protocol phase this block is executed in (staging a proposal, validating a
    /// received proposal, or executing a confirmed certificate). Recorded on execution
    /// spans and used to label execution metrics.
    phase: BlockExecutionPhase,
    block_height: BlockHeight,
    timestamp: Timestamp,
    authenticated_owner: Option<AccountOwner>,
    resource_controller: &'resources mut ResourceController<Option<AccountOwner>, ResourceTracker>,
    local_time: Timestamp,
    #[debug(skip_if = Option::is_none)]
    replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    next_application_index: u32,
    next_chain_index: u32,
    #[debug(skip_if = Vec::is_empty)]
    oracle_responses: Vec<Vec<OracleResponse>>,
    #[debug(skip_if = Vec::is_empty)]
    events: Vec<Vec<Event>>,
    #[debug(skip_if = Vec::is_empty)]
    blobs: Vec<Vec<Blob>>,
    #[debug(skip_if = Vec::is_empty)]
    messages: Vec<Vec<OutgoingMessage>>,
    #[debug(skip_if = Vec::is_empty)]
    operation_results: Vec<OperationResult>,
    // Index of the currently executed transaction in a block.
    transaction_index: u32,

    // Blobs published in the block.
    published_blobs: BTreeMap<BlobId, &'blobs Blob>,

    // Checkpoint inputs computed pre-block (state dump split into blobs and the per-origin
    // inbox cursors), to be handed to the matching `SystemOperation::Checkpoint`
    // operation handler when it runs.
    #[debug(skip_if = Option::is_none)]
    prepared_checkpoint: Option<PreparedCheckpoint>,
}

impl<'resources, 'blobs> BlockExecutionTracker<'resources, 'blobs> {
    /// Creates a new BlockExecutionTracker.
    pub fn new(
        resource_controller: &'resources mut ResourceController<
            Option<AccountOwner>,
            ResourceTracker,
        >,
        published_blobs: BTreeMap<BlobId, &'blobs Blob>,
        local_time: Timestamp,
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
        proposal: &ProposedBlock,
        phase: BlockExecutionPhase,
    ) -> Result<Self, ChainError> {
        resource_controller
            .track_block_size(EMPTY_BLOCK_SIZE)
            .with_execution_context(ChainExecutionContext::Block)?;

        Ok(Self {
            chain_id: proposal.chain_id,
            phase,
            block_height: proposal.height,
            timestamp: proposal.timestamp,
            authenticated_owner: proposal.authenticated_owner,
            resource_controller,
            local_time,
            replaying_oracle_responses,
            next_application_index: 0,
            next_chain_index: 0,
            oracle_responses: Vec::new(),
            events: Vec::new(),
            blobs: Vec::new(),
            messages: Vec::new(),
            operation_results: Vec::new(),
            transaction_index: 0,
            published_blobs,
            prepared_checkpoint: None,
        })
    }

    /// Stashes pre-computed checkpoint inputs to be handed to the matching
    /// `SystemOperation::Checkpoint` handler when its transaction runs.
    pub fn set_prepared_checkpoint(&mut self, prepared: PreparedCheckpoint) {
        self.prepared_checkpoint = Some(prepared);
    }

    /// Executes a transaction in the context of the block.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id,
        block_height = %self.block_height,
        phase = self.phase.as_str(),
        transaction_index = %self.transaction_index,
        transaction_type = %transaction.as_ref(),
    ))]
    pub async fn execute_transaction<C>(
        &mut self,
        transaction: &Transaction,
        round: Option<u32>,
        chain: &mut ExecutionStateView<C>,
    ) -> Result<(), ChainError>
    where
        C: Context + Clone + 'static,
        C::Extra: ExecutionRuntimeContext,
    {
        let chain_execution_context = self.chain_execution_context(transaction);
        let mut txn_tracker = self.new_transaction_tracker()?;

        match transaction {
            Transaction::ReceiveMessages(incoming_bundle) => {
                self.resource_controller_mut()
                    .track_block_size_of(&incoming_bundle)
                    .with_execution_context(chain_execution_context)?;
                for posted_message in incoming_bundle.messages() {
                    Box::pin(self.execute_message_in_block(
                        chain,
                        posted_message,
                        incoming_bundle,
                        round,
                        &mut txn_tracker,
                    ))
                    .await?;
                }
                let progress = chain.system.progress.get_mut();
                progress.num_incoming_bundles = progress
                    .num_incoming_bundles
                    .checked_add(1)
                    .ok_or(ArithmeticError::Overflow)?;
            }
            Transaction::ExecuteOperation(operation) => {
                self.resource_controller_mut()
                    .with_state(&mut chain.system)
                    .await?
                    .track_block_size_of(&operation)
                    .with_execution_context(chain_execution_context)?;
                #[cfg(with_metrics)]
                let operation_latency =
                    metrics::OPERATION_EXECUTION_LATENCY.with_label_values(&[self.phase.as_str()]);
                #[cfg(with_metrics)]
                let _operation_latency = operation_latency.measure_latency_us();
                let context = OperationContext {
                    chain_id: self.chain_id,
                    height: self.block_height,
                    round,
                    authenticated_owner: self.authenticated_owner,
                    timestamp: self.timestamp,
                };
                let mut actor =
                    ExecutionStateActor::new(chain, &mut txn_tracker, self.resource_controller);
                Box::pin(actor.execute_operation(context, operation.clone()))
                    .await
                    .with_execution_context(chain_execution_context)?;
                self.resource_controller_mut()
                    .with_state(&mut chain.system)
                    .await?
                    .track_operation(operation)
                    .with_execution_context(chain_execution_context)?;
                let progress = chain.system.progress.get_mut();
                progress.num_operations = progress
                    .num_operations
                    .checked_add(1)
                    .ok_or(ArithmeticError::Overflow)?;
            }
        }

        let txn_outcome = txn_tracker
            .into_outcome()
            .with_execution_context(chain_execution_context)?;
        let progress = chain.system.progress.get_mut();
        progress.num_outgoing_messages = progress
            .num_outgoing_messages
            .checked_add(
                u32::try_from(txn_outcome.outgoing_messages.len())
                    .map_err(|_| ArithmeticError::Overflow)?,
            )
            .ok_or(ArithmeticError::Overflow)?;
        self.process_txn_outcome(txn_outcome, &mut chain.system, chain_execution_context)
            .await?;
        Ok(())
    }

    /// Returns a new TransactionTracker for the current transaction.
    fn new_transaction_tracker(&self) -> Result<TransactionTracker, ChainError> {
        let mut tracker = TransactionTracker::new(
            self.local_time,
            self.transaction_index,
            self.next_application_index,
            self.next_chain_index,
            self.oracle_responses()?,
            &self.blobs,
        );
        // Cloning is cheap — blob bytes are behind an `Arc`.
        if let Some(prepared) = self.prepared_checkpoint.as_ref() {
            tracker.set_prepared_checkpoint(prepared.clone());
        }
        Ok(tracker)
    }

    /// Executes a message as part of an incoming bundle in a block.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id,
        block_height = %self.block_height,
        origin = %incoming_bundle.origin,
        action = ?incoming_bundle.action,
        is_bouncing = %posted_message.is_bouncing(),
    ))]
    async fn execute_message_in_block<C>(
        &mut self,
        chain: &mut ExecutionStateView<C>,
        posted_message: &PostedMessage,
        incoming_bundle: &IncomingBundle,
        round: Option<u32>,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(), ChainError>
    where
        C: Context + Clone + 'static,
        C::Extra: ExecutionRuntimeContext,
    {
        #[cfg(with_metrics)]
        let message_latency =
            metrics::MESSAGE_EXECUTION_LATENCY.with_label_values(&[self.phase.as_str()]);
        #[cfg(with_metrics)]
        let _message_latency = message_latency.measure_latency_us();
        let context = MessageContext {
            chain_id: self.chain_id,
            origin: incoming_bundle.origin,
            origin_certificate_hash: incoming_bundle.bundle.certificate_hash,
            origin_timestamp: incoming_bundle.bundle.timestamp,
            is_bouncing: posted_message.is_bouncing(),
            height: self.block_height,
            round,
            authenticated_owner: posted_message.authenticated_owner,
            refund_grant_to: posted_message.refund_grant_to,
            timestamp: self.timestamp,
        };
        let mut grant = posted_message.grant;
        match incoming_bundle.action {
            MessageAction::Accept => {
                let chain_execution_context =
                    ChainExecutionContext::IncomingBundle(txn_tracker.transaction_index());
                // Once a chain is closed, accepting incoming messages is not allowed.
                ensure!(!chain.system.closed.get(), ChainError::ClosedChain);

                let mut actor =
                    ExecutionStateActor::new(chain, txn_tracker, self.resource_controller);
                Box::pin(actor.execute_message(
                    context,
                    posted_message.message.clone(),
                    (grant > Amount::ZERO).then_some(&mut grant),
                ))
                .await
                .with_execution_context(chain_execution_context)?;
                actor
                    .send_refund(context, grant)
                    .with_execution_context(chain_execution_context)?;
            }
            MessageAction::Reject => {
                // If rejecting a message fails, the entire block proposal should be
                // scrapped.
                ensure!(
                    !posted_message.is_protected() || *chain.system.closed.get(),
                    ChainError::CannotRejectMessage {
                        chain_id: self.chain_id,
                        origin: incoming_bundle.origin,
                        posted_message: Box::new(posted_message.clone()),
                    }
                );
                debug!(
                    chain_id = %self.chain_id,
                    origin = %incoming_bundle.origin,
                    "Rejecting incoming message"
                );
                let mut actor =
                    ExecutionStateActor::new(chain, txn_tracker, self.resource_controller);
                if posted_message.is_tracked() {
                    // Bounce the message.
                    actor
                        .bounce_message(context, grant, posted_message.message.clone())
                        .with_execution_context(ChainExecutionContext::Block)?;
                } else {
                    // Nothing to do except maybe refund the grant.
                    actor
                        .send_refund(context, grant)
                        .with_execution_context(ChainExecutionContext::Block)?;
                }
            }
        }
        // Record that this origin has sent us a real message; we owe them a
        // `SystemMessage::CheckpointAck` at our next checkpoint. `CheckpointAck`
        // messages themselves are excluded so they don't keep the notification
        // ping-pong alive forever.
        if !posted_message.message.is_checkpoint_ack() {
            chain
                .system
                .pending_checkpoint_ack_targets
                .insert(&incoming_bundle.origin)?;
        }
        Ok(())
    }

    /// Returns oracle responses for the current transaction.
    fn oracle_responses(&self) -> Result<Option<Vec<OracleResponse>>, ChainError> {
        if let Some(responses) = self.replaying_oracle_responses.as_ref() {
            match responses.get(self.transaction_index as usize) {
                Some(responses) => Ok(Some(responses.clone())),
                None => Err(ChainError::MissingOracleResponseList),
            }
        } else {
            Ok(None)
        }
    }

    /// Processes the transaction outcome.
    ///
    /// Updates block tracker with indexes for the next messages, applications, etc.
    /// so that the execution of the next transaction doesn't overwrite the previous ones.
    ///
    /// Tracks the resources used by the transaction - size of the incoming and outgoing messages, blobs, etc.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id,
        block_height = %self.block_height,
        transaction_index = %self.transaction_index,
        outgoing_messages_count = %txn_outcome.outgoing_messages.len(),
        events_count = %txn_outcome.events.len(),
        blobs_count = %txn_outcome.blobs.len(),
    ))]
    pub async fn process_txn_outcome<C>(
        &mut self,
        txn_outcome: TransactionOutcome,
        view: &mut SystemExecutionStateView<C>,
        context: ChainExecutionContext,
    ) -> Result<(), ChainError>
    where
        C: Context + Clone + 'static,
    {
        let mut resource_controller = self.resource_controller.with_state(view).await?;

        for message_out in &txn_outcome.outgoing_messages {
            if message_out.kind == MessageKind::Bouncing {
                continue; // Bouncing messages are free.
            }
            if let Message::User { application_id, .. } = &message_out.message {
                if resource_controller.policy().is_free_app(application_id) {
                    continue; // Outgoing message fees are waived for free apps.
                }
            }
            resource_controller
                .track_message(&message_out.message)
                .with_execution_context(context)?;
        }

        resource_controller
            .track_block_size_of(&(
                &txn_outcome.oracle_responses,
                &txn_outcome.outgoing_messages,
                &txn_outcome.events,
                &txn_outcome.blobs,
            ))
            .with_execution_context(context)?;

        // Account for blobs published by this transaction directly.
        for blob in &txn_outcome.blobs {
            if txn_outcome.free_blob_ids.contains(&blob.id()) {
                continue; // Blob publishing fees are waived for free apps.
            }
            resource_controller
                .track_blob_published(blob)
                .with_execution_context(context)?;
        }

        // Account for blobs published indirectly but referenced by the transaction.
        for blob_id in &txn_outcome.blobs_published {
            if let Some(blob) = self.published_blobs.get(blob_id) {
                resource_controller
                    .track_blob_published(blob)
                    .with_execution_context(context)?;
            } else {
                return Err(ChainError::InternalError(format!(
                    "Missing published blob {blob_id}"
                )));
            }
        }

        self.resource_controller
            .track_block_size_of(&(&txn_outcome.operation_result))
            .with_execution_context(context)?;

        self.next_application_index = txn_outcome.next_application_index;
        self.next_chain_index = txn_outcome.next_chain_index;
        self.oracle_responses.push(txn_outcome.oracle_responses);
        self.events.push(txn_outcome.events);
        self.blobs.push(txn_outcome.blobs);
        self.messages.push(txn_outcome.outgoing_messages);
        if matches!(context, ChainExecutionContext::Operation(_)) {
            self.operation_results
                .push(OperationResult(txn_outcome.operation_result));
        }
        self.transaction_index += 1;
        Ok(())
    }

    /// Returns recipient chain IDs for outgoing messages in the block.
    pub fn recipients(&self) -> BTreeSet<ChainId> {
        self.messages
            .iter()
            .flatten()
            .map(|msg| msg.destination)
            .collect()
    }

    /// For each recipient that received at least one non-`SystemMessage::CheckpointAck`
    /// message in this block, returns the transaction indices that produced such a
    /// message. The chain-level bookkeeping (`previous_message_blocks`,
    /// `unfinalized_message_blocks`) is updated only for these recipients, so that
    /// `CheckpointAck`-only blocks don't pin a slot that would never get trimmed (the
    /// recipient never acknowledges back). The transaction indices feed
    /// `unfinalized_message_blocks` as full `(block.height, tx_index)` cursors, so a
    /// later ack with a cursor past the last unfinalized bundle can fully evict the
    /// recipient.
    pub fn non_checkpoint_ack_tx_indices(&self) -> BTreeMap<ChainId, BTreeSet<u32>> {
        let mut result = BTreeMap::<ChainId, BTreeSet<u32>>::new();
        for (tx_idx, msgs) in (0u32..).zip(&self.messages) {
            for msg in msgs {
                if !msg.message.is_checkpoint_ack() {
                    result.entry(msg.destination).or_default().insert(tx_idx);
                }
            }
        }
        result
    }

    /// Returns stream IDs for events published in the block.
    pub fn event_streams(&self) -> BTreeSet<StreamId> {
        self.events
            .iter()
            .flatten()
            .map(|event| event.stream_id.clone())
            .collect()
    }

    /// Returns a mutable reference to the resource controller.
    pub fn resource_controller_mut(
        &mut self,
    ) -> &mut ResourceController<Option<AccountOwner>, ResourceTracker> {
        self.resource_controller
    }

    /// Creates a checkpoint of the tracker's mutable state.
    ///
    /// This captures all state that could be modified during transaction execution,
    /// allowing restoration if execution fails.
    pub fn create_checkpoint(&self) -> TrackerCheckpoint {
        TrackerCheckpoint {
            resource_tracker: self.resource_controller.tracker,
            next_application_index: self.next_application_index,
            next_chain_index: self.next_chain_index,
            transaction_index: self.transaction_index,
            oracle_responses_len: self.oracle_responses.len(),
            events_len: self.events.len(),
            blobs_len: self.blobs.len(),
            messages_len: self.messages.len(),
            operation_results_len: self.operation_results.len(),
        }
    }

    /// Restores the tracker's mutable state from a checkpoint.
    ///
    /// This reverts all state to what it was when the checkpoint was saved,
    /// as if the failed transaction execution never happened.
    pub fn restore_checkpoint(&mut self, checkpoint: &TrackerCheckpoint) {
        // Destructure to ensure all fields are handled (compiler will warn on new fields).
        let TrackerCheckpoint {
            resource_tracker,
            next_application_index,
            next_chain_index,
            transaction_index,
            oracle_responses_len,
            events_len,
            blobs_len,
            messages_len,
            operation_results_len,
        } = checkpoint;

        self.resource_controller.tracker = *resource_tracker;
        self.next_application_index = *next_application_index;
        self.next_chain_index = *next_chain_index;
        self.transaction_index = *transaction_index;
        self.oracle_responses.truncate(*oracle_responses_len);
        self.events.truncate(*events_len);
        self.blobs.truncate(*blobs_len);
        self.messages.truncate(*messages_len);
        self.operation_results.truncate(*operation_results_len);
    }

    /// Finalizes the execution and returns the collected results.
    ///
    /// This method should be called after all transactions have been processed.
    /// The `expected_outcomes_count` should be the number of transactions in the final block.
    /// Panics if the number of lists of oracle responses, outgoing messages,
    /// events, or blobs does not match the expected counts.
    pub fn finalize(self, expected_outcomes_count: usize) -> FinalizeExecutionResult {
        // Asserts that the number of outcomes matches the expected count.
        assert_eq!(self.oracle_responses.len(), expected_outcomes_count);
        assert_eq!(self.messages.len(), expected_outcomes_count);
        assert_eq!(self.events.len(), expected_outcomes_count);
        assert_eq!(self.blobs.len(), expected_outcomes_count);

        #[cfg(with_metrics)]
        crate::chain::metrics::track_block_metrics(&self.resource_controller.tracker, self.phase);

        let resource_tracker = self.resource_controller.tracker;

        (
            self.messages,
            self.oracle_responses,
            self.events,
            self.blobs,
            self.operation_results,
            resource_tracker,
        )
    }

    /// Returns the execution context for the current transaction.
    fn chain_execution_context(&self, transaction: &Transaction) -> ChainExecutionContext {
        match transaction {
            Transaction::ReceiveMessages(_) => {
                ChainExecutionContext::IncomingBundle(self.transaction_index)
            }
            Transaction::ExecuteOperation(_) => {
                ChainExecutionContext::Operation(self.transaction_index)
            }
        }
    }
}

pub(crate) type FinalizeExecutionResult = (
    Vec<Vec<OutgoingMessage>>,
    Vec<Vec<OracleResponse>>,
    Vec<Vec<Event>>,
    Vec<Vec<Blob>>,
    Vec<OperationResult>,
    ResourceTracker,
);

/// Checkpoint of the tracker's mutable state for restoration on failure.
#[derive(Clone)]
pub struct TrackerCheckpoint {
    pub(crate) resource_tracker: ResourceTracker,
    pub(crate) next_application_index: u32,
    pub(crate) next_chain_index: u32,
    pub(crate) transaction_index: u32,
    pub(crate) oracle_responses_len: usize,
    pub(crate) events_len: usize,
    pub(crate) blobs_len: usize,
    pub(crate) messages_len: usize,
    pub(crate) operation_results_len: usize,
}
