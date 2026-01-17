// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use custom_debug_derive::Debug;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency;
use linera_base::{
    data_types::{Amount, Blob, BlockHeight, Event, OracleResponse, Timestamp},
    ensure,
    identifiers::{AccountOwner, BlobId, ChainId, StreamId},
};
use linera_execution::{
    execution_state_actor::ExecutionStateActor, ExecutionRuntimeContext, ExecutionStateView,
    MessageContext, MessageKind, OperationContext, OutgoingMessage, ResourceController,
    ResourceTracker, SystemExecutionStateView, TransactionOutcome, TransactionTracker,
};
use linera_views::context::Context;
use tracing::instrument;

#[cfg(with_metrics)]
use crate::chain::metrics;
use crate::{
    chain::EMPTY_BLOCK_SIZE,
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

    // We expect the number of outcomes to be equal to the number of transactions in the block.
    expected_outcomes_count: usize,
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
    ) -> Result<Self, ChainError> {
        resource_controller
            .track_block_size(EMPTY_BLOCK_SIZE)
            .with_execution_context(ChainExecutionContext::Block)?;

        Ok(Self {
            chain_id: proposal.chain_id,
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
            expected_outcomes_count: proposal.transactions.len(),
        })
    }

    /// Executes a transaction in the context of the block.
    #[instrument(skip_all, fields(
        chain_id = %self.chain_id,
        block_height = %self.block_height,
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
            }
            Transaction::ExecuteOperation(operation) => {
                self.resource_controller_mut()
                    .with_state(&mut chain.system)
                    .await?
                    .track_block_size_of(&operation)
                    .with_execution_context(chain_execution_context)?;
                #[cfg(with_metrics)]
                let _operation_latency = metrics::OPERATION_EXECUTION_LATENCY.measure_latency_us();
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
            }
        }

        let txn_outcome = txn_tracker
            .into_outcome()
            .with_execution_context(chain_execution_context)?;
        self.process_txn_outcome(txn_outcome, &mut chain.system, chain_execution_context)
            .await?;
        Ok(())
    }

    /// Returns a new TransactionTracker for the current transaction.
    fn new_transaction_tracker(&mut self) -> Result<TransactionTracker, ChainError> {
        Ok(TransactionTracker::new(
            self.local_time,
            self.transaction_index,
            self.next_application_index,
            self.next_chain_index,
            self.oracle_responses()?,
            &self.blobs,
        ))
    }

    /// Executes a message as part of an incoming bundle in a block.
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
        let _message_latency = metrics::MESSAGE_EXECUTION_LATENCY.measure_latency_us();
        let context = MessageContext {
            chain_id: self.chain_id,
            origin: incoming_bundle.origin,
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

    /// Finalizes the execution and returns the collected results.
    ///
    /// This method should be called after all transactions have been processed.
    /// Panics if the number of lists of oracle responses, outgoing messages,
    /// events, or blobs does not match the expected counts.
    pub fn finalize(self) -> FinalizeExecutionResult {
        // Asserts that the number of outcomes matches the expected count.
        assert_eq!(self.oracle_responses.len(), self.expected_outcomes_count);
        assert_eq!(self.messages.len(), self.expected_outcomes_count);
        assert_eq!(self.events.len(), self.expected_outcomes_count);
        assert_eq!(self.blobs.len(), self.expected_outcomes_count);

        #[cfg(with_metrics)]
        crate::chain::metrics::track_block_metrics(&self.resource_controller.tracker);

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
