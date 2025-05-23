// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Blob, Event, OracleResponse, Timestamp},
    identifiers::{AccountOwner, BlobId, ChainId},
};
use linera_execution::{
    OutgoingMessage, ResourceController, ResourceTracker, SystemExecutionStateView,
    TransactionOutcome, TransactionTracker,
};
use linera_views::context::Context;

use crate::{
    chain::EMPTY_BLOCK_SIZE,
    data_types::{OperationResult, ProposedBlock, Transaction},
    ChainError, ChainExecutionContext, ExecutionResultExt,
};

/// Tracks execution of transactions within a block.
/// Captures the resource policy, produced messages, oracle responses and events.
#[derive(Debug)]
pub struct BlockExecutionTracker<'resources, 'blobs> {
    resource_controller: &'resources mut ResourceController<Option<AccountOwner>, ResourceTracker>,
    local_time: Timestamp,
    #[debug(skip_if = Option::is_none)]
    pub replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    pub next_message_index: u32,
    pub next_application_index: u32,
    pub next_chain_index: u32,
    #[debug(skip_if = Vec::is_empty)]
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    #[debug(skip_if = Vec::is_empty)]
    pub events: Vec<Vec<Event>>,
    #[debug(skip_if = Vec::is_empty)]
    pub blobs: Vec<Vec<Blob>>,
    #[debug(skip_if = Vec::is_empty)]
    pub messages: Vec<Vec<OutgoingMessage>>,
    #[debug(skip_if = Vec::is_empty)]
    pub operation_results: Vec<OperationResult>,
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
            resource_controller,
            local_time,
            replaying_oracle_responses,
            next_message_index: 0,
            next_application_index: 0,
            next_chain_index: 0,
            oracle_responses: Vec::new(),
            events: Vec::new(),
            blobs: Vec::new(),
            messages: Vec::new(),
            operation_results: Vec::new(),
            transaction_index: 0,
            published_blobs,
            expected_outcomes_count: proposal.incoming_bundles.len() + proposal.operations.len(),
        })
    }

    /// Returns a new TransactionTracker for the current transaction.
    pub fn new_transaction_tracker(&mut self) -> Result<TransactionTracker, ChainError> {
        Ok(TransactionTracker::new(
            self.local_time,
            self.transaction_index,
            self.next_message_index,
            self.next_application_index,
            self.next_chain_index,
            self.oracle_responses()?,
        ))
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
        txn_outcome: &TransactionOutcome,
        view: &mut SystemExecutionStateView<C>,
        context: ChainExecutionContext,
    ) -> Result<(), ChainError>
    where
        C: Context + Clone + Send + Sync + 'static,
    {
        self.next_message_index = txn_outcome.next_message_index;
        self.next_application_index = txn_outcome.next_application_index;
        self.next_chain_index = txn_outcome.next_chain_index;
        self.oracle_responses
            .push(txn_outcome.oracle_responses.clone());
        self.events.push(txn_outcome.events.clone());
        self.blobs.push(txn_outcome.blobs.clone());
        self.messages.push(txn_outcome.outgoing_messages.clone());
        if matches!(context, ChainExecutionContext::Operation(_)) {
            self.operation_results
                .push(OperationResult(txn_outcome.operation_result.clone()));
        }

        let mut resource_controller = self.resource_controller.with_state(view).await?;

        for message_out in &txn_outcome.outgoing_messages {
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

        self.transaction_index += 1;
        Ok(())
    }

    /// Returns recipient chain ids for outgoing messages in the block.
    pub fn recipients(&self) -> BTreeSet<ChainId> {
        self.messages
            .iter()
            .flatten()
            .map(|msg| msg.destination)
            .collect()
    }

    /// Returns the execution context for the current transaction.
    pub fn chain_execution_context(&self, transaction: &Transaction<'_>) -> ChainExecutionContext {
        match transaction {
            Transaction::ReceiveMessages(_) => {
                ChainExecutionContext::IncomingBundle(self.transaction_index)
            }
            Transaction::ExecuteOperation(_) => {
                ChainExecutionContext::Operation(self.transaction_index)
            }
        }
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
    /// Panics if the number of outcomes does match the expected count.
    pub fn finalize(self) -> FinalizeExecutionResult {
        // Asserts that the number of outcomes matches the expected count.
        assert_eq!(self.oracle_responses.len(), self.expected_outcomes_count);
        assert_eq!(self.messages.len(), self.expected_outcomes_count);
        assert_eq!(self.events.len(), self.expected_outcomes_count);
        assert_eq!(self.blobs.len(), self.expected_outcomes_count);

        #[cfg(with_metrics)]
        crate::chain::metrics::track_block_metrics(&self.resource_controller.tracker);

        (
            self.messages,
            self.oracle_responses,
            self.events,
            self.blobs,
            self.operation_results,
        )
    }
}

pub(crate) type FinalizeExecutionResult = (
    Vec<Vec<OutgoingMessage>>,
    Vec<Vec<OracleResponse>>,
    Vec<Vec<Event>>,
    Vec<Vec<Blob>>,
    Vec<OperationResult>,
);
