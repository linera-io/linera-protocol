// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Blob, Event, OracleResponse, Timestamp},
    identifiers::ChainId,
};
use linera_execution::{OutgoingMessage, TransactionOutcome, TransactionTracker};

use crate::{
    data_types::{OperationResult, Transaction},
    ChainError, ChainExecutionContext,
};

/// Tracks execution of transactions within a block.
/// Captures the resource policy, produced messages, oracle events.
#[derive(Debug, Default)]
pub struct BlockExecutionTracker {
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
}

impl BlockExecutionTracker {
    /// Creates a new BlockExecutionTracker.
    pub fn new(
        local_time: Timestamp,
        replaying_oracle_responses: Option<Vec<Vec<OracleResponse>>>,
    ) -> Self {
        Self {
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
        }
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

    /// Processes the transaction outcome and updates the tracker.
    pub fn add_txn_outcome(&mut self, txn_outcome: &TransactionOutcome) {
        self.next_message_index = txn_outcome.next_message_index;
        self.next_application_index = txn_outcome.next_application_index;
        self.next_chain_index = txn_outcome.next_chain_index;
        self.oracle_responses
            .push(txn_outcome.oracle_responses.clone());
        self.events.push(txn_outcome.events.clone());
        self.blobs.push(txn_outcome.blobs.clone());
        self.messages.push(txn_outcome.outgoing_messages.clone());
        self.operation_results
            .push(OperationResult(txn_outcome.operation_result.clone()));

        self.transaction_index += 1;
    }

    /// Returns recipient chain ids for outgoing messages in the block.
    pub fn recipients(&self) -> BTreeSet<ChainId> {
        self.messages
            .iter()
            .flatten()
            .map(|msg| msg.destination)
            .collect()
    }

    /// Asserts that the number of outcomes matches the expected count.
    pub fn assert_outcomes_count(&self, txn_count: usize) {
        assert_eq!(self.oracle_responses.len(), txn_count);
        assert_eq!(self.messages.len(), txn_count);
        assert_eq!(self.events.len(), txn_count);
        assert_eq!(self.blobs.len(), txn_count);
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
}
