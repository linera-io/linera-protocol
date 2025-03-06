// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::vec;

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Amount, ArithmeticError, Event, OracleResponse},
    ensure,
    identifiers::{ApplicationId, ChainId, ChannelFullName, StreamId},
};

use crate::{
    ExecutionError, ExecutionOutcome, RawExecutionOutcome, SystemExecutionError, SystemMessage,
};

/// Tracks oracle responses and execution outcomes of an ongoing transaction execution, as well
/// as replayed oracle responses.
#[derive(Debug, Default)]
pub struct TransactionTracker {
    #[debug(skip_if = Option::is_none)]
    replaying_oracle_responses: Option<vec::IntoIter<OracleResponse>>,
    #[debug(skip_if = Vec::is_empty)]
    oracle_responses: Vec<OracleResponse>,
    #[debug(skip_if = Vec::is_empty)]
    outcomes: Vec<ExecutionOutcome>,
    next_message_index: u32,
    /// Events recorded by contracts' `emit` calls.
    events: Vec<Event>,
    /// Subscribe chains to channels.
    subscribe: Vec<(ChannelFullName, ChainId)>,
    /// Unsubscribe chains from channels.
    unsubscribe: Vec<(ChannelFullName, ChainId)>,
    /// Operation outcome.
    operation_result: Option<Vec<u8>>,
}

/// The [`TransactionTracker`] contents after a transaction has finished.
#[derive(Debug, Default)]
pub struct TransactionOutcome {
    #[debug(skip_if = Vec::is_empty)]
    pub oracle_responses: Vec<OracleResponse>,
    #[debug(skip_if = Vec::is_empty)]
    pub outcomes: Vec<ExecutionOutcome>,
    pub next_message_index: u32,
    /// Events recorded by contracts' `emit` calls.
    pub events: Vec<Event>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelFullName, ChainId)>,
    /// Unsubscribe chains from channels.
    pub unsubscribe: Vec<(ChannelFullName, ChainId)>,
    /// Operation outcome.
    pub operation_result: Vec<u8>,
}

impl TransactionTracker {
    pub fn new(next_message_index: u32, oracle_responses: Option<Vec<OracleResponse>>) -> Self {
        TransactionTracker {
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            next_message_index,
            ..Self::default()
        }
    }

    pub fn next_message_index(&self) -> u32 {
        self.next_message_index
    }

    pub fn add_system_outcome(
        &mut self,
        outcome: RawExecutionOutcome<SystemMessage, Amount>,
    ) -> Result<(), ArithmeticError> {
        self.add_outcome(ExecutionOutcome::System(outcome))
    }

    pub fn add_user_outcome(
        &mut self,
        application_id: ApplicationId,
        outcome: RawExecutionOutcome<Vec<u8>, Amount>,
    ) -> Result<(), ArithmeticError> {
        self.add_outcome(ExecutionOutcome::User(application_id, outcome))
    }

    pub fn add_outcomes(
        &mut self,
        outcomes: impl IntoIterator<Item = ExecutionOutcome>,
    ) -> Result<(), ArithmeticError> {
        for outcome in outcomes {
            self.add_outcome(outcome)?;
        }
        Ok(())
    }

    fn add_outcome(&mut self, outcome: ExecutionOutcome) -> Result<(), ArithmeticError> {
        let message_count =
            u32::try_from(outcome.message_count()).map_err(|_| ArithmeticError::Overflow)?;
        self.next_message_index = self
            .next_message_index
            .checked_add(message_count)
            .ok_or(ArithmeticError::Overflow)?;
        self.outcomes.push(outcome);
        Ok(())
    }

    pub fn add_event(&mut self, stream_id: StreamId, key: Vec<u8>, value: Vec<u8>) {
        self.events.push(Event {
            stream_id,
            key,
            value,
        });
    }

    pub fn subscribe(&mut self, name: ChannelFullName, subscriber: ChainId) {
        self.subscribe.push((name, subscriber));
    }

    pub fn unsubscribe(&mut self, name: ChannelFullName, subscriber: ChainId) {
        self.unsubscribe.push((name, subscriber));
    }

    pub fn add_oracle_response(&mut self, oracle_response: OracleResponse) {
        self.oracle_responses.push(oracle_response);
    }

    pub fn add_operation_result(&mut self, outcome: Option<Vec<u8>>) {
        self.operation_result = outcome
    }

    /// Adds the oracle response to the record.
    /// If replaying, it also checks that it matches the next replayed one and returns `true`.
    pub fn replay_oracle_response(
        &mut self,
        oracle_response: OracleResponse,
    ) -> Result<bool, SystemExecutionError> {
        let replaying = if let Some(recorded_response) = self.next_replayed_oracle_response()? {
            ensure!(
                recorded_response == oracle_response,
                SystemExecutionError::OracleResponseMismatch
            );
            true
        } else {
            false
        };
        self.add_oracle_response(oracle_response);
        Ok(replaying)
    }

    pub fn next_replayed_oracle_response(
        &mut self,
    ) -> Result<Option<OracleResponse>, SystemExecutionError> {
        let Some(responses) = &mut self.replaying_oracle_responses else {
            return Ok(None); // Not in replay mode.
        };
        let response = responses
            .next()
            .ok_or_else(|| SystemExecutionError::MissingOracleResponse)?;
        Ok(Some(response))
    }

    pub fn into_outcome(self) -> Result<TransactionOutcome, ExecutionError> {
        let TransactionTracker {
            replaying_oracle_responses,
            oracle_responses,
            outcomes,
            next_message_index,
            events,
            subscribe,
            unsubscribe,
            operation_result,
        } = self;
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        Ok(TransactionOutcome {
            outcomes,
            oracle_responses,
            next_message_index,
            events,
            subscribe,
            unsubscribe,
            operation_result: operation_result.unwrap_or_default(),
        })
    }

    pub(crate) fn outcomes_mut(&mut self) -> &mut Vec<ExecutionOutcome> {
        &mut self.outcomes
    }
}
