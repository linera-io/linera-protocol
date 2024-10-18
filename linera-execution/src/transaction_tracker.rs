// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc, vec};

use linera_base::{
    data_types::{Amount, ArithmeticError, OracleResponse, UserApplicationDescription},
    ensure,
    identifiers::ApplicationId,
};

use crate::{
    ExecutionError, ExecutionOutcome, RawExecutionOutcome, SystemExecutionError, SystemMessage,
};

/// Tracks oracle responses and execution outcomes of an ongoing transaction execution, as well
/// as replayed oracle responses.
#[derive(Debug, Default)]
pub struct TransactionTracker {
    replaying_oracle_responses: Option<vec::IntoIter<OracleResponse>>,
    oracle_responses: Vec<OracleResponse>,
    outcomes: Vec<ExecutionOutcome>,
    next_message_index: u32,
    pending_applications: Arc<BTreeMap<ApplicationId, UserApplicationDescription>>,
}

impl TransactionTracker {
    pub fn new(
        next_message_index: u32,
        oracle_responses: Option<Vec<OracleResponse>>,
        pending_applications: Arc<BTreeMap<ApplicationId, UserApplicationDescription>>,
    ) -> Self {
        TransactionTracker {
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            next_message_index,
            oracle_responses: Vec::new(),
            outcomes: Vec::new(),
            pending_applications,
        }
    }

    pub fn get_pending_application_description(
        &self,
        application_id: ApplicationId,
    ) -> Option<UserApplicationDescription> {
        self.pending_applications.get(&application_id).cloned()
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

    pub fn add_oracle_response(&mut self, oracle_response: OracleResponse) {
        self.oracle_responses.push(oracle_response);
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

    pub fn destructure(
        self,
    ) -> Result<(Vec<ExecutionOutcome>, Vec<OracleResponse>, u32), ExecutionError> {
        let TransactionTracker {
            replaying_oracle_responses,
            oracle_responses,
            outcomes,
            next_message_index,
            pending_applications: _pending_blobs,
        } = self;
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        Ok((outcomes, oracle_responses, next_message_index))
    }
}
