// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::vec;

use linera_base::{
    data_types::{Amount, ArithmeticError, OracleResponse},
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
}

impl TransactionTracker {
    /// Creates a tracker with oracle responses to be replayed.
    pub fn with_oracle_responses(oracle_responses: Vec<OracleResponse>) -> Self {
        TransactionTracker {
            replaying_oracle_responses: Some(oracle_responses.into_iter()),
            ..TransactionTracker::default()
        }
    }

    pub fn add_system_outcome(&mut self, outcome: RawExecutionOutcome<SystemMessage, Amount>) {
        self.outcomes.push(ExecutionOutcome::System(outcome));
    }

    pub fn add_user_outcome(
        &mut self,
        application_id: ApplicationId,
        outcome: RawExecutionOutcome<Vec<u8>, Amount>,
    ) {
        self.outcomes
            .push(ExecutionOutcome::User(application_id, outcome))
    }

    pub fn add_outcomes(&mut self, outcomes: impl IntoIterator<Item = ExecutionOutcome>) {
        self.outcomes.extend(outcomes);
    }

    pub fn add_oracle_response(&mut self, oracle_response: OracleResponse) {
        self.oracle_responses.push(oracle_response);
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

    pub fn message_count(&self) -> Result<u32, ArithmeticError> {
        let mut count = 0usize;
        for outcome in &self.outcomes {
            count = count
                .checked_add(outcome.message_count())
                .ok_or(ArithmeticError::Overflow)?;
        }
        u32::try_from(count).map_err(|_| ArithmeticError::Overflow)
    }

    pub fn destructure(
        self,
    ) -> Result<(Vec<ExecutionOutcome>, Vec<OracleResponse>), ExecutionError> {
        let TransactionTracker {
            replaying_oracle_responses,
            oracle_responses,
            outcomes,
        } = self;
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        Ok((outcomes, oracle_responses))
    }

    pub(crate) fn outcomes_mut(&mut self) -> &mut Vec<ExecutionOutcome> {
        &mut self.outcomes
    }
}
