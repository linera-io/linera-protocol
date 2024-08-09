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
    next_message_index: u32,
}

impl TransactionTracker {
    pub fn new(next_message_index: u32, oracle_responses: Option<Vec<OracleResponse>>) -> Self {
        TransactionTracker {
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            next_message_index,
            oracle_responses: Vec::new(),
            outcomes: Vec::new(),
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

    pub fn destructure(
        self,
    ) -> Result<(Vec<ExecutionOutcome>, Vec<OracleResponse>, u32), ExecutionError> {
        let TransactionTracker {
            replaying_oracle_responses,
            oracle_responses,
            outcomes,
            next_message_index,
        } = self;
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        Ok((outcomes, oracle_responses, next_message_index))
    }

    pub(crate) fn outcomes_mut(&mut self) -> &mut Vec<ExecutionOutcome> {
        &mut self.outcomes
    }
}
