// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::BcsSignable,
    error::Error,
    execution::{
        ApplicationResult, EffectContext, OperationContext, SYSTEM,
    },
    messages::*,
    system::SystemExecutionState,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap};

/// The execution state of all applications.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// System application.
    pub system: SystemExecutionState,
    /// User applications.
    pub users: HashMap<ApplicationId, Vec<u8>>,
}

impl ExecutionState {
    pub fn apply_operation(
        &mut self,
        application_id: ApplicationId,
        context: &OperationContext,
        operation: &Operation,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match operation {
                Operation::System(op) => {
                    let result = self.system.apply_operation(context, op)?;
                    Ok(ApplicationResult::System(result))
                }
                _ => Err(Error::InvalidOperation),
            }
        } else {
            let application = linera_base::execution::get_user_application(application_id)?;
            let state = self.users.entry(application_id).or_default();
            match operation {
                Operation::System(_) => Err(Error::InvalidOperation),
                Operation::User(operation) => {
                    let result = application.apply_operation(context, state, operation)?;
                    Ok(ApplicationResult::User(result))
                }
            }
        }
    }

    pub fn apply_effect(
        &mut self,
        application_id: ApplicationId,
        context: &EffectContext,
        effect: &Effect,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match effect {
                Effect::System(effect) => {
                    let result = self.system.apply_effect(context, effect)?;
                    Ok(ApplicationResult::System(result))
                }
                _ => Err(Error::InvalidEffect),
            }
        } else {
            let application = linera_base::execution::get_user_application(application_id)?;
            let state = self.users.entry(application_id).or_default();
            match effect {
                Effect::System(_) => Err(Error::InvalidEffect),
                Effect::User(effect) => {
                    let result = application.apply_effect(context, state, effect)?;
                    Ok(ApplicationResult::User(result))
                }
            }
        }
    }
}

impl From<SystemExecutionState> for ExecutionState {
    fn from(system: SystemExecutionState) -> Self {
        Self {
            system,
            users: HashMap::new(),
        }
    }
}

impl BcsSignable for ExecutionState {}
