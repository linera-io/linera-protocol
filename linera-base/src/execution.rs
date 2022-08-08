// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{crypto::BcsSignable, error::Error, messages::*, system::SystemExecutionState};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub static SYSTEM: ApplicationId = ApplicationId(0);

pub static USER_APPLICATIONS: Lazy<
    Mutex<HashMap<ApplicationId, Arc<dyn UserApplication + Send + Sync + 'static>>>,
> = Lazy::new(|| {
    let m = HashMap::new();
    Mutex::new(m)
});

pub trait UserApplication {
    fn apply_operation(
        &self,
        state: &mut Vec<u8>,
        operation: &[u8],
    ) -> Result<ApplicationResult, Error>;

    fn apply_effect(
        &self,
        state: &mut Vec<u8>,
        operation: &[u8],
    ) -> Result<ApplicationResult, Error>;
}

#[derive(Debug, Default)]
pub struct ApplicationResult {
    pub effects: Vec<(Destination, Effect)>,
    pub subscribe: Option<(String, ChainId)>,
    pub unsubscribe: Option<(String, ChainId)>,
}

/// The authentication execution state of all applications.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// System application.
    pub system: SystemExecutionState,
    /// User applications.
    pub users: HashMap<ApplicationId, Vec<u8>>,
}

impl ExecutionState {
    pub fn new(id: ChainId) -> Self {
        SystemExecutionState::new(id).into()
    }

    fn get_user_application(
        application_id: ApplicationId,
    ) -> Result<Arc<dyn UserApplication + Send + Sync + 'static>, Error> {
        let applications = USER_APPLICATIONS.lock().unwrap();
        Ok(applications
            .get(&application_id)
            .ok_or(Error::UnknownApplication)?
            .clone())
    }

    pub(crate) fn apply_operation(
        &mut self,
        application_id: ApplicationId,
        height: BlockHeight,
        index: usize,
        operation: &Operation,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match operation {
                Operation::System(op) => self.system.apply_operation(height, index, op),
                _ => Err(Error::InvalidOperation),
            }
        } else {
            let application = Self::get_user_application(application_id)?;
            let state = self.users.entry(application_id).or_default();
            match operation {
                Operation::System(_) => Err(Error::InvalidOperation),
                Operation::UserBincodeEncoded(operation) => {
                    application.apply_operation(state, operation)
                }
            }
        }
    }

    pub(crate) fn apply_effect(
        &mut self,
        application_id: ApplicationId,
        effect: &Effect,
    ) -> Result<ApplicationResult, Error> {
        if application_id == SYSTEM {
            match effect {
                Effect::System(effect) => self.system.apply_effect(effect),
                _ => Err(Error::InvalidEffect),
            }
        } else {
            let application = Self::get_user_application(application_id)?;
            let state = self.users.entry(application_id).or_default();
            match effect {
                Effect::System(_) => Err(Error::InvalidEffect),
                Effect::UserBincodeEncoded(effect) => application.apply_effect(state, effect),
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
