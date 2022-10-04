// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{error::Error, messages::*, system::SystemEffect};
use once_cell::sync::Lazy;
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

pub fn get_user_application(
    application_id: ApplicationId,
) -> Result<Arc<dyn UserApplication + Send + Sync + 'static>, Error> {
    let applications = USER_APPLICATIONS.lock().unwrap();
    Ok(applications
        .get(&application_id)
        .ok_or(Error::UnknownApplication)?
        .clone())
}

pub trait UserApplication {
    fn apply_operation(
        &self,
        context: &OperationContext,
        state: &mut Vec<u8>,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;

    fn apply_effect(
        &self,
        context: &EffectContext,
        state: &mut Vec<u8>,
        operation: &[u8],
    ) -> Result<RawApplicationResult<Vec<u8>>, Error>;
}

#[derive(Debug, Clone)]
pub struct OperationContext {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct EffectContext {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub effect_id: EffectId,
}

impl From<OperationContext> for EffectId {
    fn from(context: OperationContext) -> Self {
        Self {
            chain_id: context.chain_id,
            height: context.height,
            index: context.index,
        }
    }
}

#[derive(Debug)]
pub struct RawApplicationResult<Effect> {
    pub effects: Vec<(Destination, Effect)>,
    pub subscribe: Option<(String, ChainId)>,
    pub unsubscribe: Option<(String, ChainId)>,
}

#[derive(Debug)]
pub enum ApplicationResult {
    System(RawApplicationResult<SystemEffect>),
    User(RawApplicationResult<Vec<u8>>),
}

impl<Effect> Default for RawApplicationResult<Effect> {
    fn default() -> Self {
        Self {
            effects: Vec::new(),
            subscribe: None,
            unsubscribe: None,
        }
    }
}
