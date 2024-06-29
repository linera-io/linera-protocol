// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Some of these items are only used by some tests, but Rust will complain about unused
// items for the tests where they aren't used
#![allow(unused_imports)]

mod mock_application;
mod system_execution_state;

use std::{sync::Arc, thread, vec};

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, MessageId},
};
use linera_views::{
    common::Context,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};

pub use self::{
    mock_application::{ExpectedCall, MockApplication, MockApplicationInstance},
    system_execution_state::SystemExecutionState,
};
use crate::{
    ApplicationRegistryView, BytecodeLocation, ExecutionRequest, ExecutionRuntimeContext,
    ExecutionStateView, QueryContext, ServiceRuntimeRequest, ServiceSyncRuntime,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};

pub fn create_dummy_user_application_description(index: u64) -> UserApplicationDescription {
    let chain_id = ChainId::root(1);
    let certificate_hash = CryptoHash::new(&FakeCertificate);
    UserApplicationDescription {
        bytecode_id: BytecodeId::new(MessageId {
            chain_id,
            height: BlockHeight(index),
            index: 0,
        }),
        bytecode_location: BytecodeLocation {
            certificate_hash,
            operation_index: 0,
        },
        creation: MessageId {
            chain_id,
            height: BlockHeight(index),
            index: 1,
        },
        required_application_ids: vec![],
        parameters: vec![],
    }
}

#[derive(Deserialize, Serialize)]
pub struct FakeCertificate;

impl BcsSignable for FakeCertificate {}

/// Creates `count` [`MockApplication`]s and registers them in the provided [`ExecutionStateView`].
///
/// Returns an iterator over pairs of [`UserApplicationId`]s and their respective
/// [`MockApplication`]s.
pub async fn register_mock_applications<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<vec::IntoIter<(UserApplicationId, MockApplication)>>
where
    C: Context<Extra = TestExecutionRuntimeContext> + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let mock_applications: Vec<_> =
        create_dummy_user_application_registrations(&mut state.system.registry, count)
            .await?
            .into_iter()
            .map(|(id, _description)| (id, MockApplication::default()))
            .collect();
    let extra = state.context().extra();

    for (id, mock_application) in &mock_applications {
        extra
            .user_contracts()
            .insert(*id, Arc::new(mock_application.clone()));
        extra
            .user_services()
            .insert(*id, Arc::new(mock_application.clone()));
    }

    Ok(mock_applications.into_iter())
}

pub async fn create_dummy_user_application_registrations<C>(
    registry: &mut ApplicationRegistryView<C>,
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription)>>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let description = create_dummy_user_application_description(index);
        let id = registry.register_application(description.clone()).await?;

        assert_eq!(registry.describe_application(id).await?, description);

        ids.push((id, description));
    }

    Ok(ids)
}

impl QueryContext {
    /// Spawns a thread running the [`ServiceSyncRuntime`] actor.
    ///
    /// Returns the endpoints to communicate with the actor.
    pub fn spawn_service_runtime_actor(
        self,
    ) -> (
        futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
        std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) {
        let (execution_state_sender, execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let (request_sender, request_receiver) = std::sync::mpsc::channel();

        thread::spawn(move || {
            ServiceSyncRuntime::new(execution_state_sender, self).run(request_receiver)
        });

        (execution_state_receiver, request_sender)
    }
}
