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
    data_types::{Blob, BlockHeight, CompressedBytecode, OracleResponse},
    identifiers::{ApplicationId, BlobId, BlobType, BytecodeId, ChainId, MessageId},
};
use linera_views::{
    context::Context,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};

pub use self::{
    mock_application::{ExpectedCall, MockApplication, MockApplicationInstance},
    system_execution_state::SystemExecutionState,
};
use crate::{
    ApplicationRegistryView, ExecutionRequest, ExecutionRuntimeContext, ExecutionStateView,
    QueryContext, ServiceRuntimeEndpoint, ServiceRuntimeRequest, ServiceSyncRuntime,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};

/// Creates a dummy [`UserApplicationDescription`] for use in tests.
pub fn create_dummy_user_application_description(
    index: u64,
) -> (UserApplicationDescription, Blob, Blob) {
    let chain_id = ChainId::root(1);
    let contract_blob = Blob::new_contract_bytecode(CompressedBytecode {
        compressed_bytes: b"contract".to_vec(),
    });
    let service_blob = Blob::new_service_bytecode(CompressedBytecode {
        compressed_bytes: b"service".to_vec(),
    });

    (
        UserApplicationDescription {
            bytecode_id: BytecodeId::new(contract_blob.id().hash, service_blob.id().hash),
            creation: MessageId {
                chain_id,
                height: BlockHeight(index),
                index: 1,
            },
            required_application_ids: vec![],
            parameters: vec![],
        },
        contract_blob,
        service_blob,
    )
}

/// Registration of [`MockApplication`]s to use in tests.
#[allow(async_fn_in_trait)]
pub trait RegisterMockApplication {
    /// Returns the chain to use for the creation of the application.
    ///
    /// This is included in the mocked [`ApplicationId`].
    fn creator_chain_id(&self) -> ChainId;

    /// Returns the amount of known registrated applications.
    ///
    /// Used to avoid duplicate registrations.
    async fn registered_application_count(&self) -> anyhow::Result<usize>;

    /// Registers a new [`MockApplication`] and returns it with the [`UserApplicationId`] that was
    /// used for it.
    async fn register_mock_application(
        &mut self,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)> {
        let (description, contract, service) = create_dummy_user_application_description(
            self.registered_application_count().await? as u64,
        );

        self.register_mock_application_with(description, contract, service)
            .await
    }

    /// Registers a new [`MockApplication`] associated with a [`UserApplicationDescription`] and
    /// its bytecode [`Blob`]s.
    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)>;
}

/// Creates `count` [`MockApplication`]s and registers them in the provided [`ExecutionStateView`].
///
/// Returns an iterator over pairs of [`UserApplicationId`]s and their respective
/// [`MockApplication`]s.
pub async fn register_mock_applications<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<vec::IntoIter<(UserApplicationId, MockApplication, Blob, Blob)>>
where
    C: Context<Extra = TestExecutionRuntimeContext> + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    let mock_applications = register_mock_applications_internal(state, count).await?;
    let extra = state.context().extra();
    for (_id, _mock_application, contract_blob, service_blob) in &mock_applications {
        extra.add_blobs(vec![contract_blob.clone(), service_blob.clone()]);
    }

    Ok(mock_applications.into_iter())
}

pub async fn register_mock_applications_internal<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, MockApplication, Blob, Blob)>>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    let mock_applications: Vec<_> =
        create_dummy_user_application_registrations(&mut state.system.registry, count)
            .await?
            .into_iter()
            .map(|(id, _description, contract_blob, service_blob)| {
                (id, MockApplication::default(), contract_blob, service_blob)
            })
            .collect();
    let extra = state.context().extra();

    for (id, mock_application, _contract_blob, _service_blob) in &mock_applications {
        extra
            .user_contracts()
            .insert(*id, mock_application.clone().into());
        extra
            .user_services()
            .insert(*id, mock_application.clone().into());
    }

    Ok(mock_applications)
}

pub async fn create_dummy_user_application_registrations<C>(
    registry: &mut ApplicationRegistryView<C>,
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription, Blob, Blob)>>
where
    C: Context + Clone + Send + Sync + 'static,
{
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let (description, contract_blob, service_blob) =
            create_dummy_user_application_description(index);
        let id = registry.register_application(description.clone()).await?;

        assert_eq!(registry.describe_application(id).await?, description);

        ids.push((id, description, contract_blob, service_blob));
    }

    Ok(ids)
}

impl QueryContext {
    /// Spawns a thread running the [`ServiceSyncRuntime`] actor.
    ///
    /// Returns the endpoints to communicate with the actor.
    pub fn spawn_service_runtime_actor(self) -> ServiceRuntimeEndpoint {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        thread::spawn(move || {
            ServiceSyncRuntime::new(execution_state_sender, self).run(runtime_request_receiver)
        });

        ServiceRuntimeEndpoint {
            incoming_execution_requests,
            runtime_request_sender,
        }
    }
}
