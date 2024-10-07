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
    execution::ServiceRuntimeEndpoint, ExecutionRequest, ExecutionRuntimeContext,
    ExecutionStateView, QueryContext, ServiceRuntimeRequest, ServiceSyncRuntime,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};

pub fn create_dummy_user_application_description(
    index: u64,
) -> (UserApplicationDescription, Blob, Blob) {
    let chain_id = ChainId::root(1);
    let contract_blob = Blob::new_contract_bytecode(CompressedBytecode {
        compressed_bytes: String::from("contract").as_bytes().to_vec(),
    })
    .unwrap();
    let service_blob = Blob::new_service_bytecode(CompressedBytecode {
        compressed_bytes: String::from("service").as_bytes().to_vec(),
    })
    .unwrap();

    (
        UserApplicationDescription {
            bytecode_id: BytecodeId::new(contract_blob.id().hash, service_blob.id().hash),
            creator_chain_id: chain_id,
            block_height: BlockHeight(index),
            operation_index: 0,
            required_application_ids: vec![],
            parameters: vec![],
        },
        contract_blob,
        service_blob,
    )
}

#[derive(Deserialize, Serialize)]
pub struct FakeBlob(String);

impl BcsSignable for FakeBlob {}

/// Creates `count` [`MockApplication`]s and registers them in the provided [`ExecutionStateView`].
///
/// Returns an iterator over pairs of [`UserApplicationId`]s and their respective
/// [`MockApplication`]s.
pub async fn register_mock_applications<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<vec::IntoIter<(UserApplicationId, MockApplication, Blob, Blob)>>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    let mock_applications: Vec<_> = create_dummy_user_applications(count)
        .await?
        .into_iter()
        .map(|(id, description, contract_blob, service_blob)| {
            (
                id,
                description,
                MockApplication::default(),
                contract_blob,
                service_blob,
            )
        })
        .collect();
    let extra = state.context().extra();

    for (id, description, mock_application, contract_blob, service_blob) in &mock_applications {
        extra
            .user_contracts()
            .insert(*id, mock_application.clone().into());
        extra
            .user_services()
            .insert(*id, mock_application.clone().into());

        extra
            .blobs()
            .insert(contract_blob.id(), contract_blob.clone());
        extra
            .blobs()
            .insert(service_blob.id(), service_blob.clone());

        let app_blob = Blob::new_application_description(description.clone())?;
        extra.blobs().insert(app_blob.id(), app_blob);
    }

    Ok(mock_applications
        .into_iter()
        .map(|(id, _, mock_application, contract_blob, service_blob)| {
            (id, mock_application, contract_blob, service_blob)
        })
        .collect::<Vec<_>>()
        .into_iter())
}

pub async fn create_dummy_user_applications(
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription, Blob, Blob)>> {
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let (description, contract_blob, service_blob) =
            create_dummy_user_application_description(index);
        ids.push((
            UserApplicationId::try_from(&description)?,
            description,
            contract_blob,
            service_blob,
        ));
    }

    Ok(ids)
}

pub fn get_application_blob_oracle_responses(app_id: &ApplicationId) -> Vec<OracleResponse> {
    vec![
        OracleResponse::Blob(BlobId::new(
            app_id.bytecode_id.contract_blob_hash,
            BlobType::ContractBytecode,
        )),
        OracleResponse::Blob(BlobId::new(
            app_id.bytecode_id.service_blob_hash,
            BlobType::ServiceBytecode,
        )),
        OracleResponse::Blob(BlobId::new(
            app_id.application_description_hash,
            BlobType::ApplicationDescription,
        )),
    ]
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
