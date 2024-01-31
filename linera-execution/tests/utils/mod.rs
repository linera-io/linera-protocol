// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod mock_application;

pub use self::mock_application::{ExpectedCall, MockApplication};
use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, MessageId},
};
use linera_execution::{
    ApplicationRegistryView, BytecodeLocation, ExecutionRuntimeContext, ExecutionStateView,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};
use linera_views::{
    common::Context,
    views::{View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, vec};

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
