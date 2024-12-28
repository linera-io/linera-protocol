// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Some of these items are only used by some tests, but Rust will complain about unused
// items for the tests where they aren't used
#![allow(unused_imports)]

mod mock_application;
mod system_execution_state;

use std::{collections::BTreeMap, sync::Arc, thread, vec};

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{Amount, Blob, BlockHeight, CompressedBytecode, OracleResponse, Timestamp},
    identifiers::{
        AccountOwner, ApplicationId, BlobId, BlobType, BytecodeId, ChainId, MessageId, Owner,
    },
};
use linera_views::{
    context::Context,
    views::{View, ViewError},
};
use proptest::{prelude::any, strategy::Strategy};
use serde::{Deserialize, Serialize};

pub use self::{
    mock_application::{ExpectedCall, MockApplication, MockApplicationInstance},
    system_execution_state::SystemExecutionState,
};
use crate::{
    ApplicationRegistryView, ExecutionRequest, ExecutionRuntimeContext, ExecutionStateView,
    MessageContext, OperationContext, QueryContext, ServiceRuntimeEndpoint, ServiceRuntimeRequest,
    ServiceSyncRuntime, SystemExecutionStateView, TestExecutionRuntimeContext,
    UserApplicationDescription, UserApplicationId,
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

/// Creates a dummy [`OperationContext`] to use in tests.
pub fn create_dummy_operation_context() -> OperationContext {
    OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
    }
}

/// Creates a dummy [`MessageContext`] to use in tests.
pub fn create_dummy_message_context(authenticated_signer: Option<Owner>) -> MessageContext {
    MessageContext {
        chain_id: ChainId::root(0),
        is_bouncing: false,
        authenticated_signer,
        refund_grant_to: None,
        height: BlockHeight(0),
        certificate_hash: CryptoHash::test_hash("block receiving a message"),
        message_id: MessageId {
            chain_id: ChainId::root(0),
            height: BlockHeight(0),
            index: 0,
        },
    }
}

/// Creates a dummy [`QueryContext`] to use in tests.
pub fn create_dummy_query_context() -> QueryContext {
    QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    }
}

/// Registration of [`MockApplication`]s to use in tests.
#[allow(async_fn_in_trait)]
pub trait RegisterMockApplication {
    /// Returns the chain to use for the creation of the application.
    ///
    /// This is included in the mocked [`ApplicationId`].
    fn creator_chain_id(&self) -> ChainId;

    /// Returns the amount of known registered applications.
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

impl<C> RegisterMockApplication for ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    fn creator_chain_id(&self) -> ChainId {
        self.system.creator_chain_id()
    }

    async fn registered_application_count(&self) -> anyhow::Result<usize> {
        self.system.registered_application_count().await
    }

    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)> {
        self.system
            .register_mock_application_with(description, contract, service)
            .await
    }
}

impl<C> RegisterMockApplication for SystemExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    fn creator_chain_id(&self) -> ChainId {
        self.description.get().expect(
            "Can't register applications on a system state with no associated `ChainDescription`",
        ).into()
    }

    async fn registered_application_count(&self) -> anyhow::Result<usize> {
        Ok(self.registry.known_applications.count().await?)
    }

    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)> {
        let id = self.registry.register_application(description).await?;
        let extra = self.context().extra();
        let mock_application = MockApplication::default();

        extra
            .user_contracts()
            .insert(id, mock_application.clone().into());
        extra
            .user_services()
            .insert(id, mock_application.clone().into());
        extra.add_blobs([contract, service]).await?;

        Ok((id, mock_application))
    }
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

/// Creates a [`Strategy`] for creating a [`BTreeMap`] of [`AccountOwner`]s with an initial
/// non-zero [`Amount`] of tokens.
pub fn test_accounts_strategy() -> impl Strategy<Value = BTreeMap<AccountOwner, Amount>> {
    proptest::collection::btree_map(
        any::<AccountOwner>(),
        (1_u128..).prop_map(Amount::from_tokens),
        0..5,
    )
}
