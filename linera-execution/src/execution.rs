// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, vec};

use futures::{FutureExt, StreamExt};
use linera_base::{
    data_types::{BlobContent, BlockHeight, StreamUpdate},
    identifiers::{AccountOwner, BlobId, StreamId},
    time::Instant,
};
use linera_views::{
    context::Context,
    key_value_store_view::KeyValueStoreView,
    map_view::MapView,
    reentrant_collection_view::HashedReentrantCollectionView,
    views::{ClonableView, ReplaceContext, View},
};
use linera_views_derive::CryptoHashView;
#[cfg(with_testing)]
use {
    crate::{
        ResourceControlPolicy, ResourceTracker, TestExecutionRuntimeContext, UserContractCode,
    },
    linera_base::data_types::Blob,
    linera_views::context::MemoryContext,
    std::sync::Arc,
};

use super::{execution_state_actor::ExecutionRequest, runtime::ServiceRuntimeRequest};
use crate::{
    execution_state_actor::ExecutionStateActor, resources::ResourceController,
    system::SystemExecutionStateView, ApplicationDescription, ApplicationId, ExecutionError,
    ExecutionRuntimeConfig, ExecutionRuntimeContext, MessageContext, OperationContext,
    ProcessStreamsContext, Query, QueryContext, QueryOutcome, ServiceSyncRuntime, Timestamp,
    TransactionTracker,
};

/// A view accessing the execution state of a chain.
#[derive(Debug, ClonableView, CryptoHashView)]
pub struct ExecutionStateView<C> {
    /// System application.
    pub system: SystemExecutionStateView<C>,
    /// User applications.
    pub users: HashedReentrantCollectionView<C, ApplicationId, KeyValueStoreView<C>>,
    /// The number of events in the streams that this chain is writing to.
    pub stream_event_counts: MapView<C, StreamId, u32>,
}

impl<C: Context, C2: Context> ReplaceContext<C2> for ExecutionStateView<C> {
    type Target = ExecutionStateView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        ExecutionStateView {
            system: self.system.with_context(ctx.clone()).await,
            users: self.users.with_context(ctx.clone()).await,
            stream_event_counts: self.stream_event_counts.with_context(ctx.clone()).await,
        }
    }
}

/// How to interact with a long-lived service runtime.
pub struct ServiceRuntimeEndpoint {
    /// How to receive requests.
    pub incoming_execution_requests: futures::channel::mpsc::UnboundedReceiver<ExecutionRequest>,
    /// How to query the runtime.
    pub runtime_request_sender: std::sync::mpsc::Sender<ServiceRuntimeRequest>,
}

#[cfg(with_testing)]
impl ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>
where
    MemoryContext<TestExecutionRuntimeContext>: Context + Clone + Send + Sync + 'static,
{
    /// Simulates the instantiation of an application.
    pub async fn simulate_instantiation(
        &mut self,
        contract: UserContractCode,
        local_time: linera_base::data_types::Timestamp,
        application_description: ApplicationDescription,
        instantiation_argument: Vec<u8>,
        contract_blob: Blob,
        service_blob: Blob,
    ) -> Result<(), ExecutionError> {
        let chain_id = application_description.creator_chain_id;
        assert_eq!(chain_id, self.context().extra().chain_id);
        let context = OperationContext {
            chain_id,
            authenticated_signer: None,
            height: application_description.block_height,
            round: None,
            timestamp: local_time,
        };

        let action = UserAction::Instantiate(context, instantiation_argument);
        let next_application_index = application_description.application_index + 1;
        let next_chain_index = 0;

        let application_id = From::from(&application_description);
        let blob = Blob::new_application_description(&application_description);

        self.system.used_blobs.insert(&blob.id())?;
        self.system.used_blobs.insert(&contract_blob.id())?;
        self.system.used_blobs.insert(&service_blob.id())?;

        self.context()
            .extra()
            .user_contracts()
            .pin()
            .insert(application_id, contract);

        self.context()
            .extra()
            .add_blobs([
                contract_blob,
                service_blob,
                Blob::new_application_description(&application_description),
            ])
            .await?;

        let tracker = ResourceTracker::default();
        let policy = ResourceControlPolicy::no_fees();
        let mut resource_controller = ResourceController::new(Arc::new(policy), tracker, None);
        let mut txn_tracker = TransactionTracker::new(
            local_time,
            0,
            next_application_index,
            next_chain_index,
            None,
            &[],
        );
        txn_tracker.add_created_blob(blob);
        ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller)
            .run_user_action(application_id, action, context.refund_grant_to(), None)
            .await?;

        Ok(())
    }
}

pub enum UserAction {
    Instantiate(OperationContext, Vec<u8>),
    Operation(OperationContext, Vec<u8>),
    Message(MessageContext, Vec<u8>),
    ProcessStreams(ProcessStreamsContext, Vec<StreamUpdate>),
}

impl UserAction {
    pub(crate) fn signer(&self) -> Option<AccountOwner> {
        match self {
            UserAction::Instantiate(context, _) => context.authenticated_signer,
            UserAction::Operation(context, _) => context.authenticated_signer,
            UserAction::ProcessStreams(_, _) => None,
            UserAction::Message(context, _) => context.authenticated_signer,
        }
    }

    pub(crate) fn height(&self) -> BlockHeight {
        match self {
            UserAction::Instantiate(context, _) => context.height,
            UserAction::Operation(context, _) => context.height,
            UserAction::ProcessStreams(context, _) => context.height,
            UserAction::Message(context, _) => context.height,
        }
    }

    pub(crate) fn round(&self) -> Option<u32> {
        match self {
            UserAction::Instantiate(context, _) => context.round,
            UserAction::Operation(context, _) => context.round,
            UserAction::ProcessStreams(context, _) => context.round,
            UserAction::Message(context, _) => context.round,
        }
    }

    pub(crate) fn timestamp(&self) -> Timestamp {
        match self {
            UserAction::Instantiate(context, _) => context.timestamp,
            UserAction::Operation(context, _) => context.timestamp,
            UserAction::ProcessStreams(context, _) => context.timestamp,
            UserAction::Message(context, _) => context.timestamp,
        }
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub async fn query_application(
        &mut self,
        context: QueryContext,
        query: Query,
        endpoint: Option<&mut ServiceRuntimeEndpoint>,
    ) -> Result<QueryOutcome, ExecutionError> {
        assert_eq!(context.chain_id, self.context().extra().chain_id());
        match query {
            Query::System(query) => {
                let outcome = self.system.handle_query(context, query).await?;
                Ok(outcome.into())
            }
            Query::User {
                application_id,
                bytes,
            } => {
                let ExecutionRuntimeConfig {} = self.context().extra().execution_runtime_config();
                let outcome = match endpoint {
                    Some(endpoint) => {
                        self.query_user_application_with_long_lived_service(
                            application_id,
                            context,
                            bytes,
                            &mut endpoint.incoming_execution_requests,
                            &mut endpoint.runtime_request_sender,
                        )
                        .await?
                    }
                    None => {
                        self.query_user_application(application_id, context, bytes)
                            .await?
                    }
                };
                Ok(outcome.into())
            }
        }
    }

    async fn query_user_application(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        self.query_user_application_with_deadline(
            application_id,
            context,
            query,
            None,
            BTreeMap::new(),
        )
        .await
    }

    pub(crate) async fn query_user_application_with_deadline(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        deadline: Option<Instant>,
        created_blobs: BTreeMap<BlobId, BlobContent>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();
        let mut txn_tracker = TransactionTracker::default().with_blobs(created_blobs);
        let mut resource_controller = ResourceController::default();
        let mut actor = ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller);
        let (code, description) = actor.load_service(application_id).await?;

        let service_runtime_task = linera_base::task::Blocking::spawn(move |mut codes| {
            let mut runtime =
                ServiceSyncRuntime::new_with_deadline(execution_state_sender, context, deadline);

            async move {
                let code = codes.next().await.expect("we send this immediately below");
                runtime.preload_service(application_id, code, description)?;
                runtime.run_query(application_id, query)
            }
        })
        .await;

        service_runtime_task.send(code)?;

        while let Some(request) = execution_state_receiver.next().await {
            actor.handle_request(request).await?;
        }

        service_runtime_task.join().await
    }

    async fn query_user_application_with_long_lived_service(
        &mut self,
        application_id: ApplicationId,
        context: QueryContext,
        query: Vec<u8>,
        incoming_execution_requests: &mut futures::channel::mpsc::UnboundedReceiver<
            ExecutionRequest,
        >,
        runtime_request_sender: &mut std::sync::mpsc::Sender<ServiceRuntimeRequest>,
    ) -> Result<QueryOutcome<Vec<u8>>, ExecutionError> {
        let (outcome_sender, outcome_receiver) = oneshot::channel();
        let mut outcome_receiver = outcome_receiver.fuse();

        runtime_request_sender
            .send(ServiceRuntimeRequest::Query {
                application_id,
                context,
                query,
                callback: outcome_sender,
            })
            .expect("Service runtime thread should only stop when `request_sender` is dropped");

        let mut txn_tracker = TransactionTracker::default();
        let mut resource_controller = ResourceController::default();
        let mut actor = ExecutionStateActor::new(self, &mut txn_tracker, &mut resource_controller);

        loop {
            futures::select! {
                maybe_request = incoming_execution_requests.next() => {
                    if let Some(request) = maybe_request {
                        actor.handle_request(request).await?;
                    }
                }
                outcome = &mut outcome_receiver => {
                    return outcome.map_err(|_| ExecutionError::MissingRuntimeResponse)?;
                }
            }
        }
    }

    pub async fn list_applications(
        &self,
    ) -> Result<Vec<(ApplicationId, ApplicationDescription)>, ExecutionError> {
        let mut applications = vec![];
        for app_id in self.users.indices().await? {
            let blob_id = app_id.description_blob_id();
            let blob_content = self.system.read_blob_content(blob_id).await?;
            let application_description = bcs::from_bytes(blob_content.bytes())?;
            applications.push((app_id, application_description));
        }
        Ok(applications)
    }
}
