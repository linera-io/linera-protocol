// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{ComplexObject, Context, EmptySubscription, Schema};
use linera_sdk::{
    abis::controller::{ControllerAbi, LocalWorkerState, ManagedService, Operation},
    graphql::GraphQLMutationRoot,
    linera_base_types::{DataBlobHash, WithServiceAbi},
    views::View,
    Service, ServiceRuntime,
};

use self::state::ControllerState;

pub struct ControllerService {
    state: Arc<ControllerState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(ControllerService);

impl WithServiceAbi for ControllerService {
    type Abi = ControllerAbi;
}

impl Service for ControllerService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = ControllerState::load(runtime.root_view_storage_context())
            .expect("Failed to load state");
        ControllerService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, query: Self::Query) -> Self::QueryResponse {
        Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .data(self.runtime.clone())
        .finish()
        .execute(query)
        .await
    }
}

#[ComplexObject]
impl ControllerState {
    /// Retrieve information on a given service.
    async fn read_service(
        &self,
        ctx: &Context<'_>,
        service_id: DataBlobHash,
    ) -> Option<ManagedService> {
        let runtime = ctx
            .data::<Arc<ServiceRuntime<ControllerService>>>()
            .unwrap();
        let bytes = runtime.read_data_blob(service_id);
        bcs::from_bytes(&bytes).ok()
    }

    /// Retrieve the local worker state.
    async fn local_worker_state(&self, ctx: &Context<'_>) -> LocalWorkerState {
        let runtime = ctx
            .data::<Arc<ServiceRuntime<ControllerService>>>()
            .unwrap();

        let local_worker = self.local_worker.get().clone();
        let local_service_ids = self
            .local_services
            .indices()
            .expect("storage")
            .into_iter()
            .collect::<Vec<_>>();
        let local_services = local_service_ids
            .into_iter()
            .map(|id| runtime.read_data_blob(id))
            .map(|bytes| bcs::from_bytes(&bytes).ok())
            .collect::<Option<_>>()
            .expect("Service IDs should be valid data blobs");
        let local_chains = self
            .local_chains
            .indices()
            .expect("storage")
            .into_iter()
            .collect();
        let local_message_policy = self
            .local_message_policy
            .index_values()
            .expect("storage")
            .into_iter()
            .collect();
        LocalWorkerState {
            local_worker,
            local_services,
            local_chains,
            local_message_policy,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use linera_sdk::{
        abis::controller::{LocalWorkerState, ManagedService, Worker},
        linera_base_types::{
            AccountOwner, ApplicationId, BlobContent, ChainId, CryptoHash, DataBlobHash,
        },
        util::BlockingWait,
        views::View,
        Service, ServiceRuntime,
    };
    use serde_json::json;

    use super::{ControllerService, ControllerState};

    fn create_service() -> ControllerService {
        let runtime = ServiceRuntime::<ControllerService>::new();
        let state = ControllerState::load(runtime.root_view_storage_context())
            .expect("Failed to read from mock key value store");

        ControllerService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    #[test]
    fn query_local_worker_state_empty() {
        let service = create_service();

        let response = service
            .handle_query(async_graphql::Request::new("{ localWorkerState }"))
            .blocking_wait()
            .data
            .into_json()
            .expect("Response should be JSON");

        let state: LocalWorkerState = serde_json::from_value(response["localWorkerState"].clone())
            .expect("Should deserialize LocalWorkerState");

        assert!(state.local_worker.is_none());
        assert!(state.local_services.is_empty());
        assert!(state.local_chains.is_empty());
    }

    #[test]
    fn query_read_service() {
        let service = create_service();

        // Create a test ManagedService and store it as a data blob.
        let managed_service = ManagedService {
            application_id: ApplicationId::default(),
            name: "test-service".to_string(),
            chain_id: ChainId::default(),
            requirements: vec!["API_KEY".to_string()],
        };

        let service_bytes = bcs::to_bytes(&managed_service).expect("Failed to serialize service");
        let service_id = DataBlobHash(CryptoHash::new(&BlobContent::new_data(
            service_bytes.clone(),
        )));
        service.runtime.set_blob(service_id, service_bytes);

        // Query the service.
        let response = service
            .handle_query(async_graphql::Request::new(format!(
                r#"{{ readService(serviceId: "{}") }}"#,
                service_id.0
            )))
            .blocking_wait()
            .data
            .into_json()
            .expect("Response should be JSON");

        let result: ManagedService = serde_json::from_value(response["readService"].clone())
            .expect("Should deserialize ManagedService");

        assert_eq!(result.name, "test-service");
        assert_eq!(result.chain_id, ChainId::default());
        assert_eq!(result.requirements, vec!["API_KEY"]);
    }

    #[test]
    fn query_read_service_empty_blob() {
        let service = create_service();

        // Create a service ID pointing to an empty blob.
        let empty_bytes = vec![];
        let blob_id = DataBlobHash(CryptoHash::new(&BlobContent::new_data(empty_bytes.clone())));
        service.runtime.set_blob(blob_id, empty_bytes);

        // Query should return null for empty/unparseable blob.
        let response = service
            .handle_query(async_graphql::Request::new(format!(
                r#"{{ readService(serviceId: "{}") }}"#,
                blob_id.0
            )))
            .blocking_wait()
            .data
            .into_json()
            .expect("Response should be JSON");

        assert_eq!(response["readService"], json!(null));
    }

    #[test]
    fn query_local_worker_state_populated() {
        let runtime = ServiceRuntime::<ControllerService>::new();
        let mut state = ControllerState::load(runtime.root_view_storage_context())
            .expect("Failed to read from mock key value store");

        // Set up a local worker.
        let worker = Worker {
            owner: AccountOwner::Address32(CryptoHash::test_hash("owner")),
            capabilities: vec!["GPU".to_string(), "API_KEY".to_string()],
        };
        state.local_worker.set(Some(worker));

        // Create and store a managed service as a blob.
        let managed_service = ManagedService {
            application_id: ApplicationId::default(),
            name: "test-engine".to_string(),
            chain_id: ChainId::default(),
            requirements: vec!["DATABASE_URL".to_string()],
        };
        let service_bytes = bcs::to_bytes(&managed_service).expect("Failed to serialize");
        let service_id = DataBlobHash(CryptoHash::new(&BlobContent::new_data(
            service_bytes.clone(),
        )));
        state
            .local_services
            .insert(&service_id)
            .expect("Failed to insert service");

        // Add some local chains.
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        state
            .local_chains
            .insert(&chain1)
            .expect("Failed to insert chain");
        state
            .local_chains
            .insert(&chain2)
            .expect("Failed to insert chain");

        let runtime = Arc::new(runtime);
        runtime.set_blob(service_id, service_bytes);

        let service = ControllerService {
            state: Arc::new(state),
            runtime,
        };

        let response = service
            .handle_query(async_graphql::Request::new("{ localWorkerState }"))
            .blocking_wait()
            .data
            .into_json()
            .expect("Response should be JSON");

        let state: LocalWorkerState = serde_json::from_value(response["localWorkerState"].clone())
            .expect("Should deserialize LocalWorkerState");

        // Verify worker.
        let local_worker = state.local_worker.expect("Worker should be present");
        assert_eq!(local_worker.capabilities, vec!["GPU", "API_KEY"]);

        // Verify services.
        assert_eq!(state.local_services.len(), 1);
        assert_eq!(state.local_services[0].name, "test-engine");

        // Verify chains.
        assert_eq!(state.local_chains.len(), 2);
        assert!(state.local_chains.contains(&chain1));
    }
}
