// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{
    linera_base_types::{ApplicationId, ChainId, WithServiceAbi},
    Service, ServiceRuntime,
};

pub struct MetaCounterService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(MetaCounterService);

impl WithServiceAbi for MetaCounterService {
    type Abi = meta_counter::MetaCounterAbi;
}

/// Custom mutation root for meta-counter.
///
/// The `Operation` type is a struct, so it can't derive `GraphQLMutationRoot`.
/// Instead we manually define GraphQL mutations that construct `Operation` values.
struct MutationRoot {
    runtime: Arc<ServiceRuntime<MetaCounterService>>,
}

#[async_graphql::Object]
#[allow(clippy::too_many_arguments)]
impl MutationRoot {
    async fn increment(
        &self,
        recipient_id: ChainId,
        value: u64,
        fuel_grant: Option<u64>,
        query_service: Option<bool>,
        authenticated: Option<bool>,
        is_tracked: Option<bool>,
    ) -> Vec<u8> {
        let operation = meta_counter::Operation {
            recipient_id,
            authenticated: authenticated.unwrap_or(false),
            is_tracked: is_tracked.unwrap_or(false),
            query_service: query_service.unwrap_or(false),
            fuel_grant: fuel_grant.unwrap_or(0),
            message: meta_counter::Message::Increment(value),
        };
        self.runtime.schedule_operation(&operation);
        vec![]
    }

    async fn fail(
        &self,
        recipient_id: ChainId,
        is_tracked: Option<bool>,
        authenticated: Option<bool>,
    ) -> Vec<u8> {
        let operation = meta_counter::Operation {
            recipient_id,
            authenticated: authenticated.unwrap_or(false),
            is_tracked: is_tracked.unwrap_or(false),
            query_service: false,
            fuel_grant: 0,
            message: meta_counter::Message::Fail,
        };
        self.runtime.schedule_operation(&operation);
        vec![]
    }
}

/// Query root that delegates to the counter application.
struct QueryRoot {
    runtime: Arc<ServiceRuntime<MetaCounterService>>,
}

#[async_graphql::Object]
impl QueryRoot {
    async fn value(&self) -> async_graphql::Result<u64> {
        let counter_id = self.runtime.application_parameters();
        let response: Response = self
            .runtime
            .query_application(counter_id, &"query { value }".into());
        let data = response.data.into_json()?;
        Ok(data["value"].as_u64().unwrap_or(0))
    }
}

impl Service for MetaCounterService {
    type Parameters = ApplicationId<counter::CounterAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        MetaCounterService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            QueryRoot {
                runtime: self.runtime.clone(),
            },
            MutationRoot {
                runtime: self.runtime.clone(),
            },
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}
