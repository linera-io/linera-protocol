// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use how_to_perform_http_requests::{Abi, Operation};
use linera_sdk::{ensure, http, linera_base_types::WithServiceAbi, Service as _, ServiceRuntime};

#[derive(Clone)]
pub struct Service {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(Service);

impl WithServiceAbi for Service {
    type Abi = Abi;
}

impl linera_sdk::Service for Service {
    type Parameters = String;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        Service {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            Query {
                service: self.clone(),
            },
            Mutation {
                service: self.clone(),
            },
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

/// The handler for service queries.
struct Query {
    service: Service,
}

#[async_graphql::Object]
impl Query {
    /// Performs an HTTP query in the service, and returns the response body if the status
    /// code is OK.
    ///
    /// Note that any headers in the response are discarded.
    pub async fn perform_http_request(&self) -> async_graphql::Result<Vec<u8>> {
        self.service.perform_http_request()
    }
}

impl Service {
    /// Performs an HTTP query in the service, and returns the response body if the status
    /// code is OK.
    ///
    /// Note that any headers in the response are discarded.
    pub fn perform_http_request(&self) -> async_graphql::Result<Vec<u8>> {
        let url = self.runtime.application_parameters();
        let response = self.runtime.http_request(http::Request::get(url));

        ensure!(
            response.status == 200,
            async_graphql::Error::new(format!(
                "HTTP request failed with status code {}",
                response.status
            ))
        );

        Ok(response.body)
    }
}

/// The handler for service mutations.
struct Mutation {
    service: Service,
}

#[async_graphql::Object]
impl Mutation {
    /// Performs an HTTP query in the service, and sends the response to the contract by scheduling
    /// an [`Operation::HandleHttpResponse`].
    pub async fn perform_http_request(&self) -> async_graphql::Result<bool> {
        let response = self.service.perform_http_request()?;

        self.service
            .runtime
            .schedule_operation(&Operation::HandleHttpResponse(response));

        Ok(true)
    }
}

#[path = "unit_tests/service.rs"]
mod unit_tests;
