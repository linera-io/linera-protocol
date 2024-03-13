#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Llm;
use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use linera_sdk::{
    base::WithServiceAbi, views::RegisterView, Service, ServiceRuntime, ViewStateStorage,
};
use log::error;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(Llm);

impl WithServiceAbi for Llm {
    type Abi = llm::LlmAbi;
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn hello(&self) -> &str {
        "world!"
    }
}

struct QueryRoot {
    response: String,
}

#[Object]
impl QueryRoot {
    async fn response(&self) -> &str {
        &self.response
    }
}

#[async_trait]
impl Service for Llm {
    type Error = ServiceError;
    type Storage = ViewStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _runtime: &ServiceRuntime,
        request: Self::Query,
    ) -> Result<Self::QueryResponse, Self::Error> {
        let file_contents = fs_err::read_to_string("/tmp/hello_world").unwrap();
        log::error!("{}", file_contents);
        let schema = Schema::build(
            QueryRoot {
                response: file_contents,
            },
            MutationRoot,
            EmptySubscription,
        )
        .finish();
        Ok(schema.execute(request).await)
    }
}

/// An error that can occur while querying the service.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// Query not supported by the application.
    #[error("Queries not supported by application")]
    QueriesNotSupported,

    /// Invalid query argument; could not deserialize request.
    #[error("Invalid query argument; could not deserialize request")]
    InvalidQuery(#[from] serde_json::Error),
    // Add error variants here.
}
