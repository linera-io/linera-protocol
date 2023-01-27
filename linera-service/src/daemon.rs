use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::sync::Arc;
use async_graphql::{EmptyMutation, EmptySubscription, Schema, Object, Error, Subscription};
use async_graphql::futures_util::Stream;
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, response, Router, Server};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::lock::Mutex;
use log::info;
use linera_base::data_types::ChainId;
use linera_core::client::{ChainClientState, ValidatorNodeProvider};
use linera_core::worker::Notification;
use linera_storage::Store;
use linera_views::views::ViewError;

/// The type of the root GraphQL schema.
type NodeSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn echo(&self, str: String) -> String {
        str
    }
}

/// Our root GraphQL subscription type.
struct SubscriptionRoot<P, S>(Arc<Mutex<ChainClientState<P, S>>>);

#[Subscription]
impl<P, S> SubscriptionRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Get a subscription to a stream of `Notification`s for a collection of `ChainId`s.
    async fn notifications(
        &self,
        chain_ids: Vec<ChainId>,
    ) -> Result<impl Stream<Item = Notification>, Error> {
        Ok(self.0.lock().await.subscribe_all(chain_ids).await?)
    }
}

/// Execute a GraphQL query and generate a response for our `Schema`.
async fn graphql_handler(schema: Extension<NodeSchema>, req: GraphQLRequest) -> GraphQLResponse
{
    schema.execute(req.into_inner()).await.into()
}

/// An HTML response constructing the GraphiQL web page.
async fn graphiql() -> impl IntoResponse {
    response::Html(
        GraphiQLSource::build()
            .endpoint("/")
            .subscription_endpoint("/ws")
            .finish(),
    )
}

/// The `Daemon` is a server that exposes a web-server to the client.
/// The daemon is primarily used to explore the state of a chain in GraphQL.
pub struct Daemon<P, S> {
    _client: ChainClientState<P, S>,
    port: NonZeroU16,
}

impl<P, S> Daemon<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Create a new instance of the daemon given a client chain and a port.
    pub fn new(_client: ChainClientState<P, S>, port: NonZeroU16) -> Self {
        Self { _client, port }
    }

    /// Run the daemon.
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

        let graphql_handler = get(graphiql).post(graphql_handler);

        let app = Router::new()
            .route("/", graphql_handler)
            .route_service("/ws", GraphQLSubscription::new(schema.clone()))
            .layer(Extension(schema));

        let port = self.port.get();

        info!("GraphiQL IDE: http://localhost:{}", port);

        Server::bind(&SocketAddr::from(([127, 0, 0, 1], port)))
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }
}
