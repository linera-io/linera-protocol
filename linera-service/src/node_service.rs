use async_graphql::{
    futures_util::Stream, http::GraphiQLSource, EmptyMutation, Error, Object, Schema, Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{response, response::IntoResponse, routing::get, Extension, Router, Server};
use futures::lock::Mutex;
use linera_base::data_types::ChainId;
use linera_chain::ChainStateView;
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    worker::Notification,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::info;
use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};

/// The type of the root GraphQL schema.
type NodeSchema<P, S> = Schema<QueryRoot<P, S>, EmptyMutation, SubscriptionRoot<P, S>>;

/// Our root GraphQL query type.
struct QueryRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

/// Our root GraphQL subscription type.
struct SubscriptionRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

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

#[Object]
impl<P, S> QueryRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn chain(&self, chain_id: ChainId) -> Result<Arc<ChainStateView<S::Context>>, Error> {
        Ok(self.0.lock().await.chain_state_view(chain_id).await?)
    }
}

/// Execute a GraphQL query and generate a response for our `Schema`.
async fn graphql_handler<P, S>(
    schema: Extension<NodeSchema<P, S>>,
    req: GraphQLRequest,
) -> GraphQLResponse
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
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

/// The `NodeService` is a server that exposes a web-server to the client.
/// The node service is primarily used to explore the state of a chain in GraphQL.
pub struct NodeService<P, S> {
    client: ChainClient<P, S>,
    port: NonZeroU16,
}

impl<P, S> NodeService<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Create a new instance of the node service given a client chain and a port.
    pub fn new(client: ChainClient<P, S>, port: NonZeroU16) -> Self {
        Self { client, port }
    }

    /// Run the node service.
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let client = Arc::new(Mutex::new(self.client));

        let schema = Schema::build(
            QueryRoot(client.clone()),
            EmptyMutation,
            SubscriptionRoot(client),
        )
        .finish();

        let graphql_handler = get(graphiql).post(graphql_handler::<P, S>);

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
