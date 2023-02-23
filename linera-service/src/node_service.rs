use async_graphql::{
    futures_util::Stream, http::GraphiQLSource, EmptyMutation, Error, Object, Schema, SimpleObject,
    Subscription,
};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    extract::Path,
    http::{StatusCode, Uri},
    response,
    response::IntoResponse,
    routing::get,
    Extension, Router, Server,
};
use futures::lock::Mutex;
use linera_base::data_types::ChainId;
use linera_chain::ChainStateView;
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    worker::Notification,
};
use linera_execution::{
    ApplicationId, Query, Response, UserApplicationDescription, UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::{error, info};
use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};
use thiserror::Error as ThisError;

/// Our root GraphQL query type.
struct QueryRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

/// Our root GraphQL subscription type.
struct SubscriptionRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

#[derive(Debug, ThisError)]
enum NodeServiceError {
    #[error("could not decode query string")]
    QueryStringError(#[from] hex::FromHexError),
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for NodeServiceError {
    fn into_response(self) -> response::Response {
        let tuple = match self {
            NodeServiceError::QueryStringError(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            NodeServiceError::BcsError(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            NodeServiceError::JsonError(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            NodeServiceError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        tuple.into_response()
    }
}

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
    async fn chain(
        &self,
        chain_id: Option<ChainId>,
    ) -> Result<Arc<ChainStateView<S::Context>>, Error> {
        Ok(self.0.lock().await.chain_state_view(chain_id).await?)
    }

    async fn applications(
        &self,
        chain_id: Option<ChainId>,
    ) -> Result<Vec<ApplicationOverview>, Error> {
        let applications = self
            .0
            .lock()
            .await
            .chain_state_view(chain_id)
            .await?
            .execution_state
            .list_applications()
            .await?;

        let overviews = applications
            .into_iter()
            .map(ApplicationOverview::from)
            .collect();

        Ok(overviews)
    }
}

#[derive(SimpleObject)]
pub struct ApplicationOverview {
    id: UserApplicationId,
    description: UserApplicationDescription,
    link: String,
}

impl From<(UserApplicationId, UserApplicationDescription)> for ApplicationOverview {
    fn from((id, description): (UserApplicationId, UserApplicationDescription)) -> Self {
        let id_bytes = bcs::to_bytes(&id).expect("user application ids should be bcs serializable");
        let encoded_id = hex::encode(id_bytes);
        Self {
            id,
            description,
            link: format!("http://localhost:{}/applications/{}", 8080, encoded_id),
        }
    }
}

/// Execute a GraphQL query and generate a response for our `Schema`.
async fn index_handler<P, S>(
    client: Extension<Arc<Mutex<ChainClient<P, S>>>>,
    req: GraphQLRequest,
) -> GraphQLResponse
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let schema = Schema::build(
        QueryRoot(client.0.clone()),
        EmptyMutation,
        SubscriptionRoot(client.0.clone()),
    )
    .finish();

    schema.execute(req.into_inner()).await.into()
}

/// Execute a GraphQL query against an application.
async fn application_handler<P, S>(
    Path(user_application_id): Path<String>,
    client: Extension<Arc<Mutex<ChainClient<P, S>>>>,
    req: GraphQLRequest,
) -> Result<GraphQLResponse, NodeServiceError>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let user_application_id_bytes = hex::decode(user_application_id)?;
    let user_application_id: UserApplicationId = bcs::from_bytes(&user_application_id_bytes)?;
    let application_id = ApplicationId::User(user_application_id);
    let serialized_request = serde_json::to_vec(&req.into_inner())?;
    let query = Query::User(serialized_request);
    let response = client
        .0
        .lock()
        .await
        .query_application(application_id, &query)
        .await?;
    let user_response_bytes = match response {
        Response::System(_) => unreachable!("cannot get a system response for a user query"),
        Response::User(user) => user,
    };
    let graphql_response: async_graphql::Response = serde_json::from_slice(&user_response_bytes)?;
    Ok(graphql_response.into())
}

/// An HTML response constructing the GraphiQL web page.
async fn graphiql(uri: Uri) -> impl IntoResponse {
    response::Html(
        GraphiQLSource::build()
            .endpoint(uri.path())
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

        let index_handler = get(graphiql).post(index_handler::<P, S>);
        let applications_handler = get(graphiql).post(application_handler::<P, S>);

        let schema = Schema::build(
            QueryRoot(client.clone()),
            EmptyMutation,
            SubscriptionRoot(client.clone()),
        )
        .finish();

        let app = Router::new()
            .route("/", index_handler)
            .route("/applications/:id", applications_handler)
            .route_service("/ws", GraphQLSubscription::new(schema.clone()))
            .layer(Extension(client))
            .layer(Extension(self.port));

        let port = self.port.get();

        info!("GraphiQL IDE: http://localhost:{}", port);

        Server::bind(&SocketAddr::from(([127, 0, 0, 1], port)))
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }
}
