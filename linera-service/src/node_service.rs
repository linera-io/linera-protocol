use async_graphql::{
    futures_util::Stream,
    http::GraphiQLSource,
    parser::types::{DocumentOperations, ExecutableDocument, OperationType},
    Error, Object, Request, Schema, ServerError, SimpleObject, Subscription,
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
use linera_base::{
    committee::Committee,
    crypto::PublicKey,
    data_types::{ChainId, Epoch, Owner},
};
use linera_chain::{data_types::Certificate, ChainStateView};
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    worker::Notification,
};
use linera_execution::{
    system::{Amount, Recipient, SystemChannel, UserData},
    ApplicationId, Bytecode, BytecodeId, Operation, Query, Response, SystemOperation,
    UserApplicationDescription, UserApplicationId,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde_json::Value;
use std::{collections::BTreeMap, net::SocketAddr, num::NonZeroU16, sync::Arc};
use thiserror::Error as ThisError;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

/// Our root GraphQL query type.
struct QueryRoot<P, S>(Arc<NodeService<P, S>>);

/// Our root GraphQL subscription type.
struct SubscriptionRoot<P, S>(Arc<NodeService<P, S>>);

/// Our root GraphQL mutation type.
struct MutationRoot<P, S>(Arc<NodeService<P, S>>);

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
    #[error("missing graphql operation")]
    MissingOperation,
    #[error("unsupported query type: subscription")]
    UnsupportedQueryType,
    #[error("graphql operations of different types submitted")]
    HeterogeneousOperations,
    #[error("failed to parse graphql query: {error}")]
    GraphQLServerError { error: String },
    #[error("malformed application response")]
    MalformedApplicationResponse,
}

impl From<ServerError> for NodeServiceError {
    fn from(value: ServerError) -> Self {
        NodeServiceError::GraphQLServerError {
            error: value.to_string(),
        }
    }
}

impl IntoResponse for NodeServiceError {
    fn into_response(self) -> response::Response {
        let tuple = match self {
            NodeServiceError::QueryStringError(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            NodeServiceError::BcsError(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            NodeServiceError::JsonError(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            NodeServiceError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            NodeServiceError::MalformedApplicationResponse => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
            NodeServiceError::MissingOperation
            | NodeServiceError::HeterogeneousOperations
            | NodeServiceError::UnsupportedQueryType => (StatusCode::BAD_REQUEST, self.to_string()),
            NodeServiceError::GraphQLServerError { error } => (StatusCode::BAD_REQUEST, error),
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
        Ok(self.0.client.lock().await.subscribe_all(chain_ids).await?)
    }
}

impl<P, S> MutationRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn execute_system_operation(
        &self,
        system_operation: SystemOperation,
    ) -> Result<Certificate, Error> {
        let application_id = ApplicationId::System;
        let operation = Operation::System(system_operation);
        let mut client = self.0.client.lock().await;
        client.process_inbox().await?;
        Ok(client.execute_operation(application_id, operation).await?)
    }
}

#[Object]
impl<P, S> MutationRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the unattributed account.
    async fn transfer(
        &self,
        owner: Option<Owner>,
        recipient: Recipient,
        amount: Amount,
        user_data: Option<UserData>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::Transfer {
            owner,
            recipient,
            amount,
            user_data: user_data.unwrap_or_default(),
        };
        self.execute_system_operation(operation).await
    }

    /// Claims `amount` units of value from the given owner's account in
    /// the remote `target` chain. Depending on its configuration (see also #464), the
    /// `target` chain may refuse to process the message.
    async fn claim(
        &self,
        owner: Owner,
        target: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: Option<UserData>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::Claim {
            owner,
            target,
            recipient,
            amount,
            user_data: user_data.unwrap_or_default(),
        };
        self.execute_system_operation(operation).await
    }

    /// Creates (or activates) a new chain by installing the given authentication key.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    async fn open_chain(
        &self,
        id: ChainId,
        public_key: PublicKey,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::OpenChain {
            id,
            public_key,
            admin_id,
            epoch,
            committees,
        };
        self.execute_system_operation(operation).await
    }

    /// Closes the chain.
    async fn close_chain(&self) -> Result<Certificate, Error> {
        let operation = SystemOperation::CloseChain;
        self.execute_system_operation(operation).await
    }

    /// Changes the authentication key of the chain.
    async fn change_owner(&self, new_public_key: PublicKey) -> Result<Certificate, Error> {
        let operation = SystemOperation::ChangeOwner { new_public_key };
        self.execute_system_operation(operation).await
    }

    /// Changes the authentication key of the chain.
    async fn change_multiple_owners(
        &self,
        new_public_keys: Vec<PublicKey>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::ChangeMultipleOwners { new_public_keys };
        self.execute_system_operation(operation).await
    }

    /// (admin chain only) Registers a new committee. This will notify the subscribers of
    /// the admin chain so that they can migrate to the new epoch (by accepting the
    /// notification as an "incoming message" in a next block).
    async fn create_committee(
        &self,
        admin_id: ChainId,
        epoch: Epoch,
        committee: Committee,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::CreateCommittee {
            admin_id,
            epoch,
            committee,
        };
        self.execute_system_operation(operation).await
    }

    /// Subscribes to a system channel.
    async fn subscribe(
        &self,
        chain_id: ChainId,
        channel: SystemChannel,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::Subscribe { chain_id, channel };
        self.execute_system_operation(operation).await
    }

    /// Unsubscribes to a system channel.
    async fn unsubscribe(
        &self,
        chain_id: ChainId,
        channel: SystemChannel,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::Unsubscribe { chain_id, channel };
        self.execute_system_operation(operation).await
    }

    /// (admin chain only) Removes a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    async fn remove_committee(
        &self,
        admin_id: ChainId,
        epoch: Epoch,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::RemoveCommittee { admin_id, epoch };
        self.execute_system_operation(operation).await
    }

    /// Publishes a new application bytecode.
    async fn publish_bytecodes(
        &self,
        contract: Bytecode,
        service: Bytecode,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::PublishBytecode { contract, service };
        self.execute_system_operation(operation).await
    }

    async fn create_application(
        &self,
        bytecode_id: BytecodeId,
        parameters: Vec<u8>,
        initialization_argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::CreateApplication {
            bytecode_id,
            parameters,
            initialization_argument,
            required_application_ids,
        };
        self.execute_system_operation(operation).await
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
        Ok(self
            .0
            .client
            .lock()
            .await
            .chain_state_view(chain_id)
            .await?)
    }

    async fn applications(
        &self,
        chain_id: Option<ChainId>,
    ) -> Result<Vec<ApplicationOverview>, Error> {
        let applications = self
            .0
            .client
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
    client: Extension<Arc<NodeService<P, S>>>,
    req: GraphQLRequest,
) -> GraphQLResponse
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let schema = Schema::build(
        QueryRoot(client.0.clone()),
        MutationRoot(client.0.clone()),
        SubscriptionRoot(client.0.clone()),
    )
    .finish();

    schema.execute(req.into_inner()).await.into()
}

/// Execute a GraphQL query against an application.
/// Pattern matches on the `OperationType` of the query and routes the query
/// accordingly.
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
    let mut req = req.into_inner();

    let parsed_query = req.parsed_query()?;
    let operation_type = operation_type(parsed_query)?;

    let user_application_id_bytes = hex::decode(user_application_id)?;
    let user_application_id: UserApplicationId = bcs::from_bytes(&user_application_id_bytes)?;
    let application_id = ApplicationId::User(user_application_id);

    let response = match operation_type {
        OperationType::Query => user_application_query(application_id, client, &req).await?,
        OperationType::Mutation => user_application_mutation(application_id, client, &req).await?,
        OperationType::Subscription => return Err(NodeServiceError::UnsupportedQueryType),
    };

    Ok(response.into())
}

/// Given a parsed GraphQL query (or `ExecutableDocument`), return the `OperationType`.
///
/// Errors:
///
/// If we have no `OperationTypes` or the `OperationTypes` heterogeneous, i.e. a query
/// was submitted with a `mutation` and `subscription`.
fn operation_type(document: &ExecutableDocument) -> Result<OperationType, NodeServiceError> {
    match &document.operations {
        DocumentOperations::Single(op) => Ok(op.node.ty),
        DocumentOperations::Multiple(ops) => {
            let mut op_types = ops.values().map(|v| v.node.ty);
            let first = op_types.next().ok_or(NodeServiceError::MissingOperation)?;
            op_types
                .all(|x| x == first)
                .then_some(first)
                .ok_or(NodeServiceError::HeterogeneousOperations)
        }
    }
}

/// Handles queries for user applications.
async fn user_application_query<P, S>(
    application_id: ApplicationId,
    client: Extension<Arc<Mutex<ChainClient<P, S>>>>,
    req: &Request,
) -> Result<async_graphql::Response, NodeServiceError>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let serialized_request = serde_json::to_vec(&req)?;
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
    Ok(serde_json::from_slice(&user_response_bytes)?)
}

/// Handles mutations for user applications.
async fn user_application_mutation<P, S>(
    application_id: ApplicationId,
    client: Extension<Arc<Mutex<ChainClient<P, S>>>>,
    req: &Request,
) -> Result<async_graphql::Response, NodeServiceError>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let graphql_res = user_application_query(application_id, client.clone(), req).await?;
    let res_json = graphql_res.data.into_json()?;
    let bcs_bytes =
        bytes_from_res(res_json).ok_or(NodeServiceError::MalformedApplicationResponse)?;
    let operation = Operation::User(bcs_bytes);

    let mut client = client.0.lock().await;
    client.process_inbox().await?;
    let certificate = client.execute_operation(application_id, operation).await?;
    let value = async_graphql::Value::from_json(serde_json::to_value(certificate)?)?;
    Ok(async_graphql::Response::new(value))
}

/// Extracts the underlying byte vector from a serialized GraphQL response
/// from an application.
fn bytes_from_res(json: Value) -> Option<Vec<u8>> {
    Some(
        json.get("executeOperation")?
            .as_array()?
            .iter()
            .map(|v| v.as_u64().unwrap() as u8)
            .collect(),
    )
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
    client: Mutex<ChainClient<P, S>>,
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
        let client = Mutex::new(client);
        Self { client, port }
    }

    /// Run the node service.
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let port = self.port.get();
        let service = Arc::new(self);

        let index_handler = get(graphiql).post(index_handler::<P, S>);
        let applications_handler = get(graphiql).post(application_handler::<P, S>);

        let schema = Schema::build(
            QueryRoot(service.clone()),
            MutationRoot(service.clone()),
            SubscriptionRoot(service.clone()),
        )
        .finish();

        let app = Router::new()
            .route("/", index_handler)
            .route("/applications/:id", applications_handler)
            .route_service("/ws", GraphQLSubscription::new(schema.clone()))
            .layer(Extension(service))
            // TODO(#551): Provide application authentication.
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        Server::bind(&SocketAddr::from(([127, 0, 0, 1], port)))
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }
}
