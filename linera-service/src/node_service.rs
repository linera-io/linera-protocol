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
use tracing::{error, info};

/// Our root GraphQL query type.
struct QueryRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

/// Our root GraphQL subscription type.
struct SubscriptionRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

/// Our root GraphQL mutation type.
struct MutationRoot<P, S>(Arc<Mutex<ChainClient<P, S>>>);

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
        Ok(self.0.lock().await.subscribe_all(chain_ids).await?)
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
        let mut client = self.0.lock().await;
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
        owner: Owner,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    ) -> Result<Certificate, Error> {
        let operation = SystemOperation::OpenChain {
            id,
            owner,
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
    async fn change_owner(&self, new_owner: Owner) -> Result<Certificate, Error> {
        let operation = SystemOperation::ChangeOwner { new_owner };
        self.execute_system_operation(operation).await
    }

    /// Changes the authentication key of the chain.
    async fn change_multiple_owners(&self, new_owners: Vec<Owner>) -> Result<Certificate, Error> {
        let operation = SystemOperation::ChangeMultipleOwners { new_owners };
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
            MutationRoot(client.clone()),
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

#[cfg(test)]
pub mod tests {
    use std::{
        env,
        path::PathBuf,
        process::Stdio,
        time::Duration,
    };
    use tempfile::{tempdir, TempDir};
    use tokio::process::{Child, Command};

    fn cargo_run(tmp_dir: &TempDir) -> Command {
        let mut command = Command::new("cargo");
        command
            .current_dir(tmp_dir.path().canonicalize().unwrap())
            .kill_on_drop(true)
            .arg("run")
            .arg("--manifest-path")
            .arg(env::current_dir().unwrap().join("Cargo.toml"));
        command
    }

    async fn generate_server_config(tmp_dir: &TempDir) {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("server")
            .arg("generate")
            .arg("--validators")
            .arg("server_1.json:grpc:127.0.0.1:9100:grpc:127.0.0.1:10100:127.0.0.1:9101:127.0.0.1:9102:127.0.0.1:9103:127.0.0.1:9104")
            .arg("server_2.json:grpc:127.0.0.1:9200:grpc:127.0.0.1:10200:127.0.0.1:9201:127.0.0.1:9202:127.0.0.1:9203:127.0.0.1:9204")
            .arg("server_3.json:grpc:127.0.0.1:9300:grpc:127.0.0.1:10300:127.0.0.1:9301:127.0.0.1:9302:127.0.0.1:9303:127.0.0.1:9304")
            .arg("server_4.json:grpc:127.0.0.1:9400:grpc:127.0.0.1:10400:127.0.0.1:9401:127.0.0.1:9402:127.0.0.1:9403:127.0.0.1:9404")
            .arg("--committee")
            .arg("committee.json")
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn generate_client_config(tmp_dir: &TempDir) {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("client")
            .arg("--")
            .arg("--wallet")
            .arg("wallet.json")
            .arg("--genesis")
            .arg("genesis.json")
            .arg("create_genesis_config")
            .arg("10")
            .arg("--initial-funding")
            .arg("10")
            .arg("--committee")
            .arg("committee.json")
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    fn run_proxy(i: usize, tmp_dir: &TempDir) -> Child {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("proxy")
            .arg("--")
            .arg(format!("server_{}.json", i))
            .spawn()
            .unwrap()
    }

    fn run_server(i: usize, j: usize, tmp_dir: &TempDir) -> Child {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("server")
            .arg("run")
            .arg("--storage")
            .arg(format!("rocksdb:server_{}_{}.db", i, j))
            .arg("--server")
            .arg(format!("server_{}.json", i))
            .arg("--shard")
            .arg(j.to_string())
            .arg("--genesis")
            .arg("genesis.json")
            .spawn()
            .unwrap()
    }

    fn run_local_net(tmp_dir: &TempDir) -> Vec<Child> {
        let mut processes = vec![];
        for i in 1..5 {
            let process = run_proxy(i, tmp_dir);
            processes.push(process);
            for j in 0..4 {
                let process = run_server(i, j, tmp_dir);
                processes.push(process);
            }
        }
        processes
    }

    async fn build_application(tmp_dir: &TempDir) -> (PathBuf, PathBuf) {
        let examples_dir = env::current_dir().unwrap().join("../linera-examples/");
        Command::new("cargo")
            .current_dir(tmp_dir.path().canonicalize().unwrap())
            .arg("build")
            .arg("--release")
            .arg("--target")
            .arg("wasm32-unknown-unknown")
            .arg("--manifest-path")
            .arg(examples_dir.join("counter-graphql/Cargo.toml"))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();

        let contract = examples_dir
            .join("target/wasm32-unknown-unknown/release/counter_graphql_contract.wasm");
        let service = examples_dir
            .join("target/wasm32-unknown-unknown/release/counter_graphql_service.wasm");

        (contract, service)
    }

    async fn publish_application(
        contract: PathBuf,
        service: PathBuf,
        tmp_dir: &TempDir,
        counter_value: usize,
    ) {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("client")
            .arg("--")
            .arg("--storage")
            .arg("rocksdb:client.db")
            .arg("--wallet")
            .arg("wallet.json")
            .arg("--genesis")
            .arg("genesis.json")
            .arg("--max-pending-messages")
            .arg("10000")
            .arg("publish")
            .arg(contract)
            .arg(service)
            .arg(counter_value.to_string())
            .spawn()
            .unwrap()
            .wait()
            .await
            .unwrap();
    }

    async fn run_node_service(tmp_dir: &TempDir) -> Child {
        let mut cargo_run = cargo_run(tmp_dir);
        cargo_run
            .arg("--bin")
            .arg("client")
            .arg("--")
            .arg("--storage")
            .arg("rocksdb:client.db")
            .arg("--wallet")
            .arg("wallet.json")
            .arg("--genesis")
            .arg("genesis.json")
            .arg("--max-pending-messages")
            .arg("10000")
            .arg("service")
            .spawn()
            .unwrap()
    }

    #[tokio::test]
    async fn end_to_end() {
        let tmp_dir = tempdir().unwrap();
        let counter_value = 35;
        generate_server_config(&tmp_dir).await;
        generate_client_config(&tmp_dir).await;
        let _local_net = run_local_net(&tmp_dir);
        let (contract, service) = build_application(&tmp_dir).await;
        // wait for net to start
        tokio::time::sleep(Duration::from_millis(10_000)).await;
        publish_application(contract, service, &tmp_dir, counter_value).await;
        let _node_service = run_node_service(&tmp_dir).await;

        tokio::time::sleep(Duration::from_millis(60_000)).await;
    }
}
