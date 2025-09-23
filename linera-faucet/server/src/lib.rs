// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! The server component of the Linera faucet.

mod database;

use std::{collections::VecDeque, future::IntoFuture, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context as _;
use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    bcs,
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Amount, ApplicationPermissions, ChainDescription, Timestamp},
    identifiers::{AccountOwner, BlobId, BlobType, ChainId},
    ownership::ChainOwnership,
};
use linera_chain::{ChainError, ChainExecutionContext};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    config::GenesisConfig,
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    data_types::ClientOutcome,
    worker::WorkerError,
    LocalNodeError,
};
use linera_execution::{
    system::{OpenChainConfig, SystemOperation},
    Committee, ExecutionError, Operation,
};
#[cfg(feature = "metrics")]
use linera_metrics::monitoring_server;
use linera_storage::{Clock as _, Storage};
use serde::Deserialize;
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::database::FaucetDatabase;

/// Returns an HTML response constructing the GraphiQL web page for the given URI.
pub(crate) async fn graphiql(uri: axum::http::Uri) -> impl axum::response::IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(uri.path())
            .subscription_endpoint("/ws")
            .finish(),
    )
}

#[cfg(test)]
mod tests;

/// The root GraphQL query type.
pub struct QueryRoot<C: ClientContext> {
    client: ChainClient<C::Environment>,
    genesis_config: Arc<GenesisConfig>,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<S> {
    faucet_storage: Arc<FaucetDatabase>,
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
    request_notifier: Arc<Notify>,
    storage: S,
}

/// The result of a successful `claim` mutation.
#[derive(SimpleObject)]
pub struct ClaimOutcome {
    /// The ID of the new chain.
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}

#[derive(Debug, Deserialize, SimpleObject)]
pub struct Validator {
    pub public_key: ValidatorPublicKey,
    pub network_address: String,
}

/// A pending chain creation request.
#[derive(Debug)]
struct PendingRequest {
    owner: AccountOwner,
    responder: oneshot::Sender<Result<ChainDescription, Error>>,
}

/// Configuration for the batch processor.
struct BatchProcessorConfig {
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
    max_batch_size: usize,
}

/// Batching coordinator for processing chain creation requests.
struct BatchProcessor<C: ClientContext> {
    config: BatchProcessorConfig,
    context: Arc<Mutex<C>>,
    client: ChainClient<C::Environment>,
    faucet_storage: Arc<FaucetDatabase>,
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
    request_notifier: Arc<Notify>,
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> QueryRoot<C>
where
    C: ClientContext + 'static,
{
    /// Returns the version information on this faucet service.
    async fn version(&self) -> linera_version::VersionInfo {
        linera_version::VersionInfo::default()
    }

    /// Returns the genesis config.
    async fn genesis_config(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(&*self.genesis_config)?)
    }

    /// Returns the current committee's validators.
    async fn current_validators(&self) -> Result<Vec<Validator>, Error> {
        let committee = self.client.local_committee().await?;
        Ok(committee
            .validators()
            .iter()
            .map(|(public_key, validator)| Validator {
                public_key: *public_key,
                network_address: validator.network_address.clone(),
            })
            .collect())
    }

    /// Returns the current committee, including weights and resource policy.
    async fn current_committee(&self) -> Result<Committee, Error> {
        Ok(self.client.local_committee().await?)
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<S> MutationRoot<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        self.do_claim(owner).await
    }
}

impl<S> MutationRoot<S>
where
    S: Storage + Send + Sync + 'static,
{
    async fn do_claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        // Check if this owner already has a chain.
        if let Some(existing_chain_id) = self
            .faucet_storage
            .get_chain_id(&owner)
            .await
            .map_err(|e| Error::new(e.to_string()))?
        {
            // Retrieve the chain description from local storage
            return get_chain_description_from_storage(&self.storage, existing_chain_id).await;
        }

        // Create a oneshot channel to receive the result.
        let (tx, rx) = oneshot::channel();

        // Add request to the queue.
        {
            let mut requests = self.pending_requests.lock().await;
            requests.push_back(PendingRequest {
                owner,
                responder: tx,
            });
        }

        // Notify the batch processor that there's a new request.
        self.request_notifier.notify_one();

        // Wait for the result
        rx.await
            .map_err(|_| Error::new("Request processing was cancelled"))?
    }
}
/// Multiplies a `u128` with a `u64` and returns the result as a 192-bit number.
fn multiply(a: u128, b: u64) -> [u64; 3] {
    let lower = u128::from(u64::MAX);
    let b = u128::from(b);
    let mut a1 = (a >> 64) * b;
    let a0 = (a & lower) * b;
    a1 += a0 >> 64;
    [(a1 >> 64) as u64, (a1 & lower) as u64, (a0 & lower) as u64]
}

/// Retrieves a chain description from storage by reading the blob directly.
///
/// This function handles errors appropriately and returns a GraphQL-compatible result.
async fn get_chain_description_from_storage<S>(
    storage: &S,
    chain_id: ChainId,
) -> Result<ChainDescription, Error>
where
    S: Storage,
{
    // Create blob ID from chain ID - the chain ID is the hash of the chain description blob
    let blob_id = BlobId::new(chain_id.0, BlobType::ChainDescription);

    // Read the blob directly from storage
    let blob = storage
        .read_blob(blob_id)
        .await
        .map_err(|e| {
            tracing::error!(
                "Failed to read chain description blob for {}: {}",
                chain_id,
                e
            );
            Error::new(format!(
                "Storage error while reading chain description: {e}"
            ))
        })?
        .ok_or_else(|| {
            tracing::error!("Chain description blob not found for chain {}", chain_id);
            Error::new(format!(
                "Chain description not found for chain {}",
                chain_id
            ))
        })?;

    // Deserialize the chain description from the blob bytes
    let description = bcs::from_bytes::<ChainDescription>(blob.bytes()).map_err(|e| {
        tracing::error!(
            "Failed to deserialize chain description for {}: {}",
            chain_id,
            e
        );
        Error::new(format!(
            "Invalid chain description data for chain {}",
            chain_id
        ))
    })?;

    Ok(description)
}

impl<C> BatchProcessor<C>
where
    C: ClientContext + 'static,
{
    /// Creates a new batch processor.
    fn new(
        config: BatchProcessorConfig,
        context: Arc<Mutex<C>>,
        client: ChainClient<C::Environment>,
        faucet_storage: Arc<FaucetDatabase>,
        pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
        request_notifier: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            context,
            client,
            faucet_storage,
            pending_requests,
            request_notifier,
        }
    }

    /// Runs the batch processor loop.
    async fn run(&mut self, cancellation_token: CancellationToken) {
        loop {
            tokio::select! {
                _ = self.request_notifier.notified() => {
                    if let Err(e) = self.process_batch().await {
                        tracing::error!("Batch processing error: {}", e);
                    }
                }
                _ = cancellation_token.cancelled() => {
                    // Process any remaining requests before shutting down
                    if let Err(e) = self.process_batch().await {
                        tracing::error!("Final batch processing error: {}", e);
                    }
                    break;
                }
            }
        }
    }

    /// Processes batches until there are no more pending requests in the queue.
    async fn process_batch(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let max_batch_size = self.config.max_batch_size;
            let mut batch_requests = Vec::new();

            // Collect requests from the queue.
            {
                let mut requests = self.pending_requests.lock().await;
                while batch_requests.len() < max_batch_size && !requests.is_empty() {
                    if let Some(request) = requests.pop_front() {
                        batch_requests.push(request);
                    }
                }
            }

            if batch_requests.is_empty() {
                return Ok(());
            }

            tracing::info!("Processing batch of {} requests", batch_requests.len());

            if let Err(err) = self.execute_batch(batch_requests).await {
                tracing::error!("Failed to execute batch: {}", err);
                return Err(err);
            }
        }
    }

    /// Executes a batch of chain creation requests.
    async fn execute_batch(
        &mut self,
        batch_requests: Vec<PendingRequest>,
    ) -> Result<(), anyhow::Error> {
        // Pre-validate: check existing chains.
        let mut valid_requests = Vec::new();

        for request in batch_requests {
            // Check if this owner already has a chain. Otherwise send response immediately.
            let response = match self.faucet_storage.get_chain_id(&request.owner).await {
                Ok(None) => {
                    valid_requests.push(request);
                    continue;
                }
                Ok(Some(existing_chain_id)) => {
                    // Retrieve the chain description from local storage.
                    get_chain_description_from_storage(
                        self.client.storage_client(),
                        existing_chain_id,
                    )
                    .await
                }
                Err(err) => {
                    tracing::error!("Database error: {err}");
                    Err(Error::new(err.to_string()))
                }
            };
            if let Err(response) = request.responder.send(response) {
                tracing::error!(
                    "Receiver dropped while sending response {response:?} for request from {}",
                    request.owner
                );
            }
        }

        if valid_requests.is_empty() {
            return Ok(());
        }

        // Rate limiting check for the batch.
        if self.config.start_timestamp < self.config.end_timestamp {
            let local_time = self.client.storage_client().clock().current_time();
            if local_time < self.config.end_timestamp {
                let full_duration = self
                    .config
                    .end_timestamp
                    .delta_since(self.config.start_timestamp)
                    .as_micros();
                let remaining_duration = self
                    .config
                    .end_timestamp
                    .delta_since(local_time)
                    .as_micros();
                let balance = self.client.local_balance().await?;

                let total_amount = self
                    .config
                    .amount
                    .saturating_mul(valid_requests.len() as u128);
                let Ok(remaining_balance) = balance.try_sub(total_amount) else {
                    // Not enough balance - reject all requests
                    for request in valid_requests {
                        let _ = request
                            .responder
                            .send(Err(Error::new("The faucet is empty.")));
                    }
                    return Ok(());
                };

                if multiply(u128::from(self.config.start_balance), remaining_duration)
                    > multiply(u128::from(remaining_balance), full_duration)
                {
                    // Rate limit exceeded - reject all requests
                    for request in valid_requests {
                        let _ = request.responder.send(Err(Error::new(
                            "Not enough unlocked balance; try again later.",
                        )));
                    }
                    return Ok(());
                }
            }
        }

        // Create OpenChain operations for all valid requests
        let mut operations = Vec::new();
        for request in &valid_requests {
            let config = OpenChainConfig {
                ownership: ChainOwnership::single(request.owner),
                balance: self.config.amount,
                application_permissions: ApplicationPermissions::default(),
            };
            operations.push(Operation::system(SystemOperation::OpenChain(config)));
        }

        // Execute all operations in a single block
        let result = self.client.execute_operations(operations, vec![]).await;
        self.context
            .lock()
            .await
            .update_wallet(&self.client)
            .await?;

        let certificate = match result {
            Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(chain_err),
            ))) => {
                match *chain_err {
                    ChainError::ExecutionError(exec_err, ChainExecutionContext::Operation(i))
                        if i > 0
                            && matches!(
                                *exec_err,
                                ExecutionError::BlockTooLarge
                                    | ExecutionError::FeesExceedFunding { .. }
                                    | ExecutionError::InsufficientBalance { .. }
                                    | ExecutionError::MaximumFuelExceeded(_)
                            ) =>
                    {
                        tracing::error!(%exec_err, "Execution of operation {i} failed; reducing batch size");
                        self.config.max_batch_size = i as usize;
                        // Put the valid requests back into the queue.
                        let mut pending_requests = self.pending_requests.lock().await;
                        for request in valid_requests.into_iter().rev() {
                            pending_requests.push_front(request);
                        }
                        return Ok(()); // Don't return an error, so we retry.
                    }
                    chain_err => {
                        return Err(
                            ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                                WorkerError::ChainError(chain_err.into()),
                            ))
                            .into(),
                        )
                    }
                }
            }
            Err(err) => return Err(err.into()),
            Ok(ClientOutcome::Committed(certificate)) => certificate,
            Ok(ClientOutcome::WaitForTimeout(timeout)) => {
                let error_msg = format!(
                    "This faucet is using a multi-owner chain and is not the leader right now. \
                    Try again at {}",
                    timeout.timestamp,
                );
                for request in valid_requests {
                    let _ = request.responder.send(Err(Error::new(error_msg.clone())));
                }
                return Ok(());
            }
        };

        // Parse chain descriptions from the block's blobs
        let blobs = certificate.block().body.blobs.iter().flatten();
        let chain_descriptions = blobs
            .map(|blob| bcs::from_bytes::<ChainDescription>(blob.bytes()))
            .collect::<Result<Vec<ChainDescription>, _>>()?;

        if chain_descriptions.len() != valid_requests.len() {
            let error_msg = format!(
                "Mismatch between operations ({}) and results ({})",
                valid_requests.len(),
                chain_descriptions.len()
            );
            for request in valid_requests {
                let _ = request.responder.send(Err(Error::new(error_msg.clone())));
            }
            anyhow::bail!(error_msg);
        }

        // Store results.
        let chains_to_store = valid_requests
            .iter()
            .zip(&chain_descriptions)
            .map(|(request, description)| (request.owner, description.id()))
            .collect();

        if let Err(e) = self
            .faucet_storage
            .store_chains_batch(chains_to_store)
            .await
        {
            let error_msg = format!("Failed to save chains to database: {}", e);
            for request in valid_requests {
                let _ = request.responder.send(Err(Error::new(error_msg.clone())));
            }
            anyhow::bail!(error_msg);
        }

        // Respond to requests.
        for (request, description) in valid_requests.into_iter().zip(chain_descriptions) {
            let _ = request.responder.send(Ok(description));
        }

        Ok(())
    }
}

/// A GraphQL interface to request a new chain with tokens.
pub struct FaucetService<C>
where
    C: ClientContext,
{
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    client: ChainClient<C::Environment>,
    genesis_config: Arc<GenesisConfig>,
    config: ChainListenerConfig,
    storage: <C::Environment as linera_core::Environment>::Storage,
    port: u16,
    #[cfg(feature = "metrics")]
    metrics_port: u16,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
    faucet_storage: Arc<FaucetDatabase>,
    storage_path: PathBuf,
    /// Batching components
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
    request_notifier: Arc<Notify>,
    max_batch_size: usize,
}

impl<C> Clone for FaucetService<C>
where
    C: ClientContext + 'static,
{
    fn clone(&self) -> Self {
        Self {
            chain_id: self.chain_id,
            context: Arc::clone(&self.context),
            client: self.client.clone(),
            genesis_config: Arc::clone(&self.genesis_config),
            config: self.config.clone(),
            storage: self.storage.clone(),
            port: self.port,
            #[cfg(feature = "metrics")]
            metrics_port: self.metrics_port,
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
            faucet_storage: Arc::clone(&self.faucet_storage),
            storage_path: self.storage_path.clone(),
            pending_requests: Arc::clone(&self.pending_requests),
            request_notifier: Arc::clone(&self.request_notifier),
            max_batch_size: self.max_batch_size,
        }
    }
}

pub struct FaucetConfig {
    pub port: u16,
    #[cfg(feature = "metrics")]
    pub metrics_port: u16,
    pub chain_id: ChainId,
    pub amount: Amount,
    pub end_timestamp: Timestamp,
    pub genesis_config: Arc<GenesisConfig>,
    pub chain_listener_config: ChainListenerConfig,
    pub storage_path: PathBuf,
    pub max_batch_size: usize,
}

impl<C> FaucetService<C>
where
    C: ClientContext + 'static,
{
    /// Creates a new instance of the faucet service.
    pub async fn new(
        config: FaucetConfig,
        context: C,
        storage: <C::Environment as linera_core::Environment>::Storage,
    ) -> anyhow::Result<Self> {
        let client = context.make_chain_client(config.chain_id);
        let context = Arc::new(Mutex::new(context));
        let start_timestamp = client.storage_client().clock().current_time();
        client.process_inbox().await?;
        let start_balance = client.local_balance().await?;

        // Use provided storage path
        let storage_path = config.storage_path.clone();

        // Initialize database.
        let faucet_storage = FaucetDatabase::new(&storage_path)
            .await
            .context("Failed to initialize faucet database")?;

        // Synchronize database with blockchain history.
        if let Err(e) = faucet_storage.sync_with_blockchain(&client).await {
            tracing::warn!("Failed to synchronize database with blockchain: {}", e);
        }

        let faucet_storage = Arc::new(faucet_storage);

        // Initialize batching components
        let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
        let request_notifier = Arc::new(Notify::new());

        Ok(Self {
            chain_id: config.chain_id,
            context,
            client,
            genesis_config: config.genesis_config,
            config: config.chain_listener_config,
            storage,
            port: config.port,
            #[cfg(feature = "metrics")]
            metrics_port: config.metrics_port,
            amount: config.amount,
            end_timestamp: config.end_timestamp,
            start_timestamp,
            start_balance,
            faucet_storage,
            storage_path,
            pending_requests,
            request_notifier,
            max_batch_size: config.max_batch_size,
        })
    }

    fn schema(
        &self,
    ) -> Schema<
        QueryRoot<C>,
        MutationRoot<<C::Environment as linera_core::Environment>::Storage>,
        EmptySubscription,
    > {
        let mutation_root = MutationRoot {
            faucet_storage: Arc::clone(&self.faucet_storage),
            pending_requests: Arc::clone(&self.pending_requests),
            request_notifier: Arc::clone(&self.request_notifier),
            storage: self.storage.clone(),
        };
        let query_root = QueryRoot {
            genesis_config: Arc::clone(&self.genesis_config),
            client: self.client.clone(),
        };
        Schema::build(query_root, mutation_root, EmptySubscription).finish()
    }

    #[cfg(feature = "metrics")]
    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.metrics_port))
    }

    /// Runs the faucet.
    #[tracing::instrument(name = "FaucetService::run", skip_all, fields(port = self.port, chain_id = ?self.chain_id))]
    pub async fn run(self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let port = self.port;
        let index_handler = axum::routing::get(graphiql).post(Self::index_handler);

        #[cfg(feature = "metrics")]
        monitoring_server::start_metrics(self.metrics_address(), cancellation_token.clone());

        let app = Router::new()
            .route("/", index_handler)
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        // Start the batch processor
        let batch_processor_config = BatchProcessorConfig {
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
            max_batch_size: self.max_batch_size,
        };
        let mut batch_processor = BatchProcessor::new(
            batch_processor_config,
            Arc::clone(&self.context),
            self.client.clone(),
            Arc::clone(&self.faucet_storage),
            Arc::clone(&self.pending_requests),
            Arc::clone(&self.request_notifier),
        );

        let chain_listener = ChainListener::new(
            self.config,
            self.context,
            self.storage,
            cancellation_token.clone(),
        )
        .run()
        .await?;
        let batch_processor_task = batch_processor.run(cancellation_token.clone());
        let tcp_listener =
            tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
        let server = axum::serve(tcp_listener, app)
            .with_graceful_shutdown(cancellation_token.cancelled_owned())
            .into_future();
        futures::select! {
            result = Box::pin(chain_listener).fuse() => result?,
            _ = Box::pin(batch_processor_task).fuse() => {},
            result = Box::pin(server).fuse() => result?,
        };

        Ok(())
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        let schema = service.0.schema();
        schema.execute(request.into_inner()).await.into()
    }
}
