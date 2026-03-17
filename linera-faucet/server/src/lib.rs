// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! The server component of the Linera faucet.

mod database;

use std::{
    collections::VecDeque, future::IntoFuture, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Instant,
};

use anyhow::Context as _;
use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::{lock::Mutex, FutureExt as _};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    bcs,
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Amount, ApplicationPermissions, ChainDescription, Epoch, TimeDelta, Timestamp},
    identifiers::{Account, AccountOwner, BlobId, BlobType, ChainId},
    ownership::ChainOwnership,
};
use linera_chain::{types::ConfirmedBlockCertificate, ChainError, ChainExecutionContext};
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

// Prometheus metrics for the faucet
#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, register_histogram_vec, register_int_counter_vec,
        register_int_gauge_vec,
    };
    use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};

    pub static CLAIM_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "faucet_claim_requests_total",
            "Total number of claim requests by result",
            &["result"],
        )
    });

    pub static CLAIM_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_claim_latency_ms",
            "End-to-end latency of claim requests in milliseconds",
            &["result"],
            exponential_bucket_interval(10.0, 40_000.0),
        )
    });

    pub static CHAINS_CREATED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "faucet_chains_created_total",
            "Total number of chains created by the faucet",
            &[],
        )
    });

    pub static BATCH_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_batch_size",
            "Number of chain creation requests per batch",
            &[],
            Some(vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]),
        )
    });

    pub static BATCH_PROCESSING_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_batch_processing_latency_ms",
            "Time to process a batch of chain creation requests in milliseconds",
            &["result"],
            exponential_bucket_interval(10.0, 40_000.0),
        )
    });

    pub static QUEUE_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_queue_size",
            "Number of pending claim requests in the queue",
            &[],
            Some(vec![
                0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
            ]),
        )
    });

    pub static QUEUE_WAIT_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_queue_wait_time_ms",
            "Time a request spends in the queue before processing in milliseconds",
            &[],
            exponential_bucket_interval(10.0, 40_000.0),
        )
    });

    pub static FAUCET_BALANCE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec(
            "faucet_balance_amount",
            "Current balance of the faucet chain",
            &[],
        )
    });

    pub static RATE_LIMIT_REJECTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "faucet_rate_limit_rejections_total",
            "Number of requests rejected due to rate limiting",
            &[],
        )
    });

    pub static INSUFFICIENT_BALANCE_REJECTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "faucet_insufficient_balance_rejections_total",
            "Number of requests rejected due to insufficient faucet balance",
            &[],
        )
    });

    pub static DATABASE_OPERATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_database_operation_latency_ms",
            "Database operation latency in milliseconds",
            &["operation"],
            exponential_bucket_interval(0.5, 2000.0),
        )
    });

    pub static RETRYABLE_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "faucet_retryable_errors_total",
            "Number of chain execution retryable errors by type",
            &["error_type"],
        )
    });

    pub static STORE_CHAIN_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "faucet_store_chain_latency_ms",
            "Latency of storing chain information in the database in milliseconds",
            &[],
            exponential_bucket_interval(10.0, 2000.0),
        )
    });
}

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
    faucet_storage: Arc<FaucetDatabase>,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<S> {
    faucet_storage: Arc<FaucetDatabase>,
    pending_requests: Arc<Mutex<VecDeque<PendingRequest>>>,
    request_notifier: Arc<Notify>,
    storage: S,
    /// Amount for initial claims (chain creation).
    initial_claim_amount: Amount,
    /// Amount for daily claims (token transfer).
    daily_claim_amount: Amount,
}

/// The result of a successful `claim` or `dailyClaim` mutation.
#[derive(Clone, Debug, SimpleObject)]
pub struct ClaimOutcome {
    /// The ID of the chain.
    pub chain_id: ChainId,
    /// The hash of the certificate containing the operation.
    pub certificate_hash: CryptoHash,
    /// The amount of tokens transferred.
    pub amount: Amount,
}

/// Information about a previous claim.
#[derive(Clone, Debug, SimpleObject)]
pub struct LastClaim {
    /// The chain ID that was created.
    pub chain_id: ChainId,
    /// The timestamp when the chain was created.
    pub timestamp: Timestamp,
}

#[derive(Debug, Deserialize, SimpleObject)]
pub struct Validator {
    pub public_key: ValidatorPublicKey,
    pub network_address: String,
}

/// The response from a pending request.
#[derive(Debug)]
enum PendingResponse {
    /// Initial claim: returns the full chain description.
    Initial(Result<Box<ChainDescription>, Error>),
    /// Daily claim: returns the claim outcome.
    Daily(Result<ClaimOutcome, Error>),
}

/// A pending chain creation or token transfer request.
///
/// If `target_chain_id` is `None`, this is an initial claim (creates a new chain).
/// If `target_chain_id` is `Some`, this is a daily claim (transfers tokens).
#[derive(Debug)]
struct PendingRequest {
    owner: AccountOwner,
    /// For daily claims, the existing chain to transfer tokens to.
    target_chain_id: Option<ChainId>,
    /// The amount of tokens to send.
    amount: Amount,
    /// For daily claims, the period number to store.
    daily_period: u64,
    responder: oneshot::Sender<PendingResponse>,
    #[cfg(with_metrics)]
    queued_at: std::time::Instant,
}

impl PendingRequest {
    fn is_daily(&self) -> bool {
        self.target_chain_id.is_some()
    }

    fn send_err(self, err: Error) {
        let response = if self.is_daily() {
            PendingResponse::Daily(Err(err))
        } else {
            PendingResponse::Initial(Err(err))
        };
        if self.responder.send(response).is_err() {
            tracing::warn!("Receiver dropped while sending error to {}.", self.owner);
        }
    }
}

/// Configuration for the batch processor.
#[derive(Clone)]
struct BatchProcessorConfig {
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

    /// Returns the current epoch of the faucet's chain.
    async fn current_epoch(&self) -> Result<Epoch, Error> {
        let info = self.client.chain_info().await?;
        Ok(info.epoch)
    }

    /// Find the existing chain with the given authentication key, if any.
    async fn chain_id(&self, owner: AccountOwner) -> Result<ChainId, Error> {
        // Check if this owner already has a chain.
        #[cfg(with_metrics)]
        let histogram = metrics::DATABASE_OPERATION_LATENCY.with_label_values(&["get_chain_id"]);
        #[cfg(with_metrics)]
        let _latency = histogram.measure_latency();

        let chain_id = self.faucet_storage.get_chain_id(&owner).await?;

        chain_id.ok_or(Error::new("This user has no chain yet"))
    }

    /// Returns the last claim for the given owner, if any.
    async fn last_claim(&self, owner: AccountOwner) -> Result<Option<LastClaim>, Error> {
        let claim_record = self.faucet_storage.last_claim(&owner).await?;

        Ok(claim_record.map(|r| LastClaim {
            chain_id: r.chain_id,
            timestamp: r.timestamp,
        }))
    }

    /// Returns the earliest time at which the owner can make a daily claim.
    /// If the returned timestamp is in the past (or now), the user can claim immediately.
    /// Returns `None` if the user has not yet completed the initial claim.
    async fn next_daily_claim(&self, owner: AccountOwner) -> Result<Option<Timestamp>, Error> {
        let initial_claim = match self.faucet_storage.last_claim(&owner).await? {
            Some(record) => record,
            None => return Ok(None),
        };

        let initial_micros = initial_claim.timestamp.micros();

        // The initial claim counts as "a claim in period 0".
        let last_period = self
            .faucet_storage
            .last_daily_claim_period(&owner)
            .await?
            .unwrap_or(0);

        // Next available at start of the next period.
        Ok(Some(Timestamp::from(
            initial_micros + (last_period + 1) * DAILY_PERIOD_MICROS,
        )))
    }
}

/// Duration of one daily claim period in microseconds (24 hours).
const DAILY_PERIOD_MICROS: u64 = TimeDelta::from_secs(24 * 60 * 60).as_micros();

/// Computes the current daily claim period from timestamps.
fn current_daily_period(initial_claim_micros: u64, now_micros: u64) -> u64 {
    now_micros.saturating_sub(initial_claim_micros) / DAILY_PERIOD_MICROS
}

/// Executes a future and records its latency in [`metrics::CLAIM_LATENCY`], labeled by outcome.
async fn record_claim_latency<T>(
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    #[cfg(with_metrics)]
    let start_time = std::time::Instant::now();

    let result = future.await;

    #[cfg(with_metrics)]
    {
        let label = if result.is_ok() { "success" } else { "error" };
        metrics::CLAIM_LATENCY
            .with_label_values(&[label])
            .observe(start_time.elapsed().as_secs_f64() * 1000.0);
    }

    result
}

#[async_graphql::Object(cache_control(no_cache))]
impl<S> MutationRoot<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        record_claim_latency(self.do_claim(owner)).await
    }

    /// Transfers a daily amount of tokens to the user's existing chain.
    /// The user must have already claimed a chain. Each user can claim once per 24-hour
    /// period, measured from their initial claim time.
    async fn daily_claim(&self, owner: AccountOwner) -> Result<ClaimOutcome, Error> {
        record_claim_latency(self.do_daily_claim(owner)).await
    }
}

impl<S> MutationRoot<S>
where
    S: Storage + Send + Sync + 'static,
{
    async fn do_claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        // Check if this owner already has a chain.
        #[cfg(with_metrics)]
        let histogram = metrics::DATABASE_OPERATION_LATENCY.with_label_values(&["get_chain_id"]);

        tracing::debug!(account_owner=?owner, "claim request received");
        #[cfg(with_metrics)]
        let _latency = histogram.measure_latency();

        let existing_chain_id = self.faucet_storage.get_chain_id(&owner).await?;

        if let Some(existing_chain_id) = existing_chain_id {
            #[cfg(with_metrics)]
            metrics::CLAIM_REQUESTS_TOTAL
                .with_label_values(&["duplicate"])
                .inc();

            let stored_chain =
                get_chain_description_from_storage(&self.storage, existing_chain_id).await;
            tracing::debug!(account_owner=?owner, "claim request processed; returning pre-existing chain");
            return stored_chain;
        }

        // Create a oneshot channel to receive the result.
        let (tx, rx) = oneshot::channel();

        // Add request to the queue.
        {
            let mut requests = self.pending_requests.lock().await;
            requests.push_back(PendingRequest {
                owner,
                target_chain_id: None,
                amount: self.initial_claim_amount,
                daily_period: 0,
                responder: tx,
                #[cfg(with_metrics)]
                queued_at: std::time::Instant::now(),
            });

            #[cfg(with_metrics)]
            metrics::QUEUE_SIZE
                .with_label_values(&[])
                .observe(requests.len() as f64);
        }

        // Notify the batch processor that there's a new request.
        self.request_notifier.notify_one();

        // Wait for the result
        let response = rx
            .await
            .map_err(|_| Error::new("Request processing was cancelled"))?;

        #[cfg(with_metrics)]
        {
            let label = match &response {
                PendingResponse::Initial(Ok(_)) => "success",
                _ => "error",
            };
            metrics::CLAIM_REQUESTS_TOTAL
                .with_label_values(&[label])
                .inc();
        }

        tracing::debug!(account_owner=?owner, "claim request processed; new chain created");
        match response {
            PendingResponse::Initial(result) => result.map(|b| *b),
            PendingResponse::Daily(_) => Err(Error::new("Unexpected response type")),
        }
    }

    async fn do_daily_claim(&self, owner: AccountOwner) -> Result<ClaimOutcome, Error> {
        if self.daily_claim_amount == Amount::ZERO {
            return Err(Error::new("Daily claims are not enabled on this faucet"));
        }

        // The user must have done the initial claim first.
        let initial_claim = self
            .faucet_storage
            .last_claim(&owner)
            .await?
            .ok_or_else(|| Error::new("You must claim a chain before making daily claims"))?;

        let now = self.storage.clock().current_time();
        let period = current_daily_period(initial_claim.timestamp.micros(), now.micros());
        let last_period = self
            .faucet_storage
            .last_daily_claim_period(&owner)
            .await?
            .unwrap_or(0);

        if period <= last_period {
            return Err(Error::new(
                "You have already claimed tokens for this period",
            ));
        }

        self.enqueue_daily_request(
            owner,
            initial_claim.chain_id,
            self.daily_claim_amount,
            period,
        )
        .await
    }

    async fn enqueue_daily_request(
        &self,
        owner: AccountOwner,
        target_chain_id: ChainId,
        amount: Amount,
        daily_period: u64,
    ) -> Result<ClaimOutcome, Error> {
        // Create a oneshot channel to receive the result.
        let (tx, rx) = oneshot::channel();

        // Add request to the queue.
        {
            let mut requests = self.pending_requests.lock().await;
            requests.push_back(PendingRequest {
                owner,
                target_chain_id: Some(target_chain_id),
                amount,
                daily_period,
                responder: tx,
                #[cfg(with_metrics)]
                queued_at: std::time::Instant::now(),
            });

            #[cfg(with_metrics)]
            metrics::QUEUE_SIZE
                .with_label_values(&[])
                .observe(requests.len() as f64);
        }

        // Notify the batch processor that there's a new request.
        self.request_notifier.notify_one();

        // Wait for the result
        let response = rx
            .await
            .map_err(|_| Error::new("Request processing was cancelled"))?;

        #[cfg(with_metrics)]
        {
            let label = match &response {
                PendingResponse::Daily(Ok(_)) => "success",
                _ => "error",
            };
            metrics::CLAIM_REQUESTS_TOTAL
                .with_label_values(&[label])
                .inc();
        }

        match response {
            PendingResponse::Daily(result) => result,
            PendingResponse::Initial(_) => Err(Error::new("Unexpected response type")),
        }
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
            tracing::error!(?chain_id, ?e, "failed to read chain description blob");
            Error::new(format!(
                "Storage error while reading chain description: {e}"
            ))
        })?
        .ok_or_else(|| {
            tracing::error!(?chain_id, "chain description blob not found for chain");
            Error::new(format!(
                "Chain description not found for chain {}",
                chain_id
            ))
        })?;

    // Deserialize the chain description from the blob bytes
    let description = bcs::from_bytes::<ChainDescription>(blob.bytes()).map_err(|e| {
        tracing::error!(?e, ?chain_id, "failed to deserialize chain description",);
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
                        tracing::error!(?e, "batch processing error");
                    }
                }
                _ = cancellation_token.cancelled() => {
                    // Process any remaining requests before shutting down
                    if let Err(e) = self.process_batch().await {
                        tracing::error!(?e, "final batch processing error");
                    }
                    break;
                }
            }
        }
    }

    /// Processes batches until there are no more pending requests in the queue.
    async fn process_batch(&mut self) -> anyhow::Result<()> {
        loop {
            let batch_requests = self.get_request_batch().await;

            if batch_requests.is_empty() {
                return Ok(());
            }

            let batch_size = batch_requests.len();
            tracing::info!(?batch_size, "processing requests");

            #[cfg(with_metrics)]
            {
                metrics::BATCH_SIZE
                    .with_label_values(&[])
                    .observe(batch_size as f64);

                metrics::QUEUE_SIZE.with_label_values(&[]).observe(0.0);
            }

            #[cfg(with_metrics)]
            let batch_start_time = std::time::Instant::now();

            let batch_result = self.execute_batch(batch_requests).await;

            #[cfg(with_metrics)]
            {
                let elapsed_ms = batch_start_time.elapsed().as_secs_f64() * 1000.0;
                let label = if batch_result.is_ok() {
                    "success"
                } else {
                    "error"
                };
                metrics::BATCH_PROCESSING_LATENCY
                    .with_label_values(&[label])
                    .observe(elapsed_ms);
            }

            if let Err(err) = batch_result {
                tracing::error!(?err, "batch execution error");
                return Err(err);
            }
        }
    }

    // Collects requests from the queue; validates and filters them.
    async fn get_request_batch(&self) -> Vec<PendingRequest> {
        let mut batch_requests = Vec::new();
        let mut requests = self.pending_requests.lock().await;
        while batch_requests.len() < self.config.max_batch_size {
            let Some(request) = requests.pop_front() else {
                break;
            };

            match self.validate_request(&request).await {
                Ok(()) => {
                    batch_requests.push(request);
                }
                Err(err) => {
                    request.send_err(err);
                }
            }
        }
        batch_requests
    }

    /// Validates a pending request based on whether it's an initial or daily claim.
    async fn validate_request(&self, request: &PendingRequest) -> Result<(), Error> {
        if request.is_daily() {
            // Verify the initial claim still exists.
            let initial_claim = match self.faucet_storage.last_claim(&request.owner).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    return Err(Error::new(
                        "You must claim a chain before making daily claims",
                    ));
                }
                Err(err) => {
                    tracing::error!("Database error: {err}");
                    return Err(Error::new(err.to_string()));
                }
            };

            // Verify the daily claim period.
            let now = self.client.storage_client().clock().current_time();
            let period = current_daily_period(initial_claim.timestamp.micros(), now.micros());
            let last_period = self
                .faucet_storage
                .last_daily_claim_period(&request.owner)
                .await?
                .unwrap_or(0);

            if period <= last_period {
                return Err(Error::new(
                    "You have already claimed tokens for this period",
                ));
            }
        } else {
            match self.faucet_storage.get_chain_id(&request.owner).await {
                Ok(None) => {}
                Ok(Some(_)) => {
                    #[cfg(with_metrics)]
                    metrics::CLAIM_REQUESTS_TOTAL
                        .with_label_values(&["duplicate"])
                        .inc();
                    return Err(Error::new("This user already has a chain"));
                }
                Err(err) => {
                    tracing::error!("Database error: {err}");
                    return Err(Error::new(err.to_string()));
                }
            }
        }
        Ok(())
    }

    /// Checks if the given requests can currently be fulfilled, based on the balance
    /// and rate limiting settings. Returns an error if not.
    async fn check_rate_limiting(&self, requests: &[PendingRequest]) -> async_graphql::Result<()> {
        let end_timestamp = self.config.end_timestamp;
        let start_timestamp = self.config.start_timestamp;
        let local_time = self.client.storage_client().clock().current_time();
        let full_duration = end_timestamp.delta_since(start_timestamp).as_micros();
        let remaining_duration = end_timestamp.delta_since(local_time).as_micros();
        let balance = self.client.local_balance().await?;

        #[cfg(with_metrics)]
        metrics::FAUCET_BALANCE
            .with_label_values(&[])
            .set(u128::from(balance) as i64);

        let total_amount = requests
            .iter()
            .fold(Amount::ZERO, |acc, r| acc.saturating_add(r.amount));
        let Ok(remaining_balance) = balance.try_sub(total_amount) else {
            // Not enough balance - reject all requests
            #[cfg(with_metrics)]
            metrics::INSUFFICIENT_BALANCE_REJECTIONS
                .with_label_values(&[])
                .inc();
            return Err(Error::new("The faucet is empty."));
        };

        // Rate limit: Locked token balance decreases lineraly with time, i.e.:
        // remaining_balance / remaining_duration >= start_balance / full_duration
        if multiply(u128::from(self.config.start_balance), remaining_duration)
            > multiply(u128::from(remaining_balance), full_duration)
        {
            #[cfg(with_metrics)]
            metrics::RATE_LIMIT_REJECTIONS.with_label_values(&[]).inc();
            return Err(Error::new("Not enough unlocked balance; try again later."));
        }
        Ok(())
    }

    /// Sends an error response to all requestors.
    fn send_err(requests: Vec<PendingRequest>, err: impl Into<async_graphql::Error>) {
        let err = err.into();
        for request in requests {
            request.send_err(err.clone());
        }
    }

    /// Executes a batch of chain creation and/or token transfer requests.
    async fn execute_batch(&mut self, requests: Vec<PendingRequest>) -> anyhow::Result<()> {
        if let Err(err) = self.check_rate_limiting(&requests).await {
            tracing::debug!("Rejecting requests due to rate limiting: {err:?}");
            Self::send_err(requests, err);
            return Ok(());
        }

        // Build operations: OpenChain for initial claims, Transfer for daily claims.
        let mut operations = Vec::new();
        for request in &requests {
            let operation = if let Some(target_chain_id) = request.target_chain_id {
                Operation::system(SystemOperation::Transfer {
                    owner: AccountOwner::CHAIN,
                    recipient: Account {
                        chain_id: target_chain_id,
                        owner: request.owner,
                    },
                    amount: request.amount,
                })
            } else {
                let config = OpenChainConfig {
                    ownership: ChainOwnership::single(request.owner),
                    balance: request.amount,
                    application_permissions: ApplicationPermissions::default(),
                };
                Operation::system(SystemOperation::OpenChain(config))
            };
            operations.push(operation);
        }

        // Execute all operations in a single block
        let execute_ops_start = Instant::now();
        let result = self.client.execute_operations(operations, vec![]).await;
        tracing::debug!(
            execute_operations_ms = execute_ops_start.elapsed().as_millis(),
            "execute_operations completed"
        );
        let waiting_for_lock_start = Instant::now();
        self.context
            .lock()
            .await
            .update_wallet(&self.client)
            .await?;
        tracing::debug!(
            wait_time_ms = waiting_for_lock_start.elapsed().as_millis(),
            "wallet updated after executing operations"
        );
        let certificate = match result {
            Err(ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                WorkerError::ChainError(chain_err),
            ))) => {
                tracing::debug!(error=?chain_err, "local worker errored when executing operations");
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
                        tracing::error!(index=?i, %exec_err, "execution of operation failed; reducing batch size");

                        #[cfg(with_metrics)]
                        {
                            let error_type = match *exec_err {
                                ExecutionError::BlockTooLarge => "block_too_large",
                                ExecutionError::FeesExceedFunding { .. } => "fees_exceed_funding",
                                ExecutionError::InsufficientBalance { .. } => {
                                    "insufficient_balance"
                                }
                                ExecutionError::MaximumFuelExceeded(_) => "fuel_exceeded",
                                _ => "other",
                            };
                            metrics::RETRYABLE_ERRORS
                                .with_label_values(&[error_type])
                                .inc();
                        }

                        self.config.max_batch_size = i as usize;
                        // Put the valid requests back into the queue.
                        let mut pending_requests = self.pending_requests.lock().await;
                        for request in requests.into_iter().rev() {
                            pending_requests.push_front(request);
                        }
                        return Ok(()); // Don't return an error, so we retry.
                    }
                    chain_err => {
                        Self::send_err(requests, chain_err.to_string());
                        return Err(
                            ChainClientError::LocalNodeError(LocalNodeError::WorkerError(
                                WorkerError::ChainError(chain_err.into()),
                            ))
                            .into(),
                        );
                    }
                }
            }
            Err(err) => {
                tracing::debug!("Error executing operations: {err}");
                Self::send_err(requests, err.to_string());
                return Err(err.into());
            }
            Ok(ClientOutcome::Committed(certificate)) => certificate,
            Ok(ClientOutcome::WaitForTimeout(timeout)) => {
                let error_msg = format!(
                    "This faucet is using a multi-owner chain and is not the leader right now. \
                    Try again at {}",
                    timeout.timestamp,
                );
                Self::send_err(requests, error_msg.clone());
                return Ok(());
            }
            Ok(ClientOutcome::Conflict(certificate)) => {
                let error_msg = format!(
                    "A different block was committed at this height: {}",
                    certificate.hash(),
                );
                Self::send_err(requests, error_msg.clone());
                return Ok(());
            }
        };

        let certificate_hash = certificate.hash();
        let block_timestamp = certificate.block().header.timestamp;

        // Parse chain descriptions from the block's blobs (for initial claims only).
        let chain_descriptions = extract_opened_single_owner_chains(&certificate)?;

        // Build a map of owner -> description for initial claims.
        let initial_desc_map: std::collections::HashMap<_, _> =
            chain_descriptions.into_iter().collect();

        // Split requests by claim type for storage.
        let initial_chains: Vec<_> = initial_desc_map
            .iter()
            .map(|(owner, description)| (*owner, description.id()))
            .collect();
        let daily_claims: Vec<_> = requests
            .iter()
            .filter_map(|r| {
                r.target_chain_id
                    .map(|chain_id| (r.owner, chain_id, r.daily_period))
            })
            .collect();

        #[cfg(with_metrics)]
        let _store_chains_start = std::time::Instant::now();

        let store_initial = async {
            if initial_chains.is_empty() {
                return Ok(());
            }
            self.faucet_storage
                .store_chains_batch(initial_chains, block_timestamp)
                .await
        };
        let store_daily = async {
            if daily_claims.is_empty() {
                return Ok(());
            }
            self.faucet_storage
                .store_daily_claims_batch(daily_claims)
                .await
        };

        if let Err(e) = futures::try_join!(store_initial, store_daily) {
            let error_msg = format!("Failed to save claims to database: {}", e);
            Self::send_err(requests, error_msg.clone());
            anyhow::bail!(error_msg);
        }
        #[cfg(with_metrics)]
        {
            let elapsed_ms = _store_chains_start.elapsed().as_secs_f64() * 1000.0;
            metrics::STORE_CHAIN_LATENCY
                .with_label_values(&[])
                .observe(elapsed_ms);
        }

        // Respond to requests.
        #[cfg(with_metrics)]
        let chains_created = initial_desc_map.len();

        for request in requests {
            #[cfg(with_metrics)]
            {
                let wait_time = request.queued_at.elapsed().as_secs_f64() * 1000.0;
                metrics::QUEUE_WAIT_TIME
                    .with_label_values(&[])
                    .observe(wait_time);
            }

            let response = if let Some(target_chain_id) = request.target_chain_id {
                PendingResponse::Daily(Ok(ClaimOutcome {
                    chain_id: target_chain_id,
                    certificate_hash,
                    amount: request.amount,
                }))
            } else if let Some(description) = initial_desc_map.get(&request.owner) {
                PendingResponse::Initial(Ok(Box::new(description.clone())))
            } else {
                PendingResponse::Initial(Err(Error::new(format!(
                    "No chain created for owner {}",
                    request.owner
                ))))
            };
            if request.responder.send(response).is_err() {
                tracing::warn!(
                    "Receiver dropped while sending response to {}.",
                    request.owner
                );
            }
        }

        #[cfg(with_metrics)]
        metrics::CHAINS_CREATED_TOTAL
            .with_label_values(&[])
            .inc_by(chains_created as u64);

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
    initial_claim_amount: Amount,
    daily_claim_amount: Amount,
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
            initial_claim_amount: self.initial_claim_amount,
            daily_claim_amount: self.daily_claim_amount,
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
    pub initial_claim_amount: Amount,
    pub daily_claim_amount: Amount,
    pub end_timestamp: Timestamp,
    pub genesis_config: std::sync::Arc<GenesisConfig>,
    pub chain_listener_config: ChainListenerConfig,
    pub storage_path: PathBuf,
    pub max_batch_size: usize,
}

impl<C> FaucetService<C>
where
    C: ClientContext + 'static,
{
    /// Creates a new instance of the faucet service.
    pub async fn new(config: FaucetConfig, context: C) -> anyhow::Result<Self> {
        let storage = context.storage().clone();
        let client = context.make_chain_client(config.chain_id).await?;
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
            storage,
            context,
            client,
            genesis_config: config.genesis_config,
            config: config.chain_listener_config,
            port: config.port,
            #[cfg(feature = "metrics")]
            metrics_port: config.metrics_port,
            initial_claim_amount: config.initial_claim_amount,
            daily_claim_amount: config.daily_claim_amount,
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
            initial_claim_amount: self.initial_claim_amount,
            daily_claim_amount: self.daily_claim_amount,
        };
        let query_root = QueryRoot {
            genesis_config: Arc::clone(&self.genesis_config),
            client: self.client.clone(),
            faucet_storage: Arc::clone(&self.faucet_storage),
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
            tokio::sync::mpsc::unbounded_channel().1,
            false, // Faucet doesn't receive messages, so no need for background sync
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

/// Returns all `(AccountOwner, ChainDescription)` pairs for single-owner chains created in this
/// block.
fn extract_opened_single_owner_chains(
    certificate: &ConfirmedBlockCertificate,
) -> anyhow::Result<Vec<(AccountOwner, ChainDescription)>> {
    let created_blobs = certificate.block().body.blobs.iter().flatten();
    created_blobs
        .filter_map(|blob| {
            if blob.content().blob_type() != BlobType::ChainDescription {
                return None;
            }
            let description = match bcs::from_bytes::<ChainDescription>(blob.bytes()) {
                Err(err) => return Some(Err(anyhow::anyhow!(err))),
                Ok(description) => description,
            };
            let chain_id = description.id();
            let owner = {
                let mut owners = description.config().ownership.all_owners();
                match (owners.next(), owners.next()) {
                    (Some(owner), None) => *owner,
                    (None, None) | (_, Some(_)) => {
                        tracing::info!("Skipping chain {chain_id}; not exactly one owner.");
                        return None;
                    }
                }
            };
            tracing::debug!("Found chain {chain_id} created for owner {owner}",);
            Some(Ok((owner, description)))
        })
        .collect::<Result<Vec<_>, _>>()
}
