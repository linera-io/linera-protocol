// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{cmp::Ordering, collections::BTreeMap, future::Future, sync::Arc};

use custom_debug_derive::Debug;
use futures::stream::{FuturesUnordered, StreamExt};
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
    time::{Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use rand::{
    distributions::{Distribution, WeightedIndex},
    prelude::SliceRandom as _,
};
use tracing::instrument;

use super::{
    cache::{RequestsCache, SubsumingKey},
    in_flight_tracker::{InFlightMatch, InFlightTracker},
    node_info::NodeInfo,
    request::{RequestKey, RequestResult},
    scoring::ScoringWeights,
};
use crate::{
    client::{
        communicate_concurrently,
        validator_manager::{in_flight_tracker::Subscribed, request::Cacheable},
        ValidatorManagerConfig,
    },
    environment::Environment,
    node::{NodeError, ValidatorNode},
    remote_node::RemoteNode,
};

#[cfg(with_metrics)]
pub(super) mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, register_int_counter,
        register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounter, IntCounterVec};

    /// Histogram of response times per validator (in milliseconds)
    pub(super) static VALIDATOR_RESPONSE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "validator_manager_response_time_ms",
            "Response time for requests to validators in milliseconds",
            &["validator"],
            exponential_bucket_latencies(10000.0), // up to 10 seconds
        )
    });

    /// Counter of total requests made to each validator
    pub(super) static VALIDATOR_REQUEST_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "validator_manager_request_total",
            "Total number of requests made to each validator",
            &["validator"],
        )
    });

    /// Counter of successful requests per validator
    pub(super) static VALIDATOR_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "validator_manager_request_success",
            "Number of successful requests to each validator",
            &["validator"],
        )
    });

    /// Counter for requests that were resolved from the response cache.
    pub(super) static REQUEST_CACHE_DEDUPLICATION: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "validator_manager_request_deduplication_total",
            "Number of requests that were deduplicated by finding the result in the cache.",
        )
    });

    /// Counter for requests that were served from cache
    pub static REQUEST_CACHE_HIT: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "validator_manager_request_cache_hit_total",
            "Number of requests that were served from cache",
        )
    });
}

/// Manages a pool of validator nodes with intelligent load balancing and performance tracking.
///
/// The `ValidatorManager` maintains performance metrics for each validator node using
/// Exponential Moving Averages (EMA) and uses these metrics to make intelligent routing
/// decisions. It prevents node overload through request capacity limits and automatically
/// retries failed requests on alternative nodes.
///
/// # Examples
///
/// ```ignore
/// // Create with default configuration (balanced scoring)
/// let manager = ValidatorManager::new(validator_nodes);
///
/// // Create with custom configuration prioritizing low latency
/// let latency_weights = ScoringWeights {
///     latency: 0.6,
///     success: 0.3,
///     load: 0.1,
/// };
/// let manager = ValidatorManager::with_config(
///     validator_nodes,
///     15,                      // max 15 concurrent requests per node
///     latency_weights,         // custom scoring weights
///     0.2,                     // higher alpha for faster adaptation
///     3000.0,                  // max expected latency (3 seconds)
///     Duration::from_secs(60), // 60 second cache TTL
///     200,                     // cache up to 200 entries
/// );
/// ```
#[derive(Debug, Clone)]
pub struct ValidatorManager<Env: Environment> {
    /// Thread-safe map of validator nodes indexed by their public keys.
    /// Each node is wrapped with EMA-based performance tracking information.
    nodes: Arc<tokio::sync::RwLock<BTreeMap<ValidatorPublicKey, NodeInfo<Env>>>>,
    /// Maximum number of concurrent requests allowed per node.
    /// Prevents overwhelming individual validators with too many parallel requests.
    max_requests_per_node: usize,
    /// Default scoring weights applied to new nodes.
    weights: ScoringWeights,
    /// Default EMA smoothing factor for new nodes.
    alpha: f64,
    /// Default maximum expected latency in milliseconds for score normalization.
    max_expected_latency: f64,
    /// Tracks in-flight requests to deduplicate concurrent requests for the same data.
    in_flight_tracker: InFlightTracker<RemoteNode<Env::ValidatorNode>>,
    /// Cache of recently completed requests with their results and timestamps.
    cache: RequestsCache<RequestKey, RequestResult>,
}

impl<Env: Environment> ValidatorManager<Env> {
    /// Creates a new `ValidatorManager` with the provided configuration.
    pub fn new(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        config: ValidatorManagerConfig,
    ) -> Self {
        Self::with_config(
            nodes,
            config.max_in_flight_requests,
            ScoringWeights::default(),
            config.alpha,
            config.max_accepted_latency_ms,
            Duration::from_millis(config.cache_ttl_ms),
            config.cache_max_size,
            Duration::from_millis(config.max_request_ttl_ms),
        )
    }

    /// Creates a new `ValidatorManager` with custom configuration.
    ///
    /// # Arguments
    /// - `nodes`: Initial set of validator nodes
    /// - `max_requests_per_node`: Maximum concurrent requests per node
    /// - `weights`: Scoring weights for performance metrics
    /// - `alpha`: EMA smoothing factor (0 < alpha < 1)
    /// - `max_expected_latency_ms`: Maximum expected latency for score normalization
    /// - `cache_ttl`: Time-to-live for cached responses
    /// - `max_cache_size`: Maximum number of entries in the cache
    /// - `max_request_ttl`: Maximum latency for an in-flight request before we stop deduplicating it
    #[expect(clippy::too_many_arguments)]
    pub fn with_config(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        max_requests_per_node: usize,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
        cache_ttl: Duration,
        max_cache_size: usize,
        max_request_ttl: Duration,
    ) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "Alpha must be in (0, 1) range");
        Self {
            nodes: Arc::new(tokio::sync::RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| {
                        (
                            node.public_key,
                            NodeInfo::with_config(
                                node,
                                weights,
                                alpha,
                                max_expected_latency_ms,
                                max_requests_per_node,
                            ),
                        )
                    })
                    .collect(),
            )),
            max_requests_per_node,
            weights,
            alpha,
            max_expected_latency: max_expected_latency_ms,
            in_flight_tracker: InFlightTracker::new(max_request_ttl),
            cache: RequestsCache::new(cache_ttl, max_cache_size),
        }
    }

    /// Executes an operation with an automatically selected peer, handling deduplication,
    /// tracking, and peer selection.
    ///
    /// This method provides a high-level API for executing operations against remote nodes
    /// while leveraging the [`ValidatorManager`]'s intelligent peer selection, performance tracking,
    /// and request deduplication capabilities.
    ///
    /// # Type Parameters
    /// - `R`: The inner result type (what the operation returns on success)
    /// - `F`: The async closure type that takes a `RemoteNode` and returns a future
    /// - `Fut`: The future type returned by the closure
    ///
    /// # Arguments
    /// - `key`: Unique identifier for request deduplication
    /// - `operation`: Async closure that takes a selected peer and performs the operation
    ///
    /// # Returns
    /// The result from the operation, potentially from cache or a deduplicated in-flight request
    ///
    /// # Example
    /// ```ignore
    /// let result: Result<Vec<ConfirmedBlockCertificate>, NodeError> = validator_manager
    ///     .with_best(
    ///         RequestKey::Certificates { chain_id, start, limit },
    ///         |peer| async move {
    ///             peer.download_certificates_from(chain_id, start, limit).await
    ///         }
    ///     )
    ///     .await;
    /// ```
    #[allow(unused)]
    async fn with_best<R, F, Fut>(&self, key: RequestKey, operation: F) -> Result<R, NodeError>
    where
        R: Cacheable + Clone + Send + 'static,
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>>,
    {
        // Select the best available peer
        let peer = self
            .select_best_peer()
            .await
            .ok_or_else(|| NodeError::WorkerError {
                error: "No validators available".to_string(),
            })?;
        self.with_peer(key, peer, operation).await
    }

    /// Executes an operation with a specific peer.
    ///
    /// Similar to [`with_best`](Self::with_best), but uses the provided peer directly
    /// instead of selecting the best available peer. This is useful when you need to
    /// query a specific validator node.
    ///
    /// # Type Parameters
    /// - `R`: The inner result type (what the operation returns on success)
    /// - `F`: The async closure type that takes a `RemoteNode` and returns a future
    /// - `Fut`: The future type returned by the closure
    ///
    /// # Arguments
    /// - `key`: Unique identifier for request deduplication
    /// - `peer`: The specific peer to use for the operation
    /// - `operation`: Async closure that takes the peer and performs the operation
    ///
    /// # Returns
    /// The result from the operation, potentially from cache or a deduplicated in-flight request
    async fn with_peer<R, F, Fut>(
        &self,
        key: RequestKey,
        peer: RemoteNode<Env::ValidatorNode>,
        operation: F,
    ) -> Result<R, NodeError>
    where
        R: Cacheable + Clone + Send + 'static,
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>>,
    {
        self.add_peer(peer.clone()).await;
        self.in_flight_tracker
            .add_alternative_peer(&key, peer.clone())
            .await;
        self.deduplicated_request(key, peer, |peer| async {
            self.track_request(peer, operation).await
        })
        .await
    }

    #[instrument(level = "trace", skip_all)]
    async fn download_blob(
        &self,
        peers: &[RemoteNode<Env::ValidatorNode>],
        blob_id: BlobId,
        timeout: Duration,
    ) -> Result<Option<Blob>, NodeError> {
        let key = RequestKey::Blob(blob_id);
        let mut peers = peers.to_vec();
        peers.shuffle(&mut rand::thread_rng());
        communicate_concurrently(
            &peers,
            async move |peer| {
                self.with_peer(key, peer, |peer| async move {
                    peer.download_blob(blob_id).await
                })
                .await
            },
            |errors| errors.last().cloned().unwrap(),
            timeout,
        )
        .await
        .map_err(|(_validator, error)| error)
    }

    /// Downloads the blobs with the given IDs. This is done in one concurrent task per blob.
    /// Uses intelligent peer selection based on scores and load balancing.
    /// Returns `None` if it couldn't find all blobs.
    #[instrument(level = "trace", skip_all)]
    pub async fn download_blobs(
        &self,
        peers: &[RemoteNode<Env::ValidatorNode>],
        blob_ids: &[BlobId],
        timeout: Duration,
    ) -> Result<Option<Vec<Blob>>, NodeError> {
        let mut stream = blob_ids
            .iter()
            .map(|blob_id| self.download_blob(peers, *blob_id, timeout))
            .collect::<FuturesUnordered<_>>();

        let mut blobs = Vec::new();
        while let Some(maybe_blob) = stream.next().await {
            blobs.push(maybe_blob?);
        }
        Ok(blobs.into_iter().collect::<Option<Vec<_>>>())
    }

    pub async fn download_certificates(
        &self,
        peer: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        let heights = (start.0..start.0 + limit)
            .map(BlockHeight)
            .collect::<Vec<_>>();
        self.with_peer(
            RequestKey::Certificates {
                chain_id,
                heights: heights.clone(),
            },
            peer.clone(),
            |peer| async move {
                peer.download_certificates_by_heights(chain_id, heights)
                    .await
            },
        )
        .await
    }

    pub async fn download_certificates_by_heights(
        &self,
        peer: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        self.with_peer(
            RequestKey::Certificates {
                chain_id,
                heights: heights.clone(),
            },
            peer.clone(),
            |peer| async move {
                peer.download_certificates_by_heights(chain_id, heights)
                    .await
            },
        )
        .await
    }

    pub async fn download_certificate_for_blob(
        &self,
        peer: &RemoteNode<Env::ValidatorNode>,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        self.with_peer(
            RequestKey::CertificateForBlob(blob_id),
            peer.clone(),
            |peer| async move { peer.download_certificate_for_blob(blob_id).await },
        )
        .await
    }

    pub async fn download_pending_blob(
        &self,
        peer: &RemoteNode<Env::ValidatorNode>,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        self.with_peer(
            RequestKey::PendingBlob { chain_id, blob_id },
            peer.clone(),
            |peer| async move { peer.node.download_pending_blob(chain_id, blob_id).await },
        )
        .await
    }

    /// Returns the alternative peers registered for an in-flight request, if any.
    ///
    /// This can be used to retry a failed request with alternative data sources
    /// that were registered during request deduplication.
    pub async fn get_alternative_peers(
        &self,
        key: &RequestKey,
    ) -> Option<Vec<RemoteNode<Env::ValidatorNode>>> {
        self.in_flight_tracker.get_alternative_peers(key).await
    }

    /// Returns current performance metrics for all managed nodes.
    ///
    /// Each entry contains:
    /// - Performance score (f64, normalized 0.0-1.0)
    /// - EMA success rate (f64, 0.0-1.0)
    /// - Total requests processed (u64)
    ///
    /// Useful for monitoring and debugging node performance.
    pub async fn get_node_scores(&self) -> BTreeMap<ValidatorPublicKey, (f64, f64, u64)> {
        let nodes = self.nodes.read().await;
        let mut result = BTreeMap::new();

        for (key, info) in nodes.iter() {
            let score = info.calculate_score().await;
            result.insert(
                *key,
                (score, info.ema_success_rate(), info.total_requests()),
            );
        }

        result
    }

    /// Wraps a request operation with performance tracking and capacity management.
    ///
    /// This method:
    /// 1. Acquires a request slot (blocks asynchronously until one is available)
    /// 2. Executes the provided operation with the selected peer
    /// 3. Measures response time
    /// 4. Updates node metrics based on success/failure
    /// 5. Releases the request slot
    ///
    /// # Arguments
    /// - `peer`: The remote node to execute the operation on
    /// - `operation`: Async closure that performs the actual request with the selected peer
    ///
    /// # Behavior
    /// If no slot is available, this method will wait asynchronously (without polling)
    /// until another request completes and releases its slot. The task will be efficiently
    /// suspended and woken by the async runtime using notification mechanisms.
    async fn track_request<T, F, Fut>(
        &self,
        peer: RemoteNode<Env::ValidatorNode>,
        operation: F,
    ) -> Result<T, NodeError>
    where
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<T, NodeError>>,
    {
        let start_time = Instant::now();
        let public_key = peer.public_key;

        // Acquire request slot
        let nodes = self.nodes.read().await;
        let node = nodes.get(&public_key).expect("Node must exist");
        let semaphore = node.in_flight_semaphore.clone();
        let permit = semaphore.acquire().await.unwrap();
        drop(nodes);

        // Execute the operation
        let result = operation(peer).await;

        // Update metrics and release slot
        let response_time_ms = start_time.elapsed().as_millis() as u64;
        drop(permit); // Explicitly drop the permit to release the slot
        let is_success = result.is_ok();
        {
            let mut nodes = self.nodes.write().await;
            if let Some(info) = nodes.get_mut(&public_key) {
                info.update_metrics(is_success, response_time_ms);
                let score = info.calculate_score().await;
                tracing::trace!(
                    node = %public_key,
                    address = %info.node.node.address(),
                    success = %is_success,
                    response_time_ms = %response_time_ms,
                    score = %score,
                    total_requests = %info.total_requests(),
                    "Request completed"
                );
            }
        }

        // Record Prometheus metrics
        #[cfg(with_metrics)]
        {
            let validator_name = public_key.to_string();
            metrics::VALIDATOR_RESPONSE_TIME
                .with_label_values(&[&validator_name])
                .observe(response_time_ms as f64);
            metrics::VALIDATOR_REQUEST_TOTAL
                .with_label_values(&[&validator_name])
                .inc();
            if is_success {
                metrics::VALIDATOR_REQUEST_SUCCESS
                    .with_label_values(&[&validator_name])
                    .inc();
            }
        }

        result
    }

    /// Deduplicates concurrent requests for the same data.
    ///
    /// If a request for the same key is already in flight, this method waits for
    /// the existing request to complete and returns its result. Otherwise, it
    /// executes the operation and broadcasts the result to all waiting callers.
    ///
    /// This method also performs **subsumption-based deduplication**: if a larger
    /// request that contains all the data needed by this request is already cached
    /// or in flight, we can extract the subset result instead of making a new request.
    ///
    /// # Arguments
    /// - `key`: Unique identifier for the request
    /// - `operation`: Async closure that performs the actual request
    ///
    /// # Returns
    /// The result from either the in-flight request or the newly executed operation
    async fn deduplicated_request<T, F, Fut, N>(
        &self,
        key: RequestKey,
        peer: N,
        operation: F,
    ) -> Result<T, NodeError>
    where
        T: Cacheable + Clone + Send + 'static,
        F: FnOnce(N) -> Fut,
        Fut: Future<Output = Result<T, NodeError>>,
    {
        // Check cache for exact or subsuming match
        if let Some(result) = self.cache.get(&key).await {
            return Ok(result);
        }

        // Check if there's an in-flight request (exact or subsuming)
        if let Some(in_flight_match) = self.in_flight_tracker.try_subscribe(&key).await {
            match in_flight_match {
                InFlightMatch::Exact(Subscribed(mut receiver)) => {
                    tracing::trace!(
                        key = ?key,
                        "deduplicating request (exact match) - joining existing in-flight request"
                    );
                    #[cfg(with_metrics)]
                    metrics::REQUEST_CACHE_DEDUPLICATION.inc();
                    // Wait for result from existing request
                    match receiver.recv().await {
                        Ok(result) => match result.as_ref().clone() {
                            Ok(res) => match T::try_from(res) {
                                Ok(converted) => {
                                    tracing::trace!(
                                        key = ?key,
                                        "received result from deduplicated in-flight request"
                                    );
                                    return Ok(converted);
                                }
                                Err(_) => {
                                    tracing::trace!(
                                        key = ?key,
                                        "failed to convert result from deduplicated in-flight request, will execute independently"
                                    );
                                }
                            },
                            Err(e) => {
                                tracing::trace!(
                                    key = ?key,
                                    error = %e,
                                    "in-flight request failed",
                                );
                                // Fall through to execute a new request
                            }
                        },
                        Err(_) => {
                            tracing::trace!(
                                key = ?key,
                                "in-flight request sender dropped"
                            );
                            // Fall through to execute a new request
                        }
                    }
                }
                InFlightMatch::Subsuming {
                    key: subsuming_key,
                    outcome: Subscribed(mut receiver),
                } => {
                    tracing::trace!(
                    key = ?key,
                    subsumed_by = ?subsuming_key,
                        "deduplicating request (subsumption) - joining larger in-flight request"
                    );
                    #[cfg(with_metrics)]
                    metrics::REQUEST_CACHE_DEDUPLICATION.inc();
                    // Wait for result from the subsuming request
                    match receiver.recv().await {
                        Ok(result) => {
                            match result.as_ref() {
                                Ok(res) => {
                                    if let Some(extracted) =
                                        key.try_extract_result(&subsuming_key, res)
                                    {
                                        tracing::trace!(
                                            key = ?key,
                                            "extracted subset result from larger in-flight request"
                                        );
                                        match T::try_from(extracted) {
                                            Ok(converted) => return Ok(converted),
                                            Err(_) => {
                                                tracing::trace!(
                                                    key = ?key,
                                                    "failed to convert extracted result, will execute independently"
                                                );
                                            }
                                        }
                                    } else {
                                        // Extraction failed, fall through to execute our own request
                                        tracing::trace!(
                                            key = ?key,
                                            "failed to extract from subsuming request, will execute independently"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        key = ?key,
                                        error = %e,
                                        "subsuming in-flight request failed",
                                    );
                                    // Fall through to execute our own request
                                }
                            }
                        }
                        Err(_) => {
                            tracing::trace!(
                                key = ?key,
                                "subsuming in-flight request sender dropped"
                            );
                        }
                    }
                }
            }
        };

        // Create new in-flight entry for this request
        self.in_flight_tracker.insert_new(key.clone()).await;

        // Execute the actual request
        tracing::trace!(key = ?key, "executing new request");
        let result = operation(peer).await;
        let result_for_broadcast: Result<RequestResult, NodeError> = result.clone().map(Into::into);
        let shared_result = Arc::new(result_for_broadcast);

        // Broadcast result and clean up
        self.in_flight_tracker
            .complete_and_broadcast(&key, shared_result.clone())
            .await;

        if let Ok(success) = shared_result.as_ref() {
            self.cache
                .store(key.clone(), Arc::new(success.clone()))
                .await;
        }
        result
    }

    /// Returns all peers ordered by their score (highest first).
    ///
    /// Only includes peers that can currently accept requests. Each peer is paired
    /// with its calculated score based on latency, success rate, and availability.
    ///
    /// # Returns
    /// A vector of `(score, peer)` tuples sorted by score in descending order.
    /// Returns an empty vector if no peers can accept requests.
    async fn peers_by_score(&self) -> Vec<(f64, RemoteNode<Env::ValidatorNode>)> {
        let nodes = self.nodes.read().await;

        // Filter nodes that can accept requests and calculate their scores
        let mut scored_nodes = Vec::new();
        for info in nodes.values() {
            let score = info.calculate_score().await;
            scored_nodes.push((score, info.node.clone()));
        }

        // Sort by score (highest first)
        scored_nodes.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));

        scored_nodes
    }

    /// Selects the best available peer using weighted random selection from top performers.
    ///
    /// This method:
    /// 1. Filters nodes that have available request capacity
    /// 2. Sorts them by performance score
    /// 3. Performs weighted random selection from the top 3 performers
    ///
    /// This approach balances between choosing high-performing nodes and distributing
    /// load across multiple validators to avoid creating hotspots.
    ///
    /// Returns `None` if no nodes are available or all are at capacity.
    async fn select_best_peer(&self) -> Option<RemoteNode<Env::ValidatorNode>> {
        let scored_nodes = self.peers_by_score().await;

        if scored_nodes.is_empty() {
            return None;
        }

        // Use weighted random selection from top performers (top 3 or all if less)
        let top_count = scored_nodes.len().min(3);
        let top_nodes = &scored_nodes[..top_count];

        // Create weights based on normalized scores
        // Add small epsilon to prevent zero weights
        let weights: Vec<f64> = top_nodes.iter().map(|(score, _)| score.max(0.01)).collect();

        if let Ok(dist) = WeightedIndex::new(&weights) {
            let mut rng = rand::thread_rng();
            let index = dist.sample(&mut rng);
            Some(top_nodes[index].1.clone())
        } else {
            // Fallback to the best node if weights are invalid
            tracing::warn!("failed to create weighted distribution, defaulting to best node");
            Some(scored_nodes[0].1.clone())
        }
    }

    /// Adds a new peer to the manager if it doesn't already exist.
    async fn add_peer(&self, node: RemoteNode<Env::ValidatorNode>) {
        let mut nodes = self.nodes.write().await;
        let public_key = node.public_key;
        nodes.entry(public_key).or_insert_with(|| {
            NodeInfo::with_config(
                node,
                self.weights,
                self.alpha,
                self.max_expected_latency,
                self.max_requests_per_node,
            )
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use linera_base::{
        crypto::{CryptoHash, InMemorySigner},
        data_types::BlockHeight,
        identifiers::ChainId,
        time::Duration,
    };
    use linera_chain::types::ConfirmedBlockCertificate;
    use tokio::sync::oneshot;

    use super::{super::request::RequestKey, *};
    use crate::{client::validator_manager::MAX_REQUEST_TTL_MS, node::NodeError};

    type TestEnvironment = crate::environment::Test;

    /// Helper function to create a test ValidatorManager with custom configuration
    fn create_test_manager(
        in_flight_timeout: Duration,
        cache_ttl: Duration,
    ) -> Arc<ValidatorManager<TestEnvironment>> {
        let mut manager = ValidatorManager::with_config(
            vec![], // No actual nodes needed for these tests
            10,
            ScoringWeights::default(),
            0.1,
            1000.0,
            cache_ttl,
            100,
            in_flight_timeout,
        );
        // Replace the tracker with one using the custom timeout
        manager.in_flight_tracker = InFlightTracker::new(in_flight_timeout);
        Arc::new(manager)
    }

    /// Helper function to create a test result
    fn test_result_ok() -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        Ok(vec![])
    }

    /// Helper function to create a test request key
    fn test_key() -> RequestKey {
        RequestKey::Certificates {
            chain_id: ChainId(CryptoHash::test_hash("test")),
            heights: vec![BlockHeight(0), BlockHeight(1)],
        }
    }

    #[tokio::test]
    async fn test_cache_hit_returns_cached_result() {
        // Create a manager with standard cache TTL
        let manager = create_test_manager(Duration::from_secs(60), Duration::from_secs(60));
        let key = test_key();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));
        let execution_count_clone = execution_count.clone();

        // First call - should execute the operation and cache the result
        let result1: Result<Vec<ConfirmedBlockCertificate>, NodeError> = manager
            .deduplicated_request(key.clone(), (), |_| async move {
                execution_count_clone.fetch_add(1, Ordering::SeqCst);
                test_result_ok()
            })
            .await;

        assert!(result1.is_ok());
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);

        // Second call - should return cached result without executing the operation
        let execution_count_clone2 = execution_count.clone();
        let result2: Result<Vec<ConfirmedBlockCertificate>, NodeError> = manager
            .deduplicated_request(key.clone(), (), |_| async move {
                execution_count_clone2.fetch_add(1, Ordering::SeqCst);
                test_result_ok()
            })
            .await;

        assert_eq!(result1, result2);
        // Operation should still only have been executed once (cache hit)
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_in_flight_request_deduplication() {
        let manager = create_test_manager(Duration::from_secs(60), Duration::from_secs(60));
        let key = test_key();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the first operation completes
        let (tx, rx) = oneshot::channel();

        // Start first request (will be slow - waits for signal)
        let manager_clone = Arc::clone(&manager);
        let key_clone = key.clone();
        let execution_count_clone = execution_count.clone();
        let first_request = tokio::spawn(async move {
            manager_clone
                .deduplicated_request(key_clone, (), |_| async move {
                    execution_count_clone.fetch_add(1, Ordering::SeqCst);
                    // Wait for signal before completing
                    rx.await.unwrap();
                    test_result_ok()
                })
                .await
        });

        // Start second request - should deduplicate and wait for the first
        let execution_count_clone2 = execution_count.clone();
        let second_request = tokio::spawn(async move {
            manager
                .deduplicated_request(key, (), |_| async move {
                    execution_count_clone2.fetch_add(1, Ordering::SeqCst);
                    test_result_ok()
                })
                .await
        });

        // Signal the first request to complete
        tx.send(()).unwrap();

        // Both requests should complete successfully
        let result1: Result<Vec<ConfirmedBlockCertificate>, NodeError> =
            first_request.await.unwrap();
        let result2: Result<Vec<ConfirmedBlockCertificate>, NodeError> =
            second_request.await.unwrap();

        assert!(result1.is_ok());
        assert_eq!(result1, result2);

        // Operation should only have been executed once (deduplication worked)
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multiple_subscribers_all_notified() {
        let manager = create_test_manager(Duration::from_secs(60), Duration::from_secs(60));
        let key = test_key();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the operation completes
        let (tx, rx) = oneshot::channel();

        // Start first request (will be slow - waits for signal)
        let manager_clone1 = Arc::clone(&manager);
        let key_clone1 = key.clone();
        let execution_count_clone = execution_count.clone();
        let first_request = tokio::spawn(async move {
            manager_clone1
                .deduplicated_request(key_clone1, (), |_| async move {
                    execution_count_clone.fetch_add(1, Ordering::SeqCst);
                    rx.await.unwrap();
                    test_result_ok()
                })
                .await
        });

        // Start multiple additional requests - all should deduplicate
        let mut handles = vec![];
        for _ in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let key_clone = key.clone();
            let execution_count_clone = execution_count.clone();
            let handle = tokio::spawn(async move {
                manager_clone
                    .deduplicated_request(key_clone, (), |_| async move {
                        execution_count_clone.fetch_add(1, Ordering::SeqCst);
                        test_result_ok()
                    })
                    .await
            });
            handles.push(handle);
        }

        // Signal the first request to complete
        tx.send(()).unwrap();

        // First request should complete successfully
        let result: Result<Vec<ConfirmedBlockCertificate>, NodeError> =
            first_request.await.unwrap();
        assert!(result.is_ok());

        // All subscriber requests should also complete successfully
        for handle in handles {
            assert_eq!(handle.await.unwrap(), result);
        }

        // Operation should only have been executed once (all requests were deduplicated)
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_timeout_triggers_new_request() {
        // Create a manager with a very short in-flight timeout
        let manager = create_test_manager(Duration::from_millis(50), Duration::from_secs(60));

        let key = test_key();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the first operation completes
        let (tx, rx) = oneshot::channel();

        // Start first request (will be slow - waits for signal)
        let manager_clone = Arc::clone(&manager);
        let key_clone = key.clone();
        let execution_count_clone = execution_count.clone();
        let first_request = tokio::spawn(async move {
            manager_clone
                .deduplicated_request(key_clone, (), |_| async move {
                    execution_count_clone.fetch_add(1, Ordering::SeqCst);
                    rx.await.unwrap();
                    test_result_ok()
                })
                .await
        });

        // Wait for the timeout to elapse
        tokio::time::sleep(Duration::from_millis(MAX_REQUEST_TTL_MS + 1)).await;

        // Start second request - should NOT deduplicate because first request exceeded timeout
        let execution_count_clone2 = execution_count.clone();
        let second_request = tokio::spawn(async move {
            manager
                .deduplicated_request(key, (), |_| async move {
                    execution_count_clone2.fetch_add(1, Ordering::SeqCst);
                    test_result_ok()
                })
                .await
        });

        // Wait for second request to complete
        let result2: Result<Vec<ConfirmedBlockCertificate>, NodeError> =
            second_request.await.unwrap();
        assert!(result2.is_ok());

        // Complete the first request
        tx.send(()).unwrap();
        let result1: Result<Vec<ConfirmedBlockCertificate>, NodeError> =
            first_request.await.unwrap();
        assert!(result1.is_ok());

        // Operation should have been executed twice (timeout triggered new request)
        assert_eq!(execution_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_slot_limiting_blocks_excess_requests() {
        // Tests the slot limiting mechanism:
        // - Creates a ValidatorManager with max_requests_per_node = 2
        // - Starts two slow requests that acquire both available slots
        // - Starts a third request and verifies it's blocked waiting for a slot (execution count stays at 2)
        // - Completes the first request to release a slot
        // - Verifies the third request now acquires the freed slot and executes (execution count becomes 3)
        // - Confirms all requests complete successfully
        use linera_base::identifiers::BlobType;

        use crate::test_utils::{MemoryStorageBuilder, TestBuilder};

        // Create a test environment with one validator
        let mut builder = TestBuilder::new(
            MemoryStorageBuilder::default(),
            1,
            0,
            InMemorySigner::new(None),
        )
        .await
        .unwrap();

        // Get the validator node
        let validator_node = builder.node(0);
        let validator_public_key = validator_node.name();

        // Create a RemoteNode wrapper
        let remote_node = RemoteNode {
            public_key: validator_public_key,
            node: validator_node,
        };

        // Create a ValidatorManager with max_requests_per_node = 2
        let max_slots = 2;
        let mut manager: ValidatorManager<TestEnvironment> = ValidatorManager::with_config(
            vec![remote_node.clone()],
            max_slots,
            ScoringWeights::default(),
            0.1,
            1000.0,
            Duration::from_secs(60),
            100,
            Duration::from_secs(60),
        );
        // Replace the tracker with one using a longer timeout for this test
        manager.in_flight_tracker = InFlightTracker::new(Duration::from_secs(60));
        let manager = Arc::new(manager);

        // Track execution state
        let execution_count = Arc::new(AtomicUsize::new(0));
        let completion_count = Arc::new(AtomicUsize::new(0));

        // Create channels to control when operations complete
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        // Start first request using with_peer (will block until signaled)
        let manager_clone1 = Arc::clone(&manager);
        let remote_node_clone1 = remote_node.clone();
        let execution_count_clone1 = execution_count.clone();
        let completion_count_clone1 = completion_count.clone();
        let key1 = RequestKey::Blob(BlobId::new(CryptoHash::test_hash("blob1"), BlobType::Data));

        let first_request = tokio::spawn(async move {
            manager_clone1
                .with_peer(key1, remote_node_clone1, |_peer| async move {
                    execution_count_clone1.fetch_add(1, Ordering::SeqCst);
                    // Simulate work by waiting for signal
                    let _ = rx1.await;
                    completion_count_clone1.fetch_add(1, Ordering::SeqCst);
                    Ok(None) // Return Option<Blob>
                })
                .await
        });

        // Give first request time to start and acquire a slot
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);

        // Start second request using with_peer (will block until signaled)
        let manager_clone2 = Arc::clone(&manager);
        let remote_node_clone2 = remote_node.clone();
        let execution_count_clone2 = execution_count.clone();
        let completion_count_clone2 = completion_count.clone();
        let key2 = RequestKey::Blob(BlobId::new(CryptoHash::test_hash("blob2"), BlobType::Data));

        let second_request = tokio::spawn(async move {
            manager_clone2
                .with_peer(key2, remote_node_clone2, |_peer| async move {
                    execution_count_clone2.fetch_add(1, Ordering::SeqCst);
                    // Simulate work by waiting for signal
                    let _ = rx2.await;
                    completion_count_clone2.fetch_add(1, Ordering::SeqCst);
                    Ok(None) // Return Option<Blob>
                })
                .await
        });

        // Give second request time to start and acquire the second slot
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(execution_count.load(Ordering::SeqCst), 2);

        // Start third request - this should be blocked waiting for a slot
        let remote_node_clone3 = remote_node.clone();
        let execution_count_clone3 = execution_count.clone();
        let completion_count_clone3 = completion_count.clone();
        let key3 = RequestKey::Blob(BlobId::new(CryptoHash::test_hash("blob3"), BlobType::Data));

        let third_request = tokio::spawn(async move {
            manager
                .with_peer(key3, remote_node_clone3, |_peer| async move {
                    execution_count_clone3.fetch_add(1, Ordering::SeqCst);
                    completion_count_clone3.fetch_add(1, Ordering::SeqCst);
                    Ok(None) // Return Option<Blob>
                })
                .await
        });

        // Give third request time to try acquiring a slot
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Third request should still be waiting (not executed yet)
        assert_eq!(
            execution_count.load(Ordering::SeqCst),
            2,
            "Third request should be waiting for a slot"
        );

        // Complete the first request to release a slot
        tx1.send(()).unwrap();

        // Wait for first request to complete and third request to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now the third request should have acquired the freed slot and started executing
        assert_eq!(
            execution_count.load(Ordering::SeqCst),
            3,
            "Third request should now be executing"
        );

        // Complete remaining requests
        tx2.send(()).unwrap();

        // Wait for all requests to complete
        let _result1 = first_request.await.unwrap();
        let _result2 = second_request.await.unwrap();
        let _result3 = third_request.await.unwrap();

        // Verify all completed
        assert_eq!(completion_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_alternative_peers_registered_on_deduplication() {
        use linera_base::identifiers::BlobType;

        use crate::test_utils::{MemoryStorageBuilder, TestBuilder};

        // Create a test environment with three validators
        let mut builder = TestBuilder::new(
            MemoryStorageBuilder::default(),
            3,
            0,
            InMemorySigner::new(None),
        )
        .await
        .unwrap();

        // Get validator nodes
        let nodes: Vec<_> = (0..3)
            .map(|i| {
                let node = builder.node(i);
                let public_key = node.name();
                RemoteNode { public_key, node }
            })
            .collect();

        // Create a ValidatorManager
        let manager: Arc<ValidatorManager<TestEnvironment>> =
            Arc::new(ValidatorManager::with_config(
                nodes.clone(),
                1,
                ScoringWeights::default(),
                0.1,
                1000.0,
                Duration::from_secs(60),
                100,
                Duration::from_millis(MAX_REQUEST_TTL_MS),
            ));

        let key = RequestKey::Blob(BlobId::new(
            CryptoHash::test_hash("test_blob"),
            BlobType::Data,
        ));

        // Create a channel to control when first request completes
        let (tx, rx) = oneshot::channel();

        // Start first request with node 0 (will block until signaled)
        let manager_clone = Arc::clone(&manager);
        let node_clone = nodes[0].clone();
        let key_clone = key.clone();
        let first_request = tokio::spawn(async move {
            manager_clone
                .with_peer(key_clone, node_clone, |_peer| async move {
                    // Wait for signal
                    rx.await.unwrap();
                    Ok(None) // Return Option<Blob>
                })
                .await
        });

        // Give first request time to start and become in-flight
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Start second and third requests with different nodes
        // These should register as alternatives and wait for the first request
        let handles: Vec<_> = vec![nodes[1].clone(), nodes[2].clone()]
            .into_iter()
            .map(|node| {
                let manager_clone = Arc::clone(&manager);
                let key_clone = key.clone();
                tokio::spawn(async move {
                    manager_clone
                        .with_peer(key_clone, node, |_peer| async move {
                            Ok(None) // Return Option<Blob>
                        })
                        .await
                })
            })
            .collect();

        // Give time for alternative peers to register
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check that nodes 1 and 2 are registered as alternatives
        let alt_peers = manager
            .get_alternative_peers(&key)
            .await
            .expect("in-flight entry")
            .into_iter()
            .map(|p| p.public_key)
            .collect::<Vec<_>>();
        assert_eq!(
            alt_peers,
            vec![nodes[1].public_key, nodes[2].public_key],
            "expected nodes 2 and 3 to be registered as alternative peers"
        );

        // Signal first request to complete
        tx.send(()).unwrap();

        // Wait for all requests to complete
        let _result1 = first_request.await.unwrap();
        for handle in handles {
            let _ = handle.await.unwrap();
        }

        // After completion, the in-flight entry should be removed
        tokio::time::sleep(Duration::from_millis(50)).await;
        let alt_peers = manager.get_alternative_peers(&key).await;
        assert!(
            alt_peers.is_none(),
            "Expected in-flight entry to be removed after completion"
        );
    }
}
