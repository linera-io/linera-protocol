// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

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
        requests_scheduler::{in_flight_tracker::Subscribed, request::Cacheable},
        RequestsSchedulerConfig,
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
            "requests_scheduler_response_time_ms",
            "Response time for requests to validators in milliseconds",
            &["validator"],
            exponential_bucket_latencies(10000.0), // up to 10 seconds
        )
    });

    /// Counter of total requests made to each validator
    pub(super) static VALIDATOR_REQUEST_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "requests_scheduler_request_total",
            "Total number of requests made to each validator",
            &["validator"],
        )
    });

    /// Counter of successful requests per validator
    pub(super) static VALIDATOR_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "requests_scheduler_request_success",
            "Number of successful requests to each validator",
            &["validator"],
        )
    });

    /// Counter for requests that were resolved from the response cache.
    pub(super) static REQUEST_CACHE_DEDUPLICATION: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "requests_scheduler_request_deduplication_total",
            "Number of requests that were deduplicated by finding the result in the cache.",
        )
    });

    /// Counter for requests that were served from cache
    pub static REQUEST_CACHE_HIT: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "requests_scheduler_request_cache_hit_total",
            "Number of requests that were served from cache",
        )
    });
}

/// Manages a pool of validator nodes with intelligent load balancing and performance tracking.
///
/// The `RequestsScheduler` maintains performance metrics for each validator node using
/// Exponential Moving Averages (EMA) and uses these metrics to make intelligent routing
/// decisions. It prevents node overload through request capacity limits and automatically
/// retries failed requests on alternative nodes.
///
/// # Examples
///
/// ```ignore
/// // Create with default configuration (balanced scoring)
/// let manager = RequestsScheduler::new(validator_nodes);
///
/// // Create with custom configuration prioritizing low latency
/// let latency_weights = ScoringWeights {
///     latency: 0.6,
///     success: 0.3,
///     load: 0.1,
/// };
/// let manager = RequestsScheduler::with_config(
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
pub struct RequestsScheduler<Env: Environment> {
    /// Thread-safe map of validator nodes indexed by their public keys.
    /// Each node is wrapped with EMA-based performance tracking information.
    nodes: Arc<tokio::sync::RwLock<BTreeMap<ValidatorPublicKey, NodeInfo<Env>>>>,
    /// Default scoring weights applied to new nodes.
    weights: ScoringWeights,
    /// Default EMA smoothing factor for new nodes.
    alpha: f64,
    /// Default maximum expected latency in milliseconds for score normalization.
    max_expected_latency: f64,
    /// Delay between starting requests to alternative peers.
    retry_delay: Duration,
    /// Tracks in-flight requests to deduplicate concurrent requests for the same data.
    in_flight_tracker: InFlightTracker<RemoteNode<Env::ValidatorNode>>,
    /// Cache of recently completed requests with their results and timestamps.
    cache: RequestsCache<RequestKey, RequestResult>,
}

impl<Env: Environment> RequestsScheduler<Env> {
    /// Creates a new `RequestsScheduler` with the provided configuration.
    pub fn new(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        config: RequestsSchedulerConfig,
    ) -> Self {
        Self::with_config(
            nodes,
            ScoringWeights::default(),
            config.alpha,
            config.max_accepted_latency_ms,
            Duration::from_millis(config.cache_ttl_ms),
            config.cache_max_size,
            Duration::from_millis(config.max_request_ttl_ms),
            Duration::from_millis(config.retry_delay_ms),
        )
    }

    /// Creates a new `RequestsScheduler` with custom configuration.
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
    /// - `retry_delay_ms`: Delay in milliseconds between starting requests to different peers.
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
        cache_ttl: Duration,
        max_cache_size: usize,
        max_request_ttl: Duration,
        retry_delay: Duration,
    ) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "Alpha must be in (0, 1) range");
        Self {
            nodes: Arc::new(tokio::sync::RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| {
                        (
                            node.public_key,
                            NodeInfo::with_config(node, weights, alpha, max_expected_latency_ms),
                        )
                    })
                    .collect(),
            )),
            weights,
            alpha,
            max_expected_latency: max_expected_latency_ms,
            retry_delay,
            in_flight_tracker: InFlightTracker::new(max_request_ttl),
            cache: RequestsCache::new(cache_ttl, max_cache_size),
        }
    }

    /// Executes an operation with an automatically selected peer, handling deduplication,
    /// tracking, and peer selection.
    ///
    /// This method provides a high-level API for executing operations against remote nodes
    /// while leveraging the [`RequestsScheduler`]'s intelligent peer selection, performance tracking,
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
    /// let result: Result<Vec<ConfirmedBlockCertificate>, NodeError> = requests_scheduler
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
        F: Fn(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>> + 'static,
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
        F: Fn(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>> + 'static,
    {
        self.add_peer(peer.clone()).await;
        self.in_flight_tracker
            .add_alternative_peer(&key, peer.clone())
            .await;

        // Clone the nodes Arc so we can move it into the closure
        let nodes = self.nodes.clone();
        self.deduplicated_request(key, peer, move |peer| {
            let fut = operation(peer.clone());
            let nodes = nodes.clone();
            async move { Self::track_request(nodes, peer, fut).await }
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
                self.with_peer(key, peer, move |peer| async move {
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
            move |peer| {
                let heights = heights.clone();
                async move {
                    Box::pin(peer.download_certificates_by_heights(chain_id, heights)).await
                }
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
            move |peer| {
                let heights = heights.clone();
                async move {
                    peer.download_certificates_by_heights(chain_id, heights)
                        .await
                }
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
            move |peer| async move { peer.download_certificate_for_blob(blob_id).await },
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
            move |peer| async move { peer.node.download_pending_blob(chain_id, blob_id).await },
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
    /// 1. Measures response time
    /// 2. Updates node metrics based on success/failure
    ///
    /// # Arguments
    /// - `nodes`: Arc to the nodes map for updating metrics
    /// - `peer`: The remote node to track metrics for
    /// - `operation`: Future that performs the actual request
    ///
    /// # Behavior
    /// Executes the provided future and tracks metrics for the given peer.
    async fn track_request<T, Fut>(
        nodes: Arc<tokio::sync::RwLock<BTreeMap<ValidatorPublicKey, NodeInfo<Env>>>>,
        peer: RemoteNode<Env::ValidatorNode>,
        operation: Fut,
    ) -> Result<T, NodeError>
    where
        Fut: Future<Output = Result<T, NodeError>> + 'static,
    {
        let start_time = Instant::now();
        let public_key = peer.public_key;

        // Execute the operation
        let result = operation.await;

        // Update metrics and release slot
        let response_time_ms = start_time.elapsed().as_millis() as u64;
        let is_success = result.is_ok();
        {
            let mut nodes_guard = nodes.write().await;
            if let Some(info) = nodes_guard.get_mut(&public_key) {
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
    async fn deduplicated_request<T, F, Fut>(
        &self,
        key: RequestKey,
        peer: RemoteNode<Env::ValidatorNode>,
        operation: F,
    ) -> Result<T, NodeError>
    where
        T: Cacheable + Clone + Send + 'static,
        F: Fn(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<T, NodeError>> + 'static,
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
                        ?key,
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
                                        ?key,
                                        "received result from deduplicated in-flight request"
                                    );
                                    return Ok(converted);
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        ?key,
                                        "failed to convert result from deduplicated in-flight request, will execute independently"
                                    );
                                }
                            },
                            Err(error) => {
                                tracing::trace!(
                                    ?key,
                                    %error,
                                    "in-flight request failed",
                                );
                                // Fall through to execute a new request
                            }
                        },
                        Err(_) => {
                            tracing::trace!(?key, "in-flight request sender dropped");
                            // Fall through to execute a new request
                        }
                    }
                }
                InFlightMatch::Subsuming {
                    key: subsuming_key,
                    outcome: Subscribed(mut receiver),
                } => {
                    tracing::trace!(
                    ?key,
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
                                            ?key,
                                            "extracted subset result from larger in-flight request"
                                        );
                                        match T::try_from(extracted) {
                                            Ok(converted) => return Ok(converted),
                                            Err(_) => {
                                                tracing::trace!(
                                                    ?key,
                                                    "failed to convert extracted result, will execute independently"
                                                );
                                            }
                                        }
                                    } else {
                                        // Extraction failed, fall through to execute our own request
                                        tracing::trace!(
                                            ?key,
                                            "failed to extract from subsuming request, will execute independently"
                                        );
                                    }
                                }
                                Err(error) => {
                                    tracing::trace!(
                                        ?key,
                                        ?error,
                                        "subsuming in-flight request failed",
                                    );
                                    // Fall through to execute our own request
                                }
                            }
                        }
                        Err(_) => {
                            tracing::trace!(?key, "subsuming in-flight request sender dropped");
                        }
                    }
                }
            }
        };

        // Create new in-flight entry for this request
        self.in_flight_tracker.insert_new(key.clone()).await;

        // Remove the peer we're about to use from alternatives (it shouldn't retry with itself)
        self.in_flight_tracker
            .remove_alternative_peer(&key, &peer)
            .await;

        // Execute request with staggered parallel - first peer starts immediately,
        // alternatives are tried after stagger delays (even if first peer is slow but not failing)
        tracing::trace!(?key, ?peer, "executing staggered parallel request");
        let result = self
            .try_staggered_parallel(&key, peer, &operation, self.retry_delay)
            .await;

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

    /// Tries alternative peers in staggered parallel fashion.
    ///
    /// Launches requests starting with the first peer, then dynamically pops alternative peers
    /// with a stagger delay between each. Returns the first successful result. This provides
    /// a balance between sequential (slow) and fully parallel (wasteful) approaches.
    ///
    /// # Arguments
    /// - `key`: The request key (for logging and popping alternatives)
    /// - `first_peer`: The initial peer to try first (at time 0)
    /// - `operation`: The operation to execute on each peer
    /// - `staggered_delay_ms`: Delay in milliseconds between starting each subsequent peer
    ///
    /// # Returns
    /// The first successful result, or the last error if all fail
    async fn try_staggered_parallel<T, F, Fut>(
        &self,
        key: &RequestKey,
        first_peer: RemoteNode<Env::ValidatorNode>,
        operation: &F,
        staggered_delay: Duration,
    ) -> Result<T, NodeError>
    where
        T: 'static,
        F: Fn(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<T, NodeError>> + 'static,
    {
        use futures::{
            future::{select, Either},
            stream::{FuturesUnordered, StreamExt},
        };
        use linera_base::time::timer::sleep;

        let mut futures: FuturesUnordered<Fut> = FuturesUnordered::new();
        let peer_index = AtomicU32::new(0);

        let push_future = |futures: &mut FuturesUnordered<Fut>, fut: Fut| {
            futures.push(fut);
            peer_index.fetch_add(1, Ordering::SeqCst)
        };

        // Start the first peer immediately (no delay)
        push_future(&mut futures, operation(first_peer));

        let mut last_error = NodeError::UnexpectedMessage;
        let mut next_delay = Box::pin(sleep(staggered_delay * peer_index.load(Ordering::SeqCst)));

        // Phase 1: Race between futures completion and delays (while alternatives might exist)
        loop {
            // Exit condition: no futures running and can't start any more
            if futures.is_empty() {
                if let Some(peer) = self.in_flight_tracker.pop_alternative_peer(key).await {
                    push_future(&mut futures, operation(peer));
                    next_delay =
                        Box::pin(sleep(staggered_delay * peer_index.load(Ordering::SeqCst)));
                } else {
                    // No futures and no alternatives - we're done
                    break;
                }
            }

            let next_result = Box::pin(futures.next());

            match select(next_result, next_delay).await {
                // A request completed
                Either::Left((Some(result), delay_fut)) => {
                    // Keep the delay future for next iteration
                    next_delay = delay_fut;

                    match result {
                        Ok(value) => {
                            tracing::trace!(?key, "staggered parallel request succeeded");
                            return Ok(value);
                        }
                        Err(error) => {
                            tracing::debug!(
                                ?key,
                                %error,
                                "staggered parallel request attempt failed"
                            );
                            last_error = error;

                            // Immediately try next alternative
                            if let Some(peer) =
                                self.in_flight_tracker.pop_alternative_peer(key).await
                            {
                                push_future(&mut futures, operation(peer));
                                next_delay = Box::pin(sleep(
                                    staggered_delay * peer_index.load(Ordering::SeqCst),
                                ));
                            }
                        }
                    }
                }
                // All running futures completed
                Either::Left((None, delay_fut)) => {
                    // Restore the delay future
                    next_delay = delay_fut;
                    // Will check at top of loop if we should try more alternatives
                    continue;
                }
                // Delay elapsed - try to start next peer
                Either::Right((_, _)) => {
                    if let Some(peer) = self.in_flight_tracker.pop_alternative_peer(key).await {
                        push_future(&mut futures, operation(peer));
                        next_delay =
                            Box::pin(sleep(staggered_delay * peer_index.load(Ordering::SeqCst)));
                    } else {
                        // No more alternatives - break out to phase 2
                        break;
                    }
                }
            }
        }

        // Phase 2: No more alternatives, just wait for remaining futures to complete
        while let Some(result) = futures.next().await {
            match result {
                Ok(value) => {
                    tracing::trace!(?key, "staggered parallel request succeeded");
                    return Ok(value);
                }
                Err(error) => {
                    tracing::debug!(
                        ?key,
                        %error,
                        "staggered parallel request attempt failed"
                    );
                    last_error = error;
                }
            }
        }

        // All attempts failed
        tracing::debug!(?key, "all staggered parallel retry attempts failed");
        Err(last_error)
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
        scored_nodes.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

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
            NodeInfo::with_config(node, self.weights, self.alpha, self.max_expected_latency)
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
    use crate::{
        client::requests_scheduler::{MAX_REQUEST_TTL_MS, STAGGERED_DELAY_MS},
        node::NodeError,
    };

    type TestEnvironment = crate::environment::Test;

    /// Helper function to create a test RequestsScheduler with custom configuration
    fn create_test_manager(
        in_flight_timeout: Duration,
        cache_ttl: Duration,
    ) -> Arc<RequestsScheduler<TestEnvironment>> {
        let mut manager = RequestsScheduler::with_config(
            vec![], // No actual nodes needed for these tests
            ScoringWeights::default(),
            0.1,
            1000.0,
            cache_ttl,
            100,
            in_flight_timeout,
            Duration::from_millis(STAGGERED_DELAY_MS),
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

    /// Helper function to create a dummy peer for testing
    fn dummy_peer() -> RemoteNode<<TestEnvironment as Environment>::ValidatorNode> {
        use crate::test_utils::{MemoryStorageBuilder, TestBuilder};

        // Create a minimal test builder to get a validator node
        let mut builder = futures::executor::block_on(async {
            TestBuilder::new(
                MemoryStorageBuilder::default(),
                1,
                0,
                linera_base::crypto::InMemorySigner::new(None),
            )
            .await
            .unwrap()
        });

        let node = builder.node(0);
        let public_key = node.name();
        RemoteNode { public_key, node }
    }

    #[tokio::test]
    async fn test_cache_hit_returns_cached_result() {
        // Create a manager with standard cache TTL
        let manager = create_test_manager(Duration::from_secs(60), Duration::from_secs(60));
        let key = test_key();
        let peer = dummy_peer();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));
        let execution_count_clone = execution_count.clone();

        // First call - should execute the operation and cache the result
        let result1: Result<Vec<ConfirmedBlockCertificate>, NodeError> = manager
            .deduplicated_request(key.clone(), peer.clone(), |_| {
                let count = execution_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    test_result_ok()
                }
            })
            .await;

        assert!(result1.is_ok());
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);

        // Second call - should return cached result without executing the operation
        let execution_count_clone2 = execution_count.clone();
        let result2: Result<Vec<ConfirmedBlockCertificate>, NodeError> = manager
            .deduplicated_request(key.clone(), peer.clone(), |_| {
                let count = execution_count_clone2.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    test_result_ok()
                }
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
        let peer = dummy_peer();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the first operation completes
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));

        // Start first request (will be slow - waits for signal)
        let manager_clone = Arc::clone(&manager);
        let key_clone = key.clone();
        let execution_count_clone = execution_count.clone();
        let rx_clone = Arc::clone(&rx);
        let peer_clone = peer.clone();
        let first_request = tokio::spawn(async move {
            manager_clone
                .deduplicated_request(key_clone, peer_clone, |_| {
                    let count = execution_count_clone.clone();
                    let rx = Arc::clone(&rx_clone);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        // Wait for signal before completing
                        if let Some(receiver) = rx.lock().await.take() {
                            receiver.await.unwrap();
                        }
                        test_result_ok()
                    }
                })
                .await
        });

        // Start second request - should deduplicate and wait for the first
        let execution_count_clone2 = execution_count.clone();
        let second_request = tokio::spawn(async move {
            manager
                .deduplicated_request(key, peer, |_| {
                    let count = execution_count_clone2.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        test_result_ok()
                    }
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
        let peer = dummy_peer();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the operation completes
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));

        // Start first request (will be slow - waits for signal)
        let manager_clone1 = Arc::clone(&manager);
        let key_clone1 = key.clone();
        let execution_count_clone = execution_count.clone();
        let rx_clone = Arc::clone(&rx);
        let peer_clone = peer.clone();
        let first_request = tokio::spawn(async move {
            manager_clone1
                .deduplicated_request(key_clone1, peer_clone, |_| {
                    let count = execution_count_clone.clone();
                    let rx = Arc::clone(&rx_clone);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        if let Some(receiver) = rx.lock().await.take() {
                            receiver.await.unwrap();
                        }
                        test_result_ok()
                    }
                })
                .await
        });

        // Start multiple additional requests - all should deduplicate
        let mut handles = vec![];
        for _ in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let key_clone = key.clone();
            let execution_count_clone = execution_count.clone();
            let peer_clone = peer.clone();
            let handle = tokio::spawn(async move {
                manager_clone
                    .deduplicated_request(key_clone, peer_clone, |_| {
                        let count = execution_count_clone.clone();
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            test_result_ok()
                        }
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
        let peer = dummy_peer();

        // Track how many times the operation is executed
        let execution_count = Arc::new(AtomicUsize::new(0));

        // Create a channel to control when the first operation completes
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));

        // Start first request (will be slow - waits for signal)
        let manager_clone = Arc::clone(&manager);
        let key_clone = key.clone();
        let execution_count_clone = execution_count.clone();
        let rx_clone = Arc::clone(&rx);
        let peer_clone = peer.clone();
        let first_request = tokio::spawn(async move {
            manager_clone
                .deduplicated_request(key_clone, peer_clone, |_| {
                    let count = execution_count_clone.clone();
                    let rx = Arc::clone(&rx_clone);
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        if let Some(receiver) = rx.lock().await.take() {
                            receiver.await.unwrap();
                        }
                        test_result_ok()
                    }
                })
                .await
        });

        // Wait for the timeout to elapse
        tokio::time::sleep(Duration::from_millis(MAX_REQUEST_TTL_MS + 1)).await;

        // Start second request - should NOT deduplicate because first request exceeded timeout
        let execution_count_clone2 = execution_count.clone();
        let second_request = tokio::spawn(async move {
            manager
                .deduplicated_request(key, peer, |_| {
                    let count = execution_count_clone2.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        test_result_ok()
                    }
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

        // Create a RequestsScheduler
        let manager: Arc<RequestsScheduler<TestEnvironment>> =
            Arc::new(RequestsScheduler::with_config(
                nodes.clone(),
                ScoringWeights::default(),
                0.1,
                1000.0,
                Duration::from_secs(60),
                100,
                Duration::from_millis(MAX_REQUEST_TTL_MS),
                Duration::from_millis(STAGGERED_DELAY_MS),
            ));

        let key = RequestKey::Blob(BlobId::new(
            CryptoHash::test_hash("test_blob"),
            BlobType::Data,
        ));

        // Create a channel to control when first request completes
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(tokio::sync::Mutex::new(Some(rx)));

        // Start first request with node 0 (will block until signaled)
        let manager_clone = Arc::clone(&manager);
        let node_clone = nodes[0].clone();
        let key_clone = key.clone();
        let rx_clone = Arc::clone(&rx);
        let first_request = tokio::spawn(async move {
            manager_clone
                .with_peer(key_clone, node_clone, move |_peer| {
                    let rx = Arc::clone(&rx_clone);
                    async move {
                        // Wait for signal
                        if let Some(receiver) = rx.lock().await.take() {
                            receiver.await.unwrap();
                        }
                        Ok(None) // Return Option<Blob>
                    }
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

        // Alternatives are being popped as staggered parallel runs.
        // The first request is blocked waiting for the signal, so staggered parallel has started
        // and may have already popped one or both alternatives. We just verify that at least
        // one alternative was registered (before being popped).
        // This test primarily validates that alternatives can be registered during deduplication.

        // Signal first request to complete
        tx.send(()).unwrap();

        // Wait for all requests to complete
        let _result1 = first_request.await.unwrap();
        for handle in handles {
            handle.await.unwrap().ok();
        }

        // After completion, the in-flight entry should be removed
        tokio::time::sleep(Duration::from_millis(50)).await;
        let alt_peers = manager.get_alternative_peers(&key).await;
        assert!(
            alt_peers.is_none(),
            "Expected in-flight entry to be removed after completion"
        );
    }

    #[tokio::test]
    async fn test_staggered_parallel_retry_on_failure() {
        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::test_utils::{MemoryStorageBuilder, TestBuilder};

        // Create a test environment with four validators
        let mut builder = TestBuilder::new(
            MemoryStorageBuilder::default(),
            4,
            0,
            InMemorySigner::new(None),
        )
        .await
        .unwrap();

        // Get validator nodes
        let nodes: Vec<_> = (0..4)
            .map(|i| {
                let node = builder.node(i);
                let public_key = node.name();
                RemoteNode { public_key, node }
            })
            .collect();

        let staggered_delay = Duration::from_millis(10);

        // Store public keys for comparison
        let node0_key = nodes[0].public_key;
        let node2_key = nodes[2].public_key;

        // Create a RequestsScheduler
        let manager: Arc<RequestsScheduler<TestEnvironment>> =
            Arc::new(RequestsScheduler::with_config(
                nodes.clone(),
                ScoringWeights::default(),
                0.1,
                1000.0,
                Duration::from_secs(60),
                100,
                Duration::from_millis(MAX_REQUEST_TTL_MS),
                staggered_delay,
            ));

        let key = test_key();

        // Track when each peer is called
        let call_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let start_time = Instant::now();

        // Track call count per peer
        let call_count = Arc::new(AtomicU64::new(0));

        let call_times_clone = Arc::clone(&call_times);
        let call_count_clone = Arc::clone(&call_count);

        // Test the staggered parallel retry logic directly
        let operation = |peer: RemoteNode<<TestEnvironment as Environment>::ValidatorNode>| {
            let times = Arc::clone(&call_times_clone);
            let count = Arc::clone(&call_count_clone);
            let start = start_time;
            async move {
                let elapsed = Instant::now().duration_since(start);
                times.lock().await.push((peer.public_key, elapsed));
                count.fetch_add(1, Ordering::SeqCst);

                if peer.public_key == node0_key {
                    // Node 0 fails quickly
                    Err(NodeError::UnexpectedMessage)
                } else if peer.public_key == node2_key {
                    // Node 2 succeeds after a delay
                    tokio::time::sleep(staggered_delay / 2).await;
                    Ok(vec![])
                } else {
                    // Other nodes take longer or fail
                    tokio::time::sleep(staggered_delay * 2).await;
                    Err(NodeError::UnexpectedMessage)
                }
            }
        };

        // Setup: Insert in-flight entry and register alternative peers
        manager.in_flight_tracker.insert_new(key.clone()).await;
        // Register nodes 3, 2, 1 as alternatives (will be popped in reverse: 1, 2, 3)
        for node in nodes.iter().skip(1).rev() {
            manager
                .in_flight_tracker
                .add_alternative_peer(&key, node.clone())
                .await;
        }

        // Use node 0 as first peer, alternatives will be popped: node 1, then 2, then 3
        let result: Result<Vec<ConfirmedBlockCertificate>, NodeError> = manager
            .try_staggered_parallel(&key, nodes[0].clone(), &operation, staggered_delay)
            .await;

        // Should succeed with result from node 2
        assert!(
            result.is_ok(),
            "Expected request to succeed with alternative peer"
        );

        // Verify timing: calls should be staggered, not sequential
        let times = call_times.lock().await;
        // Can't test exactly 2 b/c we sleep _inside_ the operation and increase right at the start of it.
        assert!(
            times.len() >= 2,
            "Should have tried at least 2 peers, got {}",
            times.len()
        );

        // First call should be at ~0ms
        assert!(
            times[0].1.as_millis() < 10,
            "First peer should be called immediately, was called at {}ms",
            times[0].1.as_millis()
        );

        // Second call should start immediately after first fails (aggressive retry)
        // When node 0 fails immediately, we immediately start node 1
        if times.len() > 1 {
            let delay = times[1].1.as_millis();
            assert!(
                delay < 10,
                "Second peer should be called immediately on first failure, got {}ms",
                delay
            );
        }

        // Total time should be significantly less than sequential.
        // With aggressive retry: node0 fails immediately (~0ms), node1 starts immediately,
        // node1 takes 20ms (and might fail or timeout), node2 starts at ~10ms (scheduled delay)
        // and succeeds at ~15ms total (10ms + 5ms internal delay)
        let total_time = Instant::now().duration_since(start_time).as_millis();
        assert!(
            total_time < 50,
            "Total time should be less than 50ms, got {}ms",
            total_time
        );
    }
}
