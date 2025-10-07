// Copyright (c) Facebook, Inc. and its affiliates.
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
use rand::distributions::{Distribution, WeightedIndex};
use tracing::instrument;

use crate::{
    environment::Environment,
    node::{NodeError, ValidatorNode},
    remote_node::RemoteNode,
};

const MAX_IN_FLIGHT_REQUESTS: usize = 1000;
const MAX_ACCEPTED_LATENCY_MS: f64 = 5000.0; // 5 seconds

/// Manages a pool of validator nodes with intelligent load balancing and performance tracking.
///
/// The `ValidatorManager` maintains performance metrics for each validator node using
/// Exponential Moving Averages (EMA) and uses these metrics to make intelligent routing
/// decisions. It prevents node overload through request capacity limits and automatically
/// retries failed requests on alternative nodes.
///
/// # Examples
///
/// ```
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
///     15,              // max 15 concurrent requests per node
///     latency_weights, // custom scoring weights
///     0.2,             // higher alpha for faster adaptation
///     3000.0,          // max expected latency (3 seconds)
///     15.0,            // max expected in-flight requests
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
    default_weights: ScoringWeights,
    /// Default EMA smoothing factor for new nodes.
    default_alpha: f64,
    /// Default maximum expected latency in milliseconds for score normalization.
    default_max_expected_latency_ms: f64,
}

impl<Env: Environment> ValidatorManager<Env> {
    /// Creates a new `ValidatorManager` with default configuration.
    pub fn new(nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>) -> Self {
        Self::with_config(
            nodes,
            MAX_IN_FLIGHT_REQUESTS,
            ScoringWeights::default(),
            0.1,
            MAX_ACCEPTED_LATENCY_MS,
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
    /// - `max_in_flight`: Maximum expected in-flight requests for score normalization
    pub fn with_config(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        max_requests_per_node: usize,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
    ) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "Alpha must be in (0, 1) range");
        Self {
            nodes: Arc::new(tokio::sync::RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| {
                        let public_key = node.public_key;
                        (
                            public_key,
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
            default_weights: weights,
            default_alpha: alpha,
            default_max_expected_latency_ms: max_expected_latency_ms,
        }
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
        let nodes = self.nodes.read().await;

        // Filter nodes that can accept requests and calculate their scores
        let mut scored_nodes = Vec::new();
        for info in nodes.values() {
            if info.can_accept_request(self.max_requests_per_node).await {
                let score = info.calculate_score().await;
                scored_nodes.push((score, info.node.clone()));
            }
        }

        if scored_nodes.is_empty() {
            return None;
        }

        // Sort by score (highest first)
        scored_nodes.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));

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
            Some(scored_nodes[0].1.clone())
        }
    }

    /// Wraps a request operation with performance tracking and capacity management.
    ///
    /// This method:
    /// 1. Selects the best available peer based on performance scores
    /// 2. Acquires a request slot (fails if no peers are available)
    /// 3. Executes the provided operation with the selected peer
    /// 4. Measures response time
    /// 5. Updates node metrics based on success/failure
    /// 6. Releases the request slot
    ///
    /// # Arguments
    /// - `operation`: Async closure that performs the actual request with the selected peer
    ///
    /// # Errors
    /// Returns `NodeError::WorkerError` if no peers are available or all are at capacity.
    /// Otherwise, propagates any error from the operation.
    async fn track_request<T, F, Fut>(&self, operation: F) -> Result<T, NodeError>
    where
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<T, NodeError>>,
    {
        // Select the best available peer
        let peer = self
            .select_best_peer()
            .await
            .ok_or_else(|| NodeError::WorkerError {
                error: "No validators available".to_string(),
            })?;

        let start_time = Instant::now();
        let public_key = peer.public_key;

        // Acquire request slot
        {
            let nodes = self.nodes.read().await;
            if let Some(info) = nodes.get(&public_key) {
                if !info.acquire_request_slot(self.max_requests_per_node).await {
                    return Err(NodeError::WorkerError {
                        error: "Node is at maximum request capacity".to_string(),
                    });
                }
            }
        }

        // Execute the operation
        let result = operation(peer).await;

        // Update metrics and release slot
        let response_time_ms = start_time.elapsed().as_millis() as u64;
        {
            let mut nodes = self.nodes.write().await;
            if let Some(info) = nodes.get_mut(&public_key) {
                info.update_metrics(result.is_ok(), response_time_ms);
                info.release_request_slot().await;
                let score = info.calculate_score().await;
                tracing::trace!(
                    node = %public_key,
                    success = %result.is_ok(),
                    response_time_ms = %response_time_ms,
                    score = %score,
                    total_requests = %info.total_requests,
                    "Request completed"
                );
            }
        }

        result
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn download_certificates(
        &self,
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        self.track_request(|peer| async move {
            peer.download_certificates_from(chain_id, start, limit)
                .await
        })
        .await
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn try_download_blob(&self, blob_id: BlobId) -> Option<Blob> {
        self.track_request(|peer| async move { Ok(peer.try_download_blob(blob_id).await) })
            .await
            .unwrap_or(None)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn download_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        self.track_request(|peer| async move {
            peer.download_certificates_by_heights(chain_id, heights)
                .await
        })
        .await
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        self.track_request(|peer| async move {
            peer.node.download_pending_blob(chain_id, blob_id).await
        })
        .await
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn download_certificate_for_blob(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        self.track_request(|peer| async move { peer.download_certificate_for_blob(blob_id).await })
            .await
    }

    /// Downloads the blobs with the given IDs. This is done in one concurrent task per blob.
    /// Uses intelligent peer selection based on scores and load balancing.
    /// Returns `None` if it couldn't find all blobs.
    #[instrument(level = "trace", skip_all)]
    pub async fn download_blobs(
        &self,
        blob_ids: &[BlobId],
        _timeout: Duration,
    ) -> Option<Vec<Blob>> {
        let mut stream = blob_ids
            .iter()
            .map(|blob_id| self.try_download_blob(*blob_id))
            .collect::<FuturesUnordered<_>>();

        let mut blobs = Vec::new();
        while let Some(maybe_blob) = stream.next().await {
            blobs.push(maybe_blob?);
        }
        Some(blobs)
    }

    pub async fn add_peer(&self, node: RemoteNode<Env::ValidatorNode>) {
        let mut nodes = self.nodes.write().await;
        let public_key = node.public_key;
        nodes.entry(public_key).or_insert_with(|| {
            NodeInfo::with_config(
                node,
                self.default_weights,
                self.default_alpha,
                self.default_max_expected_latency_ms,
                self.max_requests_per_node,
            )
        });
    }

    pub async fn add_peers(
        &self,
        new_nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
    ) {
        let mut nodes = self.nodes.write().await;
        for node in new_nodes {
            let public_key = node.public_key;
            nodes.entry(public_key).or_insert_with(|| {
                NodeInfo::with_config(
                    node,
                    self.default_weights,
                    self.default_alpha,
                    self.default_max_expected_latency_ms,
                    self.max_requests_per_node,
                )
            });
        }
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
            result.insert(*key, (score, info.ema_success_rate, info.total_requests));
        }

        result
    }
}

/// Configurable weights for the scoring algorithm.
///
/// These weights determine the relative importance of different metrics
/// when calculating a node's performance score. All weights should sum to 1.0.
///
/// # Examples
///
/// ```
/// // Prioritize response time and success rate equally
/// let balanced_weights = ScoringWeights {
///     latency: 0.4,
///     success: 0.4,
///     load: 0.2,
/// };
///
/// // Prioritize low latency above all else
/// let latency_focused = ScoringWeights {
///     latency: 0.7,
///     success: 0.2,
///     load: 0.1,
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ScoringWeights {
    /// Weight for latency metric (lower latency = higher score)
    pub latency: f64,
    /// Weight for success rate metric (higher success = higher score)
    pub success: f64,
    /// Weight for load metric (lower load = higher score)
    pub load: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            latency: 0.4, // 40% weight on response time
            success: 0.4, // 40% weight on success rate
            load: 0.2,    // 20% weight on current load
        }
    }
}

/// Tracks performance metrics and request capacity for a validator node using
/// Exponential Moving Averages (EMA) for adaptive scoring.
///
/// This struct wraps a `RemoteNode` with performance tracking that adapts quickly
/// to changing network conditions. The scoring system uses EMAs to weight recent
/// performance more heavily than historical data.
#[derive(Debug, Clone)]
struct NodeInfo<Env: Environment> {
    /// The underlying validator node connection
    node: RemoteNode<Env::ValidatorNode>,

    /// Exponential Moving Average of latency in milliseconds
    /// Adapts quickly to changes in response time
    ema_latency_ms: f64,

    /// Exponential Moving Average of success rate (0.0 to 1.0)
    /// Tracks recent success/failure patterns
    ema_success_rate: f64,

    /// Thread-safe counter tracking the number of concurrent requests currently in flight
    /// Used to prevent overwhelming individual nodes with too many parallel requests
    in_flight_requests: Arc<tokio::sync::RwLock<usize>>,

    /// Total number of requests processed (for monitoring and cold-start handling)
    total_requests: u64,

    /// Configuration for scoring weights
    weights: ScoringWeights,

    /// EMA smoothing factor (0 < alpha < 1)
    /// Higher values give more weight to recent observations
    alpha: f64,

    /// Maximum expected latency in milliseconds for score normalization
    max_expected_latency_ms: f64,

    /// Maximum expected in-flight requests for score normalization
    max_in_flight: usize,
}

impl<Env: Environment> NodeInfo<Env> {
    /// Creates a new `NodeInfo` with optimistic initial values.
    ///
    /// New nodes start with:
    /// - 100ms expected latency (reasonable default)
    /// - 100% success rate (optimistic start)
    /// - Zero in-flight requests
    /// - Default weights and smoothing factor
    /// - Default normalization bounds
    #[allow(unused)]
    fn new(node: RemoteNode<Env::ValidatorNode>) -> Self {
        Self::with_config(
            node,
            ScoringWeights::default(),
            0.1,
            MAX_ACCEPTED_LATENCY_MS,
            MAX_IN_FLIGHT_REQUESTS,
        )
    }

    /// Creates a new `NodeInfo` with custom configuration.
    fn with_config(
        node: RemoteNode<Env::ValidatorNode>,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
        max_in_flight: usize,
    ) -> Self {
        Self {
            node,
            ema_latency_ms: 100.0, // Start with reasonable latency expectation
            ema_success_rate: 1.0, // Start optimistically with 100% success
            in_flight_requests: Arc::new(tokio::sync::RwLock::new(0)),
            total_requests: 0,
            weights,
            alpha: alpha.clamp(0.01, 0.5), // Ensure alpha is in reasonable range
            max_expected_latency_ms,
            max_in_flight,
        }
    }

    /// Calculates a normalized performance score (0.0 to 1.0) using weighted metrics.
    ///
    /// The score combines three normalized components:
    /// - **Latency score**: Inversely proportional to EMA latency
    /// - **Success score**: Directly proportional to EMA success rate
    /// - **Load score**: Inversely proportional to current load
    ///
    /// Returns a score from 0.0 to 1.0, where higher values indicate better performance.
    async fn calculate_score(&self) -> f64 {
        // 1. Normalize Latency (lower is better, so we invert)
        let latency_score = 1.0
            - (self.ema_latency_ms.min(self.max_expected_latency_ms)
                / self.max_expected_latency_ms);

        // 2. Success Rate is already normalized [0, 1]
        let success_score = self.ema_success_rate;

        // 3. Normalize Load (lower is better, so we invert)
        let current_load = *self.in_flight_requests.read().await as f64;
        let load_score =
            1.0 - (current_load.min(self.max_in_flight as f64) / self.max_in_flight as f64);

        // 4. Apply cold-start penalty for nodes with very few requests
        let confidence_factor = (self.total_requests as f64 / 10.0).min(1.0);

        // 5. Combine with weights
        let raw_score = (self.weights.latency * latency_score)
            + (self.weights.success * success_score)
            + (self.weights.load * load_score);

        // Apply confidence factor to penalize nodes with too few samples
        raw_score * (0.5 + 0.5 * confidence_factor)
    }

    /// Checks if the node can accept another request without exceeding capacity.
    ///
    /// This is a read-only check that doesn't reserve a slot.
    /// Use `acquire_request_slot` to atomically check and reserve.
    async fn can_accept_request(&self, max_requests: usize) -> bool {
        let current = *self.in_flight_requests.read().await;
        current < max_requests
    }

    /// Atomically attempts to reserve a request slot for this node.
    ///
    /// Returns `true` if a slot was successfully acquired, `false` if the node
    /// is already at maximum capacity. This prevents overwhelming individual
    /// nodes with too many concurrent requests.
    async fn acquire_request_slot(&self, max_requests: usize) -> bool {
        let mut current = self.in_flight_requests.write().await;
        if *current < max_requests {
            *current += 1;
            true
        } else {
            false
        }
    }

    /// Releases a previously acquired request slot.
    ///
    /// Should be called when a request completes (successfully or not) to free
    /// capacity for new requests. Uses saturating subtraction to prevent underflow.
    async fn release_request_slot(&self) {
        let mut current = self.in_flight_requests.write().await;
        *current = current.saturating_sub(1);
    }

    /// Updates performance metrics using Exponential Moving Average.
    ///
    /// # Arguments
    /// - `success`: Whether the request completed successfully
    /// - `response_time_ms`: The request's response time in milliseconds
    ///
    /// Uses EMA formula: new_value = (alpha * observation) + ((1 - alpha) * old_value)
    /// This gives more weight to recent observations while maintaining some history.
    fn update_metrics(&mut self, success: bool, response_time_ms: u64) {
        let response_time_f64 = response_time_ms as f64;

        // Update latency EMA
        self.ema_latency_ms =
            (self.alpha * response_time_f64) + ((1.0 - self.alpha) * self.ema_latency_ms);

        // Update success rate EMA
        let success_value = if success { 1.0 } else { 0.0 };
        self.ema_success_rate =
            (self.alpha * success_value) + ((1.0 - self.alpha) * self.ema_success_rate);

        self.total_requests += 1;
    }
}
