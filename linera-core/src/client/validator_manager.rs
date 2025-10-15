// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::Arc,
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
use tokio::sync::broadcast;
use tracing::instrument;

use crate::{
    client::communicate_concurrently,
    environment::Environment,
    node::{NodeError, ValidatorNode},
    remote_node::RemoteNode,
};

const MAX_IN_FLIGHT_REQUESTS: usize = 100;
const MAX_ACCEPTED_LATENCY_MS: f64 = 5000.0;
const CACHE_TTL_SEC: u64 = 2;
const CACHE_MAX_SIZE: usize = 1000;

/// Unique identifier for different types of download requests.
///
/// Used for request deduplication to avoid redundant downloads of the same data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RequestKey {
    /// Download certificates from a starting height
    Certificates {
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    },
    /// Download certificates by specific heights
    CertificatesByHeights {
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    },
    /// Download a blob by ID
    Blob(BlobId),
    /// Download a pending blob
    PendingBlob { chain_id: ChainId, blob_id: BlobId },
    /// Download certificate for a specific blob
    CertificateForBlob(BlobId),
}

impl RequestKey {
    /// Returns the chain ID associated with the request, if applicable.
    fn chain_id(&self) -> Option<ChainId> {
        match self {
            RequestKey::Certificates { chain_id, .. } => Some(*chain_id),
            RequestKey::CertificatesByHeights { chain_id, .. } => Some(*chain_id),
            RequestKey::PendingBlob { chain_id, .. } => Some(*chain_id),
            _ => None,
        }
    }

    /// Converts certificate-related requests to a common representation of (chain_id, sorted heights).
    ///
    /// This helper method normalizes both `Certificates` and `CertificatesByHeights` variants
    /// into a uniform format for easier comparison and overlap detection.
    ///
    /// # Returns
    /// - `Some((chain_id, heights))` for certificate requests, where heights are sorted
    /// - `None` for non-certificate requests (Blob, PendingBlob, CertificateForBlob)
    fn height_range(&self) -> Option<Vec<BlockHeight>> {
        match self {
            RequestKey::Certificates { start, limit, .. } => {
                let heights: Vec<BlockHeight> = (0..*limit)
                    .map(|offset| BlockHeight(start.0 + offset))
                    .collect();
                Some(heights)
            }
            RequestKey::CertificatesByHeights { heights, .. } => Some(heights.clone()),
            _ => None,
        }
    }

    /// Checks if this request fully subsumes another request.
    ///
    /// Request A subsumes request B if A's result would contain all the data that
    /// B's result would contain. This means B's request is redundant if A is already
    /// in-flight or cached.
    ///
    /// # Examples
    /// ```ignore
    /// let large = RequestKey::Certificates { chain_id, start: 10, limit: 10 }; // [10..20]
    /// let small = RequestKey::CertificatesByHeights { chain_id, heights: vec![12,13,14] };
    /// assert!(large.subsumes(&small)); // large contains all of small's heights
    /// ```
    pub fn subsumes(&self, other: &RequestKey) -> bool {
        // Different chains can't subsume each other
        if self.chain_id() != other.chain_id() {
            return false;
        }

        let heights1 = match self.height_range() {
            Some(range) => range,
            None => return self == other, // Non-certificate requests must match exactly
        };
        let heights2 = match other.height_range() {
            Some(range) => range,
            None => return false, // Can't subsume different variant types
        };

        // Check if all heights in other are contained in self
        heights2.iter().all(|h| heights1.contains(h))
    }

    /// Attempts to extract a subset result for this request from a larger request's result.
    ///
    /// This is used when a request A subsumes this request B. We can extract B's result
    /// from A's result by filtering the certificates to only those requested by B.
    ///
    /// # Arguments
    /// - `from`: The key of the larger request that subsumes this one
    /// - `result`: The result from the larger request
    ///
    /// # Returns
    /// - `Some(RequestResult)` with the extracted subset if possible
    /// - `None` if extraction is not possible (wrong variant, different chain, etc.)
    pub fn can_extract_result(
        &self,
        from: &RequestKey,
        result: &RequestResult,
    ) -> Option<RequestResult> {
        // Only certificate results can be extracted
        let certificates = match result {
            RequestResult::Certificates(Ok(certs)) => certs,
            _ => return None,
        };

        if self.chain_id().is_none() || from.chain_id().is_none() {
            return None;
        }

        let heights_self = self.height_range()?;

        // Filter certificates to only those at the requested heights
        let filtered: Vec<_> = certificates
            .iter()
            .filter(|cert| heights_self.contains(&cert.value().height()))
            .cloned()
            .collect();

        Some(RequestResult::Certificates(Ok(filtered)))
    }
}

/// Result types that can be shared across deduplicated requests
#[derive(Debug, Clone)]
pub enum RequestResult {
    Certificates(Result<Vec<ConfirmedBlockCertificate>, NodeError>),
    Blob(Result<Option<Blob>, NodeError>),
    BlobContent(Result<BlobContent, NodeError>),
    Certificate(Box<Result<ConfirmedBlockCertificate, NodeError>>),
}

impl RequestResult {
    /// Returns true if the result represents a successful operation
    fn is_ok(&self) -> bool {
        match self {
            RequestResult::Certificates(result) => result.is_ok(),
            RequestResult::Blob(result) => result.is_ok(),
            RequestResult::BlobContent(result) => result.is_ok(),
            RequestResult::Certificate(result) => result.is_ok(),
        }
    }
}

impl From<RequestResult> for Result<Vec<ConfirmedBlockCertificate>, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificates(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<Vec<ConfirmedBlockCertificate>, NodeError>> for RequestResult {
    fn from(result: Result<Vec<ConfirmedBlockCertificate>, NodeError>) -> Self {
        RequestResult::Certificates(result)
    }
}

impl From<RequestResult> for Result<Option<Blob>, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Blob(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<Option<Blob>, NodeError>> for RequestResult {
    fn from(result: Result<Option<Blob>, NodeError>) -> Self {
        RequestResult::Blob(result)
    }
}

impl From<RequestResult> for Result<BlobContent, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::BlobContent(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<BlobContent, NodeError>> for RequestResult {
    fn from(result: Result<BlobContent, NodeError>) -> Self {
        RequestResult::BlobContent(result)
    }
}

impl From<RequestResult> for Result<ConfirmedBlockCertificate, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificate(r) => *r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<ConfirmedBlockCertificate, NodeError>> for RequestResult {
    fn from(result: Result<ConfirmedBlockCertificate, NodeError>) -> Self {
        RequestResult::Certificate(Box::new(result))
    }
}

/// Cached result entry with timestamp for TTL expiration
#[derive(Debug, Clone)]
struct CacheEntry {
    result: Arc<RequestResult>,
    cached_at: Instant,
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
    default_weights: ScoringWeights,
    /// Default EMA smoothing factor for new nodes.
    default_alpha: f64,
    /// Default maximum expected latency in milliseconds for score normalization.
    default_max_expected_latency_ms: f64,
    /// Tracks in-flight requests to deduplicate concurrent requests for the same data.
    /// Maps request keys to broadcast senders that notify all waiters when the request completes.
    in_flight_requests:
        Arc<tokio::sync::RwLock<HashMap<RequestKey, broadcast::Sender<Arc<RequestResult>>>>>,
    /// Cache of recently completed requests with their results and timestamps.
    /// Used to avoid re-executing requests for the same data within the TTL window.
    cache: Arc<tokio::sync::RwLock<HashMap<RequestKey, CacheEntry>>>,
    /// Time-to-live for cached entries. Entries older than this duration are considered expired.
    cache_ttl: Duration,
    /// Maximum number of entries to store in the cache. When exceeded, oldest entries are evicted (LRU).
    max_cache_size: usize,
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
            Duration::from_secs(CACHE_TTL_SEC), // 60 second cache TTL
            CACHE_MAX_SIZE,                     // cache up to 100 entries
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
    pub fn with_config(
        nodes: impl IntoIterator<Item = RemoteNode<Env::ValidatorNode>>,
        max_requests_per_node: usize,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
        cache_ttl: Duration,
        max_cache_size: usize,
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
            in_flight_requests: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            cache_ttl,
            max_cache_size,
        }
    }

    /// Executes an operation with an automatically selected peer, handling deduplication,
    /// tracking, and peer selection.
    ///
    /// This method provides a high-level API for executing operations against remote nodes
    /// while leveraging the ValidatorManager's intelligent peer selection, performance tracking,
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
    pub async fn with_best<R, F, Fut>(&self, key: RequestKey, operation: F) -> Result<R, NodeError>
    where
        Result<R, NodeError>: From<RequestResult> + Into<RequestResult> + Clone + Send + 'static,
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>>,
    {
        self.deduplicated_request(key, || async {
            // Select the best available peer
            let peer = self
                .select_best_peer()
                .await
                .ok_or_else(|| NodeError::WorkerError {
                    error: "No validators available".to_string(),
                })?;
            self.track_request(peer, operation).await
        })
        .await
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
    pub async fn with_peer<R, F, Fut>(
        &self,
        key: RequestKey,
        peer: RemoteNode<Env::ValidatorNode>,
        operation: F,
    ) -> Result<R, NodeError>
    where
        Result<R, NodeError>: From<RequestResult> + Into<RequestResult> + Clone + Send + 'static,
        F: FnOnce(RemoteNode<Env::ValidatorNode>) -> Fut,
        Fut: Future<Output = Result<R, NodeError>>,
    {
        self.add_peer(peer.clone()).await;
        self.deduplicated_request(key, || async { self.track_request(peer, operation).await })
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
        self.try_acquire_slot(&public_key).await;

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
                    address = %info.node.node.address(),
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

    /// Deduplicates concurrent requests for the same data.
    ///
    /// If a request for the same key is already in-flight, this method waits for
    /// the existing request to complete and returns its result. Otherwise, it
    /// executes the operation and broadcasts the result to all waiting callers.
    ///
    /// This method also performs **subsumption-based deduplication**: if a larger
    /// request that contains all the data needed by this request is already cached
    /// or in-flight, we can extract the subset result instead of making a new request.
    ///
    /// # Arguments
    /// - `key`: Unique identifier for the request
    /// - `operation`: Async closure that performs the actual request
    ///
    /// # Returns
    /// The result from either the in-flight request or the newly executed operation
    async fn deduplicated_request<T, F, Fut>(&self, key: RequestKey, operation: F) -> T
    where
        T: From<RequestResult> + Into<RequestResult> + Clone + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        // Check cache for exact match first
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&key) {
                tracing::trace!(
                    key = ?key,
                    "cache hit (exact match) - returning cached result"
                );
                return T::from((*entry.result).clone());
            }

            // Check cache for subsuming requests
            for (cached_key, entry) in cache.iter() {
                if cached_key.subsumes(&key) {
                    if let Some(extracted) = key.can_extract_result(cached_key, &entry.result) {
                        tracing::trace!(
                            key = ?key,
                            subsumed_by = ?cached_key,
                            "cache hit (subsumption) - extracted result from larger cached request"
                        );
                        return T::from(extracted);
                    }
                }
            }
        }

        // Check if exact request is already in-flight
        let mut in_flight = self.in_flight_requests.write().await;

        if let Some(sender) = in_flight.get(&key) {
            tracing::trace!(
                key = ?key,
                "deduplicating request (exact match) - joining existing in-flight request"
            );
            let mut receiver = sender.subscribe();
            drop(in_flight);
            // Wait for result from existing request
            match receiver.recv().await {
                Ok(result) => return T::from((*result).clone()),
                Err(_) => {
                    tracing::trace!(
                        key = ?key,
                        "in-flight request sender dropped"
                    );
                }
            }
        } else {
            // Check for subsuming in-flight requests
            for (in_flight_key, sender) in in_flight.iter() {
                if in_flight_key.subsumes(&key) {
                    let subsuming_key = in_flight_key.clone(); // Clone the key before dropping lock
                    tracing::trace!(
                        key = ?key,
                        subsumed_by = ?subsuming_key,
                        "deduplicating request (subsumption) - joining larger in-flight request"
                    );
                    let mut receiver = sender.subscribe();
                    drop(in_flight);
                    // Wait for result from the subsuming request
                    match receiver.recv().await {
                        Ok(result) => {
                            if let Some(extracted) = key.can_extract_result(&subsuming_key, &result)
                            {
                                tracing::trace!(
                                    key = ?key,
                                    "extracted subset result from larger in-flight request"
                                );
                                return T::from(extracted);
                            } else {
                                // Extraction failed, fall through to execute our own request
                                tracing::trace!(
                                    key = ?key,
                                    "failed to extract from subsuming request, will execute independently"
                                );
                            }
                        }
                        Err(_) => {
                            tracing::trace!(
                                key = ?key,
                                "subsuming in-flight request sender dropped"
                            );
                        }
                    }
                    // Re-acquire lock since we dropped it
                    in_flight = self.in_flight_requests.write().await;
                    break;
                }
            }

            // Create new broadcast channel for this request
            let (sender, _receiver) = broadcast::channel(1);
            in_flight.insert(key.clone(), sender);
            drop(in_flight);
        }

        // Execute the actual request
        tracing::trace!(key = ?key, "executing new request");
        let result = operation().await;
        let result_for_broadcast: RequestResult = result.clone().into();
        let shared_result = Arc::new(result_for_broadcast);

        // Broadcast result and clean up
        {
            let mut in_flight = self.in_flight_requests.write().await;
            if let Some(sender) = in_flight.remove(&key) {
                tracing::info!(
                    key = ?key,
                    waiters = sender.receiver_count(),
                    "request completed; broadcasting result to waiters",
                );
                if sender.receiver_count() != 0 {
                    let _ = sender.send(shared_result.clone()).unwrap();
                }
            }
        }

        // Store in cache only if the result is successful
        if shared_result.is_ok() {
            self.store_in_cache(key.clone(), shared_result).await;
        }
        result
    }

    /// Stores a result in the cache with LRU eviction if cache is full.
    ///
    /// If the cache is at capacity, this method removes the oldest entry before
    /// inserting the new one. Entries are considered "oldest" based on their cached_at timestamp.
    async fn store_in_cache(&self, key: RequestKey, result: Arc<RequestResult>) {
        self.evict_expired_cache_entries().await; // Clean up expired entries first
        let mut cache = self.cache.write().await;
        // Insert new entry
        cache.insert(
            key.clone(),
            CacheEntry {
                result,
                cached_at: Instant::now(),
            },
        );
        tracing::trace!(
            key = ?key,
            "stored result in cache"
        );
    }

    /// Removes all cache entries that are older than the configured cache TTL.
    ///
    /// This method scans the cache and removes entries where the time elapsed since
    /// `cached_at` exceeds `cache_ttl`. It's useful for explicitly cleaning up stale
    /// cache entries rather than relying on lazy expiration checks.
    async fn evict_expired_cache_entries(&self) -> usize {
        let mut cache = self.cache.write().await;
        let now = Instant::now();
        if cache.len() <= self.max_cache_size {
            return 0; // No need to evict if under max size
        }

        let expired_keys: Vec<RequestKey> = cache
            .iter()
            .filter_map(|(key, entry)| {
                if now.duration_since(entry.cached_at) > self.cache_ttl {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in &expired_keys {
            cache.remove(key);
        }

        if !expired_keys.is_empty() {
            tracing::trace!(count = expired_keys.len(), "evicted expired cache entries");
        }
        expired_keys.len()
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
            if info.can_accept_request(self.max_requests_per_node).await {
                let score = info.calculate_score().await;
                scored_nodes.push((score, info.node.clone()));
            }
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
            Some(scored_nodes[0].1.clone())
        }
    }

    /// Attempts to acquire a request slot for a specific peer, blocking until one is available.
    ///
    /// This method uses async notifications to efficiently wait for slot availability without
    /// polling. When a slot is not immediately available, the task will be suspended and woken
    /// by the async runtime when another task releases a slot.
    ///
    /// # Arguments
    /// - `peer_key`: The public key of the validator peer
    ///
    /// # Behavior
    /// The method will:
    /// - Subscribe to slot release notifications first (critical for avoiding missed notifications)
    /// - Try to acquire a slot
    /// - If unsuccessful, wait to be notified when a slot becomes available
    /// - Retry acquisition when notified
    /// - Continue until a slot is successfully acquired
    async fn try_acquire_slot(&self, peer_key: &ValidatorPublicKey) {
        // CRITICAL: Get the notifier and subscribe BEFORE checking availability.
        // This ensures we don't miss any notifications that happen between
        // checking and waiting. We clone the Arc to the Notify so we can
        // hold it beyond the lock scope.
        let notify = {
            let nodes = self.nodes.read().await;
            nodes.get(peer_key).map(|info| info.slot_available.clone())
        };

        // If peer doesn't exist, return immediately
        let notify = match notify {
            Some(n) => n,
            None => return,
        };

        loop {
            // Subscribe to notifications before trying to acquire
            let notified = notify.notified();

            // Try to acquire a slot
            let nodes = self.nodes.read().await;
            let slot_available = if let Some(info) = nodes.get(peer_key) {
                info.acquire_request_slot(self.max_requests_per_node).await
            } else {
                false
            };
            drop(nodes);

            // If we acquired a slot, return immediately
            if slot_available {
                return;
            }
            // Wait to be notified when a slot becomes available
            notified.await;
            // Loop and retry acquisition
        }
    }

    /// Adds a new peer to the manager if it doesn't already exist.
    async fn add_peer(&self, node: RemoteNode<Env::ValidatorNode>) {
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
}

/// Configurable weights for the scoring algorithm.
///
/// These weights determine the relative importance of different metrics
/// when calculating a node's performance score. All weights should sum to 1.0.
///
/// # Examples
///
/// ```ignore
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

    /// Notifier for slot releases. Tasks waiting for a slot to become available
    /// will be woken when a request completes and releases its slot.
    slot_available: Arc<tokio::sync::Notify>,

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
            slot_available: Arc::new(tokio::sync::Notify::new()),
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
    /// Notifies one waiting task that a slot is now available.
    async fn release_request_slot(&self) {
        let mut current = self.in_flight_requests.write().await;
        *current = current.saturating_sub(1);
        drop(current); // Release the lock before notifying
        self.slot_available.notify_one();
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

#[cfg(test)]
mod tests {
    use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

    use super::RequestKey;

    #[test]
    fn test_subsumes_complete_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let large = RequestKey::Certificates {
            chain_id,
            start: BlockHeight(10),
            limit: 10,
        }; // [10..20]
        let small = RequestKey::CertificatesByHeights {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(13), BlockHeight(14)],
        };
        assert!(large.subsumes(&small));
        assert!(!small.subsumes(&large));
    }

    #[test]
    fn test_subsumes_partial_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            start: BlockHeight(10),
            limit: 5,
        }; // [10,11,12,13,14]
        let req2 = RequestKey::CertificatesByHeights {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(15)], // 15 is outside range
        };
        assert!(!req1.subsumes(&req2)); // Can't subsume, 15 is not in req1
    }

    #[test]
    fn test_subsumes_different_chains() {
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        let req1 = RequestKey::Certificates {
            chain_id: chain1,
            start: BlockHeight(10),
            limit: 10,
        };
        let req2 = RequestKey::CertificatesByHeights {
            chain_id: chain2,
            heights: vec![BlockHeight(12)],
        };
        assert!(!req1.subsumes(&req2));
    }
}
