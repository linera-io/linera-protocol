// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use custom_debug_derive::Debug;

use super::{scoring::ScoringWeights, MAX_ACCEPTED_LATENCY_MS, MAX_IN_FLIGHT_REQUESTS};
use crate::{environment::Environment, remote_node::RemoteNode};

/// Tracks performance metrics and request capacity for a validator node using
/// Exponential Moving Averages (EMA) for adaptive scoring.
///
/// This struct wraps a `RemoteNode` with performance tracking that adapts quickly
/// to changing network conditions. The scoring system uses EMAs to weight recent
/// performance more heavily than historical data.
#[derive(Debug, Clone)]
pub(super) struct NodeInfo<Env: Environment> {
    /// The underlying validator node connection
    pub(super) node: RemoteNode<Env::ValidatorNode>,

    /// Exponential Moving Average of latency in milliseconds
    /// Adapts quickly to changes in response time
    pub(super) ema_latency_ms: f64,

    /// Exponential Moving Average of success rate (0.0 to 1.0)
    /// Tracks recent success/failure patterns
    pub(super) ema_success_rate: f64,

    /// Thread-safe counter tracking the number of concurrent requests currently in flight
    /// Used to prevent overwhelming individual nodes with too many parallel requests
    pub(super) in_flight_requests: Arc<tokio::sync::RwLock<usize>>,

    /// Notifier for slot releases. Tasks waiting for a slot to become available
    /// will be woken when a request completes and releases its slot.
    pub(super) slot_available: Arc<tokio::sync::Notify>,

    /// Total number of requests processed (for monitoring and cold-start handling)
    pub(super) total_requests: u64,

    /// Configuration for scoring weights
    pub(super) weights: ScoringWeights,

    /// EMA smoothing factor (0 < alpha < 1)
    /// Higher values give more weight to recent observations
    pub(super) alpha: f64,

    /// Maximum expected latency in milliseconds for score normalization
    pub(super) max_expected_latency_ms: f64,

    /// Maximum expected in-flight requests for score normalization
    pub(super) max_in_flight: usize,
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
    pub(super) fn new(node: RemoteNode<Env::ValidatorNode>) -> Self {
        Self::with_config(
            node,
            ScoringWeights::default(),
            0.1,
            MAX_ACCEPTED_LATENCY_MS,
            MAX_IN_FLIGHT_REQUESTS,
        )
    }

    /// Creates a new `NodeInfo` with custom configuration.
    pub(super) fn with_config(
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
    pub(super) async fn calculate_score(&self) -> f64 {
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
    pub(super) async fn can_accept_request(&self, max_requests: usize) -> bool {
        let current = *self.in_flight_requests.read().await;
        current < max_requests
    }

    /// Atomically attempts to reserve a request slot for this node.
    ///
    /// Returns `true` if a slot was successfully acquired, `false` if the node
    /// is already at maximum capacity. This prevents overwhelming individual
    /// nodes with too many concurrent requests.
    pub(super) async fn acquire_request_slot(&self, max_requests: usize) -> bool {
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
    pub(super) async fn release_request_slot(&self) {
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
    pub(super) fn update_metrics(&mut self, success: bool, response_time_ms: u64) {
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
