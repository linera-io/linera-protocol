// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use custom_debug_derive::Debug;

use super::scoring::ScoringWeights;
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
    ema_latency_ms: f64,

    /// Exponential Moving Average of success rate (0.0 to 1.0)
    /// Tracks recent success/failure patterns
    ema_success_rate: f64,

    /// Total number of requests processed (for monitoring and cold-start handling)
    total_requests: u64,

    /// Configuration for scoring weights
    weights: ScoringWeights,

    /// EMA smoothing factor (0 < alpha < 1)
    /// Higher values give more weight to recent observations
    alpha: f64,

    /// Maximum expected latency in milliseconds for score normalization
    max_expected_latency_ms: f64,
}

impl<Env: Environment> NodeInfo<Env> {
    /// Creates a new `NodeInfo` with custom configuration.
    pub(super) fn with_config(
        node: RemoteNode<Env::ValidatorNode>,
        weights: ScoringWeights,
        alpha: f64,
        max_expected_latency_ms: f64,
    ) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "Alpha must be in (0, 1) range");
        Self {
            node,
            ema_latency_ms: 100.0, // Start with reasonable latency expectation
            ema_success_rate: 1.0, // Start optimistically with 100% success
            total_requests: 0,
            weights,
            alpha,
            max_expected_latency_ms,
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

        // 4. Apply cold-start penalty for nodes with very few requests
        let confidence_factor = (self.total_requests as f64 / 10.0).min(1.0);

        // 5. Combine with weights
        let raw_score =
            (self.weights.latency * latency_score) + (self.weights.success * success_score);

        // Apply confidence factor to penalize nodes with too few samples
        raw_score * (0.5 + 0.5 * confidence_factor)
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

    /// Returns the current EMA success rate.
    pub(super) fn ema_success_rate(&self) -> f64 {
        self.ema_success_rate
    }

    /// Returns the total number of requests processed.
    pub(super) fn total_requests(&self) -> u64 {
        self.total_requests
    }
}
