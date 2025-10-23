// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
