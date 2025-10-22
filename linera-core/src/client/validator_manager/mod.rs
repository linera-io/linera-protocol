// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages communication with validator nodes, including connection pooling,
//! load balancing, request deduplication, caching, and performance tracking.

mod cache;
mod in_flight_tracker;
mod manager;
mod node_info;
mod request;
mod scoring;

pub use manager::ValidatorManager;
pub use scoring::ScoringWeights;

// Module constants - default values for ValidatorManagerConfig
pub const MAX_IN_FLIGHT_REQUESTS: usize = 100;
pub const MAX_ACCEPTED_LATENCY_MS: f64 = 5000.0;
pub const CACHE_TTL_SEC: u64 = 2;
pub const CACHE_MAX_SIZE: usize = 1000;
pub const MAX_REQUEST_TTL_MS: u64 = 200;
pub const ALPHA_SMOOTHING_FACTOR: f64 = 0.1;

/// Configuration for the `ValidatorManager`.
#[derive(Debug, Clone)]
pub struct ValidatorManagerConfig {
    /// Maximum concurrent requests per validator node
    pub max_in_flight_requests: usize,
    /// Maximum expected latency in milliseconds for score normalization
    pub max_accepted_latency_ms: f64,
    /// Time-to-live for cached responses in seconds
    pub cache_ttl_sec: u64,
    /// Maximum number of entries in the cache
    pub cache_max_size: usize,
    /// Maximum latency for an in-flight request before we stop deduplicating it (in milliseconds)
    pub max_request_ttl_ms: u64,
    /// Smoothing factor for Exponential Moving Averages (0 < alpha < 1)
    pub alpha: f64,
}

impl Default for ValidatorManagerConfig {
    fn default() -> Self {
        Self {
            max_in_flight_requests: MAX_IN_FLIGHT_REQUESTS,
            max_accepted_latency_ms: MAX_ACCEPTED_LATENCY_MS,
            cache_ttl_sec: CACHE_TTL_SEC,
            cache_max_size: CACHE_MAX_SIZE,
            max_request_ttl_ms: MAX_REQUEST_TTL_MS,
            alpha: ALPHA_SMOOTHING_FACTOR,
        }
    }
}
