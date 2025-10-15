// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages communication with validator nodes, including connection pooling,
//! load balancing, request deduplication, caching, and performance tracking.

mod manager;
mod node_info;
mod request;
mod scoring;

pub use manager::ValidatorManager;
pub use request::RequestKey;
pub use scoring::ScoringWeights;

// Module constants
pub(super) const MAX_IN_FLIGHT_REQUESTS: usize = 100;
pub(super) const MAX_ACCEPTED_LATENCY_MS: f64 = 5000.0;
pub(super) const CACHE_TTL_SEC: u64 = 2;
pub(super) const CACHE_MAX_SIZE: usize = 1000;
