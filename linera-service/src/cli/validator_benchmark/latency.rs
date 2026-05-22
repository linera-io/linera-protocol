// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! In-memory latency sample accumulator and percentile summary.
//!
//! Sample volumes per bucket are small (a few hundred to a few tens of
//! thousands), so raw samples are kept in a `Vec` and percentiles are computed
//! by sorting on demand. No HDR histogram dependency is needed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Accumulates successful-request durations and categorized errors for a single
/// measurement bucket (one layer / chain / concurrency level).
#[derive(Debug, Default, Clone)]
pub struct Samples {
    durations_ms: Vec<f64>,
    errors: BTreeMap<String, u64>,
}

impl Samples {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful request latency in milliseconds.
    pub fn record_success(&mut self, ms: f64) {
        self.durations_ms.push(ms);
    }

    /// Record a failed request under a category label (e.g. `timeout`).
    pub fn record_error(&mut self, category: impl Into<String>) {
        *self.errors.entry(category.into()).or_default() += 1;
    }

    /// Fold another bucket's samples and errors into this one.
    ///
    /// Used by the stress runner to merge per-task `Samples` without a shared
    /// mutex (which would distort the latency measurement under contention).
    pub fn merge_from(&mut self, other: Samples) {
        let Samples {
            mut durations_ms,
            errors,
        } = other;
        self.durations_ms.append(&mut durations_ms);
        for (category, count) in errors {
            *self.errors.entry(category).or_default() += count;
        }
    }

    /// Compute the distribution summary over the recorded samples.
    pub fn summary(&self) -> LatencySummary {
        let mut sorted = self.durations_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = sorted.len();

        // Nearest-rank percentile (1-based rank), clamped to the sample range.
        let percentile = |q: f64| -> f64 {
            if sorted.is_empty() {
                return 0.0;
            }
            let rank = (q * count as f64).ceil() as usize;
            let idx = rank.saturating_sub(1).min(count - 1);
            sorted[idx]
        };

        let mean = if count == 0 {
            0.0
        } else {
            sorted.iter().sum::<f64>() / count as f64
        };
        let stddev = if count < 2 {
            0.0
        } else {
            let variance = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / count as f64;
            variance.sqrt()
        };

        LatencySummary {
            count: count as u64,
            min: sorted.first().copied().unwrap_or(0.0),
            max: sorted.last().copied().unwrap_or(0.0),
            mean,
            stddev,
            p50: percentile(0.50),
            p95: percentile(0.95),
            p99: percentile(0.99),
            errors: self
                .errors
                .iter()
                .map(|(category, count)| ErrorBucket {
                    category: category.clone(),
                    count: *count,
                })
                .collect(),
        }
    }
}

/// Distribution summary for a measurement bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub errors: Vec<ErrorBucket>,
}

/// A count of failures sharing a category label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBucket {
    pub category: String,
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn summary_of_known_sample_set() {
        let mut s = Samples::new();
        for i in 1..=100u64 {
            s.record_success(i as f64);
        }
        let sum = s.summary();
        assert_eq!(sum.count, 100);
        assert_eq!(sum.min, 1.0);
        assert_eq!(sum.max, 100.0);
        assert!((sum.p50 - 50.0).abs() < 1.0);
        assert!((sum.p95 - 95.0).abs() < 1.0);
        assert!((sum.p99 - 99.0).abs() < 1.0);
        assert!((sum.mean - 50.5).abs() < 0.01);
    }

    #[test]
    fn errors_categorized() {
        let mut s = Samples::new();
        s.record_error("timeout");
        s.record_error("timeout");
        s.record_error("unavailable");
        let sum = s.summary();
        assert_eq!(sum.count, 0);
        let by_cat: BTreeMap<_, _> = sum
            .errors
            .iter()
            .map(|e| (e.category.clone(), e.count))
            .collect();
        assert_eq!(by_cat.get("timeout"), Some(&2));
        assert_eq!(by_cat.get("unavailable"), Some(&1));
    }

    #[test]
    fn empty_summary_is_zero() {
        let s = Samples::new();
        let sum = s.summary();
        assert_eq!(sum.count, 0);
        assert_eq!(sum.errors.len(), 0);
        assert_eq!(sum.min, 0.0);
        assert_eq!(sum.max, 0.0);
    }

    #[test]
    fn merge_combines_samples_and_errors() {
        let mut a = Samples::new();
        a.record_success(1.0);
        a.record_error("timeout");
        let mut b = Samples::new();
        b.record_success(2.0);
        b.record_error("timeout");
        b.record_error("other");
        a.merge_from(b);
        let sum = a.summary();
        assert_eq!(sum.count, 2);
        let by: BTreeMap<_, _> = sum
            .errors
            .iter()
            .map(|e| (e.category.clone(), e.count))
            .collect();
        assert_eq!(by["timeout"], 2);
        assert_eq!(by["other"], 1);
    }
}
