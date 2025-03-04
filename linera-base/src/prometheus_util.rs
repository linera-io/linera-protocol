// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines utility functions for interacting with Prometheus (logging metrics, etc)

use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_vec,
    register_int_counter_vec, HistogramVec, IntCounterVec, Opts,
};

use crate::time::Instant;

const LINERA_NAMESPACE: &str = "linera";

/// Wrapper around Prometheus register_int_counter_vec! macro which also sets the `linera` namespace
pub fn register_int_counter_vec(
    name: &str,
    description: &str,
    label_names: &[&str],
) -> IntCounterVec {
    let counter_opts = Opts::new(name, description).namespace(LINERA_NAMESPACE);
    register_int_counter_vec!(counter_opts, label_names).expect("IntCounter can be created")
}

/// Wrapper around Prometheus `register_histogram_vec!` macro which also sets the `linera` namespace
pub fn register_histogram_vec(
    name: &str,
    description: &str,
    label_names: &[&str],
    buckets: Option<Vec<f64>>,
) -> HistogramVec {
    let histogram_opts = if let Some(buckets) = buckets {
        histogram_opts!(name, description, buckets).namespace(LINERA_NAMESPACE)
    } else {
        histogram_opts!(name, description).namespace(LINERA_NAMESPACE)
    };

    register_histogram_vec!(histogram_opts, label_names).expect("Histogram can be created")
}

/// Construct the bucket interval exponentially starting from a value and an ending value.
pub fn exponential_bucket_interval(start_value: f64, end_value: f64) -> Option<Vec<f64>> {
    let quot = end_value / start_value;
    let factor = 3.0_f64;
    let count_approx = quot.ln() / factor.ln();
    let count = count_approx.round() as usize;
    let mut buckets = exponential_buckets(start_value, factor, count)
        .expect("Exponential buckets creation should not fail!");
    if let Some(last) = buckets.last() {
        if *last < end_value {
            buckets.push(end_value);
        }
    }
    Some(buckets)
}

/// Construct the latencies exponentially starting from 0.001 and ending at the maximum latency
pub fn exponential_bucket_latencies(max_latency: f64) -> Option<Vec<f64>> {
    exponential_bucket_interval(0.001_f64, max_latency)
}

/// Construct the bucket interval linearly starting from a value and an ending value.
pub fn linear_bucket_interval(start_value: f64, width: f64, end_value: f64) -> Option<Vec<f64>> {
    let count = (end_value - start_value) / width;
    let count = count.round() as usize;
    let mut buckets = linear_buckets(start_value, width, count)
        .expect("Linear buckets creation should not fail!");
    buckets.push(end_value);
    Some(buckets)
}

/// Construct the latencies linearly starting from 1 and ending at the maximum latency
pub fn linear_bucket_latencies(max_latency: f64) -> Option<Vec<f64>> {
    linear_bucket_interval(1.0, 50.0, max_latency)
}

/// A guard for an active latency measurement.
///
/// Finishes the measurement when dropped, and then updates the `Metric`.
pub struct ActiveMeasurementGuard<'metric, Metric>
where
    Metric: MeasureLatency,
{
    start: Instant,
    metric: Option<&'metric Metric>,
}

impl<Metric> ActiveMeasurementGuard<'_, Metric>
where
    Metric: MeasureLatency,
{
    /// Finishes the measurement, updates the `Metric` and returns the measured latency in
    /// milliseconds.
    pub fn finish(mut self) -> f64 {
        self.finish_by_ref()
    }

    /// Finishes the measurement without taking ownership of this [`ActiveMeasurementGuard`],
    /// updates the `Metric` and returns the measured latency in milliseconds.
    fn finish_by_ref(&mut self) -> f64 {
        match self.metric.take() {
            Some(metric) => {
                let latency = self.start.elapsed().as_secs_f64() * 1000.0;
                metric.finish_measurement(latency);
                latency
            }
            None => {
                // This is getting called from `Drop` after `finish` has already been
                // executed
                f64::NAN
            }
        }
    }
}

impl<Metric> Drop for ActiveMeasurementGuard<'_, Metric>
where
    Metric: MeasureLatency,
{
    fn drop(&mut self) {
        self.finish_by_ref();
    }
}

/// An extension trait for metrics that can be used to measure latencies.
pub trait MeasureLatency: Sized {
    /// Starts measuring the latency, finishing when the returned
    /// [`ActiveMeasurementGuard`] is dropped.
    fn measure_latency(&self) -> ActiveMeasurementGuard<'_, Self>;

    /// Updates the metric with measured latency in `milliseconds`.
    fn finish_measurement(&self, milliseconds: f64);
}

impl MeasureLatency for HistogramVec {
    fn measure_latency(&self) -> ActiveMeasurementGuard<'_, Self> {
        ActiveMeasurementGuard {
            start: Instant::now(),
            metric: Some(self),
        }
    }

    fn finish_measurement(&self, milliseconds: f64) {
        self.with_label_values(&[]).observe(milliseconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function for approximate floating point comparison
    fn assert_float_vec_eq(left: &[f64], right: &[f64]) {
        const EPSILON: f64 = 1e-10;

        assert_eq!(left.len(), right.len(), "Vectors have different lengths");
        for (i, (l, r)) in left.iter().zip(right.iter()).enumerate() {
            assert!(
                (l - r).abs() < EPSILON,
                "Vectors differ at index {}: {} != {}",
                i,
                l,
                r
            );
        }
    }

    #[test]
    fn test_linear_bucket_interval() {
        // Case 1: Width divides range evenly - small values
        let buckets = linear_bucket_interval(0.05, 0.01, 0.1).unwrap();
        assert_float_vec_eq(&buckets, &[0.05, 0.06, 0.07, 0.08, 0.09, 0.1]);

        // Case 2: Width divides range evenly - large values
        let buckets = linear_bucket_interval(100.0, 50.0, 500.0).unwrap();
        assert_float_vec_eq(
            &buckets,
            &[
                100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0,
            ],
        );

        // Case 3: Width doesn't divide range evenly - small values
        let buckets = linear_bucket_interval(0.05, 0.12, 0.5).unwrap();
        assert_float_vec_eq(&buckets, &[0.05, 0.17, 0.29, 0.41, 0.5]);

        // Case 4: Width doesn't divide range evenly - large values
        let buckets = linear_bucket_interval(100.0, 150.0, 500.0).unwrap();
        assert_float_vec_eq(&buckets, &[100.0, 250.0, 400.0, 500.0]);
    }
}
