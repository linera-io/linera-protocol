// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines util functions for interacting with Prometheus (logging metrics, etc)

use prometheus::{
    histogram_opts, register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec,
    Opts,
};

use crate::time::Instant;

const LINERA_NAMESPACE: &str = "linera";

/// Wrapper arount prometheus register_int_counter_vec! macro which also sets the linera namespace
pub fn register_int_counter_vec(
    name: &str,
    description: &str,
    label_names: &[&str],
) -> IntCounterVec {
    let counter_opts = Opts::new(name, description).namespace(LINERA_NAMESPACE);
    register_int_counter_vec!(counter_opts, label_names).expect("IntCounter can be created")
}

/// Wrapper arount prometheus register_histogram_vec! macro which also sets the linera namespace
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
    /// Finishes the measurement, updates the `Metric` and the returns the measured latency in
    /// milliseconds.
    pub fn finish(mut self) -> f64 {
        self.finish_by_ref()
    }

    /// Finishes the measurement without taking ownership of this [`ActiveMeasurementGuard`],
    /// updates the `Metric` and the returns the measured latency in milliseconds.
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
