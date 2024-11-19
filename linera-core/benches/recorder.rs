// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use criterion::measurement::{Measurement, ValueFormatter};
use prometheus::proto::MetricType;

/// A `BenchRecorder` together with a set of counter names. This can be used in benchmarks that
/// measure the sum of the selected counters instead of wall clock time.
pub struct BenchRecorderMeasurement {
    counter_names: HashSet<&'static str>,
}

impl BenchRecorderMeasurement {
    pub fn new(counters: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            counter_names: counters.into_iter().collect(),
        }
    }
}

impl Measurement for BenchRecorderMeasurement {
    type Intermediate = u64;

    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        let mut total = 0;
        let metric_families = prometheus::gather();
        for metric_family in metric_families {
            if self.counter_names.contains(metric_family.get_name()) {
                let metric_type = metric_family.get_field_type();
                assert_eq!(metric_type, MetricType::COUNTER);
                for metric in metric_family.get_metric() {
                    total += metric.get_counter().get_value() as Self::Intermediate;
                }
            }
        }
        total
    }

    fn end(&self, intermediate: Self::Intermediate) -> Self::Value {
        self.start() - intermediate
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn criterion::measurement::ValueFormatter {
        struct CountFormatter;

        const COUNT_FORMATTER: CountFormatter = CountFormatter;

        impl ValueFormatter for CountFormatter {
            fn scale_values(&self, _typical_value: f64, _values: &mut [f64]) -> &'static str {
                ""
            }

            fn scale_throughputs(
                &self,
                _typical_value: f64,
                _throughput: &criterion::Throughput,
                _values: &mut [f64],
            ) -> &'static str {
                ""
            }

            fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
                ""
            }
        }

        &COUNT_FORMATTER
    }
}
