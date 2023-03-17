use criterion::measurement::{Measurement, ValueFormatter};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use portable_atomic::{AtomicU64, Ordering};
use std::{collections::HashSet, sync::Arc};

/// A metrics recorder that can be used for measurements with Criterion.
#[derive(Clone)]
pub struct BenchRecorder(Arc<Registry<Key, AtomicStorage>>);

impl Default for BenchRecorder {
    fn default() -> Self {
        BenchRecorder(Arc::new(Registry::atomic()))
    }
}

impl BenchRecorder {
    /// Installs this as the global recorder.
    pub fn install(self) -> Result<(), metrics::SetRecorderError> {
        metrics::set_boxed_recorder(Box::new(self))
    }

    /// Visits every counter stored in this registry.
    pub fn visit_counters<F>(&self, collect: F)
    where
        F: FnMut(&Key, &Arc<AtomicU64>),
    {
        self.0.visit_counters(collect)
    }
}

impl Recorder for BenchRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key) -> Counter {
        self.0
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        self.0
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        self.0
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

/// A `BenchRecorder` together with a set of counter names. This can be used in benachmarks that
/// measure the sum of the selected counters instead of wall clock time.
pub struct BenchRecorderMeasurement {
    recorder: BenchRecorder,
    counter_names: HashSet<&'static str>,
}

impl BenchRecorderMeasurement {
    pub fn new(
        recorder: BenchRecorder,
        counter_names: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        let counters = counter_names.into_iter().collect();
        Self {
            recorder,
            counter_names: counters,
        }
    }
}

impl Measurement for BenchRecorderMeasurement {
    type Intermediate = u64;

    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        let mut total = 0;
        self.recorder.visit_counters(|key, counter| {
            if self.counter_names.contains(key.name()) {
                total += counter.load(Ordering::SeqCst);
            }
        });
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
