// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::OnceLock};

use prometheus::{
    core::{Collector, Desc},
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};
use tokio::runtime::Handle;

struct TokioRuntimeCollector {
    handle: Handle,
    workers: Desc,
    alive_tasks: Desc,
    global_queue: Desc,
    busy_seconds: Desc,
    parks: Desc,
    park_unparks: Desc,
}

impl TokioRuntimeCollector {
    fn new(handle: Handle) -> Self {
        let desc = |name: &str, help: &str, labels: Vec<String>| {
            Desc::new(name.into(), help.into(), labels, HashMap::new())
                .expect("static metric descriptor is always valid")
        };
        let workers = desc(
            "linera_tokio_workers",
            "Number of worker threads in the tokio runtime.",
            vec![],
        );
        let alive_tasks = desc(
            "linera_tokio_alive_tasks",
            "Number of tasks currently alive in the tokio runtime.",
            vec![],
        );
        let global_queue = desc(
            "linera_tokio_global_queue_depth",
            "Number of tasks in the runtime's global injection queue.",
            vec![],
        );
        let worker_label = vec!["worker".to_string()];
        let busy_seconds = desc(
            "linera_tokio_worker_busy_seconds_total",
            "Cumulative time each worker has spent executing tasks, in seconds.",
            worker_label.clone(),
        );
        let parks = desc(
            "linera_tokio_worker_parks_total",
            "Cumulative number of times each worker has parked (ran out of work).",
            worker_label.clone(),
        );
        let park_unparks = desc(
            "linera_tokio_worker_park_unparks_total",
            "Monotonically increasing count of park and unpark events combined per worker. \
             An odd value means the worker is currently parked.",
            worker_label,
        );
        Self {
            handle,
            workers,
            alive_tasks,
            global_queue,
            busy_seconds,
            parks,
            park_unparks,
        }
    }
}

impl Collector for TokioRuntimeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.workers,
            &self.alive_tasks,
            &self.global_queue,
            &self.busy_seconds,
            &self.parks,
            &self.park_unparks,
        ]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let m = self.handle.metrics();
        let num_workers = m.num_workers();
        let mut families = Vec::with_capacity(6);

        families.push(gauge_family(&self.workers, num_workers as f64));
        families.push(gauge_family(&self.alive_tasks, m.num_alive_tasks() as f64));
        families.push(gauge_family(
            &self.global_queue,
            m.global_queue_depth() as f64,
        ));

        #[cfg(target_has_atomic = "64")]
        {
            let mut busy = counter_family(&self.busy_seconds);
            let mut parks = counter_family(&self.parks);
            let mut park_unparks = counter_family(&self.park_unparks);
            for i in 0..num_workers {
                let label = worker_label(i);
                busy.mut_metric().push(counter_metric(
                    m.worker_total_busy_duration(i).as_secs_f64(),
                    &label,
                ));
                parks
                    .mut_metric()
                    .push(counter_metric(m.worker_park_count(i) as f64, &label));
                park_unparks
                    .mut_metric()
                    .push(counter_metric(m.worker_park_unpark_count(i) as f64, &label));
            }
            families.push(busy);
            families.push(parks);
            families.push(park_unparks);
        }

        families
    }
}

fn gauge_family(desc: &Desc, value: f64) -> MetricFamily {
    let mut gauge = Gauge::new();
    gauge.set_value(value);
    let mut metric = Metric::new();
    metric.set_gauge(gauge);
    let mut family = MetricFamily::new();
    family.set_name(desc.fq_name.clone());
    family.set_help(desc.help.clone());
    family.set_field_type(MetricType::GAUGE);
    family.mut_metric().push(metric);
    family
}

fn counter_family(desc: &Desc) -> MetricFamily {
    let mut family = MetricFamily::new();
    family.set_name(desc.fq_name.clone());
    family.set_help(desc.help.clone());
    family.set_field_type(MetricType::COUNTER);
    family
}

fn counter_metric(value: f64, labels: &[LabelPair]) -> Metric {
    let mut counter = Counter::new();
    counter.set_value(value);
    let mut metric = Metric::new();
    metric.set_counter(counter);
    metric.set_label(labels.to_vec().into());
    metric
}

fn worker_label(i: usize) -> [LabelPair; 1] {
    let mut lp = LabelPair::new();
    lp.set_name("worker".into());
    lp.set_value(i.to_string());
    [lp]
}

/// Must be called from within a tokio runtime context.
pub(crate) fn register() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let collector = Box::new(TokioRuntimeCollector::new(Handle::current()));
        if let Err(e) = prometheus::register(collector) {
            tracing::warn!("failed to register tokio runtime metrics: {e}");
        }
    });
}
