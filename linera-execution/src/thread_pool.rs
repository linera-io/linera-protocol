// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
An instrumented wrapper around [`web_thread_pool::Pool`], the fixed-capacity pool of OS threads
that runs synchronous contract and service code off the async executor.

The pool is a permit gate: a caller takes a free slot, or grows the pool up to its capacity, or
else waits for another caller to return one — with no timeout. Saturation is invisible from the
outside, because a caller waiting for a slot is a *suspended future* rather than a blocked thread:
it appears in neither an off-CPU profile nor any log, and the process quietly stops executing
blocks. A slot holder can also need a second slot before it releases its first — a contract
querying a service oracle does exactly that — so the wait can be indefinite rather than merely
long. This module reports the wait while it is still happening, which is what separates a
saturated pool from a wedged one.
*/

use std::future::Future;

use futures::{
    future::{self, Either},
    pin_mut,
};
use linera_base::time::{Duration, Instant};
use tracing::warn;
use web_thread_select::Post;

// The `purpose` domain is closed, so every child of the labelled metrics below can be created at
// registration rather than appearing only once its path has first run.

/// Execution of a contract.
pub const CONTRACT: &str = "contract";
/// A one-off service query, including the one a contract makes through a service oracle.
pub const SERVICE_QUERY: &str = "service_query";
/// A long-lived service runtime actor, which holds its slot for the life of its chain worker.
pub const SERVICE_ACTOR: &str = "service_actor";
/// Decompression of contract bytecode.
pub const DECOMPRESS_CONTRACT: &str = "decompress_contract";
/// Decompression of service bytecode.
pub const DECOMPRESS_SERVICE: &str = "decompress_service";

/// Every purpose, so that the labelled metrics can create all their children at registration.
#[cfg(with_metrics)]
const PURPOSES: [&str; 5] = [
    CONTRACT,
    SERVICE_QUERY,
    SERVICE_ACTOR,
    DECOMPRESS_CONTRACT,
    DECOMPRESS_SERVICE,
];

/// How long a caller waits for a slot before warning, and between subsequent warnings.
const SLOW_ACQUISITION_WARN_INTERVAL: Duration = Duration::from_secs(5);

#[cfg(with_metrics)]
pub(crate) mod metrics {
    use linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, register_int_gauge_vec,
    };
    use prometheus::{HistogramVec, IntGaugeVec};

    use super::PURPOSES;

    const PURPOSE_LABEL: &str = "purpose";

    linera_base::declare_metrics! {
        /// Histogram of how long a caller waited for a thread-pool slot.
        pub static ACQUISITION_LATENCY: HistogramVec = {
            let histogram = register_histogram_vec(
                "thread_pool_acquisition_latency",
                "Time spent waiting for an execution thread-pool slot",
                &[PURPOSE_LABEL],
                exponential_bucket_latencies(60_000.0),
            );
            for purpose in PURPOSES {
                histogram.with_label_values(&[purpose]);
            }
            histogram
        };

        /// Gauge of the callers currently waiting for a thread-pool slot. A value that stays
        /// above zero while no acquisitions complete is the signature of a wedged pool.
        pub static WAITING: IntGaugeVec = {
            let gauge = register_int_gauge_vec(
                "thread_pool_waiting",
                "Callers currently waiting for an execution thread-pool slot",
                &[PURPOSE_LABEL],
            );
            for purpose in PURPOSES {
                gauge.with_label_values(&[purpose]);
            }
            gauge
        };
    }

    /// Decrements [`WAITING`] on drop, so that a caller cancelled mid-wait does not leak a count.
    pub(super) struct WaitingGuard(&'static str);

    impl WaitingGuard {
        pub(super) fn new(purpose: &'static str) -> Self {
            WAITING.with_label_values(&[purpose]).inc();
            WaitingGuard(purpose)
        }
    }

    impl Drop for WaitingGuard {
        fn drop(&mut self) {
            WAITING.with_label_values(&[self.0]).dec();
        }
    }
}

/// A fixed-capacity pool of OS threads for running synchronous contract and service code.
pub struct ThreadPool {
    inner: web_thread_pool::Pool,
    capacity: usize,
}

impl ThreadPool {
    /// Creates a pool that will grow to at most `capacity` threads.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: web_thread_pool::Pool::new(capacity),
            capacity,
        }
    }

    /// Runs `code` on a pool thread, waiting for a slot if none is free.
    pub async fn run<Context: Post, F: Future<Output: Post> + 'static>(
        &self,
        purpose: &'static str,
        context: Context,
        code: impl FnOnce(Context) -> F + Send + 'static,
    ) -> web_thread_pool::Task<F::Output> {
        self.reporting_the_wait(purpose, self.inner.run(context, code))
            .await
    }

    /// Like [`ThreadPool::run`], but the output can be sent through Rust memory without posting.
    pub async fn run_send<Context: Post, F: Future<Output: Send> + 'static>(
        &self,
        purpose: &'static str,
        context: Context,
        code: impl FnOnce(Context) -> F + Send + 'static,
    ) -> web_thread_pool::SendTask<F::Output> {
        self.reporting_the_wait(purpose, self.inner.run_send(context, code))
            .await
    }

    /// Awaits a slot acquisition, recording how long it took and warning every
    /// [`SLOW_ACQUISITION_WARN_INTERVAL`] until it succeeds. The warning is raced against the
    /// acquisition because reporting on completion says nothing when the slot never arrives.
    async fn reporting_the_wait<T>(
        &self,
        purpose: &'static str,
        acquisition: impl Future<Output = T>,
    ) -> T {
        #[cfg(with_metrics)]
        let _waiting = metrics::WaitingGuard::new(purpose);
        let start = Instant::now();
        pin_mut!(acquisition);
        loop {
            let warn_after = linera_base::time::timer::sleep(SLOW_ACQUISITION_WARN_INTERVAL);
            pin_mut!(warn_after);
            match future::select(acquisition.as_mut(), warn_after).await {
                Either::Left((slot, _)) => {
                    #[cfg(with_metrics)]
                    metrics::ACQUISITION_LATENCY
                        .with_label_values(&[purpose])
                        .observe(start.elapsed().as_secs_f64() * 1000.0);
                    return slot;
                }
                Either::Right((_, _)) => warn!(
                    purpose,
                    capacity = self.capacity,
                    waited_ms = start.elapsed().as_millis(),
                    "Still waiting for an execution thread-pool slot; every thread is claimed"
                ),
            }
        }
    }
}

#[cfg(all(test, with_metrics))]
mod tests {
    use futures::{channel::oneshot, poll};

    use super::*;

    /// Every `purpose` child must exist before any slot has been taken, so that a pool which has
    /// not yet saturated reads as zero rather than as a metric that was removed.
    #[test]
    fn purpose_labelled_metrics_export_every_purpose_before_any_use() {
        crate::init_metrics();

        let families = prometheus::gather();
        for name in [
            "linera_thread_pool_acquisition_latency",
            "linera_thread_pool_waiting",
        ] {
            let family = families
                .iter()
                .find(|family| family.get_name() == name)
                .unwrap_or_else(|| panic!("{name} must be exported once init_metrics has run"));
            let mut purposes = family
                .get_metric()
                .iter()
                .flat_map(|metric| metric.get_label())
                .map(|label| label.get_value())
                .collect::<Vec<_>>();
            purposes.sort_unstable();
            let mut expected = PURPOSES;
            expected.sort_unstable();
            assert_eq!(purposes, expected, "{name} must export every purpose");
        }
    }

    /// A caller blocked on a full pool has to be visible *while* it is still blocked: reporting
    /// it only once the slot arrives would say nothing at all in the case that never completes.
    #[tokio::test]
    async fn a_blocked_acquirer_is_counted_while_it_waits() {
        let pool = ThreadPool::new(1);
        let waiting = || metrics::WAITING.with_label_values(&[CONTRACT]).get();
        let before = waiting();

        let (release, released) = oneshot::channel();
        let occupant = pool
            .run_send(CONTRACT, (), move |()| async move {
                let _ = released.await;
            })
            .await;
        assert_eq!(
            waiting(),
            before,
            "an uncontended slot must not register a wait"
        );

        let mut blocked = Box::pin(pool.run_send(CONTRACT, (), |()| async move {}));
        assert!(
            poll!(blocked.as_mut()).is_pending(),
            "the only slot is taken, so the second caller must not get one"
        );
        assert_eq!(
            waiting(),
            before + 1,
            "a caller without a slot must be counted"
        );

        release.send(()).unwrap();
        occupant.await.unwrap();
        blocked.await.await.unwrap();
        assert_eq!(
            waiting(),
            before,
            "the count must be released with the slot"
        );
    }
}
