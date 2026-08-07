// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-wide reporting of panics.
//!
//! Tokio catches a panic at the task boundary and hands it to whoever joins the task, so a
//! panicking task neither stops the runtime nor, on its own, produces anything a monitoring
//! system can act on: the default hook writes to standard error and nothing else. The hook
//! installed here reports the panic through `tracing` and the metrics registry first, so
//! that panics are visible wherever the process's other logs and metrics are collected.

use std::{
    panic::PanicHookInfo,
    sync::{Mutex, Once},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use prometheus::IntCounter;

    use crate::prometheus_util::register_int_counter;

    /// Panics observed by the hook installed by [`super::init`].
    ///
    /// A panic does not stop the process, so this counter is often the only durable signal
    /// that one happened: whatever the panicking task was responsible for has stopped, and
    /// the effect on the rest of the process depends entirely on who was joining it. Any
    /// increase deserves investigation.
    pub(super) static PANICS: LazyLock<IntCounter> =
        LazyLock::new(|| register_int_counter("linera_panics_total", "Number of panics observed"));
}

/// A panic hook, in the form [`std::panic::take_hook`] returns it.
type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send>;

/// The hook that was installed before ours, which we delegate to so that the standard
/// message and the `RUST_BACKTRACE` backtrace are still printed.
static PREVIOUS_HOOK: Mutex<Option<PanicHook>> = Mutex::new(None);

static INIT: Once = Once::new();

/// Installs a panic hook that reports panics through `tracing` and the metrics registry
/// before delegating to the hook that was previously installed.
///
/// Calling this more than once has no further effect. It does not change what a panic
/// *does* — the process still unwinds the panicking task and keeps running — only what is
/// recorded about it.
pub fn init() {
    INIT.call_once(|| {
        *PREVIOUS_HOOK.lock().expect("hook mutex is never poisoned") =
            Some(std::panic::take_hook());
        std::panic::set_hook(Box::new(report_panic));
    });
}

/// The panic message, for the two payload types `panic!` produces.
///
/// `PanicHookInfo::payload_as_str` does the same thing, but is not yet stable in the
/// toolchain the release branches pin, and this code is backported to them.
fn payload_message<'a>(info: &'a PanicHookInfo<'_>) -> &'a str {
    let payload = info.payload();
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string payload>")
}

fn report_panic(info: &PanicHookInfo<'_>) {
    #[cfg(with_metrics)]
    metrics::PANICS.inc();

    let thread = std::thread::current();
    tracing::error!(
        thread = thread.name().unwrap_or("<unnamed>"),
        location = info.location().map(tracing::field::display),
        message = payload_message(info),
        "Panic",
    );

    // The lock is only ever held while installing the hook, and the hook is installed
    // once, so a panic here would mean a panic inside `init` itself.
    if let Ok(guard) = PREVIOUS_HOOK.lock() {
        if let Some(previous) = guard.as_ref() {
            previous(info);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::AssertUnwindSafe,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    /// Installing the hook twice must not chain it to itself, which would make every panic
    /// report grow by one line per call.
    #[test]
    fn test_init_is_idempotent() {
        static DELEGATIONS: AtomicUsize = AtomicUsize::new(0);

        std::panic::set_hook(Box::new(|_| {
            DELEGATIONS.fetch_add(1, Ordering::SeqCst);
        }));
        init();
        init();

        let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| panic!("boom"))).is_err();

        assert!(panicked);
        assert_eq!(
            DELEGATIONS.load(Ordering::SeqCst),
            1,
            "the hook installed before `init` ran exactly once",
        );
    }
}
