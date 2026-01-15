// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

/// Guard that flushes Chrome trace file when dropped.
///
/// Store this guard in a variable that lives for the duration of your program.
/// When it's dropped, the trace file will be completed and closed.
pub type ChromeTraceGuard = tracing_chrome::FlushGuard;

/// Builds a Chrome trace layer and guard.
///
/// Returns a subscriber and guard. The subscriber should be used with `with_default`
/// to avoid global state conflicts.
pub fn build(
    log_name: &str,
    writer: impl std::io::Write + Send + 'static,
) -> (impl tracing::Subscriber + Send + Sync, ChromeTraceGuard) {
    let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
        .writer(writer)
        .build();

    let config = crate::tracing::get_env_config(log_name);
    let maybe_log_file_layer = config.maybe_log_file_layer();
    let stderr_layer = config.stderr_layer();

    let subscriber = tracing_subscriber::registry()
        .with(chrome_layer)
        .with(config.env_filter)
        .with(maybe_log_file_layer)
        .with(stderr_layer);

    (subscriber, guard)
}

/// Initializes tracing with Chrome Trace JSON exporter.
///
/// Returns a guard that must be kept alive for the duration of the program.
/// When the guard is dropped, the trace data is flushed and completed.
///
/// Exports traces to Chrome Trace JSON format which can be visualized in:
/// - Chrome: `chrome://tracing`
/// - Perfetto UI: <https://ui.perfetto.dev>
///
/// Note: Uses `try_init()` to avoid panicking if a global subscriber is already set.
/// In that case, tracing may not work as expected.
pub fn init(log_name: &str, writer: impl std::io::Write + Send + 'static) -> ChromeTraceGuard {
    let (subscriber, guard) = build(log_name, writer);
    // May fail if a global subscriber is already set.
    subscriber.try_init().ok();
    guard
}
