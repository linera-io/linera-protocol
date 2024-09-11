// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides unified handling for tracing subscribers within Linera binaries.

use std::{
    env,
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};

use is_terminal::IsTerminal as _;
use tracing::Subscriber;
use tracing_subscriber::{
    fmt::{
        self,
        format::{FmtSpan, Format, Full},
        time::FormatTime,
        FormatFields, MakeWriter,
    },
    layer::{Layer, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt,
};

/// Initializes tracing in a standard way.
///
/// The environment variables `RUST_LOG`, `RUST_LOG_SPAN_EVENTS`, and `RUST_LOG_FORMAT`
/// can be used to control the verbosity, the span event verbosity, and the output format,
/// respectively.
///
/// The `LINERA_LOG_DIR` environment variable can be used to configure a directory to
/// store log files. If it is set, a file named `log_name` with the `log` extension is
/// created in the directory.
pub fn init(log_name: &str) {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let span_events = std::env::var("RUST_LOG_SPAN_EVENTS")
        .ok()
        .map(|s| fmt_span_from_str(&s))
        .unwrap_or(FmtSpan::NONE);

    let format = std::env::var("RUST_LOG_FORMAT").ok();

    let color_output =
        !std::env::var("NO_COLOR").is_ok_and(|x| !x.is_empty()) && std::io::stderr().is_terminal();

    let stderr_layer = prepare_formatted_layer(
        format.as_deref(),
        fmt::layer()
            .with_span_events(span_events.clone())
            .with_writer(std::io::stderr)
            .with_ansi(color_output),
    );

    let maybe_log_file_layer = open_log_file(log_name).map(|file_writer| {
        prepare_formatted_layer(
            format.as_deref(),
            fmt::layer()
                .with_span_events(span_events)
                .with_writer(Arc::new(file_writer))
                .with_ansi(false),
        )
    });

    tracing_subscriber::registry()
        .with(env_filter)
        .with(stderr_layer)
        .with(maybe_log_file_layer)
        .init();
}

/// Opens a log file for writing.
///
/// The location of the file is determined by the `LINERA_LOG_DIR` environment variable,
/// and its name by the `log_name` parameter.
///
/// Returns [`None`] if the `LINERA_LOG_DIR` environment variable is not set.
fn open_log_file(log_name: &str) -> Option<File> {
    let log_directory = env::var_os("LINERA_LOG_DIR")?;
    let mut log_file_path = Path::new(&log_directory).join(log_name);
    log_file_path.set_extension("log");

    Some(
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(log_file_path)
            .expect("Failed to open log file for writing"),
    )
}

/// Applies a requested `formatting` to the log output of the provided `layer`.
///
/// Returns a boxed [`Layer`] with the formatting applied to the original `layer`.
fn prepare_formatted_layer<S, N, W, T>(
    formatting: Option<&str>,
    layer: fmt::Layer<S, N, Format<Full, T>, W>,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + Send + Sync + 'static,
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
    T: FormatTime + Send + Sync + 'static,
{
    match formatting.unwrap_or("plain") {
        "json" => layer.json().boxed(),
        "pretty" => layer.pretty().boxed(),
        "plain" => layer.boxed(),
        format => {
            panic!("Invalid RUST_LOG_FORMAT: `{format}`.  Valid values are `json` or `pretty`.")
        }
    }
}

fn fmt_span_from_str(events: &str) -> FmtSpan {
    let mut fmt_span = FmtSpan::NONE;
    for event in events.split(',') {
        fmt_span |= match event {
            "new" => FmtSpan::NEW,
            "enter" => FmtSpan::ENTER,
            "exit" => FmtSpan::EXIT,
            "close" => FmtSpan::CLOSE,
            "active" => FmtSpan::ACTIVE,
            "full" => FmtSpan::FULL,
            _ => FmtSpan::NONE,
        };
    }
    fmt_span
}
