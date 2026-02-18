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
    EnvFilter,
};
#[cfg(not(target_arch = "wasm32"))]
use {
    opentelemetry::trace::TraceContextExt as _, tracing_opentelemetry::OtelData,
    tracing_subscriber::fmt::FormatEvent,
};

#[cfg(not(target_arch = "wasm32"))]
pub use crate::tracing_opentelemetry::{
    init_with_chrome_trace_exporter, init_with_opentelemetry, ChromeTraceGuard,
};

pub(crate) struct EnvConfig {
    pub(crate) env_filter: EnvFilter,
    span_events: FmtSpan,
    format: Option<String>,
    color_output: bool,
    log_name: String,
}

impl EnvConfig {
    pub(crate) fn stderr_layer<S>(&self) -> Box<dyn Layer<S> + Send + Sync>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        prepare_formatted_layer(
            self.format.as_deref(),
            fmt::layer()
                .with_span_events(self.span_events.clone())
                .with_writer(std::io::stderr)
                .with_ansi(self.color_output),
        )
    }

    pub(crate) fn maybe_log_file_layer<S>(&self) -> Option<Box<dyn Layer<S> + Send + Sync>>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        open_log_file(&self.log_name).map(|file_writer| {
            prepare_formatted_layer(
                self.format.as_deref(),
                fmt::layer()
                    .with_span_events(self.span_events.clone())
                    .with_writer(Arc::new(file_writer))
                    .with_ansi(false),
            )
        })
    }
}

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
    let config = get_env_config(log_name);
    let maybe_log_file_layer = config.maybe_log_file_layer();
    let stderr_layer = config.stderr_layer();

    tracing_subscriber::registry()
        .with(config.env_filter)
        .with(maybe_log_file_layer)
        .with(stderr_layer)
        .init();
}

pub(crate) fn get_env_config(log_name: &str) -> EnvConfig {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let span_events = std::env::var("RUST_LOG_SPAN_EVENTS")
        .ok()
        .map_or(FmtSpan::NONE, |s| fmt_span_from_str(&s));

    let format = std::env::var("RUST_LOG_FORMAT").ok();
    let color_output =
        !std::env::var("NO_COLOR").is_ok_and(|x| !x.is_empty()) && std::io::stderr().is_terminal();

    EnvConfig {
        env_filter,
        span_events,
        format,
        color_output,
        log_name: log_name.to_string(),
    }
}

/// Opens a log file for writing.
///
/// The location of the file is determined by the `LINERA_LOG_DIR` environment variable,
/// and its name by the `log_name` parameter.
///
/// Returns [`None`] if the `LINERA_LOG_DIR` environment variable is not set.
pub(crate) fn open_log_file(log_name: &str) -> Option<File> {
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

#[cfg(not(target_arch = "wasm32"))]
struct WithTraceContext;

#[cfg(not(target_arch = "wasm32"))]
impl<S, N> FormatEvent<S, N> for WithTraceContext
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &fmt::FmtContext<'_, S, N>,
        mut writer: fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        if let Some(scope) = ctx.event_scope() {
            for span in scope {
                let extensions = span.extensions();
                if let Some(otel_data) = extensions.get::<OtelData>() {
                    // For root spans, trace_id is on the builder.
                    // For child spans, it's inherited from the parent context.
                    let trace_id = otel_data
                        .builder
                        .trace_id
                        .unwrap_or_else(|| otel_data.parent_cx.span().span_context().trace_id());
                    if trace_id != opentelemetry::trace::TraceId::INVALID {
                        write!(writer, "traceID={trace_id} ")?;
                    }
                    if let Some(span_id) = otel_data.builder.span_id {
                        write!(writer, "spanID={span_id} ")?;
                    }
                    break;
                }
            }
        }
        Format::default().format_event(ctx, writer, event)
    }
}

/// Applies a requested `formatting` to the log output of the provided `layer`.
///
/// Returns a boxed [`Layer`] with the formatting applied to the original `layer`.
pub(crate) fn prepare_formatted_layer<S, N, W, T>(
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
        "plain" => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                layer.event_format(WithTraceContext).boxed()
            }
            #[cfg(target_arch = "wasm32")]
            {
                layer.boxed()
            }
        }
        format => {
            panic!("Invalid RUST_LOG_FORMAT: `{format}`.  Valid values are `json` or `pretty`.")
        }
    }
}

pub(crate) fn fmt_span_from_str(events: &str) -> FmtSpan {
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
