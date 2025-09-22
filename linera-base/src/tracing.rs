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
#[cfg(all(not(target_arch = "wasm32"), feature = "tempo"))]
use tracing_subscriber::filter::{filter_fn, FilterExt as _};
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
#[cfg(all(not(target_arch = "wasm32"), feature = "tempo"))]
use {
    opentelemetry::{global, trace::TracerProvider},
    opentelemetry_otlp::{SpanExporter, WithExportConfig},
    opentelemetry_sdk::{
        trace::{self as sdktrace, SdkTracerProvider},
        Resource,
    },
    tracing_opentelemetry::OpenTelemetryLayer,
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
    init_internal(log_name, false);
}

/// Initializes tracing with full OpenTelemetry support.
///
/// **IMPORTANT**: This function must be called from within a Tokio runtime context
/// as it initializes OpenTelemetry background tasks for span batching and export.
///
/// This sets up complete tracing with OpenTelemetry integration, including the
/// OpenTelemetry layer in the subscriber to export spans to Tempo.
///
/// ## Span Filtering for Performance
///
/// By default, spans created by `#[instrument]` are logged to console AND sent
/// to OpenTelemetry. In order to not spam stderr, you can set a low level, and use the
/// `telemetry_only` target:
///
/// ```rust
/// use tracing::{instrument, Level};
///
/// // Always sent to telemetry; console output controlled by level
/// #[instrument(level = "trace", target = "telemetry_only")]
/// fn my_called_too_frequently_function() {
///     // Will be sent to OpenTelemetry regardless of RUST_LOG level
///     // Will only appear in console if RUST_LOG includes trace level
/// }
///
/// // Higher level - more likely to appear in console
/// #[instrument(level = "info", target = "telemetry_only")]
/// fn my_important_function() {
///     // Will be sent to OpenTelemetry regardless of RUST_LOG level
///     // Will appear in console if RUST_LOG includes info level or higher
/// }
/// ```
///
/// **Key behaviors:**
/// - If span level >= RUST_LOG level: span goes to BOTH telemetry AND console (regardless of target)
/// - If span level < RUST_LOG level AND target = "telemetry_only": span goes to telemetry ONLY
/// - If span level < RUST_LOG level AND target != "telemetry_only": span is filtered out completely
/// - Default level for `telemetry_only` should be `trace` for minimal console noise
/// - All explicit log calls (tracing::info!(), etc.) are always printed regardless of span filtering
pub async fn init_with_opentelemetry(log_name: &str) {
    #[cfg(feature = "tempo")]
    {
        init_internal(log_name, true);
    }

    #[cfg(not(feature = "tempo"))]
    {
        tracing::warn!(
            "OpenTelemetry initialization requested but 'tempo' feature is not enabled. \
             Initializing standard tracing without OpenTelemetry support."
        );
        init_internal(log_name, false);
    }
}

fn init_internal(log_name: &str, with_opentelemetry: bool) {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let span_events = std::env::var("RUST_LOG_SPAN_EVENTS")
        .ok()
        .map_or(FmtSpan::NONE, |s| fmt_span_from_str(&s));

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

    #[cfg(any(target_arch = "wasm32", not(feature = "tempo")))]
    {
        let _ = with_opentelemetry;
        tracing_subscriber::registry()
            .with(env_filter)
            .with(maybe_log_file_layer)
            .with(stderr_layer)
            .init();
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "tempo"))]
    {
        if with_opentelemetry {
            // Initialize OpenTelemetry within async context
            let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://tempo.tempo.svc.cluster.local:4317".to_string());

            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(otlp_endpoint)
                .build()
                .expect("Failed to create OTLP exporter");

            let resource = Resource::builder()
                .with_service_name(log_name.to_string())
                .build();

            let tracer_provider = SdkTracerProvider::builder()
                .with_resource(resource)
                .with_batch_exporter(exporter)
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                .build();

            // Set the global tracer provider
            global::set_tracer_provider(tracer_provider.clone());

            let tracer = tracer_provider.tracer("linera");

            let telemetry_only_filter =
                filter_fn(|metadata| metadata.is_span() && metadata.target() == "telemetry_only");

            let otel_env_filter = tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy();

            let opentelemetry_filter = otel_env_filter.or(telemetry_only_filter);

            let opentelemetry_layer =
                OpenTelemetryLayer::new(tracer).with_filter(opentelemetry_filter);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(maybe_log_file_layer)
                .with(stderr_layer)
                .with(opentelemetry_layer)
                .init();
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(maybe_log_file_layer)
                .with(stderr_layer)
                .init();
        }
    }
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
