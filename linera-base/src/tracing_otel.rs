// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry integration for tracing with OTLP export to Tempo and Chrome trace export.

use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
#[cfg(feature = "tempo")]
use {
    opentelemetry::{global, trace::TracerProvider},
    opentelemetry_otlp::{SpanExporter, WithExportConfig},
    opentelemetry_sdk::{trace::SdkTracerProvider, Resource},
    tracing_opentelemetry::OpenTelemetryLayer,
    tracing_subscriber::{
        filter::{filter_fn, FilterExt as _},
        layer::Layer,
    },
};

/// Initializes tracing with OpenTelemetry OTLP exporter to Tempo.
///
/// Exports traces to Tempo using the OTLP protocol. Requires the `tempo` feature.
/// Only enables OpenTelemetry if OTEL_EXPORTER_OTLP_ENDPOINT env var is set.
/// This prevents DNS errors in environments where OpenTelemetry is not deployed.
#[cfg(feature = "tempo")]
pub fn init_with_opentelemetry(log_name: &str, otlp_endpoint: Option<&str>) {
    // Check if OpenTelemetry endpoint is configured via parameter or env var
    let endpoint = match otlp_endpoint {
        Some(ep) => ep.to_string(),
        None => match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => {
                eprintln!(
                    "OTEL_EXPORTER_OTLP_ENDPOINT not set and no endpoint provided. \
                     Falling back to standard tracing without OpenTelemetry support."
                );
                crate::tracing::init(log_name);
                return;
            }
        },
    };

    let resource = Resource::builder()
        .with_service_name(log_name.to_string())
        .build();

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("Failed to create OTLP exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("linera");

    let telemetry_only_filter =
        filter_fn(|metadata| metadata.is_span() && metadata.target() == "telemetry_only");

    let otel_env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();

    let opentelemetry_filter = otel_env_filter.or(telemetry_only_filter);
    let opentelemetry_layer = OpenTelemetryLayer::new(tracer).with_filter(opentelemetry_filter);

    let config = crate::tracing::get_env_config(log_name);
    let maybe_log_file_layer = config.maybe_log_file_layer();
    let stderr_layer = config.stderr_layer();

    tracing_subscriber::registry()
        .with(opentelemetry_layer)
        .with(config.env_filter)
        .with(maybe_log_file_layer)
        .with(stderr_layer)
        .init();
}

/// Fallback when tempo feature is not enabled.
#[cfg(not(feature = "tempo"))]
pub fn init_with_opentelemetry(log_name: &str, _otlp_endpoint: Option<&str>) {
    eprintln!(
        "OTLP export requires the 'tempo' feature to be enabled! Falling back to default tracing initialization."
    );
    crate::tracing::init(log_name);
}

/// Guard that flushes Chrome trace file when dropped.
///
/// Store this guard in a variable that lives for the duration of your program.
/// When it's dropped, the trace file will be completed and closed.
pub type ChromeTraceGuard = tracing_chrome::FlushGuard;

/// Initializes tracing with Chrome Trace JSON exporter.
///
/// Returns a guard that must be kept alive for the duration of the program.
/// When the guard is dropped, the trace data is flushed and completed.
///
/// Exports traces to Chrome Trace JSON format which can be visualized in:
/// - Chrome: `chrome://tracing`
/// - Perfetto UI: <https://ui.perfetto.dev>
pub fn init_with_chrome_trace_exporter<W>(log_name: &str, writer: W) -> ChromeTraceGuard
where
    W: std::io::Write + Send + 'static,
{
    let (chrome_layer, guard) = ChromeLayerBuilder::new().writer(writer).build();

    let config = crate::tracing::get_env_config(log_name);
    let maybe_log_file_layer = config.maybe_log_file_layer();
    let stderr_layer = config.stderr_layer();

    tracing_subscriber::registry()
        .with(chrome_layer)
        .with(config.env_filter)
        .with(maybe_log_file_layer)
        .with(stderr_layer)
        .init();

    guard
}
