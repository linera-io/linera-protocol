// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry integration for tracing with OTLP export and Chrome trace export.

#[cfg(all(with_testing, feature = "opentelemetry"))]
use opentelemetry_sdk::trace::InMemorySpanExporter;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
#[cfg(feature = "opentelemetry")]
use {
    opentelemetry::{global, propagation::TextMapCompositePropagator, trace::TracerProvider},
    opentelemetry_otlp::{SpanExporter, WithExportConfig},
    opentelemetry_sdk::{
        propagation::{BaggagePropagator, TraceContextPropagator},
        trace::SdkTracerProvider,
        Resource,
    },
    tracing_opentelemetry::OpenTelemetryLayer,
    tracing_subscriber::{
        filter::{filter_fn, FilterFn},
        layer::Layer,
    },
};

/// Creates a filter that excludes spans with the `opentelemetry.skip` field.
///
/// Any span that declares an `opentelemetry.skip` field will be excluded from export,
/// regardless of the field's value. This is a limitation of the tracing metadata API.
///
/// Usage examples:
/// ```ignore
/// // Always skip this span
/// #[tracing::instrument(fields(opentelemetry.skip = true))]
/// fn internal_helper() { }
///
/// // Conditionally skip based on a parameter
/// #[tracing::instrument(fields(opentelemetry.skip = should_skip))]
/// fn my_function(should_skip: bool) {
///     // Will be skipped if should_skip is true when called
///     // Note: The field must be declared in the span, so the span is
///     // created with knowledge that it might be skipped
/// }
/// ```
#[cfg(feature = "opentelemetry")]
fn opentelemetry_skip_filter() -> FilterFn<impl Fn(&tracing::Metadata<'_>) -> bool> {
    filter_fn(|metadata| {
        if !metadata.is_span() {
            return false;
        }
        metadata.fields().field("opentelemetry.skip").is_none()
    })
}

/// Initializes tracing with a custom OpenTelemetry tracer provider.
///
/// This is an internal function used by both production and test code.
#[cfg(feature = "opentelemetry")]
fn init_with_tracer_provider(log_name: &str, tracer_provider: SdkTracerProvider) {
    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("linera");

    let opentelemetry_layer =
        OpenTelemetryLayer::new(tracer).with_filter(opentelemetry_skip_filter());

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

/// Builds an OpenTelemetry layer with the opentelemetry.skip filter.
///
/// This is used for testing to avoid setting the global subscriber.
/// Returns the layer, exporter, and tracer provider (which must be kept alive and shutdown).
#[cfg(all(with_testing, feature = "opentelemetry"))]
pub fn build_opentelemetry_layer_with_test_exporter(
    log_name: &str,
) -> (
    impl tracing_subscriber::Layer<tracing_subscriber::Registry>,
    InMemorySpanExporter,
    SdkTracerProvider,
) {
    let exporter = InMemorySpanExporter::default();
    let exporter_clone = exporter.clone();

    let resource = Resource::builder()
        .with_service_name(log_name.to_string())
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_simple_exporter(exporter)
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("linera");
    let opentelemetry_layer =
        OpenTelemetryLayer::new(tracer).with_filter(opentelemetry_skip_filter());

    (opentelemetry_layer, exporter_clone, tracer_provider)
}

/// Initializes tracing with OpenTelemetry OTLP exporter.
///
/// Exports traces using the OTLP protocol to any OpenTelemetry-compatible backend.
/// Requires the `opentelemetry` feature.
/// Only enables OpenTelemetry if LINERA_OTLP_EXPORTER_ENDPOINT env var is set.
/// This prevents DNS errors in environments where OpenTelemetry is not deployed.
#[cfg(feature = "opentelemetry")]
pub fn init_with_opentelemetry(log_name: &str, otlp_endpoint: Option<&str>) {
    // Always set up the propagator for baggage support, even without OTLP export.
    // This enables traffic_type labeling (organic vs synthetic) to work regardless
    // of whether traces are being exported.
    let propagator = TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]);
    global::set_text_map_propagator(propagator);

    // Check if OpenTelemetry endpoint is configured via parameter or env var
    let endpoint = match otlp_endpoint {
        Some(ep) if !ep.is_empty() => ep.to_string(),
        _ => match std::env::var("LINERA_OTLP_EXPORTER_ENDPOINT") {
            Ok(ep) if !ep.is_empty() => ep,
            _ => {
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

    init_with_tracer_provider(log_name, tracer_provider);
}

/// Fallback when opentelemetry feature is not enabled.
#[cfg(not(feature = "opentelemetry"))]
pub fn init_with_opentelemetry(log_name: &str, otlp_endpoint: Option<&str>) {
    crate::tracing::init(log_name);
    let endpoint_requested = matches!(otlp_endpoint, Some(ep) if !ep.is_empty())
        || matches!(std::env::var("LINERA_OTLP_EXPORTER_ENDPOINT"), Ok(ep) if !ep.is_empty());
    if endpoint_requested {
        tracing::warn!(
            "OTLP export requires the 'opentelemetry' feature to be enabled. \
             Falling back to default tracing initialization."
        );
    }
}

/// Guard that flushes Chrome trace file when dropped.
///
/// Store this guard in a variable that lives for the duration of your program.
/// When it's dropped, the trace file will be completed and closed.
pub type ChromeTraceGuard = tracing_chrome::FlushGuard;

/// Builds a Chrome trace layer and guard.
///
/// Returns a subscriber and guard. The subscriber should be used with `with_default`
/// to avoid global state conflicts.
pub fn build_chrome_trace_layer_with_exporter<W>(
    log_name: &str,
    writer: W,
) -> (impl tracing::Subscriber + Send + Sync, ChromeTraceGuard)
where
    W: std::io::Write + Send + 'static,
{
    let (chrome_layer, guard) = ChromeLayerBuilder::new().writer(writer).build();

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
pub fn init_with_chrome_trace_exporter<W>(log_name: &str, writer: W) -> ChromeTraceGuard
where
    W: std::io::Write + Send + 'static,
{
    let (subscriber, guard) = build_chrome_trace_layer_with_exporter(log_name, writer);
    let _ = subscriber.try_init();
    guard
}
