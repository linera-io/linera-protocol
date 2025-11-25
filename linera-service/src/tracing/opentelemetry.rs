// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry integration for tracing with OTLP export and Chrome trace export.

use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
#[cfg(with_testing)]
use opentelemetry_sdk::trace::InMemorySpanExporter;
use opentelemetry_sdk::{
    trace::{BatchSpanProcessor, SdkTracerProvider},
    Resource,
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{
    filter::{filter_fn, FilterFn},
    layer::Layer,
    prelude::__tracing_subscriber_SubscriberExt as _,
    util::SubscriberInitExt,
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
#[cfg(with_testing)]
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
pub fn init(log_name: &str, otlp_endpoint: Option<&str>) {
    // Check if OpenTelemetry endpoint is configured via parameter or env var
    let endpoint = match otlp_endpoint {
        Some(ep) if !ep.is_empty() => ep.to_string(),
        _ => match std::env::var("LINERA_OTLP_EXPORTER_ENDPOINT") {
            Ok(ep) if !ep.is_empty() => ep,
            _ => {
                eprintln!(
                    "LINERA_OTLP_EXPORTER_ENDPOINT not set and no endpoint provided. \
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

    // Configure batch processor for high-throughput scenarios
    // Larger queue (16k instead of 2k default) to handle benchmark load
    // Faster export (100ms instead of 5s default) to prevent queue buildup
    let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_max_queue_size(16384) // 8x default, enough for 8 shards under load
        .with_max_export_batch_size(2048) // Larger batches for efficiency
        .with_scheduled_delay(std::time::Duration::from_millis(100)) // Fast export to prevent queue buildup
        .build();

    let batch_processor = BatchSpanProcessor::new(exporter, batch_config);

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(batch_processor)
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .build();

    init_with_tracer_provider(log_name, tracer_provider);
}
