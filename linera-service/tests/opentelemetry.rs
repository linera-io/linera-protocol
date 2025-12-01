// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tracing::{info_span, instrument};
use tracing_subscriber::{layer::SubscriberExt as _, registry::Registry};

#[instrument]
fn span_with_export() {
    tracing::info!("This span should be exported");
}

#[instrument(skip_all, fields(opentelemetry.skip = true))]
fn span_without_export() {
    tracing::info!("This span should NOT be exported");
}

#[test]
fn test_opentelemetry_filters_skip() {
    let (opentelemetry_layer, exporter, tracer_provider) =
        linera_service::tracing::opentelemetry::build_opentelemetry_layer_with_test_exporter(
            "test_opentelemetry",
        );

    let subscriber = Registry::default().with(opentelemetry_layer);

    tracing::subscriber::with_default(subscriber, || {
        span_with_export();
        span_without_export();

        let manual_exported_span = info_span!("manual_exported").entered();
        tracing::info!("Manual span without skip");
        drop(manual_exported_span);

        let manual_skipped_span = info_span!("manual_skipped", opentelemetry.skip = true).entered();
        tracing::info!("Manual span with opentelemetry.skip");
        drop(manual_skipped_span);
    });

    drop(tracer_provider);

    let exported_spans = exporter
        .get_finished_spans()
        .expect("Failed to get exported spans");

    let span_names: Vec<String> = exported_spans.iter().map(|s| s.name.to_string()).collect();

    assert!(
        span_names.contains(&"span_with_export".to_string()),
        "Regular span should be exported to OpenTelemetry. Found spans: {:?}",
        span_names
    );
    assert!(
        !span_names.contains(&"span_without_export".to_string()),
        "Span with opentelemetry.skip should NOT be exported to OpenTelemetry. Found spans: {:?}",
        span_names
    );
    assert!(
        span_names.contains(&"manual_exported".to_string()),
        "Manual span without skip should be exported. Found spans: {:?}",
        span_names
    );
    assert!(
        !span_names.contains(&"manual_skipped".to_string()),
        "Manual span with opentelemetry.skip should NOT be exported. Found spans: {:?}",
        span_names
    );
}
