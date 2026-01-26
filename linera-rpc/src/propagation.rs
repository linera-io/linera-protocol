// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry context propagation for gRPC.
//!
//! This module provides utilities for propagating OpenTelemetry context (trace context and baggage)
//! across gRPC service boundaries using tonic metadata.
//!
//! # Usage
//!
//! ## Client-side injection
//!
//! ```ignore
//! use linera_rpc::propagation::inject_context;
//! use opentelemetry::Context;
//!
//! let mut request = tonic::Request::new(payload);
//! inject_context(&Context::current(), request.metadata_mut());
//! ```
//!
//! ## Server-side extraction
//!
//! ```ignore
//! use linera_rpc::propagation::extract_context;
//!
//! let cx = extract_context(request.metadata());
//! // Use cx.with_baggage() to access baggage values
//! ```

use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    Context,
};
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tracing::warn;

/// Wrapper for injecting context into tonic metadata.
struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        match MetadataKey::from_bytes(key.as_bytes()) {
            Ok(key) => match MetadataValue::try_from(&value) {
                Ok(value) => {
                    self.0.insert(key, value);
                }
                Err(error) => {
                    warn!(
                        value,
                        error = format!("{error:#}"),
                        "failed to parse metadata value"
                    );
                }
            },
            Err(error) => {
                warn!(
                    key,
                    error = format!("{error:#}"),
                    "failed to parse metadata key"
                );
            }
        }
    }
}

/// Wrapper for extracting context from tonic metadata.
struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                tonic::metadata::KeyRef::Ascii(key) => Some(key.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Injects the OpenTelemetry context into tonic metadata.
///
/// This injects both W3C TraceContext (`traceparent`, `tracestate`) and
/// W3C Baggage (`baggage`) headers into the metadata, enabling distributed
/// tracing and baggage propagation across gRPC service boundaries.
///
/// # Arguments
///
/// * `cx` - The OpenTelemetry context to inject
/// * `metadata` - The tonic metadata map to inject into
pub fn inject_context(cx: &Context, metadata: &mut MetadataMap) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(cx, &mut MetadataInjector(metadata));
    });
}

/// Extracts the OpenTelemetry context from tonic metadata.
///
/// This extracts both W3C TraceContext and W3C Baggage headers from the
/// metadata, returning a context that can be used as a parent for new spans
/// or to read baggage values.
///
/// # Arguments
///
/// * `metadata` - The tonic metadata map to extract from
///
/// # Returns
///
/// The extracted OpenTelemetry context, or an empty context if no propagation
/// headers were found.
pub fn extract_context(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}

/// Baggage key for traffic type labeling.
///
/// Used to distinguish organic traffic from synthetic (benchmark) traffic.
/// Valid values are "organic" and "synthetic".
pub const TRAFFIC_TYPE_KEY: &str = "traffic_type";

/// Traffic type for normal production traffic.
pub const TRAFFIC_TYPE_ORGANIC: &str = "organic";

/// Traffic type for synthetic benchmark traffic.
pub const TRAFFIC_TYPE_SYNTHETIC: &str = "synthetic";

/// Traffic type when OpenTelemetry feature is disabled.
pub const TRAFFIC_TYPE_UNKNOWN: &str = "unknown";

/// Environment variable to override the traffic type.
///
/// Set this to "synthetic" to mark all outgoing requests as benchmark traffic.
/// This is useful for benchmark tools that cannot easily set OpenTelemetry baggage.
pub const TRAFFIC_TYPE_ENV_VAR: &str = "LINERA_TRAFFIC_TYPE";

/// Returns the current OpenTelemetry context, enriched with traffic type baggage
/// if the `LINERA_TRAFFIC_TYPE` environment variable is set.
///
/// This function provides a workaround for async code that cannot hold a `ContextGuard`
/// across `.await` points (since `ContextGuard` is `!Send`).
///
/// If `LINERA_TRAFFIC_TYPE=synthetic` is set, the returned context will have
/// synthetic traffic baggage attached.
pub fn get_context_with_traffic_type() -> Context {
    use opentelemetry::{baggage::BaggageExt, Key, KeyValue};

    let cx = Context::current();

    // Check if the environment variable is set to "synthetic"
    if std::env::var(TRAFFIC_TYPE_ENV_VAR)
        .map(|v| v == TRAFFIC_TYPE_SYNTHETIC)
        .unwrap_or(false)
    {
        cx.with_baggage(vec![KeyValue::new(
            Key::new(TRAFFIC_TYPE_KEY),
            TRAFFIC_TYPE_SYNTHETIC,
        )])
    } else {
        cx
    }
}

/// Extracts the traffic type from the current context.
///
/// Returns "organic" if no traffic type baggage is set.
pub fn get_traffic_type(cx: &Context) -> &'static str {
    use opentelemetry::baggage::BaggageExt;

    cx.baggage()
        .get(TRAFFIC_TYPE_KEY)
        .map(|v| v.as_str())
        .and_then(|v| {
            if v == TRAFFIC_TYPE_SYNTHETIC {
                Some(TRAFFIC_TYPE_SYNTHETIC)
            } else {
                None
            }
        })
        .unwrap_or(TRAFFIC_TYPE_ORGANIC)
}

// ============================================================================
// Tower Layer for Server-Side Context Extraction
// ============================================================================

use std::task::{Context as TaskContext, Poll};

use futures::{future::BoxFuture, FutureExt};
use tower::{Layer, Service};

/// Tower layer that extracts OpenTelemetry context from incoming gRPC requests.
///
/// This layer extracts W3C TraceContext and Baggage headers from the request
/// metadata and stores the extracted context in the request extensions.
///
/// # Usage
///
/// ```ignore
/// use linera_rpc::propagation::OtelContextLayer;
/// use tower::ServiceBuilder;
///
/// let service = ServiceBuilder::new()
///     .layer(OtelContextLayer)
///     .service(my_service);
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct OtelContextLayer;

impl<S> Layer<S> for OtelContextLayer {
    type Service = OtelContextService<S>;

    fn layer(&self, service: S) -> Self::Service {
        OtelContextService { inner: service }
    }
}

/// Service wrapper that extracts OpenTelemetry context from requests.
#[derive(Clone, Debug)]
pub struct OtelContextService<S> {
    inner: S,
}

/// Extension type to store the extracted OpenTelemetry context.
#[derive(Clone, Debug)]
pub struct ExtractedOtelContext(pub Context);

impl<S, B> Service<http::Request<B>> for OtelContextService<S>
where
    S: Service<http::Request<B>> + Clone + Send + 'static,
    S::Future: Send,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        use tracing::Instrument;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        // Extract context from HTTP headers (which tonic uses for gRPC metadata)
        let cx = global::get_text_map_propagator(|propagator| {
            propagator.extract(&HttpHeaderExtractor(request.headers()))
        });

        // Store extracted context in request extensions for downstream use
        request
            .extensions_mut()
            .insert(ExtractedOtelContext(cx.clone()));

        // Create a span and set the extracted context as its parent.
        // This links the incoming trace to all spans created during request handling.
        // We use Instrument to run the async work inside this span, avoiding the
        // !Send issue with ContextGuard.
        let span = tracing::info_span!("grpc_request");
        span.set_parent(cx);

        let mut inner = self.inner.clone();
        async move { inner.call(request).await }
            .instrument(span)
            .boxed()
    }
}

/// Extractor for HTTP headers (used by tonic for gRPC metadata).
struct HttpHeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HttpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Gets the traffic type from an http::Request's extensions.
///
/// Returns "organic" if no context was extracted or no traffic type baggage was set.
pub fn get_traffic_type_from_request<B>(request: &http::Request<B>) -> &'static str {
    request
        .extensions()
        .get::<ExtractedOtelContext>()
        .map_or(TRAFFIC_TYPE_ORGANIC, |ext| get_traffic_type(&ext.0))
}

/// Gets the traffic type from a tonic::Request's extensions.
///
/// Returns "organic" if no context was extracted or no traffic type baggage was set.
/// Tower middleware extensions are preserved in tonic::Request extensions.
pub fn get_traffic_type_from_tonic_request<T>(request: &tonic::Request<T>) -> &'static str {
    request
        .extensions()
        .get::<ExtractedOtelContext>()
        .map_or(TRAFFIC_TYPE_ORGANIC, |ext| get_traffic_type(&ext.0))
}

/// Gets the OpenTelemetry context from a tonic::Request's extensions.
///
/// Returns `None` if no context was extracted by OtelContextLayer.
/// Tower middleware extensions are preserved in tonic::Request extensions.
pub fn get_otel_context_from_tonic_request<T>(request: &tonic::Request<T>) -> Option<Context> {
    request
        .extensions()
        .get::<ExtractedOtelContext>()
        .map(|ext| ext.0.clone())
}

/// Creates a new tonic::Request with the OpenTelemetry context injected into metadata.
///
/// This is used to propagate context when forwarding requests to downstream services.
/// If `cx` is `None`, returns a request without injected context.
pub fn create_request_with_context<T>(inner: T, cx: Option<&Context>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(inner);
    if let Some(cx) = cx {
        inject_context(cx, request.metadata_mut());
    }
    request
}

#[cfg(test)]
mod tests {
    use opentelemetry::{baggage::BaggageExt, Key, KeyValue};

    use super::*;

    #[test]
    fn test_inject_and_extract_baggage() {
        // Set up a composite propagator
        use opentelemetry::propagation::TextMapCompositePropagator;
        use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};

        let propagator = TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]);
        global::set_text_map_propagator(propagator);

        // Create context with baggage
        let cx = Context::current().with_baggage(vec![KeyValue::new(
            Key::new(TRAFFIC_TYPE_KEY),
            TRAFFIC_TYPE_SYNTHETIC,
        )]);

        // Inject into metadata
        let mut metadata = MetadataMap::new();
        inject_context(&cx, &mut metadata);

        // Verify baggage header was injected
        assert!(
            metadata.get("baggage").is_some(),
            "baggage header should be present"
        );

        // Extract and verify
        let extracted_cx = extract_context(&metadata);
        let traffic_type = get_traffic_type(&extracted_cx);
        assert_eq!(traffic_type, TRAFFIC_TYPE_SYNTHETIC);
    }

    #[test]
    fn test_default_traffic_type_is_organic() {
        let cx = Context::current();
        assert_eq!(get_traffic_type(&cx), TRAFFIC_TYPE_ORGANIC);
    }
}
