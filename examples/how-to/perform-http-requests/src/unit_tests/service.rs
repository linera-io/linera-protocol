// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(test)]

//! Unit tests for the service.

use std::sync::Arc;

use assert_matches::assert_matches;
use how_to_perform_http_requests::Operation;
use linera_sdk::{http, util::BlockingWait, Service as _, ServiceRuntime};

use super::Service;

/// A dummy URL to use in the tests.
const TEST_BASE_URL: &str = "http://some.test.url";

/// Tests if an HTTP request is performed by a service query.
#[test]
fn service_query_performs_http_request() {
    let http_response = b"Hello, world!";

    let mut service = create_service();
    let runtime = Arc::get_mut(&mut service.runtime).expect("Runtime should not be shared");

    runtime.add_expected_http_request(
        http::Request::get(TEST_BASE_URL),
        http::Response::ok(http_response),
    );

    let request = async_graphql::Request::new("query { performHttpRequest }");

    let response = service.handle_query(request).blocking_wait();

    let response_bytes = extract_response_bytes(response);

    assert_eq!(response_bytes, http_response);
}

/// Tests if a failed HTTP request performed by a service query leads to a GraphQL error.
#[test]
fn service_query_returns_http_request_error() {
    let mut service = create_service();
    let runtime = Arc::get_mut(&mut service.runtime).expect("Runtime should not be shared");

    runtime.add_expected_http_request(
        http::Request::get(TEST_BASE_URL),
        http::Response::unauthorized(),
    );

    let request = async_graphql::Request::new("query { performHttpRequest }");

    let response = service.handle_query(request).blocking_wait();

    let error = extract_error_string(response);

    assert_eq!(error, "HTTP request failed with status code 401");
}

/// Tests if the service sends the HTTP response to the contract.
#[test]
fn service_sends_http_response_to_contract() {
    let http_response = b"Hello, contract!";

    let mut service = create_service();
    let runtime = Arc::get_mut(&mut service.runtime).expect("Runtime should not be shared");

    runtime.add_expected_http_request(
        http::Request::get(TEST_BASE_URL),
        http::Response::ok(http_response),
    );

    let request = async_graphql::Request::new("mutation { performHttpRequest }");

    service.handle_query(request).blocking_wait();

    let operations = service.runtime.scheduled_operations::<Operation>();

    assert_eq!(
        operations,
        vec![Operation::HandleHttpResponse(http_response.to_vec())]
    );
}

/// Tests if the service requests the contract to perform an HTTP request.
#[test]
fn service_requests_contract_to_perform_http_request() {
    let service = create_service();

    let request = async_graphql::Request::new("mutation { performHttpRequestInContract }");

    service.handle_query(request).blocking_wait();

    let operations = service.runtime.scheduled_operations::<Operation>();

    assert_eq!(operations, vec![Operation::PerformHttpRequest]);
}

/// Creates a [`Service`] instance for testing.
fn create_service() -> Service {
    let runtime = ServiceRuntime::new().with_application_parameters(TEST_BASE_URL.to_owned());

    Service {
        runtime: Arc::new(runtime),
    }
}

/// Extracts the HTTP response bytes from an [`async_graphql::Response`].
fn extract_response_bytes(response: async_graphql::Response) -> Vec<u8> {
    assert!(response.errors.is_empty());

    let async_graphql::Value::Object(response_data) = response.data else {
        panic!("Unexpected response from service: {response:#?}");
    };
    let async_graphql::Value::List(ref response_list) = response_data["performHttpRequest"] else {
        panic!("Unexpected response for `performHttpRequest` query: {response_data:#?}");
    };

    response_list
        .iter()
        .map(|value| {
            let async_graphql::Value::Number(ref number) = value else {
                panic!("Unexpected value in response list: {value:#?}");
            };
            number
                .as_i64()
                .expect("Invalid integer in response list: {number:#?}")
                .try_into()
                .expect("Invalid byte in response list: {number:#?}")
        })
        .collect()
}

/// Extracts the GraphQL error message from an [`async_graphql::Response`].
fn extract_error_string(response: async_graphql::Response) -> String {
    assert_matches!(response.data, async_graphql::Value::Null);

    let mut errors = response.errors;

    assert_eq!(
        errors.len(),
        1,
        "Unexpected error list from service: {errors:#?}"
    );

    errors
        .pop()
        .expect("There should be exactly one error, as asserted above")
        .message
}
