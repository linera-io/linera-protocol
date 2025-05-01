// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests that perform real HTTP requests to a local HTTP server.

#![cfg(not(target_arch = "wasm32"))]

use assert_matches::assert_matches;
use axum::{routing::get, Router};
use how_to_perform_http_requests::Abi;
use linera_sdk::test::{
    ExecutionError, HttpServer, QueryOutcome, TestValidator, WasmExecutionError,
};

/// Tests if service query performs HTTP request to allowed host.
#[test_log::test(tokio::test)]
async fn service_query_performs_http_request() -> anyhow::Result<()> {
    const HTTP_RESPONSE_BODY: &str = "Hello, world!";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY }))).await?;
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    let QueryOutcome { response, .. } = chain
        .graphql_query(application_id, "query { performHttpRequest }")
        .await;

    let Some(byte_list) = response["performHttpRequest"].as_array() else {
        panic!("Expected a list of bytes representing the response body, got {response:#}");
    };

    let bytes = byte_list
        .iter()
        .map(|value| {
            value
                .as_i64()
                .ok_or(())
                .and_then(|integer| integer.try_into().map_err(|_| ()))
        })
        .collect::<Result<Vec<u8>, _>>()
        .unwrap_or_else(|()| {
            panic!("Expected a list of bytes representing the response body, got {byte_list:#?}")
        });

    assert_eq!(bytes, HTTP_RESPONSE_BODY.as_bytes());

    Ok(())
}

/// Tests if service query can't perform HTTP requests to hosts that aren't allowed.
#[test_log::test(tokio::test)]
async fn service_query_cant_send_http_request_to_unauthorized_host() {
    let url = "http://localhost/";

    let (_validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url.to_owned(), ()).await;

    let error = chain
        .try_graphql_query(application_id, "query { performHttpRequest }")
        .await
        .expect_err("Expected GraphQL query to fail");

    assert_matches!(
        error.expect_execution_error(),
        ExecutionError::UnauthorizedHttpRequest(attempted_url)
            if attempted_url.to_string() == url
    );
}

/// Tests if the service sends a valid HTTP response to the contract.
#[test_log::test(tokio::test)]
async fn service_sends_valid_http_response_to_contract() -> anyhow::Result<()> {
    const HTTP_RESPONSE_BODY: &str = "Hello, world!";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY }))).await?;
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    chain
        .graphql_mutation(application_id, "mutation { performHttpRequest }")
        .await;

    Ok(())
}

/// Tests if the contract rejects an invalid HTTP response sent by the service.
#[test_log::test(tokio::test)]
async fn contract_rejects_invalid_http_response_from_service() {
    const HTTP_RESPONSE_BODY: &str = "Untrusted response";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY })))
            .await
            .expect("Failed to start test HTTP server");
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url.clone(), ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    let error = chain
        .try_graphql_mutation(application_id, "mutation { performHttpRequest }")
        .await
        .expect_err("Expected GraphQL mutation to fail");

    assert_matches!(
        error.expect_proposal_execution_error(0),
        ExecutionError::WasmError(WasmExecutionError::ExecuteModule(_))
    );
}

/// Tests if the contract accepts a valid HTTP response it obtains by itself.
#[test_log::test(tokio::test)]
async fn contract_accepts_valid_http_response_it_obtains_by_itself() -> anyhow::Result<()> {
    const HTTP_RESPONSE_BODY: &str = "Hello, world!";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY }))).await?;
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    chain
        .graphql_mutation(application_id, "mutation { performHttpRequestInContract }")
        .await;

    Ok(())
}

/// Tests if the contract rejects an invalid HTTP response it obtains by itself.
#[test_log::test(tokio::test)]
async fn contract_rejects_invalid_http_response_it_obtains_by_itself() {
    const HTTP_RESPONSE_BODY: &str = "Untrusted response";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY })))
            .await
            .expect("Failed to start test HTTP server");
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    let error = chain
        .try_graphql_mutation(application_id, "mutation { performHttpRequestInContract }")
        .await
        .expect_err("Expected GraphQL mutation to fail");

    assert_matches!(
        error.expect_proposal_execution_error(0),
        ExecutionError::WasmError(WasmExecutionError::ExecuteModule(_))
    );
}

/// Tests if the contract accepts a valid HTTP response it obtains from the service acting as an
/// oracle.
#[test_log::test(tokio::test)]
async fn contract_accepts_valid_http_response_from_oracle() -> anyhow::Result<()> {
    const HTTP_RESPONSE_BODY: &str = "Hello, world!";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY }))).await?;
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    chain
        .graphql_mutation(application_id, "mutation { performHttpRequestAsOracle }")
        .await;

    Ok(())
}

/// Tests if the contract rejects an invalid HTTP response it obtains from the service acting as an
/// oracle.
#[test_log::test(tokio::test)]
async fn contract_rejects_invalid_http_response_from_oracle() {
    const HTTP_RESPONSE_BODY: &str = "Invalid response";

    let http_server =
        HttpServer::start(Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY })))
            .await
            .expect("Failed to start test HTTP server");
    let port = http_server.port();
    let url = format!("http://localhost:{port}/");

    let (mut validator, application_id, chain) =
        TestValidator::with_current_application::<Abi, _, _>(url, ()).await;

    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    let error = chain
        .try_graphql_mutation(application_id, "mutation { performHttpRequestAsOracle }")
        .await
        .expect_err("Expected GraphQL mutation to fail");

    assert_matches!(
        error.expect_proposal_execution_error(0),
        ExecutionError::WasmError(WasmExecutionError::ExecuteModule(_))
    );
}
