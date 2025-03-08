// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(test)]

//! Unit tests for the contract.

use how_to_perform_http_requests::Operation;
use linera_sdk::{http, util::BlockingWait as _, Contract as _, ContractRuntime};

use super::Contract;

/// Tests if the contract accepts a valid HTTP response obtained off-chain.
///
/// The contract should not panic if it receives a HTTP response that it can trust. In
/// this example application, that just means an HTTP response exactly to one the contract
/// expects, but in most applications this would involve signing the response in the HTTP
/// server and checking the signature in the contract.
#[test]
fn accepts_valid_off_chain_response() {
    let mut contract = create_contract();

    contract
        .execute_operation(Operation::HandleHttpResponse(b"Hello, world!".to_vec()))
        .blocking_wait();
}

/// Tests if the contract rejects an invalid HTTP response obtained off-chain.
///
/// The contract should panic if it receives a HTTP response that it can't trust. In
/// this example application, that just means an HTTP response different from one it
/// expects, but in most applications this would involve checking the signature of the
/// response to see if it was signed by a trusted party that created the response.
#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn rejects_invalid_off_chain_response() {
    let mut contract = create_contract();

    contract
        .execute_operation(Operation::HandleHttpResponse(b"Fake response".to_vec()))
        .blocking_wait();
}

/// Tests if the contract performs an HTTP request and accepts it if it receives a valid
/// response.
#[test]
fn accepts_response_obtained_by_contract() {
    let url = "http://some.test.url".to_owned();
    let mut contract = create_contract();

    contract
        .runtime
        .set_application_parameters(url.clone())
        .add_expected_http_request(
            http::Request::get(url),
            http::Response::ok(b"Hello, world!".to_vec()),
        );

    contract
        .execute_operation(Operation::PerformHttpRequest)
        .blocking_wait();
}

/// Creates a [`Contract`] instance for testing.
fn create_contract() -> Contract {
    let runtime = ContractRuntime::new();

    Contract { runtime }
}
