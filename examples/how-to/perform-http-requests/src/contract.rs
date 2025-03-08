// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use how_to_perform_http_requests::{Abi, Operation};
use linera_sdk::{http, linera_base_types::WithContractAbi, Contract as _, ContractRuntime};

pub struct Contract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(Contract);

impl WithContractAbi for Contract {
    type Abi = Abi;
}

impl linera_sdk::Contract for Contract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = String;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        Contract { runtime }
    }

    async fn instantiate(&mut self, (): Self::InstantiationArgument) {
        // Check that the global parameters can be deserialized correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            Operation::HandleHttpResponse(response_body) => {
                self.handle_http_response(response_body)
            }
            Operation::PerformHttpRequest => self.perform_http_request(),
        }
    }

    async fn execute_message(&mut self, (): Self::Message) {
        panic!("This application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}

impl Contract {
    /// Handles an HTTP response, ensuring it is valid.
    ///
    /// Because the `response_body` can come from outside the contract in an
    /// [`Operation::HandleHttpResponse`], it could be forged. Therefore, the contract should
    /// assume that the `response_body` is untrusted, and should perform validation and
    /// verification steps to ensure that the `response_body` is real and can be trusted.
    ///
    /// Usually this is done by verifying that the response is signed by the trusted HTTP server.
    /// In this example, the verification is simulated by checking that the `response_body` is
    /// exactly an expected value.
    fn handle_http_response(&self, response_body: Vec<u8>) {
        assert_eq!(response_body, b"Hello, world!");
    }

    /// Performs an HTTP request directly in the contract.
    ///
    /// This only works if the HTTP response (including any HTTP headers the response contains) is
    /// the same in a quorum of validators. Otherwise, the contract should call the service as an
    /// oracle to perform the HTTP request and the service should only return the data that will be
    /// the same in a quorum of validators.
    fn perform_http_request(&mut self) {
        let url = self.runtime.application_parameters();
        let response = self.runtime.http_request(http::Request::get(url));

        self.handle_http_response(response.body);
    }
}

#[path = "unit_tests/contract.rs"]
mod unit_tests;
