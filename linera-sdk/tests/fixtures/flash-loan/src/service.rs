// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{linera_base_types::WithServiceAbi, Service, ServiceRuntime};

pub struct FlashLoanService;

linera_sdk::service!(FlashLoanService);

impl WithServiceAbi for FlashLoanService {
    type Abi = flash_loan::FlashLoanAbi;
}

impl Service for FlashLoanService {
    type Parameters = flash_loan::FlashLoanParameters;

    async fn new(_runtime: ServiceRuntime<Self>) -> Self {
        FlashLoanService
    }

    async fn handle_query(&self, _query: ()) {}
}
