// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use linera_sdk::{linera_base_types::WithServiceAbi, Service, ServiceRuntime};
use test_runtime::{TestRuntimeOperation, TestRuntimeRequest};

pub struct TestRuntimeService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(TestRuntimeService);

impl WithServiceAbi for TestRuntimeService {
    type Abi = test_runtime::TestRuntimeAbi;
}

impl Service for TestRuntimeService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        TestRuntimeService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: TestRuntimeRequest) {
        match request {
            TestRuntimeRequest::Test => {
                let operation = TestRuntimeOperation::Test;
                self.runtime.schedule_operation(&operation);
            }
        }
    }
}
