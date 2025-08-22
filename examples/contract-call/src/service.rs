// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use contract_call::{ContractTransferAbi, Operation, Parameters, Query};
use linera_sdk::{linera_base_types::WithServiceAbi, Service, ServiceRuntime};

pub struct ContractTransferService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(ContractTransferService);

impl WithServiceAbi for ContractTransferService {
    type Abi = ContractTransferAbi;
}

impl Service for ContractTransferService {
    type Parameters = Parameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        ContractTransferService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Query) {
        match request {
            Query::DirectTransfer {
                source,
                destination,
                amount,
            } => {
                let operation = Operation::DirectTransfer {
                    source,
                    destination,
                    amount,
                };
                self.runtime.schedule_operation(&operation);
            }
            Query::IndirectTransfer {
                source,
                destination,
                amount,
            } => {
                let operation = Operation::IndirectTransfer {
                    source,
                    destination,
                    amount,
                };
                self.runtime.schedule_operation(&operation);
            }
        }
    }
}
