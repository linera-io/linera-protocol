// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{DataBlobHash, WithContractAbi},
    Contract, ContractRuntime,
};
use publish_read_data_blob::{Operation, PublishReadDataBlobAbi};

pub struct PublishReadDataBlobContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(PublishReadDataBlobContract);

impl WithContractAbi for PublishReadDataBlobContract {
    type Abi = PublishReadDataBlobAbi;
}

impl Contract for PublishReadDataBlobContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        PublishReadDataBlobContract { runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::CreateDataBlob(data) => {
                self.runtime.create_data_blob(data);
            }
            Operation::ReadDataBlob(hash, expected_data) => {
                let data = self.runtime.read_data_blob(hash);
                assert_eq!(
                    data, expected_data,
                    "Read data does not match expected data"
                );
            }
            Operation::CreateAndReadDataBlob(data) => {
                let hash: DataBlobHash = self.runtime.create_data_blob(data.clone());
                let data_read = self.runtime.read_data_blob(hash);
                assert_eq!(data_read, data);
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Publish-Read Data Blob application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
