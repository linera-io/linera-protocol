// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{DataBlobHash, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use publish_read_data_blob::{Operation, PublishReadDataBlobAbi};

use self::state::PublishReadDataBlobState;

pub struct PublishReadDataBlobContract {
    state: PublishReadDataBlobState,
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
        let state = PublishReadDataBlobState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        PublishReadDataBlobContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::CreateDataBlob(data) => {
                let hash: DataBlobHash = self.runtime.create_data_blob(data);
                self.state.hash.set(Some(hash));
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
                self.state.hash.set(Some(hash));
                let data_read = self.runtime.read_data_blob(hash);
                assert_eq!(data_read, data);
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Publish-Read Data Blob application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
