// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use contract_call::{ContractTransferAbi, Operation, Parameters};
use linera_sdk::{
    linera_base_types::{Account, ApplicationId, WithContractAbi},
    Contract, ContractRuntime,
};

pub struct ContractTransferContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(ContractTransferContract);

impl WithContractAbi for ContractTransferContract {
    type Abi = ContractTransferAbi;
}

impl Contract for ContractTransferContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = Parameters;
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        ContractTransferContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::DirectTransfer {
                source,
                destination,
                amount,
            } => {
                // Direct transfer using runtime.transfer
                self.runtime.transfer(source, destination, amount);
            }
            Operation::IndirectTransfer {
                source,
                destination,
                amount,
            } => {
                // Indirect transfer: create application, transfer to it, then transfer from it
                let module_id = self.runtime.application_parameters().module_id;
                //                let typed_module_id: ModuleId<ContractTransferAbi, Parameters, ()> = module_id.with_abi();

                // Create a new application instance
                let parameters = self.runtime.application_parameters();
                let application_id = self
                    .runtime
                    .create_application::<ContractTransferAbi, Parameters, ()>(
                        module_id,
                        &parameters,
                        &(),
                        vec![],
                    );

                // Transfer from source to the created application
                let chain_id = self.runtime.chain_id();
                let app_account = Account {
                    chain_id,
                    owner: application_id.into(),
                };
                self.runtime.transfer(source, app_account, amount);

                // Authenticated calls should be fully visible.
                let operation = Operation::TestSomeAuthenticatedSignerCaller;
                self.runtime
                    .call_application(true, application_id, &operation);

                // Non-authenticated calls should be fully non-visible
                let operation = Operation::TestNoneAuthenticatedSignerCaller;
                self.runtime
                    .call_application(false, application_id, &operation);

                // Non-authenticated calls should be fully non-visible
                let operation = Operation::TestNoneAuthenticatedSignerCaller;
                self.runtime
                    .call_application(false, application_id, &operation);

                // Authenticated calls should be fully visible.
                let operation = Operation::TestSomeAuthenticatedSignerCaller;
                self.runtime
                    .call_application(true, application_id, &operation);

                // Call the created application to transfer from itself to destination.
                // Authenticated or not, the system is able to access the caller
                // for making the transfer.
                let operation = Operation::DirectTransfer {
                    source: ApplicationId::into(application_id),
                    destination,
                    amount,
                };
                self.runtime
                    .call_application(false, application_id, &operation);
            }
            Operation::TestNoneAuthenticatedSignerCaller => {
                assert!(self.runtime.authenticated_signer().is_none());
                assert!(self.runtime.authenticated_caller_id().is_none());
            }
            Operation::TestSomeAuthenticatedSignerCaller => {
                assert!(self.runtime.authenticated_signer().is_some());
                assert!(self.runtime.authenticated_caller_id().is_some());
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Contract transfer application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
