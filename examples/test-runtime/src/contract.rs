// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{
    linera_base_types::{ResourceControlPolicy, WithContractAbi},
    Contract, ContractRuntime,
};
use test_runtime::{TestRuntimeAbi, TestRuntimeOperation};

pub struct TestRuntimeContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(TestRuntimeContract);

impl WithContractAbi for TestRuntimeContract {
    type Abi = TestRuntimeAbi;
}

impl Contract for TestRuntimeContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        TestRuntimeContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {}

    async fn execute_operation(&mut self, operation: TestRuntimeOperation) {
        match operation {
            TestRuntimeOperation::Test => {
                let policy: ResourceControlPolicy = self.runtime.resource_control_policy();
                assert_eq!(policy, ResourceControlPolicy::default());
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("TestRuntime application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}

#[cfg(test)]
mod tests {
    use futures::FutureExt as _;
    use linera_sdk::{Contract, ContractRuntime};

    use super::{TestRuntimeContract, TestRuntimeOperation};

    #[test]
    fn operation() {
        let runtime = ContractRuntime::new().with_application_parameters(());

        let mut contract = TestRuntimeContract { runtime };

        let operation = TestRuntimeOperation::Test;

        contract
            .execute_operation(operation)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");
    }
}
