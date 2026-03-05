// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use random_number::RandomNumberAbi;

use self::state::RandomNumberState;

pub struct RandomNumberContract {
    state: RandomNumberState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(RandomNumberContract);

impl WithContractAbi for RandomNumberContract {
    type Abi = RandomNumberAbi;
}

impl Contract for RandomNumberContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = RandomNumberState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RandomNumberContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {}

    async fn execute_operation(&mut self, _operation: ()) {
        let seed1 = self.runtime.random_number();
        let seed2 = self.runtime.random_number();
        assert_ne!(seed1, seed2, "Two consecutive random seeds must be distinct");

        let mut rng1 = StdRng::seed_from_u64(seed1);
        let mut rng2 = StdRng::seed_from_u64(seed2);
        let val1: u64 = rng1.gen();
        let val2: u64 = rng2.gen();
        assert_ne!(val1, val2, "Random values from differently seeded RNGs must be distinct");

        self.state.seed1.set(seed1);
        self.state.seed2.set(seed2);
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("RandomNumber application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
