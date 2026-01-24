// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::ComplexObject;
use gol_challenge::{GolChallengeAbi, Message, Operation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use state::{GolChallengeState, Solution};

pub struct GolChallengeContract {
    state: GolChallengeState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(GolChallengeContract);

impl WithContractAbi for GolChallengeContract {
    type Abi = GolChallengeAbi;
}

impl Contract for GolChallengeContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        GolChallengeContract { state, runtime }
    }

    async fn instantiate(&mut self, _arg: ()) {
        log::trace!("Instantiating");
        self.runtime.application_parameters(); // Verifies that these are empty.
    }

    async fn execute_operation(&mut self, operation: Operation) {
        log::trace!("Handling operation {:?}", operation);
        let Operation::SubmitSolution {
            puzzle_id,
            board,
            steps,
        } = operation;
        let puzzle_bytes = self.runtime.read_data_blob(puzzle_id);
        let puzzle = bcs::from_bytes(&puzzle_bytes).expect("Deserialize puzzle");
        board
            .check_puzzle(&puzzle, steps)
            .expect("Invalid solution");
        let timestamp = self.runtime.system_time();
        let solution = Solution {
            puzzle_id,
            board,
            steps,
            timestamp,
        };
        self.state
            .solutions
            .insert(&puzzle_id, solution)
            .expect("Store solution");
    }

    async fn execute_message(&mut self, message: Message) {
        log::trace!("Handling message {:?}", message);
        unreachable!();
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

/// This implementation is only nonempty in the service.
#[ComplexObject]
impl GolChallengeState {}
