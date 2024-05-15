// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use hex_game::{Board, HexAbi, MoveOutcome, Operation};
use linera_sdk::{
    base::{Owner, WithContractAbi},
    views::{RootView, View, ViewStorageContext},
    Contract, ContractRuntime,
};
use state::HexState;

pub struct HexContract {
    state: HexState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(HexContract);

impl WithContractAbi for HexContract {
    type Abi = HexAbi;
}

impl Contract for HexContract {
    type Message = ();
    type InstantiationArgument = ([Owner; 2], u16);
    type Parameters = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = HexState::load(ViewStorageContext::from(runtime.key_value_store()))
            .await
            .expect("Failed to load state");
        HexContract { state, runtime }
    }

    async fn instantiate(&mut self, (owners, size): Self::InstantiationArgument) {
        self.runtime.application_parameters(); // Verifies that these are empty.
        self.state.owners.set(Some(owners));
        self.state.board.set(Board::new(size));
    }

    async fn execute_operation(&mut self, operation: Operation) -> MoveOutcome {
        let Operation::MakeMove { x, y } = operation;
        let active = self.state.board.get().active_player();
        assert_eq!(
            self.runtime.authenticated_signer(),
            Some(self.state.owners.get().unwrap()[active.index()]),
            "Move must be signed by the player whose turn it is."
        );
        self.state.board.get_mut().make_move(x, y)
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("The Hex application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
