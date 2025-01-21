// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::ComplexObject;
use hex_game::{Board, Clock, HexAbi, HexOutcome, Operation, Timeouts};
use linera_sdk::{
    base::{
        Amount, ApplicationPermissions, ChainId, ChainOwnership, Owner, TimeoutConfig,
        WithContractAbi,
    },
    views::{RootView, View},
    Contract, ContractRuntime,
};
use serde::{Deserialize, Serialize};
use state::{GameChain, HexState};

pub struct HexContract {
    state: HexState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(HexContract);

impl WithContractAbi for HexContract {
    type Abi = HexAbi;
}

impl Contract for HexContract {
    type Message = Message;
    type InstantiationArgument = Timeouts;
    type Parameters = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = HexState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        HexContract { state, runtime }
    }

    async fn instantiate(&mut self, arg: Timeouts) {
        log::trace!("Instantiating");
        self.runtime.application_parameters(); // Verifies that these are empty.
        self.state.timeouts.set(arg);
    }

    async fn execute_operation(&mut self, operation: Operation) -> HexOutcome {
        log::trace!("Handling operation {:?}", operation);
        let outcome = match operation {
            Operation::MakeMove { x, y } => self.execute_make_move(x, y),
            Operation::ClaimVictory => self.execute_claim_victory(),
            Operation::Start {
                players,
                board_size,
                fee_budget,
                timeouts,
            } => {
                self.execute_start(players, board_size, fee_budget, timeouts)
                    .await
            }
        };
        self.handle_winner(outcome)
    }

    async fn execute_message(&mut self, message: Message) {
        log::trace!("Handling message {:?}", message);
        match message {
            Message::Start {
                players,
                board_size,
                timeouts,
            } => {
                let clock = Clock::new(self.runtime.system_time(), &timeouts);
                self.state.clock.set(clock);
                self.state.owners.set(Some(players));
                self.state.board.set(Board::new(board_size));
            }
            Message::End { winner, loser } => {
                let message_id = self.runtime.message_id().unwrap();
                for owner in [&winner, &loser] {
                    let chain_set = self
                        .state
                        .game_chains
                        .get_mut_or_default(owner)
                        .await
                        .unwrap();
                    chain_set.retain(|game_chain| game_chain.chain_id != message_id.chain_id);
                    if chain_set.is_empty() {
                        self.state.game_chains.remove(owner).unwrap();
                    }
                }
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl HexContract {
    fn execute_make_move(&mut self, x: u16, y: u16) -> HexOutcome {
        assert!(self.runtime.chain_id() != self.main_chain_id());
        let active = self.state.board.get().active_player();
        let clock = self.state.clock.get_mut();
        let block_time = self.runtime.system_time();
        assert_eq!(
            self.runtime.authenticated_signer(),
            Some(self.state.owners.get().unwrap()[active.index()]),
            "Move must be signed by the player whose turn it is."
        );
        self.runtime
            .assert_before(block_time.saturating_add(clock.block_delay));
        clock.make_move(block_time, active);
        self.state.board.get_mut().make_move(x, y)
    }

    fn execute_claim_victory(&mut self) -> HexOutcome {
        assert!(self.runtime.chain_id() != self.main_chain_id());
        let active = self.state.board.get().active_player();
        let clock = self.state.clock.get_mut();
        let block_time = self.runtime.system_time();
        assert_eq!(
            self.runtime.authenticated_signer(),
            Some(self.state.owners.get().unwrap()[active.other().index()]),
            "Victory can only be claimed by the player whose turn it is not."
        );
        assert!(
            clock.timed_out(block_time, active),
            "Player has not timed out yet."
        );
        assert!(
            self.state.board.get().winner().is_none(),
            "The game has already ended."
        );
        HexOutcome::Winner(active.other())
    }

    async fn execute_start(
        &mut self,
        players: [Owner; 2],
        board_size: u16,
        fee_budget: Amount,
        timeouts: Option<Timeouts>,
    ) -> HexOutcome {
        assert_eq!(self.runtime.chain_id(), self.main_chain_id());
        let ownership = ChainOwnership::multiple(
            [(players[0], 100), (players[1], 100)],
            100,
            TimeoutConfig::default(),
        );
        let app_id = self.runtime.application_id();
        let permissions = ApplicationPermissions::new_single(app_id.forget_abi());
        let (message_id, chain_id) = self.runtime.open_chain(ownership, permissions, fee_budget);
        for owner in &players {
            self.state
                .game_chains
                .get_mut_or_default(owner)
                .await
                .unwrap()
                .insert(GameChain {
                    message_id,
                    chain_id,
                });
        }
        self.runtime.send_message(
            chain_id,
            Message::Start {
                players,
                board_size,
                timeouts: timeouts.unwrap_or_else(|| self.state.timeouts.get().clone()),
            },
        );
        HexOutcome::Ok
    }

    fn handle_winner(&mut self, outcome: HexOutcome) -> HexOutcome {
        let HexOutcome::Winner(player) = outcome else {
            return outcome;
        };
        let winner = self.state.owners.get().unwrap()[player.index()];
        let loser = self.state.owners.get().unwrap()[player.other().index()];
        let chain_id = self.main_chain_id();
        let message = Message::End { winner, loser };
        self.runtime.send_message(chain_id, message);
        self.runtime.close_chain().unwrap();
        outcome
    }

    fn main_chain_id(&mut self) -> ChainId {
        self.runtime.application_creator_chain_id()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// Initializes a game. Sent from the main chain to a temporary chain.
    Start {
        /// The players.
        players: [Owner; 2],
        /// The side length of the board. A typical size is 11.
        board_size: u16,
        /// Settings that determine how much time the players have to think about their turns.
        timeouts: Timeouts,
    },
    /// Reports the outcome of a game. Sent from a closed chain to the main chain.
    End { winner: Owner, loser: Owner },
}

/// This implementation is only nonempty in the service.
#[ComplexObject]
impl HexState {}
