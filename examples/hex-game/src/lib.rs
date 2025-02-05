// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Hex game */

use std::iter;

use async_graphql::{Enum, InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    base::{Amount, ContractAbi, Owner, ServiceAbi, TimeDelta, Timestamp},
    graphql::GraphQLMutationRoot,
};
use serde::{Deserialize, Serialize};

pub struct HexAbi;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Make a move, and place a stone onto cell `(x, y)`.
    MakeMove { x: u16, y: u16 },
    /// Claim victory if the opponent has timed out.
    ClaimVictory,
    /// Start a game on a new temporary chain, with the given settings.
    Start {
        /// The public keys of player 1 and 2, respectively.
        players: [Owner; 2],
        /// The side length of the board. A typical size is 11.
        board_size: u16,
        /// An amount transferred to the temporary chain to cover the fees.
        fee_budget: Amount,
        /// Settings that determine how much time the players have to think about their turns.
        /// If this is `None`, the defaults are used.
        timeouts: Option<Timeouts>,
    },
}

impl ContractAbi for HexAbi {
    type Operation = Operation;
    type Response = HexOutcome;
}

impl ServiceAbi for HexAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Settings that determine how much time the players have to think about their turns.
#[derive(Clone, Debug, Deserialize, Serialize, SimpleObject, InputObject)]
#[graphql(input_name = "TimeoutsInput")]
#[serde(rename_all = "camelCase")]
pub struct Timeouts {
    /// The initial time each player has to think about their turns.
    pub start_time: TimeDelta,
    /// The duration that is added to the clock after each turn.
    pub increment: TimeDelta,
    /// The maximum time that is allowed to pass between a block proposal and validation.
    /// This should be long enough to confirm a block, but short enough for the block timestamp
    /// to accurately reflect the current time.
    pub block_delay: TimeDelta,
}

impl Default for Timeouts {
    fn default() -> Timeouts {
        Timeouts {
            start_time: TimeDelta::from_secs(60),
            increment: TimeDelta::from_secs(30),
            block_delay: TimeDelta::from_secs(5),
        }
    }
}

/// A clock to track both players' time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SimpleObject)]
pub struct Clock {
    time_left: [TimeDelta; 2],
    increment: TimeDelta,
    current_turn_start: Timestamp,
    pub block_delay: TimeDelta,
}

impl Clock {
    /// Initializes the clock.
    pub fn new(block_time: Timestamp, timeouts: &Timeouts) -> Self {
        Self {
            time_left: [timeouts.start_time, timeouts.start_time],
            increment: timeouts.increment,
            current_turn_start: block_time,
            block_delay: timeouts.block_delay,
        }
    }

    /// Records a player making a move in the current block.
    pub fn make_move(&mut self, block_time: Timestamp, player: Player) {
        let duration = block_time.delta_since(self.current_turn_start);
        let i = player.index();
        assert!(self.time_left[i] >= duration);
        self.time_left[i] = self.time_left[i]
            .saturating_sub(duration)
            .saturating_add(self.increment);
        self.current_turn_start = block_time;
    }

    /// Returns whether the given player has timed out.
    pub fn timed_out(&self, block_time: Timestamp, player: Player) -> bool {
        self.time_left[player.index()] < block_time.delta_since(self.current_turn_start)
    }
}

/// The outcome of a valid move.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HexOutcome {
    /// A player wins the game.
    Winner(Player),
    /// The game continues.
    Ok,
}

/// A player: `One` or `Two`
///
/// It's player `One`'s turn whenever the number of stones on the board is even, so they make
/// the first move. Otherwise it's `Two`'s turn.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum Player {
    #[default]
    /// Player one
    One,
    /// Player two
    Two,
}

impl Player {
    /// Returns the opponent of `self`.
    pub fn other(self) -> Self {
        match self {
            Player::One => Player::Two,
            Player::Two => Player::One,
        }
    }

    /// Returns `0` for player `One` and `1` for player `Two`.
    pub fn index(&self) -> usize {
        match self {
            Player::One => 0,
            Player::Two => 1,
        }
    }
}

/// The state of a cell on the board.
#[derive(Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, SimpleObject)]
pub struct Cell {
    /// `None` if the cell is empty; otherwise the player who placed a stone here.
    stone: Option<Player>,
    /// This is `true` if the cell belongs to player `One` and is connected to the left edge
    /// of the board via other cells containing a stone placed by player `One`, or if it
    /// belongs to player `Two` and is connected to the top edge of the board via other cells
    /// containing stones placed by player `Two`.
    ///
    /// So the game ends if this is `true` for any cell at the right or bottom edge.
    connected: bool,
}

impl Cell {
    /// Returns whether this cell is connected to the top or left edge and belongs to the
    /// given player.
    fn is_connected(&self, player: Player) -> bool {
        self.connected && self.stone == Some(player)
    }
}

/// The state of a Hex game.
#[derive(Clone, Default, Serialize, Deserialize, SimpleObject)]
pub struct Board {
    /// The cells, row-by-row.
    ///
    /// Cell `(x, y)` has index `(x + y * size)`.
    cells: Vec<Cell>,
    /// The width and height of the board, in cells.
    size: u16,
    /// The player whose turn it is. If the game has ended, this player loses.
    active: Player,
}

impl Board {
    /// Creates a new board with the given size and player owners.
    pub fn new(size: u16) -> Self {
        let size_usize = usize::from(size);
        let cell_count = size_usize
            .checked_mul(size_usize)
            .expect("Board is too large.");
        let cells = vec![Cell::default(); cell_count];
        Board {
            size,
            cells,
            active: Player::One,
        }
    }

    /// Updates the board: The active player places a stone on cell `(x, y)`.
    ///
    /// Panics if the move is invalid.
    pub fn make_move(&mut self, x: u16, y: u16) -> HexOutcome {
        assert!(self.winner().is_none(), "Game has ended.");
        assert!(x < self.size && y < self.size, "Invalid coordinates.");
        assert!(self.cell(x, y).stone.is_none(), "Cell is not empty.");
        self.place_stone(x, y);
        if let Some(winner) = self.winner() {
            return HexOutcome::Winner(winner);
        }
        HexOutcome::Ok
    }

    /// Places the active player's stone on the given cell, updates all cells'
    /// `connected` flags accordingly. The new cell _must_ be empty!
    fn place_stone(&mut self, x: u16, y: u16) {
        let player = self.active;
        self.cell_mut(x, y).stone = Some(player);
        self.active = player.other();
        if !((x == 0 && player == Player::One)
            || (y == 0 && player == Player::Two)
            || self
                .neighbors(x, y)
                .any(|(nx, ny)| self.cell(nx, ny).is_connected(player)))
        {
            return;
        }
        let mut stack = vec![(x, y)];
        while let Some((x, y)) = stack.pop() {
            self.cell_mut(x, y).connected = true;
            stack.extend(self.neighbors(x, y).filter(|(nx, ny)| {
                let cell = self.cell(*nx, *ny);
                !cell.connected && cell.stone == Some(player)
            }));
        }
    }

    /// Returns the winner, or `None` if the game is still in progress.
    pub fn winner(&self) -> Option<Player> {
        let s = self.size - 1;
        for i in 0..self.size {
            if self.cell(s, i).is_connected(Player::One) {
                return Some(Player::One);
            }
            if self.cell(i, s).is_connected(Player::Two) {
                return Some(Player::Two);
            }
        }
        None
    }

    /// Returns the `Owner` controlling the active player.
    pub fn active_player(&self) -> Player {
        self.active
    }

    /// Returns the cell `(x, y)`.
    fn cell(&self, x: u16, y: u16) -> Cell {
        self.cells[x as usize + (y as usize) * (self.size as usize)]
    }

    /// Returns a mutable reference to cell `(x, y)`.
    fn cell_mut(&mut self, x: u16, y: u16) -> &mut Cell {
        &mut self.cells[x as usize + (y as usize) * (self.size as usize)]
    }

    /// Returns all neighbors of cell `(x, y)`, in the order:
    /// left, top left, right, bottom right, top right, bottom left
    fn neighbors(&self, x: u16, y: u16) -> impl Iterator<Item = (u16, u16)> {
        iter::empty()
            .chain((x > 0).then(|| (x - 1, y)))
            .chain((y > 0).then(|| (x, y - 1)))
            .chain((x + 1 < self.size).then(|| (x + 1, y)))
            .chain((y + 1 < self.size).then(|| (x, y + 1)))
            .chain((x + 1 < self.size && y > 0).then(|| (x + 1, y - 1)))
            .chain((y + 1 < self.size && x > 0).then(|| (x - 1, y + 1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_game() {
        let mut board = Board::new(3);
        // Make a few moves; no winner yet:
        // 0 2 2
        //  2 1 0
        //   1 0 1
        assert_eq!(Player::One, board.active_player());
        assert_eq!(HexOutcome::Ok, board.make_move(0, 2));
        assert_eq!(Player::Two, board.active_player());
        assert_eq!(HexOutcome::Ok, board.make_move(0, 1));
        assert_eq!(HexOutcome::Ok, board.make_move(1, 1));
        assert_eq!(HexOutcome::Ok, board.make_move(1, 0));
        assert_eq!(HexOutcome::Ok, board.make_move(2, 2));
        assert_eq!(HexOutcome::Ok, board.make_move(2, 0));
        assert!(board.winner().is_none());
        // Player 1 connects left to right and wins:
        assert_eq!(HexOutcome::Winner(Player::One), board.make_move(2, 1));
        assert_eq!(Some(Player::One), board.winner());
    }
}
