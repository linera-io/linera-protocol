// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! Main library for the Game-of-Life challenge. Should compile for Wasm and Rust. */

#![deny(missing_docs)]

use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    ops::Range,
};

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, ServiceAbi},
    DataBlobHash,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The ABI of the Game-of-Life challenge.
pub struct GolChallengeAbi;

/// Error type for invalid puzzle solutions.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum InvalidSolution {
    /// The board size does not match the puzzle size.
    #[error("Board size {board_size} does not match puzzle size {puzzle_size}")]
    SizeMismatch {
        /// The size of the board.
        board_size: u16,
        /// The expected size from the puzzle.
        puzzle_size: u16,
    },

    /// The number of steps is outside the allowed range.
    #[error("Steps {steps} is outside the allowed range [{min_steps}, {max_steps}]")]
    StepsOutOfRange {
        /// The number of steps attempted.
        steps: u16,
        /// The minimum allowed steps.
        min_steps: u16,
        /// The maximum allowed steps.
        max_steps: u16,
    },

    /// The initial conditions are not satisfied.
    #[error("Initial conditions are not satisfied")]
    InitialConditionsNotMet,

    /// The final conditions are not satisfied after running the simulation.
    #[error("Final conditions are not satisfied after {steps} steps")]
    FinalConditionsNotMet {
        /// The number of steps that were executed.
        steps: u16,
    },
}

/// Type for on-chain operations of the Game-of-Life challenge.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Submit a solution for verification.
    SubmitSolution {
        /// The ID of the puzzle in blob storage.
        puzzle_id: DataBlobHash,
        /// The board of the solution.
        board: Board,
        /// The number of steps of the solution.
        steps: u16,
    },
}

/// A GoL puzzle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Puzzle {
    /// A title for this puzzle.
    pub title: String,
    /// A summary of the goal of the puzzle.
    pub summary: String,
    /// A summary of the goal of the puzzle.
    pub difficulty: Difficulty,
    /// The grid size.
    pub size: u16,
    /// A minimal number of steps.
    pub minimal_steps: u16,
    /// A maximal number of steps.
    pub maximal_steps: u16,
    /// The initial conditions.
    pub initial_conditions: Vec<Condition>,
    /// The final conditions.
    pub final_conditions: Vec<Condition>,
}

/// The difficulty of a puzzle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Difficulty {
    /// Easy.
    Easy,
    /// Medium difficulty.
    Medium,
    /// Hard.
    Hard,
}

/// A condition on a board.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    /// Testing a single cell.
    Position {
        /// The position being tested.
        position: Position,
        /// The desired state of the cell.
        is_live: bool,
    },
    /// Testing a rectangle of cells.
    Rectangle {
        /// The range of `x`-coordinates being tested.
        x_range: Range<u16>,
        /// The range of `y`-coordinates being tested.
        y_range: Range<u16>,
        /// The minimum number of live cells in the rectangles.
        min_live_count: u32,
        /// The maximum number of live cells in the rectangles.
        max_live_count: u32,
    },
}

impl ContractAbi for GolChallengeAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for GolChallengeAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// A position in the GoL board.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Position {
    /// The first coordinate.
    x: u16,
    /// The second coordinate.
    y: u16,
}

async_graphql::scalar!(Position);

/// The state of a GoL board. We use a sparse encoding for storage efficiency reasons.
#[derive(Debug, Clone, Serialize, Deserialize, InputObject, SimpleObject)]
pub struct Board {
    /// The width and height of the board, in cells.
    size: u16,
    /// The coordinates of the live cells.
    live_cells: Vec<Position>,
}

/// The state of a GoL cell. Used for computations.
#[derive(Default)]
struct Cell {
    /// Odd if the cell is alive, even otherwise. Higher bits may be used for holding the
    /// number of (alive) neighbors.
    value: u8,
}

impl Board {
    /// Creates a new board with the given size and player owners.
    pub fn new(size: u16) -> Self {
        Board {
            size,
            live_cells: Vec::new(),
        }
    }

    /// Returns all neighbors of cell `(x, y)`.
    fn neighbors(&self, position: Position) -> impl Iterator<Item = (u16, u16)> {
        let Position { x, y } = position;
        iter::empty()
            .chain((x > 0 && y > 0).then(|| (x - 1, y - 1)))
            .chain((x > 0).then(|| (x - 1, y)))
            .chain((x > 0 && y + 1 < self.size).then(|| (x - 1, y + 1)))
            .chain((y > 0).then(|| (x, y - 1)))
            .chain((y + 1 < self.size).then(|| (x, y + 1)))
            .chain((x + 1 < self.size && y > 0).then(|| (x + 1, y - 1)))
            .chain((x + 1 < self.size).then(|| (x + 1, y)))
            .chain((x + 1 < self.size && y + 1 < self.size).then(|| (x + 1, y + 1)))
    }

    fn analyze_neighbors(&self) -> BTreeMap<Position, Cell> {
        let mut cells = BTreeMap::<_, Cell>::new();
        for position in &self.live_cells {
            cells.entry(*position).or_default().set_live();
            for (x, y) in self.neighbors(*position) {
                cells
                    .entry(Position { x, y })
                    .or_default()
                    .increment_neighbor_count();
            }
        }
        cells
    }

    /// Apply the GoL rules to advance board by one step.
    pub fn advance_once(&self) -> Self {
        let live_cells = self
            .analyze_neighbors()
            .into_iter()
            .filter_map(|(position, cell)| {
                if cell.should_be_live_next() {
                    Some(position)
                } else {
                    None
                }
            })
            .collect();
        Self {
            size: self.size,
            live_cells,
        }
    }

    /// Apply the GoL rules to advance board by the given number of steps.
    pub fn advance(&self, steps: u16) -> Self {
        let mut board = self.clone();
        for _ in 0..steps {
            board = board.advance_once();
        }
        board
    }

    /// Apply the GoL rules to advance board until the stopping condition is met. Return
    /// the board and the number of steps, if we succeeded on or before `max_steps`.
    pub fn advance_until(&self, conditions: &[Condition], max_steps: u16) -> Option<(u16, Self)> {
        let mut board = self.clone();
        for i in 0..=max_steps {
            if board.check_conditions(conditions) {
                return Some((i, board));
            }
            board = board.advance_once();
        }
        None
    }

    fn index_cells(&self) -> BTreeMap<u16, BTreeSet<u16>> {
        let mut index = BTreeMap::<_, BTreeSet<_>>::new();
        for Position { x, y } in self.live_cells.iter().cloned() {
            index.entry(x).or_default().insert(y);
        }
        index
    }

    /// Print the board as ASCII art.
    /// Live cells are represented by '●' and dead cells by '·'.
    pub fn to_string(&self) -> String {
        let live_cells = self.index_cells();
        let mut result = String::new();

        // Add top border with column numbers for reference
        result.push_str("   ");
        for x in 0..self.size {
            result.push_str(&format!("{:2}", x % 10));
        }
        result.push('\n');

        for y in 0..self.size {
            // Add row number for reference
            result.push_str(&format!("{:2} ", y));

            for x in 0..self.size {
                let is_live = live_cells.get(&x).map_or(false, |set| set.contains(&y));
                result.push(' ');
                if is_live {
                    result.push('●');
                } else {
                    result.push('·');
                }
            }
            result.push('\n');
        }

        result
    }

    /// Print the board as compact ASCII art without numbers.
    /// Live cells are represented by '●' and dead cells by '·'.
    pub fn to_compact_string(&self) -> String {
        let live_cells = self.index_cells();
        let mut result = String::new();

        for y in 0..self.size {
            for x in 0..self.size {
                let is_live = live_cells.get(&x).map_or(false, |set| set.contains(&y));
                if is_live {
                    result.push('●');
                } else {
                    result.push('·');
                }
            }
            result.push('\n');
        }

        result
    }

    fn check_conditions(&self, conditions: &[Condition]) -> bool {
        let live_cells = self.index_cells();
        conditions
            .iter()
            .all(|condition| condition.check(&live_cells))
    }

    /// Check that the board satisfies the given puzzle.
    pub fn check_puzzle(&self, puzzle: &Puzzle, steps: u16) -> Result<(), InvalidSolution> {
        if self.size != puzzle.size {
            return Err(InvalidSolution::SizeMismatch {
                board_size: self.size,
                puzzle_size: puzzle.size,
            });
        }
        if steps < puzzle.minimal_steps || steps > puzzle.maximal_steps {
            return Err(InvalidSolution::StepsOutOfRange {
                steps,
                min_steps: puzzle.minimal_steps,
                max_steps: puzzle.maximal_steps,
            });
        }
        if !self.check_conditions(&puzzle.initial_conditions) {
            return Err(InvalidSolution::InitialConditionsNotMet);
        }
        let final_board = self.advance(steps);
        if final_board.check_conditions(&puzzle.final_conditions) {
            Ok(())
        } else {
            Err(InvalidSolution::FinalConditionsNotMet { steps })
        }
    }
}

impl Cell {
    fn set_live(&mut self) {
        self.value |= 1;
    }

    fn increment_neighbor_count(&mut self) {
        self.value += 2;
    }

    fn should_be_live_next(self) -> bool {
        self.value >= 5 && self.value <= 7
    }
}

impl Condition {
    fn check(&self, live_cells: &BTreeMap<u16, BTreeSet<u16>>) -> bool {
        match self {
            Self::Position { position, is_live } => match live_cells.get(&position.x) {
                Some(set) => *is_live == set.contains(&position.y),
                None => !*is_live,
            },
            Self::Rectangle {
                x_range,
                y_range,
                min_live_count,
                max_live_count,
            } => {
                let mut count = 0;
                for (_, set) in live_cells.range(x_range.clone()) {
                    count += set.range(y_range.clone()).count() as u32;
                    if count > *max_live_count {
                        return false;
                    }
                }
                count >= *min_live_count
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_board_creation() {
        let board = Board::new(10);
        assert_eq!(board.size, 10);
        assert!(board.live_cells.is_empty());
    }

    #[test]
    fn test_neighbors() {
        let board = Board::new(5);
        let position = Position { x: 2, y: 2 };
        let neighbors: Vec<_> = board.neighbors(position).collect();
        assert_eq!(neighbors.len(), 8);
        assert!(neighbors.contains(&(1, 1)));
        assert!(neighbors.contains(&(1, 2)));
        assert!(neighbors.contains(&(1, 3)));
        assert!(neighbors.contains(&(2, 1)));
        assert!(neighbors.contains(&(2, 3)));
        assert!(neighbors.contains(&(3, 1)));
        assert!(neighbors.contains(&(3, 2)));
        assert!(neighbors.contains(&(3, 3)));
    }

    #[test]
    fn test_neighbors_edge_cases() {
        let board = Board::new(5);

        // Corner position (0, 0)
        let corner_neighbors: Vec<_> = board.neighbors(Position { x: 0, y: 0 }).collect();
        assert_eq!(corner_neighbors.len(), 3);
        assert!(corner_neighbors.contains(&(0, 1)));
        assert!(corner_neighbors.contains(&(1, 0)));
        assert!(corner_neighbors.contains(&(1, 1)));

        // Edge position (0, 2)
        let edge_neighbors: Vec<_> = board.neighbors(Position { x: 0, y: 2 }).collect();
        assert_eq!(edge_neighbors.len(), 5);
    }

    #[test]
    fn test_advance_empty_board() {
        let board = Board::new(5);
        let next_board = board.advance_once();
        assert!(next_board.live_cells.is_empty());
    }

    #[test]
    fn test_advance_single_cell_dies() {
        let mut board = Board::new(5);
        board.live_cells.push(Position { x: 2, y: 2 });

        let next_board = board.advance_once();
        assert!(next_board.live_cells.is_empty());
    }

    #[test]
    fn test_advance_block_pattern_stable() {
        // Block pattern (2x2 square) should remain stable
        let mut board = Board::new(5);
        board.live_cells.extend([
            Position { x: 1, y: 1 },
            Position { x: 1, y: 2 },
            Position { x: 2, y: 1 },
            Position { x: 2, y: 2 },
        ]);

        let next_board = board.advance_once();
        assert_eq!(next_board.live_cells.len(), 4);
        assert!(next_board.live_cells.contains(&Position { x: 1, y: 1 }));
        assert!(next_board.live_cells.contains(&Position { x: 1, y: 2 }));
        assert!(next_board.live_cells.contains(&Position { x: 2, y: 1 }));
        assert!(next_board.live_cells.contains(&Position { x: 2, y: 2 }));
    }

    #[test]
    fn test_advance_blinker_pattern() {
        // Blinker pattern should oscillate between horizontal and vertical
        let mut board = Board::new(5);
        board.live_cells.extend([
            Position { x: 2, y: 1 },
            Position { x: 2, y: 2 },
            Position { x: 2, y: 3 },
        ]);

        let next_board = board.advance_once();
        assert_eq!(next_board.live_cells.len(), 3);
        assert!(next_board.live_cells.contains(&Position { x: 1, y: 2 }));
        assert!(next_board.live_cells.contains(&Position { x: 2, y: 2 }));
        assert!(next_board.live_cells.contains(&Position { x: 3, y: 2 }));

        // Advance again to get back to original
        let next_next_board = next_board.advance_once();
        assert_eq!(next_next_board.live_cells.len(), 3);
        assert!(next_next_board
            .live_cells
            .contains(&Position { x: 2, y: 1 }));
        assert!(next_next_board
            .live_cells
            .contains(&Position { x: 2, y: 2 }));
        assert!(next_next_board
            .live_cells
            .contains(&Position { x: 2, y: 3 }));
    }

    #[test]
    fn test_advance_birth_of_new_cell() {
        // Three cells in an L shape should create a fourth cell
        let mut board = Board::new(5);
        board.live_cells.extend([
            Position { x: 1, y: 1 },
            Position { x: 1, y: 2 },
            Position { x: 2, y: 1 },
        ]);

        let next_board = board.advance_once();
        assert!(next_board.live_cells.contains(&Position { x: 2, y: 2 }));
    }

    #[test]
    fn test_cell_should_be_live_next() {
        // Test the game of life rules

        // Dead cell with no neighbors stays dead
        let cell = Cell::default();
        assert!(!cell.should_be_live_next());

        // Dead cell with 3 neighbors becomes alive
        let cell = Cell { value: 6 }; // 3 neighbors * 2 = 6
        assert!(cell.should_be_live_next());

        // Live cell with 2 neighbors stays alive
        let cell = Cell { value: 5 }; // live (1) + 2 neighbors * 2 = 5
        assert!(cell.should_be_live_next());

        // Live cell with 3 neighbors stays alive
        let cell = Cell { value: 7 }; // live (1) + 3 neighbors * 2 = 7
        assert!(cell.should_be_live_next());

        // Live cell with 1 neighbor dies
        let cell = Cell { value: 3 }; // live (1) + 1 neighbor * 2 = 3
        assert!(!cell.should_be_live_next());

        // Live cell with 4 neighbors dies
        let cell = Cell { value: 9 }; // live (1) + 4 neighbors * 2 = 9
        assert!(!cell.should_be_live_next());
    }

    #[test]
    fn test_position_condition_check() {
        let mut live_cells = BTreeMap::new();
        live_cells.insert(1, BTreeSet::from([2, 3]));
        live_cells.insert(2, BTreeSet::from([1]));

        // Test live cell condition
        let condition = Condition::Position {
            position: Position { x: 1, y: 2 },
            is_live: true,
        };
        assert!(condition.check(&live_cells));

        // Test dead cell condition
        let condition = Condition::Position {
            position: Position { x: 1, y: 2 },
            is_live: false,
        };
        assert!(!condition.check(&live_cells));

        // Test empty position
        let condition = Condition::Position {
            position: Position { x: 0, y: 0 },
            is_live: false,
        };
        assert!(condition.check(&live_cells));

        let condition = Condition::Position {
            position: Position { x: 0, y: 0 },
            is_live: true,
        };
        assert!(!condition.check(&live_cells));
    }

    #[test]
    fn test_rectangle_condition_check() {
        let mut live_cells = BTreeMap::new();
        live_cells.insert(1, BTreeSet::from([1, 2]));
        live_cells.insert(2, BTreeSet::from([1, 2, 3]));
        live_cells.insert(3, BTreeSet::from([2]));

        // Rectangle containing 4 cells (at positions (1,1), (1,2), (2,1), (2,2))
        let condition = Condition::Rectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 3,
            max_live_count: 5,
        };
        assert!(condition.check(&live_cells));

        // Same rectangle but expecting too many cells
        let condition = Condition::Rectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 5,
            max_live_count: 10,
        };
        assert!(!condition.check(&live_cells));

        // Same rectangle but allowing too few cells
        let condition = Condition::Rectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 1,
            max_live_count: 3,
        };
        assert!(!condition.check(&live_cells));
    }

    #[test]
    fn test_check_puzzle_size_mismatch() {
        let board = Board::new(5);
        let puzzle = Puzzle {
            title: "Test".to_string(),
            summary: "Test puzzle".to_string(),
            difficulty: Difficulty::Easy,
            size: 10,
            minimal_steps: 1,
            maximal_steps: 5,
            initial_conditions: vec![],
            final_conditions: vec![],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 3),
            Err(InvalidSolution::SizeMismatch {
                board_size: 5,
                puzzle_size: 10
            })
        );
    }

    #[test]
    fn test_check_puzzle_steps_out_of_range() {
        let board = Board::new(5);
        let puzzle = Puzzle {
            title: "Test".to_string(),
            summary: "Test puzzle".to_string(),
            difficulty: Difficulty::Easy,
            size: 5,
            minimal_steps: 2,
            maximal_steps: 4,
            initial_conditions: vec![],
            final_conditions: vec![],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 1),
            Err(InvalidSolution::StepsOutOfRange {
                steps: 1,
                min_steps: 2,
                max_steps: 4
            })
        ); // Too few steps
        assert_eq!(
            board.check_puzzle(&puzzle, 5),
            Err(InvalidSolution::StepsOutOfRange {
                steps: 5,
                min_steps: 2,
                max_steps: 4
            })
        ); // Too many steps
        assert_eq!(board.check_puzzle(&puzzle, 3), Ok(())); // Valid steps
    }

    #[test]
    fn test_check_puzzle_initial_conditions_fail() {
        let board = Board::new(5);
        let puzzle = Puzzle {
            title: "Test".to_string(),
            summary: "Test puzzle".to_string(),
            difficulty: Difficulty::Easy,
            size: 5,
            minimal_steps: 1,
            maximal_steps: 5,
            initial_conditions: vec![Condition::Position {
                position: Position { x: 0, y: 0 },
                is_live: true,
            }],
            final_conditions: vec![],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 3),
            Err(InvalidSolution::InitialConditionsNotMet)
        );
    }

    #[test]
    fn test_check_puzzle_complete_workflow() {
        // Create a board with a blinker pattern
        let mut board = Board::new(5);
        board.live_cells.extend([
            Position { x: 2, y: 1 },
            Position { x: 2, y: 2 },
            Position { x: 2, y: 3 },
        ]);

        let puzzle = Puzzle {
            title: "Blinker Test".to_string(),
            summary: "Test blinker oscillation".to_string(),
            difficulty: Difficulty::Easy,
            size: 5,
            minimal_steps: 1,
            maximal_steps: 3,
            initial_conditions: vec![
                Condition::Position {
                    position: Position { x: 2, y: 1 },
                    is_live: true,
                },
                Condition::Position {
                    position: Position { x: 2, y: 2 },
                    is_live: true,
                },
                Condition::Position {
                    position: Position { x: 2, y: 3 },
                    is_live: true,
                },
            ],
            final_conditions: vec![
                Condition::Position {
                    position: Position { x: 2, y: 1 },
                    is_live: true,
                },
                Condition::Position {
                    position: Position { x: 2, y: 2 },
                    is_live: true,
                },
                Condition::Position {
                    position: Position { x: 2, y: 3 },
                    is_live: true,
                },
            ],
        };

        // Test that after 2 steps (full blinker cycle), we get back to the initial pattern
        assert_eq!(board.check_puzzle(&puzzle, 2), Ok(()));
    }

    #[test]
    fn test_glider_puzzle_with_rectangle_conditions() {
        // Create a 16x16 board divided into 4 squares (8x8 each):
        // Use a glider that travels from bottom-left to top-right

        let mut board = Board::new(16);
        // Place a glider pattern in the bottom-left square.
        board.live_cells.extend([
            Position { x: 1, y: 0 },
            Position { x: 2, y: 1 },
            Position { x: 0, y: 2 },
            Position { x: 1, y: 2 },
            Position { x: 2, y: 2 },
        ]);

        let puzzle = Puzzle {
            title: "Glider Migration".to_string(),
            summary: "Glider travels from top-left to bottom-right square".to_string(),
            difficulty: Difficulty::Hard,
            size: 16,
            minimal_steps: 0,
            maximal_steps: 40,
            initial_conditions: vec![
                // All 5 cells should be in top-left square (0-7, 0-7).
                Condition::Rectangle {
                    x_range: 0..8,
                    y_range: 0..8,
                    min_live_count: 5,
                    max_live_count: 5,
                },
                // No cells elsewhere.
                Condition::Rectangle {
                    x_range: 8..16,
                    y_range: 0..8,
                    min_live_count: 0,
                    max_live_count: 0,
                },
                Condition::Rectangle {
                    x_range: 0..16,
                    y_range: 8..16,
                    min_live_count: 0,
                    max_live_count: 0,
                },
            ],
            final_conditions: vec![
                // All 5 cells should be in top-right square (8-15, 8-15)
                Condition::Rectangle {
                    x_range: 8..16,
                    y_range: 8..16,
                    min_live_count: 5,
                    max_live_count: 5,
                },
                // No cells elsewhere.
                Condition::Rectangle {
                    x_range: 0..8,
                    y_range: 0..16,
                    min_live_count: 0,
                    max_live_count: 0,
                },
                Condition::Rectangle {
                    x_range: 8..16,
                    y_range: 0..8,
                    min_live_count: 0,
                    max_live_count: 0,
                },
            ],
        };

        assert!(board.advance_until(&puzzle.final_conditions, 30).is_none());
        assert_eq!(
            board.advance_until(&puzzle.final_conditions, 40).unwrap().0,
            31
        );
        assert_eq!(board.check_puzzle(&puzzle, 31), Ok(()));
    }

    #[test]
    fn test_to_string() {
        // Test with a simple glider pattern
        let mut board = Board::new(8);
        board.live_cells.extend([
            Position { x: 1, y: 0 }, // .●.
            Position { x: 2, y: 1 }, // ..●
            Position { x: 0, y: 2 }, // ●●●
            Position { x: 1, y: 2 },
            Position { x: 2, y: 2 },
        ]);

        let ascii = board.to_string();
        assert_eq!(
            String::from("\n") + &ascii,
            r#"
    0 1 2 3 4 5 6 7
 0  · ● · · · · · ·
 1  · · ● · · · · ·
 2  ● ● ● · · · · ·
 3  · · · · · · · ·
 4  · · · · · · · ·
 5  · · · · · · · ·
 6  · · · · · · · ·
 7  · · · · · · · ·
"#
        );

        let ascii = board.to_compact_string();
        assert_eq!(
            String::from("\n") + &ascii,
            r#"
·●······
··●·····
●●●·····
········
········
········
········
········
"#
        );
    }

    #[test]
    fn test_check_puzzle_final_conditions_not_met() {
        // Create a simple board with a single cell
        let mut board = Board::new(5);
        board.live_cells.push(Position { x: 2, y: 2 });

        let puzzle = Puzzle {
            title: "Impossible Test".to_string(),
            summary: "Test final conditions failure".to_string(),
            difficulty: Difficulty::Easy,
            size: 5,
            minimal_steps: 1,
            maximal_steps: 1,
            initial_conditions: vec![Condition::Position {
                position: Position { x: 2, y: 2 },
                is_live: true,
            }],
            final_conditions: vec![Condition::Position {
                position: Position { x: 2, y: 2 },
                is_live: true, // But single cell dies after 1 step
            }],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 1),
            Err(InvalidSolution::FinalConditionsNotMet { steps: 1 })
        );
    }
}
