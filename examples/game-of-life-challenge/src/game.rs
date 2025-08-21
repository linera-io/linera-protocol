// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    iter,
    ops::Range,
};

use async_graphql::{Enum, InputObject, SimpleObject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A GoL puzzle.
#[derive(Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct Puzzle {
    /// A title for this puzzle.
    pub title: String,
    /// A summary of the goal of the puzzle.
    pub summary: String,
    /// The difficulty level, according to the puzzle's creator.
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, Enum)]
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
    TestPosition {
        /// The position being tested.
        position: Position,
        /// The desired state of the cell.
        is_live: bool,
    },
    /// Testing a rectangle of cells.
    TestRectangle {
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

/// Error type for invalid puzzle solutions.
#[derive(Debug, Error, Clone, PartialEq, Serialize, Deserialize)]
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

    /// One or more initial conditions are not satisfied.
    #[error("Initial condition {condition_index} failed: {reason}")]
    InitialConditionFailed {
        /// The index of the failed condition.
        condition_index: usize,
        /// The specific reason why the condition failed.
        reason: ConditionFailureReason,
    },

    /// One or more final conditions are not satisfied after running the simulation.
    #[error("Final condition {condition_index} failed after {steps} steps: {reason}")]
    FinalConditionFailed {
        /// The index of the failed condition.
        condition_index: usize,
        /// The number of steps that were executed.
        steps: u16,
        /// The specific reason why the condition failed.
        reason: ConditionFailureReason,
    },
}

/// Specific reasons why a condition failed.
#[derive(Debug, Error, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConditionFailureReason {
    /// A position condition failed.
    #[error("Position ({x}, {y}) expected to be {expected_state} but was {actual_state}")]
    PositionMismatch {
        /// The x coordinate.
        x: u16,
        /// The y coordinate.
        y: u16,
        /// Whether the cell was expected to be alive.
        expected_state: bool,
        /// Whether the cell was actually alive.
        actual_state: bool,
    },

    /// A rectangle condition failed due to count mismatch.
    #[error("Rectangle [{x_start}..{x_end}, {y_start}..{y_end}] expected {min_count}-{max_count} live cells but found {actual_count}")]
    RectangleCountMismatch {
        /// Start of x range.
        x_start: u16,
        /// End of x range.
        x_end: u16,
        /// Start of y range.
        y_start: u16,
        /// End of y range.
        y_end: u16,
        /// Minimum expected live cells.
        min_count: u32,
        /// Maximum expected live cells.
        max_count: u32,
        /// Actual number of live cells found.
        actual_count: u32,
    },
}

/// A position in the GoL board.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Position {
    /// The first coordinate.
    pub x: u16,
    /// The second coordinate.
    pub y: u16,
}

async_graphql::scalar!(Position);
async_graphql::scalar!(Condition);
async_graphql::scalar!(InvalidSolution);
async_graphql::scalar!(ConditionFailureReason);

/// Result of validating a puzzle solution.
#[derive(Debug, Clone, SimpleObject)]
pub struct ValidationResult {
    /// Whether the solution is valid.
    pub is_valid: bool,
    /// Error message if validation failed.
    pub error_message: Option<String>,
    /// Detailed error information if validation failed.
    pub error_details: Option<InvalidSolution>,
}

/// The state of a GoL board. We use a sparse encoding for storage efficiency reasons.
#[derive(Debug, Clone, Serialize, Deserialize, InputObject, SimpleObject)]
#[graphql(input_name = "BoardInput")]
pub struct Board {
    /// The width and height of the board, in cells.
    size: u16,
    /// The coordinates of the live cells.
    // NOTE: Serde treats `BTreeSet` as a sequence, therefore BCS won't be able to enforce
    // a strict-ordering of positions in the BCS bytes for this field.
    live_cells: BTreeSet<Position>,
}

/// The state of a GoL cell. Used for computations.
#[derive(Default)]
struct Cell {
    /// Odd if the cell is alive, even otherwise. Higher bits may be used for holding the
    /// number of (alive) neighbors.
    value: u8,
}

/// Another representation the board, allowing direct access to the cells.
#[derive(Debug, Clone)]
struct DirectBoard {
    /// The width and height of the board, in cells.
    size: u16,
    /// The coordinates of the live cells indexed along the `x` then `y` axis.
    index: BTreeMap<u16, BTreeSet<u16>>,
}

impl Display for DirectBoard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if formatter.alternate() {
            write!(formatter, "{}", self.to_pretty_string())?;
        } else {
            write!(formatter, "{}", self.to_compact_string())?;
        }
        Ok(())
    }
}

impl DirectBoard {
    /// Print the board as ASCII art.
    /// Live cells are represented by '●' and dead cells by '·'.
    fn to_pretty_string(&self) -> String {
        let mut result = String::new();

        // Add top border with column numbers for reference.
        result.push_str("   ");
        for x in 0..self.size {
            result.push_str(&format!("{:2}", x % 10));
        }
        result.push('\n');

        for y in 0..self.size {
            // Add row number for reference.
            result.push_str(&format!("{:2} ", y));

            for x in 0..self.size {
                result.push(' ');
                if self.is_live(Position { x, y }) {
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
    fn to_compact_string(&self) -> String {
        let mut result = String::new();

        for y in 0..self.size {
            for x in 0..self.size {
                if self.is_live(Position { x, y }) {
                    result.push('●');
                } else {
                    result.push('·');
                }
            }
            result.push('\n');
        }

        result
    }

    fn is_live(&self, position: Position) -> bool {
        let Position { x, y } = position;
        self.index.get(&x).is_some_and(|set| set.contains(&y))
    }

    fn check_conditions(
        &self,
        conditions: &[Condition],
    ) -> Result<(), (usize, ConditionFailureReason)> {
        for (index, condition) in conditions.iter().enumerate() {
            condition.check(self).map_err(|reason| (index, reason))?;
        }
        Ok(())
    }
}

impl Display for Board {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_direct_board().fmt(formatter)
    }
}

impl Board {
    /// Creates a new board with the given size and player owners.
    pub fn new(size: u16) -> Self {
        Board {
            size,
            live_cells: BTreeSet::new(),
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
    /// the board and the number of steps, if we succeed on or before `max_steps`.
    /// Otherwise return the last error.
    pub fn advance_until(
        &self,
        conditions: &[Condition],
        max_steps: u16,
    ) -> Result<(u16, Self), (usize, ConditionFailureReason)> {
        let mut board = self.clone();
        let mut i = 0;
        loop {
            let result = board.check_conditions(conditions);
            match result {
                Ok(()) => {
                    return Ok((i, board));
                }
                Err(error) => {
                    if i == max_steps {
                        return Err(error);
                    }
                    i += 1;
                    board = board.advance_once();
                }
            }
        }
    }

    fn to_direct_board(&self) -> DirectBoard {
        let mut index = BTreeMap::<_, BTreeSet<_>>::new();
        for Position { x, y } in self.live_cells.iter().cloned() {
            index.entry(x).or_default().insert(y);
        }
        DirectBoard {
            size: self.size,
            index,
        }
    }

    /// Check that the board satisfies a given set of conditions.
    pub fn check_conditions(
        &self,
        conditions: &[Condition],
    ) -> Result<(), (usize, ConditionFailureReason)> {
        self.to_direct_board().check_conditions(conditions)
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
        if let Err((condition_index, reason)) = self.check_conditions(&puzzle.initial_conditions) {
            return Err(InvalidSolution::InitialConditionFailed {
                condition_index,
                reason,
            });
        }
        let final_board = self.advance(steps);
        if let Err((condition_index, reason)) =
            final_board.check_conditions(&puzzle.final_conditions)
        {
            Err(InvalidSolution::FinalConditionFailed {
                condition_index,
                steps,
                reason,
            })
        } else {
            Ok(())
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

    // Implement the rules of the Game of Life.
    // * Dead cell with 3 neighbors (6) becomes alive.
    // * Live cell with 2 neighbors (5) stays alive.
    // * Live cell with 3 neighbors (7) stays alive.
    fn should_be_live_next(self) -> bool {
        self.value >= 5 && self.value <= 7
    }
}

impl Condition {
    fn check(&self, board: &DirectBoard) -> Result<(), ConditionFailureReason> {
        match self {
            Self::TestPosition { position, is_live } => {
                let actual_state = board.is_live(*position);
                if *is_live == actual_state {
                    Ok(())
                } else {
                    Err(ConditionFailureReason::PositionMismatch {
                        x: position.x,
                        y: position.y,
                        expected_state: *is_live,
                        actual_state,
                    })
                }
            }
            Self::TestRectangle {
                x_range,
                y_range,
                min_live_count,
                max_live_count,
            } => {
                let mut count = 0;
                for (_, set) in board.index.range(x_range.clone()) {
                    count += set.range(y_range.clone()).count() as u32;
                }

                if count >= *min_live_count && count <= *max_live_count {
                    Ok(())
                } else {
                    Err(ConditionFailureReason::RectangleCountMismatch {
                        x_start: x_range.start,
                        x_end: x_range.end,
                        y_start: y_range.start,
                        y_end: y_range.end,
                        min_count: *min_live_count,
                        max_count: *max_live_count,
                        actual_count: count,
                    })
                }
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
        board.live_cells.insert(Position { x: 2, y: 2 });

        let next_board = board.advance_once();
        assert!(next_board.live_cells.is_empty());
    }

    #[test]
    fn test_advance_block_pattern_stable() {
        // Block pattern (2x2 square) should remain stable.
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
        // Blinker pattern should oscillate between horizontal and vertical.
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

        // Advance again to get back to original.
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
        // Three cells in an L shape should create a fourth cell.
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
    fn test_position_condition_check() {
        let mut index = BTreeMap::new();
        index.insert(1, BTreeSet::from([2, 3]));
        index.insert(2, BTreeSet::from([1]));
        let board = DirectBoard { index, size: 4 };

        // Test live cell condition..
        let condition = Condition::TestPosition {
            position: Position { x: 1, y: 2 },
            is_live: true,
        };
        assert!(condition.check(&board).is_ok());

        // Test dead cell condition..
        let condition = Condition::TestPosition {
            position: Position { x: 1, y: 2 },
            is_live: false,
        };
        assert!(condition.check(&board).is_err());

        // Test empty position..
        let condition = Condition::TestPosition {
            position: Position { x: 0, y: 0 },
            is_live: false,
        };
        assert!(condition.check(&board).is_ok());

        let condition = Condition::TestPosition {
            position: Position { x: 0, y: 0 },
            is_live: true,
        };
        assert!(condition.check(&board).is_err());
    }

    #[test]
    fn test_rectangle_condition_check() {
        let mut index = BTreeMap::new();
        index.insert(1, BTreeSet::from([1, 2]));
        index.insert(2, BTreeSet::from([1, 2, 3]));
        index.insert(3, BTreeSet::from([2]));
        let board = DirectBoard { index, size: 5 };

        // Rectangle containing 4 cells (at positions (1,1), (1,2), (2,1), (2,2)).
        let condition = Condition::TestRectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 3,
            max_live_count: 5,
        };
        assert!(condition.check(&board).is_ok());

        // Same rectangle but expecting too many cells.
        let condition = Condition::TestRectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 5,
            max_live_count: 10,
        };
        assert!(condition.check(&board).is_err());

        // Same rectangle but allowing too few cells.
        let condition = Condition::TestRectangle {
            x_range: 1..3,
            y_range: 1..3,
            min_live_count: 1,
            max_live_count: 3,
        };
        assert!(condition.check(&board).is_err());
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
        ); // Too few steps.
        assert_eq!(
            board.check_puzzle(&puzzle, 5),
            Err(InvalidSolution::StepsOutOfRange {
                steps: 5,
                min_steps: 2,
                max_steps: 4
            })
        ); // Too many steps.
        assert_eq!(board.check_puzzle(&puzzle, 3), Ok(())); // Valid steps.
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
            initial_conditions: vec![Condition::TestPosition {
                position: Position { x: 0, y: 0 },
                is_live: true,
            }],
            final_conditions: vec![],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 3),
            Err(InvalidSolution::InitialConditionFailed {
                condition_index: 0,
                reason: ConditionFailureReason::PositionMismatch {
                    x: 0,
                    y: 0,
                    expected_state: true,
                    actual_state: false,
                }
            })
        );
    }

    #[test]
    fn test_check_puzzle_complete_workflow() {
        // Create a board with a blinker pattern.
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
                Condition::TestPosition {
                    position: Position { x: 2, y: 1 },
                    is_live: true,
                },
                Condition::TestPosition {
                    position: Position { x: 2, y: 2 },
                    is_live: true,
                },
                Condition::TestPosition {
                    position: Position { x: 2, y: 3 },
                    is_live: true,
                },
            ],
            final_conditions: vec![
                Condition::TestPosition {
                    position: Position { x: 2, y: 1 },
                    is_live: true,
                },
                Condition::TestPosition {
                    position: Position { x: 2, y: 2 },
                    is_live: true,
                },
                Condition::TestPosition {
                    position: Position { x: 2, y: 3 },
                    is_live: true,
                },
            ],
        };

        // Test that after 2 steps (full blinker cycle), we get back to the initial pattern.
        assert_eq!(board.check_puzzle(&puzzle, 2), Ok(()));
    }

    #[test]
    fn test_glider_puzzle_with_rectangle_conditions() {
        // Create a 16x16 board divided into 4 squares (8x8 each):
        // Use a glider that travels from bottom-left to top-right.

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
                Condition::TestRectangle {
                    x_range: 0..8,
                    y_range: 0..8,
                    min_live_count: 5,
                    max_live_count: 5,
                },
                // No cells elsewhere.
                Condition::TestRectangle {
                    x_range: 8..16,
                    y_range: 0..8,
                    min_live_count: 0,
                    max_live_count: 0,
                },
                Condition::TestRectangle {
                    x_range: 0..16,
                    y_range: 8..16,
                    min_live_count: 0,
                    max_live_count: 0,
                },
            ],
            final_conditions: vec![
                // All 5 cells should be in top-right square (8-15, 8-15)
                Condition::TestRectangle {
                    x_range: 8..16,
                    y_range: 8..16,
                    min_live_count: 5,
                    max_live_count: 5,
                },
                // No cells elsewhere.
                Condition::TestRectangle {
                    x_range: 0..8,
                    y_range: 0..16,
                    min_live_count: 0,
                    max_live_count: 0,
                },
                Condition::TestRectangle {
                    x_range: 8..16,
                    y_range: 0..8,
                    min_live_count: 0,
                    max_live_count: 0,
                },
            ],
        };

        assert!(board.advance_until(&puzzle.final_conditions, 30).is_err());
        assert_eq!(
            board.advance_until(&puzzle.final_conditions, 40).unwrap().0,
            31
        );
        assert_eq!(board.check_puzzle(&puzzle, 31), Ok(()));
    }

    #[test]
    fn test_to_string() {
        // Test with a simple glider pattern.
        let mut board = Board::new(8);
        board.live_cells.extend([
            Position { x: 1, y: 0 }, // .●.
            Position { x: 2, y: 1 }, // ..●
            Position { x: 0, y: 2 }, // ●●●
            Position { x: 1, y: 2 },
            Position { x: 2, y: 2 },
        ]);

        let ascii = format!("{:#}", board);
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

        let ascii = board.to_string();
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
        // Create a simple board with a single cell.
        let mut board = Board::new(5);
        board.live_cells.insert(Position { x: 2, y: 2 });

        let puzzle = Puzzle {
            title: "Impossible Test".to_string(),
            summary: "Test final conditions failure".to_string(),
            difficulty: Difficulty::Easy,
            size: 5,
            minimal_steps: 1,
            maximal_steps: 1,
            initial_conditions: vec![Condition::TestPosition {
                position: Position { x: 2, y: 2 },
                is_live: true,
            }],
            final_conditions: vec![Condition::TestPosition {
                position: Position { x: 2, y: 2 },
                is_live: true, // But single cell dies after 1 step.
            }],
        };

        assert_eq!(
            board.check_puzzle(&puzzle, 1),
            Err(InvalidSolution::FinalConditionFailed {
                condition_index: 0,
                steps: 1,
                reason: ConditionFailureReason::PositionMismatch {
                    x: 2,
                    y: 2,
                    expected_state: true,
                    actual_state: false,
                }
            })
        );
    }

    #[test]
    fn test_detailed_error_messages() {
        // Test detailed error messages for different failure scenarios.
        let mut board = Board::new(8);
        board.live_cells.extend([
            Position { x: 1, y: 1 },
            Position { x: 2, y: 2 },
            Position { x: 3, y: 3 },
        ]);

        let puzzle = Puzzle {
            title: "Detailed Error Test".to_string(),
            summary: "Test detailed error reporting".to_string(),
            difficulty: Difficulty::Easy,
            size: 8,
            minimal_steps: 1,
            maximal_steps: 2,
            initial_conditions: vec![
                // This should pass.
                Condition::TestPosition {
                    position: Position { x: 1, y: 1 },
                    is_live: true,
                },
                // This should fail - position (0,0) should be alive but isn't.
                Condition::TestPosition {
                    position: Position { x: 0, y: 0 },
                    is_live: true,
                },
                // This rectangle condition should also fail.
                Condition::TestRectangle {
                    x_range: 4..8,
                    y_range: 4..8,
                    min_live_count: 2,
                    max_live_count: 4,
                },
            ],
            final_conditions: vec![],
        };

        // Test initial condition failure with detailed information.
        match board.check_puzzle(&puzzle, 1) {
            Err(InvalidSolution::InitialConditionFailed {
                condition_index,
                reason,
            }) => {
                assert_eq!(condition_index, 1); // Second condition should fail.
                match reason {
                    ConditionFailureReason::PositionMismatch {
                        x,
                        y,
                        expected_state,
                        actual_state,
                    } => {
                        assert_eq!(x, 0);
                        assert_eq!(y, 0);
                        assert!(expected_state);
                        assert!(!actual_state);
                    }
                    _ => panic!("Expected PositionMismatch error"),
                }
            }
            other => panic!("Expected InitialConditionFailed, got {:?}", other),
        }

        // Test rectangle condition failure.
        let mut board2 = Board::new(8);
        board2.live_cells.insert(Position { x: 0, y: 0 }); // Satisfy first condition.

        let puzzle2 = Puzzle {
            title: "Rectangle Error Test".to_string(),
            summary: "Test rectangle error reporting".to_string(),
            difficulty: Difficulty::Easy,
            size: 8,
            minimal_steps: 1,
            maximal_steps: 2,
            initial_conditions: vec![
                Condition::TestPosition {
                    position: Position { x: 0, y: 0 },
                    is_live: true,
                },
                Condition::TestRectangle {
                    x_range: 2..6,
                    y_range: 2..6,
                    min_live_count: 3,
                    max_live_count: 5,
                },
            ],
            final_conditions: vec![],
        };

        match board2.check_puzzle(&puzzle2, 1) {
            Err(InvalidSolution::InitialConditionFailed {
                condition_index,
                reason,
            }) => {
                assert_eq!(condition_index, 1); // Rectangle condition should fail.
                match reason {
                    ConditionFailureReason::RectangleCountMismatch {
                        x_start,
                        x_end,
                        y_start,
                        y_end,
                        min_count,
                        max_count,
                        actual_count,
                    } => {
                        assert_eq!(x_start, 2);
                        assert_eq!(x_end, 6);
                        assert_eq!(y_start, 2);
                        assert_eq!(y_end, 6);
                        assert_eq!(min_count, 3);
                        assert_eq!(max_count, 5);
                        assert_eq!(actual_count, 0);
                    }
                    _ => panic!("Expected RectangleCountMismatch error"),
                }
            }
            other => panic!("Expected InitialConditionFailed, got {:?}", other),
        }
    }
}
