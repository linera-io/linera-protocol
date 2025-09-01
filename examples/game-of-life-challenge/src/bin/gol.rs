// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
use gol_challenge::game::{Board, Condition, Difficulty, Position, Puzzle};

#[derive(Parser)]
#[command(name = "gol")]
#[command(about = "Game of Life puzzle creation and management tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new puzzle file and optionally a solution
    CreatePuzzle {
        /// Path to the output puzzle file
        puzzle_path: PathBuf,
        /// Optional path to output a board file
        #[arg(long)]
        board: Option<PathBuf>,
    },
    /// Print the contents of a puzzle file
    PrintPuzzle {
        /// Path to the puzzle file to print
        path: PathBuf,
    },
    /// Print the contents of a board file
    PrintBoard {
        /// Path to the board file to print
        path: PathBuf,
    },
    /// Check if a board solves a puzzle
    CheckBoard {
        /// Path to the puzzle file
        puzzle: PathBuf,
        /// Path to the board file
        board: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::CreatePuzzle { puzzle_path, board } => {
            let puzzle = create_puzzle(&puzzle_path)?;
            println!("Created puzzle file: {}", puzzle_path.display());

            if let Some(board_path) = board {
                create_solution(&puzzle, &board_path)?;
                println!("Created board file: {}", board_path.display());
            }
        }
        Commands::PrintPuzzle { path } => {
            print_puzzle(&path)?;
        }
        Commands::PrintBoard { path } => {
            print_board(&path)?;
        }
        Commands::CheckBoard { puzzle, board } => {
            check_board(&puzzle, &board)?;
        }
    }

    Ok(())
}

fn create_puzzle(path: &PathBuf) -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a fixed test puzzle for now
    let puzzle = Puzzle {
        title: "Sample Puzzle".to_string(),
        summary: "A simple test puzzle with mixed constraints".to_string(),
        difficulty: Difficulty::Medium,
        size: 5,
        minimal_steps: 1,
        maximal_steps: 3,
        initial_conditions: vec![
            Condition::TestPosition {
                position: Position { x: 2, y: 2 },
                is_live: true,
            },
            Condition::TestRectangle {
                x_range: 1..4,
                y_range: 1..4,
                min_live_count: 2,
                max_live_count: 5,
            },
            Condition::TestPosition {
                position: Position { x: 0, y: 0 },
                is_live: true,
            },
        ],
        final_conditions: vec![
            Condition::TestPosition {
                position: Position { x: 4, y: 4 },
                is_live: false,
            },
            Condition::TestRectangle {
                x_range: 0..2,
                y_range: 3..5,
                min_live_count: 1,
                max_live_count: 2,
            },
        ],
    };

    let puzzle_bytes = bcs::to_bytes(&puzzle)?;
    fs::write(path, puzzle_bytes)?;

    Ok(puzzle)
}

fn print_puzzle(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let puzzle_bytes = fs::read(path)?;
    let puzzle: Puzzle = bcs::from_bytes(&puzzle_bytes)?;
    println!("{:#}", puzzle);
    Ok(())
}

fn create_solution(puzzle: &Puzzle, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple board: a board with the minimal required live cells
    // For demonstration, we'll create a board that satisfies the initial conditions
    let mut live_cells = Vec::new();

    // Add cells required by initial conditions
    for condition in &puzzle.initial_conditions {
        match condition {
            Condition::TestPosition {
                position,
                is_live: true,
            } => {
                live_cells.push(*position);
            }
            Condition::TestRectangle {
                x_range,
                y_range,
                min_live_count,
                ..
            } => {
                // Add minimum required live cells in the rectangle area
                let mut count = 0;
                for y in y_range.clone() {
                    for x in x_range.clone() {
                        if count < *min_live_count {
                            live_cells.push(Position { x, y });
                            count += 1;
                        }
                    }
                }
            }
            _ => {} // Skip other condition types for now
        }
    }

    // For now, create an empty board - we'll improve this later
    let board = Board::new(puzzle.size);
    // TODO: Need a way to create board with live cells
    println!("Warning: Created empty board for solution (live cells not set yet)");

    // Serialize to BCS format
    let solution_bytes = bcs::to_bytes(&board)?;

    // Write to file
    fs::write(path, solution_bytes)?;

    Ok(())
}

fn print_board(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let board_bytes = fs::read(path)?;
    let board: Board = bcs::from_bytes(&board_bytes)?;
    println!("Board:");
    println!("{:#}", board);
    Ok(())
}

fn check_board(
    puzzle_path: &PathBuf,
    board_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read puzzle file
    let puzzle_bytes = fs::read(puzzle_path)?;
    let puzzle: Puzzle = bcs::from_bytes(&puzzle_bytes)?;

    // Read board file
    let board_bytes = fs::read(board_path)?;
    let board: Board = bcs::from_bytes(&board_bytes)?;

    // Check if board solves the puzzle
    match board.check_puzzle(&puzzle) {
        Ok(steps) => {
            println!("✅ Solution is VALID!");
            println!("   Initial board passes all initial conditions");
            println!(
                "   After {} steps, board passes all final conditions",
                steps
            );
        }
        Err(error) => {
            println!("❌ Solution is INVALID!");
            println!("   Error: {}", error);
        }
    }

    Ok(())
}
