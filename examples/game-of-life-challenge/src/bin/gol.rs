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
    /// Generate multiple puzzle files with their solutions
    CreatePuzzles {
        /// Optional output directory (defaults to current directory)
        #[arg(short, long)]
        output_dir: Option<PathBuf>,
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
    CheckSolution {
        /// Path to the puzzle file
        puzzle: PathBuf,
        /// Path to the board file
        board: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::CreatePuzzles { output_dir } => {
            let output_path = output_dir.unwrap_or_else(|| PathBuf::from("."));
            create_puzzles(&output_path)?;
        }
        Commands::PrintPuzzle { path } => {
            print_puzzle(&path)?;
        }
        Commands::PrintBoard { path } => {
            print_board(&path)?;
        }
        Commands::CheckSolution { puzzle, board } => {
            check_solution(&puzzle, &board)?;
        }
    }

    Ok(())
}

fn create_puzzles(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Puzzle 1: Create a Block Pattern
    let block_puzzle = create_block_puzzle()?;
    let block_solution = create_block_solution(&block_puzzle)?;

    let puzzle_path = output_dir.join("01_block_pattern.bcs");
    let solution_path = output_dir.join("01_block_pattern_solution.bcs");

    let puzzle_bytes = bcs::to_bytes(&block_puzzle)?;
    let solution_bytes = bcs::to_bytes(&block_solution)?;

    fs::write(&puzzle_path, puzzle_bytes)?;
    fs::write(&solution_path, solution_bytes)?;

    println!("Created puzzle: {}", puzzle_path.display());
    println!("{block_puzzle:#}");
    println!("Created solution: {}", solution_path.display());
    println!("{block_solution:#}");
    let steps = block_solution.check_puzzle(&block_puzzle)?;
    println!("Verified solution: {steps} steps");

    Ok(())
}

fn create_block_puzzle() -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a puzzle where the goal is to form a 2x2 block in the center
    let puzzle = Puzzle {
        title: "Block Formation".to_string(),
        summary: "Create a stable 2x2 block pattern in the center of the board".to_string(),
        difficulty: Difficulty::Easy,
        size: 6,
        minimal_steps: 1,
        maximal_steps: 5,
        is_strict: false,
        initial_conditions: vec![
            // Allow any initial configuration with 4-8 live cells
            Condition::TestRectangle {
                x_range: 0..6,
                y_range: 0..6,
                min_live_count: 4,
                max_live_count: 8,
            },
        ],
        final_conditions: vec![
            // Must have exactly 4 cells in the center 2x2 block
            Condition::TestPosition {
                position: Position { x: 2, y: 2 },
                is_live: true,
            },
            Condition::TestPosition {
                position: Position { x: 2, y: 3 },
                is_live: true,
            },
            Condition::TestPosition {
                position: Position { x: 3, y: 2 },
                is_live: true,
            },
            Condition::TestPosition {
                position: Position { x: 3, y: 3 },
                is_live: true,
            },
            // Ensure no other cells are alive (the block should be stable)
            Condition::TestRectangle {
                x_range: 0..6,
                y_range: 0..6,
                min_live_count: 4,
                max_live_count: 4,
            },
        ],
    };

    Ok(puzzle)
}

fn create_block_solution(puzzle: &Puzzle) -> Result<Board, Box<dyn std::error::Error>> {
    // Create a solution: start with a block pattern that's already stable
    let live_cells = vec![
        Position { x: 2, y: 2 },
        Position { x: 2, y: 3 },
        Position { x: 3, y: 2 },
        Position { x: 3, y: 3 },
    ];

    let board = Board::with_live_cells(puzzle.size, live_cells);

    Ok(board)
}

fn print_puzzle(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let puzzle_bytes = fs::read(path)?;
    let puzzle: Puzzle = bcs::from_bytes(&puzzle_bytes)?;
    println!("{:#}", puzzle);
    Ok(())
}

fn print_board(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let board_bytes = fs::read(path)?;
    let board: Board = bcs::from_bytes(&board_bytes)?;
    println!("Board:");
    println!("{:#}", board);
    Ok(())
}

fn check_solution(
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
