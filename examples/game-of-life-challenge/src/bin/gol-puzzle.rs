// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use std::{fs, path::PathBuf};

use clap::{Parser, Subcommand};
use gol_challenge::game::{Condition, Difficulty, Position, Puzzle};

#[derive(Parser)]
#[command(name = "gol-puzzle")]
#[command(about = "Game of Life puzzle creation and management tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new puzzle file
    Create {
        /// Path to the output puzzle file
        path: PathBuf,
    },
    /// Print the contents of a puzzle file
    Print {
        /// Path to the puzzle file to print
        path: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create { path } => {
            create_puzzle(&path)?;
            println!("Created puzzle file: {}", path.display());
        }
        Commands::Print { path } => {
            print_puzzle(&path)?;
        }
    }

    Ok(())
}

fn create_puzzle(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
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

    // Serialize to BCS format
    let puzzle_bytes = bcs::to_bytes(&puzzle)?;

    // Write to file
    fs::write(path, puzzle_bytes)?;

    Ok(())
}

fn print_puzzle(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Read the file
    let puzzle_bytes = fs::read(path)?;

    // Deserialize from BCS format
    let puzzle: Puzzle = bcs::from_bytes(&puzzle_bytes)?;

    // Pretty print the puzzle
    println!("{:#}", puzzle);

    Ok(())
}
