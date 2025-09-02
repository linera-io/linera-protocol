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

    // Generate all static pattern puzzles
    let puzzles: Vec<(
        &str,
        fn() -> Result<Puzzle, Box<dyn std::error::Error>>,
        fn(&Puzzle) -> Result<Board, Box<dyn std::error::Error>>,
    )> = vec![
        ("01_block_pattern", create_block_puzzle, create_block_solution),
        ("02_beehive_pattern", create_beehive_puzzle, create_beehive_solution),
        ("03_loaf_pattern", create_loaf_puzzle, create_loaf_solution),
        ("04_boat_pattern", create_boat_puzzle, create_boat_solution),
        ("05_tub_pattern", create_tub_puzzle, create_tub_solution),
    ];

    for (name, puzzle_creator, solution_creator) in puzzles {
        let puzzle = puzzle_creator()?;
        let solution = solution_creator(&puzzle)?;

        let puzzle_path = output_dir.join(format!("{}.bcs", name));
        let solution_path = output_dir.join(format!("{}_solution.bcs", name));

        let puzzle_bytes = bcs::to_bytes(&puzzle)?;
        let solution_bytes = bcs::to_bytes(&solution)?;

        fs::write(&puzzle_path, puzzle_bytes)?;
        fs::write(&solution_path, solution_bytes)?;

        println!("Created puzzle: {}", puzzle_path.display());
        println!("{puzzle:#}");
        println!("Created solution: {}", solution_path.display());
        println!("{solution:#}");
        let steps = solution.check_puzzle(&puzzle)?;
        println!("Verified solution: {steps} steps");
        println!();
    }

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

fn create_beehive_puzzle() -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a puzzle where the goal is to form a beehive pattern
    let puzzle = Puzzle {
        title: "Beehive Formation".to_string(),
        summary: "Create a stable beehive pattern (6-cell hexagonal shape)".to_string(),
        difficulty: Difficulty::Easy,
        size: 7,
        minimal_steps: 1,
        maximal_steps: 5,
        is_strict: false,
        initial_conditions: vec![
            Condition::TestRectangle {
                x_range: 0..7,
                y_range: 0..7,
                min_live_count: 6,
                max_live_count: 10,
            },
        ],
        final_conditions: vec![
            // Beehive pattern: hexagonal shape
            //  ●●
            // ●  ●
            //  ●●
            Condition::TestPosition { position: Position { x: 2, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 1, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 4, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 2, y: 3 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 3 }, is_live: true },
            // Ensure exactly 6 cells
            Condition::TestRectangle {
                x_range: 0..7,
                y_range: 0..7,
                min_live_count: 6,
                max_live_count: 6,
            },
        ],
    };

    Ok(puzzle)
}

fn create_beehive_solution(puzzle: &Puzzle) -> Result<Board, Box<dyn std::error::Error>> {
    let live_cells = vec![
        Position { x: 2, y: 1 },
        Position { x: 3, y: 1 },
        Position { x: 1, y: 2 },
        Position { x: 4, y: 2 },
        Position { x: 2, y: 3 },
        Position { x: 3, y: 3 },
    ];

    let board = Board::with_live_cells(puzzle.size, live_cells);

    Ok(board)
}

fn create_loaf_puzzle() -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a puzzle where the goal is to form a loaf pattern
    let puzzle = Puzzle {
        title: "Loaf Formation".to_string(),
        summary: "Create a stable loaf pattern (8-cell bread loaf shape)".to_string(),
        difficulty: Difficulty::Medium,
        size: 8,
        minimal_steps: 1,
        maximal_steps: 5,
        is_strict: false,
        initial_conditions: vec![
            Condition::TestRectangle {
                x_range: 0..8,
                y_range: 0..8,
                min_live_count: 7,
                max_live_count: 10,
            },
        ],
        final_conditions: vec![
            // Loaf pattern:
            //  ●●
            // ●  ●
            //  ● ●
            //   ●
            Condition::TestPosition { position: Position { x: 2, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 1, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 4, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 2, y: 3 }, is_live: true },
            Condition::TestPosition { position: Position { x: 4, y: 3 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 4 }, is_live: true },
            // Ensure exactly 7 cells (I miscounted - loaf has 7 cells)
            Condition::TestRectangle {
                x_range: 0..8,
                y_range: 0..8,
                min_live_count: 7,
                max_live_count: 7,
            },
        ],
    };

    Ok(puzzle)
}

fn create_loaf_solution(puzzle: &Puzzle) -> Result<Board, Box<dyn std::error::Error>> {
    let live_cells = vec![
        Position { x: 2, y: 1 },
        Position { x: 3, y: 1 },
        Position { x: 1, y: 2 },
        Position { x: 4, y: 2 },
        Position { x: 2, y: 3 },
        Position { x: 4, y: 3 },
        Position { x: 3, y: 4 },
    ];

    let board = Board::with_live_cells(puzzle.size, live_cells);

    Ok(board)
}

fn create_boat_puzzle() -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a puzzle where the goal is to form a boat pattern
    let puzzle = Puzzle {
        title: "Boat Formation".to_string(),
        summary: "Create a stable boat pattern (5-cell boat shape)".to_string(),
        difficulty: Difficulty::Easy,
        size: 6,
        minimal_steps: 1,
        maximal_steps: 5,
        is_strict: false,
        initial_conditions: vec![
            Condition::TestRectangle {
                x_range: 0..6,
                y_range: 0..6,
                min_live_count: 5,
                max_live_count: 8,
            },
        ],
        final_conditions: vec![
            // Boat pattern:
            // ●●
            // ● ●
            //  ●
            Condition::TestPosition { position: Position { x: 1, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 2, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 1, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 2, y: 3 }, is_live: true },
            // Ensure exactly 5 cells
            Condition::TestRectangle {
                x_range: 0..6,
                y_range: 0..6,
                min_live_count: 5,
                max_live_count: 5,
            },
        ],
    };

    Ok(puzzle)
}

fn create_boat_solution(puzzle: &Puzzle) -> Result<Board, Box<dyn std::error::Error>> {
    let live_cells = vec![
        Position { x: 1, y: 1 },
        Position { x: 2, y: 1 },
        Position { x: 1, y: 2 },
        Position { x: 3, y: 2 },
        Position { x: 2, y: 3 },
    ];

    let board = Board::with_live_cells(puzzle.size, live_cells);

    Ok(board)
}

fn create_tub_puzzle() -> Result<Puzzle, Box<dyn std::error::Error>> {
    // Create a puzzle where the goal is to form a tub pattern
    let puzzle = Puzzle {
        title: "Tub Formation".to_string(),
        summary: "Create a stable tub pattern (4-cell hollow square)".to_string(),
        difficulty: Difficulty::Easy,
        size: 5,
        minimal_steps: 1,
        maximal_steps: 5,
        is_strict: false,
        initial_conditions: vec![
            Condition::TestRectangle {
                x_range: 0..5,
                y_range: 0..5,
                min_live_count: 4,
                max_live_count: 7,
            },
        ],
        final_conditions: vec![
            // Tub pattern:
            //  ●
            // ● ●
            //  ●
            Condition::TestPosition { position: Position { x: 2, y: 1 }, is_live: true },
            Condition::TestPosition { position: Position { x: 1, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 3, y: 2 }, is_live: true },
            Condition::TestPosition { position: Position { x: 2, y: 3 }, is_live: true },
            // Ensure exactly 4 cells
            Condition::TestRectangle {
                x_range: 0..5,
                y_range: 0..5,
                min_live_count: 4,
                max_live_count: 4,
            },
        ],
    };

    Ok(puzzle)
}

fn create_tub_solution(puzzle: &Puzzle) -> Result<Board, Box<dyn std::error::Error>> {
    let live_cells = vec![
        Position { x: 2, y: 1 },
        Position { x: 1, y: 2 },
        Position { x: 3, y: 2 },
        Position { x: 2, y: 3 },
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
