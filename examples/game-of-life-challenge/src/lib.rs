// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! Main library for the Game-of-Life challenge. Should compile for Wasm and Rust. */

#![deny(missing_docs)]

/// Core library for the game engine and puzzles.
pub mod game;

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, DataBlobHash, ServiceAbi},
};
use serde::{Deserialize, Serialize};

use crate::game::Board;

/// The ABI of the Game-of-Life challenge.
pub struct GolChallengeAbi;

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

impl ContractAbi for GolChallengeAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for GolChallengeAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// The message type for the Game-of-Life challenge (unused).
#[derive(Debug, Serialize, Deserialize)]
pub struct Message;

/// ABI format definitions for the Game-of-Life challenge application.
#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{game::Board, GolChallengeAbi, Message, Operation};

    /// The GameOfLife application.
    pub struct GameOfLifeApplication;

    impl BcsApplication for GameOfLifeApplication {
        type Abi = GolChallengeAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<Operation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types to populate the registry
            tracer.trace_type::<Board>(&samples)?;

            let registry = tracer.registry()?;

            Ok(Formats {
                registry,
                operation,
                response,
                message,
                event_value,
            })
        }
    }
}
