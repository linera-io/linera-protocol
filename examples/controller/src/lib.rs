// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI types for the Controller Example Application */

use std::collections::HashSet;

use linera_sdk::{
    abis::controller::{ControllerCommand, ManagedServiceId, WorkerCommand},
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, BlockHeight, ChainId},
};
use serde::{Deserialize, Serialize};

/// Cross-chain messages exchanged by the Controller application.
#[derive(Clone, Debug, Deserialize, Serialize, GraphQLMutationRoot)]
#[doc(hidden)]
pub enum Message {
    // -- Message to the controller chain --
    ExecuteWorkerCommand {
        owner: AccountOwner,
        command: WorkerCommand,
    },
    ExecuteControllerCommand {
        admin: AccountOwner,
        command: ControllerCommand,
    },
    // -- Messages sent to the workers' control chains from the controller chain --
    Reset,
    Start {
        service_id: ManagedServiceId,
        owners_to_remove: HashSet<AccountOwner>,
        start_height: Option<BlockHeight>,
    },
    Stop {
        service_id: ManagedServiceId,
        new_owners: HashSet<AccountOwner>,
    },
    FollowChain {
        chain_id: ChainId,
    },
    ForgetChain {
        chain_id: ChainId,
    },
    // -- Messages sent from the worker's control chain to a service chain --
    AddOwners {
        service_id: ManagedServiceId,
        new_owners: HashSet<AccountOwner>,
    },
    RemoveOwners {
        owners_to_remove: HashSet<AccountOwner>,
    },
    // -- Messages sent from a service chain to the worker's control chain --
    OwnersAdded {
        service_id: ManagedServiceId,
        added_at: BlockHeight,
    },
    // -- Messages sent from the workers' control chains to the controller chain --
    HandoffStarted {
        service_id: ManagedServiceId,
        target_block_height: BlockHeight,
    },
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::{
        abis::controller::{
            ControllerAbi, ControllerCommand, ManagedServiceId, Operation, Worker, WorkerCommand,
        },
        formats::{BcsApplication, Formats, TracerExt},
        linera_base_types::AccountOwner,
    };
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::Message;

    /// The Controller application.
    pub struct ControllerApplication;

    impl BcsApplication for ControllerApplication {
        type Abi = ControllerAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let operation = tracer.trace_stable_enum_type::<Operation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<WorkerCommand>(&samples)?;
            tracer.trace_type::<ControllerCommand>(&samples)?;
            tracer.trace_type::<Worker>(&samples)?;
            tracer.trace_type::<ManagedServiceId>(&samples)?;
            tracer.trace_type::<AccountOwner>(&samples)?;

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
