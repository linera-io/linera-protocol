// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use async_graphql::{scalar, Request, Response, SimpleObject};
use linera_sdk_derive::GraphQLMutationRootInCrate;
use serde::{Deserialize, Serialize};

use crate::linera_base_types::{
    AccountOwner, ApplicationId, ChainId, ContractAbi, DataBlobHash, MessagePolicy, ServiceAbi,
};

pub struct ControllerAbi;

impl ContractAbi for ControllerAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for ControllerAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Service are identified by the blob ID of the description.
pub type ManagedServiceId = DataBlobHash;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRootInCrate)]
pub enum Operation {
    /// Worker commands
    ExecuteWorkerCommand {
        owner: AccountOwner,
        command: WorkerCommand,
    },
    /// Execute a controller command
    ExecuteControllerCommand {
        admin: AccountOwner,
        command: ControllerCommand,
    },
}

/// A worker command
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum WorkerCommand {
    /// Executed by workers to register themselves.
    RegisterWorker { capabilities: Vec<String> },
    /// Executed by workers to de-register themselves.
    DeregisterWorker,
}

scalar!(WorkerCommand);

/// A controller command
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ControllerCommand {
    /// Set the admin owners.
    SetAdmins { admins: Option<Vec<AccountOwner>> },
    /// Remove a worker. (This should not usually happen, but some workers may be broken
    /// and need to be cleaned up.)
    RemoveWorker { worker_id: ChainId },
    /// Update the state of a particular service to be running on the specific workers.
    UpdateService {
        service_id: ManagedServiceId,
        workers: Vec<ChainId>,
    },
    /// Remove a service from the map entirely.
    RemoveService { service_id: ManagedServiceId },
    /// Set the states of all services at once, possibly removing some of them.
    UpdateAllServices {
        services: Vec<(ManagedServiceId, Vec<ChainId>)>,
    },
    /// Update the state of a particular chain to be listened to on the specific workers.
    UpdateChain {
        chain_id: ChainId,
        workers: Vec<ChainId>,
    },
    /// Remove a chain from the map entirely.
    RemoveChain { chain_id: ChainId },
    /// Set the states of all chains at once, possibly removing some of them.
    UpdateAllChains {
        chains: Vec<(ChainId, Vec<ChainId>)>,
    },
}

scalar!(ControllerCommand);

/// The description of a service worker.
#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct Worker {
    /// The address used by the worker.
    pub owner: AccountOwner,
    /// Some tags denoting the capabilities of this worker. Each capability has a value
    /// that the worker will read from its local environment and pass to the applications.
    pub capabilities: Vec<String>,
}

/// The description of a service managed by the controller.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedService {
    /// The application ID running the service (e.g. pm-engine)
    pub application_id: ApplicationId,
    /// The role assumed by this service within the application (e.g. engine, event,
    /// market-maker).
    pub name: String,
    /// The chain on which the service is run. Note that this is different from the worker
    /// chains which typically only run the controller application for managing the worker
    /// itself.
    pub chain_id: ChainId,
    /// The required capabilities for a worker to be useful (e.g. some API key).
    /// Concretely, the worker will read its environment variable
    pub requirements: Vec<String>,
}

scalar!(ManagedService);

/// The local state of a worker.
// This is used to facilitate service queries.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LocalWorkerState {
    /// The description of this worker as we registered it.
    pub local_worker: Option<Worker>,
    /// The services currently running locally.
    pub local_services: Vec<ManagedService>,
    /// The chains currently followed locally (besides ours and the active service
    /// chains).
    pub local_chains: Vec<ChainId>,
    /// The message policy that should be followed by the worker.
    pub local_message_policy: BTreeMap<ChainId, MessagePolicy>,
}

scalar!(LocalWorkerState);

/// Messages that can be exchanged across chains from the same application instance.
#[derive(Clone, Debug, Deserialize, Serialize, GraphQLMutationRootInCrate)]
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
    // -- Messages sent to the workers from the controller chain --
    Reset,
    Start {
        service_id: ManagedServiceId,
    },
    Stop {
        service_id: ManagedServiceId,
    },
    FollowChain {
        chain_id: ChainId,
    },
    ForgetChain {
        chain_id: ChainId,
    },
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{
        ControllerAbi, ControllerCommand, ManagedServiceId, Message, Operation, Worker,
        WorkerCommand,
    };
    use crate::{
        formats::{BcsApplication, Formats},
        linera_base_types::AccountOwner,
    };

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
            let (operation, _) = tracer.trace_type::<Operation>(&samples)?;
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
