// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{scalar, Request, Response, SimpleObject};
use linera_sdk_derive::GraphQLMutationRootInCrate;
use serde::{Deserialize, Serialize};

use crate::linera_base_types::{
    AccountOwner, ApplicationId, ChainId, ContractAbi, DataBlobHash, ServiceAbi,
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
}

scalar!(LocalWorkerState);
