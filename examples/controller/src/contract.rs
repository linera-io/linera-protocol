// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::collections::HashSet;

use async_graphql::ComplexObject;
use linera_sdk::{
    abis::controller::{
        ControllerAbi, ControllerCommand, ManagedServiceId, Operation, Worker, WorkerCommand,
    },
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, ChainId, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use serde::{Deserialize, Serialize};

use self::state::ControllerState;

pub struct ControllerContract {
    state: ControllerState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(ControllerContract);

impl WithContractAbi for ControllerContract {
    type Abi = ControllerAbi;
}

#[derive(Clone, Debug, Deserialize, Serialize, GraphQLMutationRoot)]
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

impl Contract for ControllerContract {
    type Message = Message;
    type Parameters = ();
    type InstantiationArgument = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = ControllerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        ControllerContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: Self::InstantiationArgument) {
        // validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        log::info!(
            "Processing operation on chain {}: {operation:?}",
            self.runtime.chain_id()
        );
        match operation {
            Operation::ExecuteWorkerCommand { owner, command } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Failed to authenticate owner for ExecuteWorkerCommand operation");
                self.prepare_worker_command_locally(owner, &command).await;
                let creator_chain_id = self.runtime.application_creator_chain_id();
                if self.runtime.chain_id() == creator_chain_id {
                    self.execute_worker_command_locally(owner, command, creator_chain_id)
                        .await;
                } else {
                    log::info!(
                        "Sending worker command from {} to {creator_chain_id} \
                        for remote execution: {owner} {command:?}",
                        self.runtime.chain_id()
                    );
                    self.runtime
                        .prepare_message(Message::ExecuteWorkerCommand { owner, command })
                        .send_to(creator_chain_id);
                }
            }
            Operation::ExecuteControllerCommand { admin, command } => {
                self.runtime
                    .check_account_permission(admin)
                    .expect("Failed to authenticate admin for ExecuteControllerCommand operation");
                let creator_chain_id = self.runtime.application_creator_chain_id();
                if self.runtime.chain_id() == creator_chain_id {
                    self.execute_controller_command_locally(admin, command)
                        .await;
                } else {
                    log::info!(
                        "Sending controller command from {} to {creator_chain_id} \
                        for remote execution: {admin} {command:?}",
                        self.runtime.chain_id()
                    );
                    self.runtime
                        .prepare_message(Message::ExecuteControllerCommand { admin, command })
                        .send_to(creator_chain_id);
                }
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        log::info!(
            "Processing message on chain {}: {message:?}",
            self.runtime.chain_id()
        );
        match message {
            Message::ExecuteWorkerCommand { owner, command } => {
                assert_eq!(
                    self.runtime.chain_id(),
                    self.runtime.application_creator_chain_id(),
                    "ExecuteAdminCommand can only be executed on the chain that created the PM engine"
                );
                let origin_chain_id = self.runtime.message_origin_chain_id().expect(
                    "Incoming message origin chain ID has to be available when executing a message",
                );
                self.execute_worker_command_locally(owner, command, origin_chain_id)
                    .await;
            }
            Message::ExecuteControllerCommand { admin, command } => {
                assert_eq!(
                    self.runtime.chain_id(),
                    self.runtime.application_creator_chain_id(),
                    "ExecuteAdminCommand can only be executed on the chain that created the PM engine"
                );
                self.execute_controller_command_locally(admin, command)
                    .await;
            }
            Message::Reset => {
                self.state.local_worker.set(None);
                self.state.local_services.clear();
            }
            Message::Start { service_id } => {
                self.state
                    .local_services
                    .insert(&service_id)
                    .expect("storage");
            }
            Message::Stop { service_id } => {
                self.state
                    .local_services
                    .remove(&service_id)
                    .expect("storage");
            }
            Message::FollowChain { chain_id } => {
                self.state.local_chains.insert(&chain_id).expect("storage");
            }
            Message::ForgetChain { chain_id } => {
                self.state.local_chains.remove(&chain_id).expect("storage");
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl ControllerContract {
    async fn prepare_worker_command_locally(
        &mut self,
        owner: AccountOwner,
        command: &WorkerCommand,
    ) {
        match command {
            WorkerCommand::RegisterWorker { capabilities } => {
                assert!(
                    self.state.local_worker.get().is_none(),
                    "Cannot register worker twice"
                );
                let worker = Worker {
                    owner,
                    capabilities: capabilities.clone(),
                };
                self.state.local_worker.set(Some(worker));
            }
            WorkerCommand::DeregisterWorker => {
                assert!(
                    self.state.local_worker.get().is_some(),
                    "Cannot unregister worker that is not registered"
                );
                self.state.local_worker.set(None);
            }
        }
    }

    async fn execute_worker_command_locally(
        &mut self,
        owner: AccountOwner,
        command: WorkerCommand,
        origin_chain_id: ChainId,
    ) {
        log::info!("Executing worker command locally: {owner} {command:?} {origin_chain_id}");
        match command {
            WorkerCommand::RegisterWorker { capabilities } => {
                let worker = Worker {
                    owner,
                    capabilities,
                };
                self.state
                    .workers
                    .insert(&origin_chain_id, worker)
                    .expect("storage");
            }
            WorkerCommand::DeregisterWorker => {
                self.state
                    .workers
                    .remove(&origin_chain_id)
                    .expect("storage");
            }
        }
    }

    async fn execute_controller_command_locally(
        &mut self,
        admin: AccountOwner,
        command: ControllerCommand,
    ) {
        log::info!("Executing controller command locally: {admin} {command:?}");
        if let Some(admins) = self.state.admins.get() {
            assert!(
                admins.contains(&admin),
                "Controller command can only be executed by an authorized admin account. Got {admin}"
            );
        } else {
            // No admin list was set yet. Hence, everyone is an owner. However, messages
            // are disallowed.
            assert!(
                self.runtime.message_origin_chain_id().is_none(),
                "Refusing to execute remote control command",
            );
        };

        match command {
            ControllerCommand::SetAdmins { admins } => {
                self.state
                    .admins
                    .set(admins.map(|v| v.into_iter().collect()));
            }
            ControllerCommand::RemoveWorker { worker_id } => {
                self.state.workers.remove(&worker_id).expect("storage");
                self.runtime
                    .prepare_message(Message::Reset)
                    .send_to(worker_id);
                let services_ids = self.state.services.indices().await.expect("storage");
                for id in services_ids {
                    let mut workers = self
                        .state
                        .services
                        .get(&id)
                        .await
                        .expect("storage")
                        .expect("value should be present");
                    if workers.remove(&worker_id) {
                        self.update_service(id, workers).await;
                    }
                }
                let chain_ids = self.state.chains.indices().await.expect("storage");
                for id in chain_ids {
                    let mut workers = self
                        .state
                        .chains
                        .get(&id)
                        .await
                        .expect("storage")
                        .expect("value should be present");
                    if workers.remove(&worker_id) {
                        self.update_chain(id, workers).await;
                    }
                }
            }
            ControllerCommand::UpdateService {
                service_id,
                workers,
            } => {
                self.update_service(service_id, workers.into_iter().collect())
                    .await;
            }
            ControllerCommand::RemoveService { service_id } => {
                self.update_service(service_id, HashSet::new()).await;
            }
            ControllerCommand::UpdateAllServices { services } => {
                let mut previous_ids = self
                    .state
                    .services
                    .indices()
                    .await
                    .expect("storage")
                    .into_iter()
                    .collect::<HashSet<_>>();
                for (id, workers) in services {
                    previous_ids.remove(&id);
                    self.update_service(id, workers.into_iter().collect()).await;
                }
                for id in previous_ids {
                    self.update_service(id, HashSet::new()).await;
                }
            }
            ControllerCommand::UpdateChain { chain_id, workers } => {
                self.update_chain(chain_id, workers.into_iter().collect())
                    .await;
            }
            ControllerCommand::RemoveChain { chain_id } => {
                self.update_chain(chain_id, HashSet::new()).await;
            }
            ControllerCommand::UpdateAllChains { chains } => {
                let mut previous_ids = self
                    .state
                    .chains
                    .indices()
                    .await
                    .expect("storage")
                    .into_iter()
                    .collect::<HashSet<_>>();
                for (id, workers) in chains {
                    previous_ids.remove(&id);
                    self.update_chain(id, workers.into_iter().collect()).await;
                }
                for id in previous_ids {
                    self.update_chain(id, HashSet::new()).await;
                }
            }
        }
    }

    async fn update_service(
        &mut self,
        service_id: ManagedServiceId,
        new_workers: HashSet<ChainId>,
    ) {
        log::info!("Updating {service_id:?}: {new_workers:?}");
        let existing_workers = self
            .state
            .services
            .get(&service_id)
            .await
            .expect("storage")
            .unwrap_or_default();

        let message = Message::Start { service_id };
        for worker in &new_workers {
            if !existing_workers.contains(worker) {
                log::info!("Notifying worker {worker}: {message:?}");
                self.runtime
                    .prepare_message(message.clone())
                    .send_to(*worker);
            }
        }
        let message = Message::Stop { service_id };
        for worker in &existing_workers {
            if !new_workers.contains(worker) {
                log::info!("Notifying worker {worker}: {message:?}");
                self.runtime
                    .prepare_message(message.clone())
                    .send_to(*worker);
            }
        }
        if new_workers.is_empty() {
            self.state.services.remove(&service_id).expect("storage");
        } else {
            self.state
                .services
                .insert(&service_id, new_workers)
                .expect("storage");
        }
    }

    async fn update_chain(&mut self, chain_id: ChainId, new_workers: HashSet<ChainId>) {
        let existing_workers = self
            .state
            .chains
            .get(&chain_id)
            .await
            .expect("storage")
            .unwrap_or_default();

        let message = Message::FollowChain { chain_id };
        for worker in &new_workers {
            if !existing_workers.contains(worker) {
                self.runtime
                    .prepare_message(message.clone())
                    .send_to(*worker);
            }
        }
        let message = Message::ForgetChain { chain_id };
        for worker in &existing_workers {
            if !new_workers.contains(worker) {
                self.runtime
                    .prepare_message(message.clone())
                    .send_to(*worker);
            }
        }
        if new_workers.is_empty() {
            self.state.chains.remove(&chain_id).expect("storage");
        } else {
            self.state
                .chains
                .insert(&chain_id, new_workers)
                .expect("storage");
        }
    }
}

/// This implementation is only nonempty in the service.
#[ComplexObject]
impl ControllerState {}

#[cfg(test)]
mod tests {
    use futures::FutureExt as _;
    use linera_sdk::{util::BlockingWait, views::View, Contract, ContractRuntime};

    use super::{ControllerContract, ControllerState};

    #[test]
    fn instantiate() {
        let _app = create_and_instantiate_app();
        // Basic instantiation test
    }

    fn create_and_instantiate_app() -> ControllerContract {
        let runtime = ContractRuntime::new().with_application_parameters(());
        let mut contract = ControllerContract {
            state: ControllerState::load(runtime.root_view_storage_context())
                .blocking_wait()
                .expect("Failed to read from mock key value store"),
            runtime,
        };

        contract
            .instantiate(())
            .now_or_never()
            .expect("Initialization of application state should not await anything");

        contract
    }
}
