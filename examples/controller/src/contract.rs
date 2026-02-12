// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::collections::HashSet;

use async_graphql::ComplexObject;
use linera_sdk::{
    abis::controller::{
        ControllerAbi, ControllerCommand, ManagedService, ManagedServiceId, Message, Operation,
        PendingService, Worker, WorkerCommand,
    },
    linera_base_types::{AccountOwner, ChainId, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::ControllerState;

pub struct ControllerContract {
    state: ControllerState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(ControllerContract);

impl WithContractAbi for ControllerContract {
    type Abi = ControllerAbi;
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
            Operation::StartLocalService { service_id } => {
                // The command can only be executed on a worker chain.
                assert!(self.state.local_worker.get().is_some());
                let pending_service = self
                    .state
                    .local_pending_services
                    .get(&service_id)
                    .await
                    .expect("storage")
                    .expect("pending service should exist");
                self.state
                    .local_pending_services
                    .remove(&service_id)
                    .expect("storage");
                self.state
                    .local_services
                    .insert(&service_id)
                    .expect("storage");
                // We can now remove the owners of the chain that are no longer needed.
                let service_blob_bytes = self.runtime.read_data_blob(service_id);
                let service_blob: ManagedService =
                    bcs::from_bytes(&service_blob_bytes).expect("bcs bytes");
                self.runtime
                    .prepare_message(Message::RemoveOwners {
                        owners_to_remove: pending_service.owners_to_remove,
                    })
                    .send_to(service_blob.chain_id);
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
                    "ExecuteWorkerCommand can only be executed on the chain that created the PM engine"
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
                    "ExecuteControllerCommand can only be executed on the chain that created the PM engine"
                );
                self.execute_controller_command_locally(admin, command)
                    .await;
            }
            Message::HandoffStarted {
                service_id,
                target_block_height,
            } => {
                assert_eq!(
                    self.runtime.chain_id(),
                    self.runtime.application_creator_chain_id(),
                    "HandoffStarted can only be executed on the chain that created the PM engine"
                );
                let service_pending_handoff = self
                    .state
                    .pending_services
                    .get(&service_id)
                    .await
                    .expect("storage")
                    .unwrap_or_default();
                self.state
                    .pending_services
                    .remove(&service_id)
                    .expect("storage");
                let message = Message::Start {
                    service_id,
                    owners_to_remove: service_pending_handoff.owners_to_remove.clone(),
                    start_height: Some(target_block_height),
                };
                for worker in service_pending_handoff.pending_workers {
                    log::info!("Notifying worker {worker}: {message:?}");
                    self.runtime
                        .prepare_message(message.clone())
                        .send_to(worker);
                }
            }
            Message::Reset => {
                self.state.local_worker.set(None);
                self.state.local_services.clear();
            }
            Message::Start {
                service_id,
                owners_to_remove,
                start_height,
            } => {
                if let Some(start_block_height) = start_height {
                    self.state
                        .local_pending_services
                        .insert(
                            &service_id,
                            PendingService {
                                owners_to_remove,
                                start_block_height,
                            },
                        )
                        .expect("storage");
                } else {
                    self.state
                        .local_services
                        .insert(&service_id)
                        .expect("storage");
                    if !owners_to_remove.is_empty() {
                        let service_blob_bytes = self.runtime.read_data_blob(service_id);
                        let service_blob: ManagedService =
                            bcs::from_bytes(&service_blob_bytes).expect("bcs bytes");
                        self.runtime
                            .prepare_message(Message::RemoveOwners { owners_to_remove })
                            .send_to(service_blob.chain_id);
                    }
                }
            }
            Message::Stop {
                service_id,
                new_owners,
            } => {
                let service_blob_bytes = self.runtime.read_data_blob(service_id);
                let service_blob: ManagedService =
                    bcs::from_bytes(&service_blob_bytes).expect("bcs bytes");
                self.runtime
                    .prepare_message(Message::AddOwners {
                        service_id,
                        new_owners,
                    })
                    .send_to(service_blob.chain_id);
            }
            Message::OwnersAdded {
                service_id,
                added_at,
            } => {
                self.state
                    .local_services
                    .remove(&service_id)
                    .expect("storage");
                self.runtime
                    .prepare_message(Message::HandoffStarted {
                        service_id,
                        target_block_height: added_at,
                    })
                    .send_to(self.runtime.application_creator_chain_id());
            }
            Message::AddOwners {
                service_id,
                new_owners,
            } => {
                self.update_owners(new_owners);
                let added_at = self.runtime.block_height();
                self.runtime
                    .prepare_message(Message::OwnersAdded {
                        service_id,
                        added_at,
                    })
                    .send_to(
                        self.runtime
                            .message_origin_chain_id()
                            .expect("message should have an origin"),
                    );
            }
            Message::RemoveOwners { owners_to_remove } => {
                self.remove_owners(owners_to_remove);
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

        let mut new_owners = HashSet::new();
        if existing_workers.is_empty() {
            // No workers to do any handoffs - just start the service directly on the
            // given workers.
            // `new_owners` will remain empty - it's okay, as we won't be sending `Stop`
            // messages, anyway.
            let message = Message::Start {
                service_id,
                owners_to_remove: HashSet::new(),
                start_height: None,
            };
            for worker in &new_workers {
                log::info!("Notifying worker {worker}: {message:?}");
                self.runtime
                    .prepare_message(message.clone())
                    .send_to(*worker);
            }
        } else {
            // We need the old workers to hand the service off to the new ones.
            let pending_service_entry = self
                .state
                .pending_services
                .get_mut_or_default(&service_id)
                .await
                .expect("storage");
            for worker_to_remove in existing_workers.difference(&new_workers) {
                let worker_key = self
                    .state
                    .workers
                    .get(worker_to_remove)
                    .await
                    .expect("storage")
                    .expect("should be removing an existing worker from a service")
                    .owner;
                pending_service_entry.owners_to_remove.insert(worker_key);
            }

            for worker in &new_workers {
                let worker_key = self
                    .state
                    .workers
                    .get(worker)
                    .await
                    .expect("storage")
                    .expect("should assign service to a registered worker")
                    .owner;
                new_owners.insert(worker_key);
                if !existing_workers.contains(worker) {
                    log::info!("Adding worker {worker} to pending services");
                    pending_service_entry.pending_workers.insert(*worker);
                }
            }
        }

        let message = Message::Stop {
            service_id,
            new_owners,
        };
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

    /// Adds `new_owners` to the set of owners of the current chain, with a weight of 100.
    /// Returns `true` if the set of owners has actually changed.
    fn update_owners(&mut self, new_owners: HashSet<AccountOwner>) -> bool {
        let mut ownership = self.runtime.chain_ownership();
        // Check if new_owners contains any owners that haven't been owners already.
        if new_owners
            .difference(&ownership.owners.keys().copied().collect())
            .count()
            == 0
        {
            return false;
        }
        ownership
            .owners
            .extend(new_owners.into_iter().zip(std::iter::repeat(100)));
        self.runtime
            .change_ownership(ownership)
            .expect("change ownership");
        true
    }

    /// Removes `owners_to_remove` from the set of owners of the current chain.
    fn remove_owners(&mut self, owners_to_remove: HashSet<AccountOwner>) {
        let mut ownership = self.runtime.chain_ownership();
        if owners_to_remove
            .intersection(&ownership.owners.keys().copied().collect())
            .count()
            == 0
        {
            // No owners to remove that are in the set of current owners.
            return;
        }
        for old_owner in owners_to_remove {
            ownership.owners.remove(&old_owner);
        }
        self.runtime
            .change_ownership(ownership)
            .expect("change ownership");
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
