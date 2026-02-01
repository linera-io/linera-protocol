// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use futures::{lock::Mutex, stream::StreamExt, FutureExt};
use linera_base::identifiers::{ApplicationId, ChainId};
use linera_client::chain_listener::{ClientContext, ListenerCommand};
use linera_core::{client::ChainClient, node::NotificationStream, worker::Reason};
use linera_sdk::abis::controller::{LocalWorkerState, Operation, WorkerCommand};
use serde_json::json;
use tokio::{
    select,
    sync::mpsc::{self, UnboundedSender},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::task_processor::{OperatorMap, TaskProcessor};

/// An update message sent to a TaskProcessor to change its set of applications.
#[derive(Debug)]
pub struct Update {
    pub application_ids: Vec<ApplicationId>,
}

struct ProcessorHandle {
    update_sender: mpsc::UnboundedSender<Update>,
}

pub struct Controller<Ctx: ClientContext> {
    chain_id: ChainId,
    controller_id: ApplicationId,
    context: Arc<Mutex<Ctx>>,
    chain_client: ChainClient<Ctx::Environment>,
    cancellation_token: CancellationToken,
    notifications: NotificationStream,
    operators: OperatorMap,
    processors: BTreeMap<ChainId, ProcessorHandle>,
    listened_local_chains: BTreeSet<ChainId>,
    command_sender: UnboundedSender<ListenerCommand>,
}

impl<Ctx> Controller<Ctx>
where
    Ctx: ClientContext + Send + Sync + 'static,
    Ctx::Environment: 'static,
    <Ctx::Environment as linera_core::Environment>::Storage: Clone,
{
    pub fn new(
        chain_id: ChainId,
        controller_id: ApplicationId,
        context: Arc<Mutex<Ctx>>,
        chain_client: ChainClient<Ctx::Environment>,
        cancellation_token: CancellationToken,
        operators: OperatorMap,
        command_sender: UnboundedSender<ListenerCommand>,
    ) -> Self {
        let notifications = chain_client.subscribe().expect("client subscription");
        Self {
            chain_id,
            controller_id,
            context,
            chain_client,
            cancellation_token,
            notifications,
            operators,
            processors: BTreeMap::new(),
            listened_local_chains: BTreeSet::new(),
            command_sender,
        }
    }

    pub async fn run(mut self) {
        info!(
            "Watching for notifications for controller chain {}",
            self.chain_id
        );
        self.process_controller_state().await;
        loop {
            select! {
                Some(notification) = self.notifications.next() => {
                    if let Reason::NewBlock { .. } = notification.reason {
                        debug!("Processing notification on controller chain {}", self.chain_id);
                        self.process_controller_state().await;
                    }
                }
                _ = self.cancellation_token.cancelled().fuse() => {
                    break;
                }
            }
        }
        debug!("Notification stream ended.");
    }

    async fn process_controller_state(&mut self) {
        let state = match self.query_controller_state().await {
            Ok(state) => state,
            Err(error) => {
                error!("Error reading controller state: {error}");
                return;
            }
        };
        let Some(worker) = state.local_worker else {
            // Worker needs to be registered.
            self.register_worker().await;
            return;
        };
        assert_eq!(
            worker.owner,
            self.chain_client
                .preferred_owner()
                .expect("The current wallet should own the chain being watched"),
            "We should be registered with the current account owner."
        );

        // Build a map of ChainId -> Vec<ApplicationId> from local_services
        let mut chain_apps: BTreeMap<ChainId, Vec<ApplicationId>> = BTreeMap::new();
        for service in &state.local_services {
            chain_apps
                .entry(service.chain_id)
                .or_default()
                .push(service.application_id);
        }

        let old_chains: BTreeSet<_> = self.processors.keys().cloned().collect();

        // Update or spawn processors for each chain
        for (service_chain_id, application_ids) in chain_apps {
            if let Err(err) = self
                .update_or_spawn_processor(service_chain_id, application_ids)
                .await
            {
                error!("Error updating or spawning processor: {err}");
                return;
            }
        }

        // Send empty updates to processors for chains no longer in the state
        // This effectively tells them to stop processing applications
        let active_chains: std::collections::BTreeSet<_> =
            state.local_services.iter().map(|s| s.chain_id).collect();
        let stale_chains: BTreeSet<_> = self
            .processors
            .keys()
            .filter(|chain_id| !active_chains.contains(chain_id))
            .cloned()
            .collect();
        for chain_id in &stale_chains {
            if let Some(handle) = self.processors.get(chain_id) {
                let update = Update {
                    application_ids: Vec::new(),
                };
                if handle.update_sender.send(update).is_err() {
                    // Processor has stopped, remove it
                    self.processors.remove(chain_id);
                }
            }
        }

        // Collect local_chains from state
        let local_chains: BTreeSet<_> = state.local_chains.iter().cloned().collect();

        // Compute all chains we were listening to (processors + local_chains)
        let old_listened: BTreeSet<_> = old_chains
            .union(&self.listened_local_chains)
            .cloned()
            .collect();

        // Compute all chains we want to listen to (active services + local_chains)
        let desired_listened: BTreeSet<_> = active_chains.union(&local_chains).cloned().collect();

        // New chains to listen (neither had processor nor were in listened_local_chains)
        let owner = worker.owner;
        let new_chains: BTreeMap<_, _> = desired_listened
            .difference(&old_listened)
            .map(|chain_id| (*chain_id, Some(owner)))
            .collect();

        // Chains to stop listening (were listened but no longer needed)
        let chains_to_stop: BTreeSet<_> = old_listened
            .difference(&desired_listened)
            .cloned()
            .collect();

        // Update listened_local_chains for next iteration
        // These are local_chains that don't have services (not in active_chains)
        self.listened_local_chains = local_chains.difference(&active_chains).cloned().collect();

        if let Err(error) = self.command_sender.send(ListenerCommand::SetMessagePolicy(
            state.local_message_policy,
        )) {
            error!(%error, "error sending a command to chain listener");
        }
        if let Err(error) = self
            .command_sender
            .send(ListenerCommand::Listen(new_chains))
        {
            error!(%error, "error sending a command to chain listener");
        }
        if let Err(error) = self
            .command_sender
            .send(ListenerCommand::StopListening(chains_to_stop))
        {
            error!(%error, "error sending a command to chain listener");
        }
    }

    async fn register_worker(&mut self) {
        let capabilities = self.operators.keys().cloned().collect();
        let command = WorkerCommand::RegisterWorker { capabilities };
        let owner = self
            .chain_client
            .preferred_owner()
            .expect("The current wallet should own the chain being watched");
        let bytes =
            bcs::to_bytes(&Operation::ExecuteWorkerCommand { owner, command }).expect("bcs bytes");
        let operation = linera_execution::Operation::User {
            application_id: self.controller_id,
            bytes,
        };
        if let Err(e) = self
            .chain_client
            .execute_operations(vec![operation], vec![])
            .await
        {
            // TODO: handle leader timeouts
            error!("Failed to execute worker on-chain registration: {e}");
        }
    }

    async fn update_or_spawn_processor(
        &mut self,
        service_chain_id: ChainId,
        application_ids: Vec<ApplicationId>,
    ) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.processors.get(&service_chain_id) {
            // Processor exists, send update
            let update = Update {
                application_ids: application_ids.clone(),
            };
            if handle.update_sender.send(update).is_err() {
                // Processor has stopped, remove and respawn
                self.processors.remove(&service_chain_id);
                self.spawn_processor(service_chain_id, application_ids)
                    .await?;
            }
        } else {
            // No processor for this chain, spawn one
            self.spawn_processor(service_chain_id, application_ids)
                .await?;
        }
        Ok(())
    }

    async fn spawn_processor(
        &mut self,
        service_chain_id: ChainId,
        application_ids: Vec<ApplicationId>,
    ) -> Result<(), anyhow::Error> {
        info!(
            "Spawning TaskProcessor for chain {} with applications {:?}",
            service_chain_id, application_ids
        );

        let (update_sender, update_receiver) = mpsc::unbounded_channel();

        let chain_client = self
            .context
            .lock()
            .await
            .make_chain_client(service_chain_id)
            .await?;
        let processor = TaskProcessor::new(
            service_chain_id,
            application_ids,
            chain_client,
            self.cancellation_token.child_token(),
            self.operators.clone(),
            Some(update_receiver),
        );

        tokio::spawn(processor.run());

        self.processors
            .insert(service_chain_id, ProcessorHandle { update_sender });

        Ok(())
    }

    async fn query_controller_state(&mut self) -> Result<LocalWorkerState, anyhow::Error> {
        let query = "query { localWorkerState }";
        let bytes = serde_json::to_vec(&json!({"query": query}))?;
        let query = linera_execution::Query::User {
            application_id: self.controller_id,
            bytes,
        };
        let linera_execution::QueryOutcome {
            response,
            operations: _,
        } = self.chain_client.query_application(query, None).await?;
        let linera_execution::QueryResponse::User(response) = response else {
            anyhow::bail!("cannot get a system response for a user query");
        };
        let mut response: serde_json::Value = serde_json::from_slice(&response)?;
        let state = serde_json::from_value(response["data"]["localWorkerState"].take())?;
        Ok(state)
    }
}
