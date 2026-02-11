// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Task processor for executing off-chain operators on behalf of on-chain applications.
//!
//! The task processor watches specified applications for requests to execute off-chain tasks,
//! runs external operator binaries, and submits the results back to the chain.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    path::PathBuf,
    sync::Arc,
};

use async_graphql::InputType as _;
use futures::{future, stream::StreamExt, FutureExt};
use linera_base::{
    data_types::{TimeDelta, Timestamp},
    identifiers::{ApplicationId, ChainId},
    task_processor::{ProcessorActions, TaskOutcome},
};
use linera_core::{client::ChainClient, node::NotificationStream, worker::Reason};
use serde_json::json;
use tokio::{io::AsyncWriteExt, process::Command, select, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::controller::Update;

/// A map from operator names to their binary paths.
pub type OperatorMap = Arc<BTreeMap<String, PathBuf>>;

/// Parse an operator mapping in the format `name=path` or just `name`.
/// If only `name` is provided, the path defaults to the name itself.
pub fn parse_operator(s: &str) -> Result<(String, PathBuf), String> {
    if let Some((name, path)) = s.split_once('=') {
        Ok((name.to_string(), PathBuf::from(path)))
    } else {
        Ok((s.to_string(), PathBuf::from(s)))
    }
}

type Deadline = Reverse<(Timestamp, Option<ApplicationId>)>;

/// A task processor that watches applications and executes off-chain operators.
pub struct TaskProcessor<Env: linera_core::Environment> {
    chain_id: ChainId,
    application_ids: Vec<ApplicationId>,
    last_requested_callbacks: BTreeMap<ApplicationId, Timestamp>,
    chain_client: ChainClient<Env>,
    cancellation_token: CancellationToken,
    notifications: NotificationStream,
    outcome_sender: mpsc::UnboundedSender<(ApplicationId, TaskOutcome)>,
    outcome_receiver: mpsc::UnboundedReceiver<(ApplicationId, TaskOutcome)>,
    update_receiver: mpsc::UnboundedReceiver<Update>,
    deadlines: BinaryHeap<Deadline>,
    operators: OperatorMap,
    last_task_handles: BTreeMap<ApplicationId, JoinHandle<()>>,
}

impl<Env: linera_core::Environment> TaskProcessor<Env> {
    /// Creates a new task processor.
    pub fn new(
        chain_id: ChainId,
        application_ids: Vec<ApplicationId>,
        chain_client: ChainClient<Env>,
        cancellation_token: CancellationToken,
        operators: OperatorMap,
        update_receiver: Option<mpsc::UnboundedReceiver<Update>>,
    ) -> Self {
        let notifications = chain_client.subscribe().expect("client subscription");
        let (outcome_sender, outcome_receiver) = mpsc::unbounded_channel();
        let update_receiver = update_receiver.unwrap_or_else(|| mpsc::unbounded_channel().1);
        Self {
            chain_id,
            application_ids,
            last_requested_callbacks: BTreeMap::new(),
            chain_client,
            cancellation_token,
            notifications,
            outcome_sender,
            outcome_receiver,
            update_receiver,
            deadlines: BinaryHeap::new(),
            operators,
            last_task_handles: BTreeMap::new(),
        }
    }

    /// Runs the task processor until the cancellation token is triggered.
    pub async fn run(mut self) {
        info!("Watching for notifications for chain {}", self.chain_id);
        self.process_actions(self.application_ids.clone()).await;
        loop {
            select! {
                Some(notification) = self.notifications.next() => {
                    if let Reason::NewBlock { .. } = notification.reason {
                        debug!(%self.chain_id, "Processing notification");
                        self.process_actions(self.application_ids.clone()).await;
                    }
                }
                _ = tokio::time::sleep(Self::duration_until_next_deadline(&self.deadlines)) => {
                    debug!("Processing event");
                    let application_ids = self.process_events();
                    self.process_actions(application_ids).await;
                }
                Some((application_id, outcome)) = self.outcome_receiver.recv() => {
                    if let Err(e) = self.submit_task_outcome(application_id, &outcome).await {
                        error!("Error while processing task outcome {outcome:?}: {e}");
                    }
                }
                Some(update) = self.update_receiver.recv() => {
                    self.apply_update(update).await;
                }
                _ = self.cancellation_token.cancelled().fuse() => {
                    break;
                }
            }
        }
        debug!("Notification stream ended.");
    }

    fn duration_until_next_deadline(deadlines: &BinaryHeap<Deadline>) -> tokio::time::Duration {
        deadlines
            .peek()
            .map_or(tokio::time::Duration::MAX, |Reverse((x, _))| {
                x.delta_since(Timestamp::now()).as_duration()
            })
    }

    async fn apply_update(&mut self, update: Update) {
        info!(
            "Applying update for chain {}: {:?}",
            self.chain_id, update.application_ids
        );

        let new_app_set: BTreeSet<_> = update.application_ids.iter().cloned().collect();
        let old_app_set: BTreeSet<_> = self.application_ids.iter().cloned().collect();

        // Retain only last_requested_callbacks for applications that are still active
        self.last_requested_callbacks
            .retain(|app_id, _| new_app_set.contains(app_id));

        // Update the application_ids
        self.application_ids = update.application_ids;

        // Process actions for newly added applications
        let new_apps = self
            .application_ids
            .iter()
            .filter(|app_id| !old_app_set.contains(app_id))
            .cloned()
            .collect::<Vec<_>>();
        if !new_apps.is_empty() {
            self.process_actions(new_apps).await;
        }
    }

    fn process_events(&mut self) -> Vec<ApplicationId> {
        let now = Timestamp::now();
        let mut application_ids = Vec::new();
        while let Some(deadline) = self.deadlines.pop() {
            if let Reverse((_, Some(id))) = deadline {
                application_ids.push(id);
            }
            let Some(Reverse((ts, _))) = self.deadlines.peek() else {
                break;
            };
            if *ts > now {
                break;
            }
        }
        application_ids
    }

    async fn process_actions(&mut self, application_ids: Vec<ApplicationId>) {
        for application_id in application_ids {
            debug!("Processing actions for {application_id}");
            let now = Timestamp::now();
            let last_requested_callback =
                self.last_requested_callbacks.get(&application_id).cloned();
            let actions = match self
                .query_actions(application_id, last_requested_callback, now)
                .await
            {
                Ok(actions) => actions,
                Err(error) => {
                    error!("Error reading application actions: {error}");
                    // Retry in at most 1 minute.
                    self.deadlines.push(Reverse((
                        now.saturating_add(TimeDelta::from_secs(60)),
                        Some(application_id),
                    )));
                    continue;
                }
            };
            if let Some(timestamp) = actions.request_callback {
                self.last_requested_callbacks.insert(application_id, now);
                self.deadlines
                    .push(Reverse((timestamp, Some(application_id))));
            }
            if !actions.execute_tasks.is_empty() {
                let sender = self.outcome_sender.clone();
                let operators = self.operators.clone();
                let previous = self.last_task_handles.remove(&application_id);
                let handle = tokio::spawn(async move {
                    // Spawn all tasks concurrently and join them.
                    let handles: Vec<_> = actions
                        .execute_tasks
                        .into_iter()
                        .map(|task| {
                            let operators = operators.clone();
                            tokio::spawn(Self::execute_task(
                                application_id,
                                task.operator,
                                task.input,
                                operators,
                            ))
                        })
                        .collect();
                    let results = future::join_all(handles).await;
                    // Wait for the previous batch to finish sending outcomes first.
                    if let Some(previous) = previous {
                        if let Err(error) = previous.await {
                            error!(%application_id, %error, "Task panicked");
                        }
                    }
                    // Submit outcomes in the original order.
                    for result in results {
                        match result {
                            Ok(Ok(outcome)) => {
                                if sender.send((application_id, outcome)).is_err() {
                                    error!("Outcome receiver dropped for {application_id}");
                                    break;
                                }
                            }
                            Ok(Err(error)) => {
                                error!(%application_id, %error, "Error executing task");
                            }
                            Err(error) => {
                                error!(%application_id, %error, "Task panicked");
                            }
                        }
                    }
                });
                self.last_task_handles.insert(application_id, handle);
            }
        }
    }

    async fn execute_task(
        application_id: ApplicationId,
        operator: String,
        input: String,
        operators: OperatorMap,
    ) -> Result<TaskOutcome, anyhow::Error> {
        let binary_path = operators
            .get(&operator)
            .ok_or_else(|| anyhow::anyhow!("unsupported operator: {}", operator))?;
        debug!("Executing task {operator} ({binary_path:?}) for {application_id}");
        let mut child = Command::new(binary_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()?;

        let mut stdin = child.stdin.take().expect("stdin should be configured");
        stdin.write_all(input.as_bytes()).await?;
        drop(stdin);

        let output = child.wait_with_output().await?;
        anyhow::ensure!(
            output.status.success(),
            "operator {} exited with status: {}",
            operator,
            output.status
        );
        let outcome = TaskOutcome {
            operator,
            output: String::from_utf8_lossy(&output.stdout).into(),
        };
        debug!("Done executing task for {application_id}");
        Ok(outcome)
    }

    async fn query_actions(
        &mut self,
        application_id: ApplicationId,
        last_requested_callback: Option<Timestamp>,
        now: Timestamp,
    ) -> Result<ProcessorActions, anyhow::Error> {
        let query = format!(
            "query {{ nextActions(lastRequestedCallback: {}, now: {}) }}",
            last_requested_callback.to_value(),
            now.to_value(),
        );
        let bytes = serde_json::to_vec(&json!({"query": query}))?;
        let query = linera_execution::Query::User {
            application_id,
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
        let actions: ProcessorActions =
            serde_json::from_value(response["data"]["nextActions"].take())?;
        Ok(actions)
    }

    async fn submit_task_outcome(
        &mut self,
        application_id: ApplicationId,
        task_outcome: &TaskOutcome,
    ) -> Result<(), anyhow::Error> {
        info!("Submitting task outcome for {application_id}: {task_outcome:?}");
        let query = format!(
            "query {{ processTaskOutcome(outcome: {{ operator: {}, output: {} }}) }}",
            task_outcome.operator.to_value(),
            task_outcome.output.to_value(),
        );
        let bytes = serde_json::to_vec(&json!({"query": query}))?;
        let query = linera_execution::Query::User {
            application_id,
            bytes,
        };
        let linera_execution::QueryOutcome {
            response: _,
            operations,
        } = self.chain_client.query_application(query, None).await?;
        if !operations.is_empty() {
            if let Err(e) = self
                .chain_client
                .execute_operations(operations, vec![])
                .await
            {
                // TODO: handle leader timeouts.
                error!("Failed to execute on-chain operations for {application_id}: {e}");
            }
        }
        Ok(())
    }
}
