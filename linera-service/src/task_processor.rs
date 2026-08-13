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
    task_processor::{ProcessorActions, Task, TaskOutcome},
};
use linera_core::{
    client::ChainClient, data_types::ClientOutcome, node::NotificationStream, worker::Reason,
};
use serde_json::json;
use tokio::{io::AsyncWriteExt, process::Command, select, sync::mpsc};
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

/// Message sent from a background batch task to the main loop on completion.
struct BatchResult {
    application_id: ApplicationId,
    /// If set, the batch failed and should be retried at this timestamp.
    retry_at: Option<Timestamp>,
}

/// A task processor that watches applications and executes off-chain operators.
pub struct TaskProcessor<Env: linera_core::Environment> {
    chain_id: ChainId,
    application_ids: Vec<ApplicationId>,
    cursors: BTreeMap<ApplicationId, String>,
    chain_client: ChainClient<Env>,
    cancellation_token: CancellationToken,
    notifications: NotificationStream,
    batch_sender: mpsc::UnboundedSender<BatchResult>,
    batch_receiver: mpsc::UnboundedReceiver<BatchResult>,
    update_receiver: mpsc::UnboundedReceiver<Update>,
    deadlines: BinaryHeap<Deadline>,
    operators: OperatorMap,
    retry_delay: TimeDelta,
    in_flight_apps: BTreeSet<ApplicationId>,
}

impl<Env: linera_core::Environment> TaskProcessor<Env> {
    /// Creates a new task processor.
    pub fn new(
        chain_id: ChainId,
        application_ids: Vec<ApplicationId>,
        chain_client: ChainClient<Env>,
        cancellation_token: CancellationToken,
        operators: OperatorMap,
        retry_delay: TimeDelta,
        update_receiver: Option<mpsc::UnboundedReceiver<Update>>,
    ) -> Self {
        let notifications = chain_client.subscribe().expect("client subscription");
        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let update_receiver = update_receiver.unwrap_or_else(|| mpsc::unbounded_channel().1);
        Self {
            chain_id,
            application_ids,
            cursors: BTreeMap::new(),
            chain_client,
            cancellation_token,
            notifications,
            batch_sender,
            batch_receiver,
            update_receiver,
            deadlines: BinaryHeap::new(),
            operators,
            retry_delay,
            in_flight_apps: BTreeSet::new(),
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
                Some(result) = self.batch_receiver.recv() => {
                    self.in_flight_apps.remove(&result.application_id);
                    // The application could have been unassigned from this processor
                    // in the meantime - do not retry if that is the case.
                    if self.application_ids.contains(&result.application_id) {
                        if let Some(retry_at) = result.retry_at {
                            self.deadlines.push(Reverse((
                                retry_at,
                                Some(result.application_id),
                            )));
                        } else {
                            // Re-process immediately to pick up new tasks.
                            self.process_actions(vec![result.application_id]).await;
                        }
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

        self.cursors
            .retain(|app_id, _| new_app_set.contains(app_id));
        self.in_flight_apps
            .retain(|app_id| new_app_set.contains(app_id));

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
            if !self.application_ids.contains(&application_id) {
                debug!("Skipping {application_id}: it's no longer assigned to this processor");
                continue;
            }
            if self.in_flight_apps.contains(&application_id) {
                debug!("Skipping {application_id}: tasks already in flight");
                continue;
            }
            debug!("Processing actions for {application_id}");
            let now = Timestamp::now();
            let app_cursor = self.cursors.get(&application_id).cloned();
            let actions = match self.query_actions(application_id, app_cursor, now).await {
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
                self.deadlines
                    .push(Reverse((timestamp, Some(application_id))));
            }
            if let Some(cursor) = actions.set_cursor {
                self.cursors.insert(application_id, cursor);
            }
            if !actions.execute_tasks.is_empty() {
                self.in_flight_apps.insert(application_id);
                let chain_client = self.chain_client.clone();
                let batch_sender = self.batch_sender.clone();
                let retry_delay = self.retry_delay;
                let operators = self.operators.clone();
                tokio::spawn(async move {
                    // Outcomes are independent only if every task in the batch is identified:
                    // an application that has to match an outcome by position would pop the
                    // wrong entry as soon as one is skipped.
                    let independent = actions.execute_tasks.iter().all(|task| task.id.is_some());
                    // Spawn all tasks concurrently and join them.
                    let handles: Vec<_> = actions
                        .execute_tasks
                        .into_iter()
                        .map(|task| {
                            let operators = operators.clone();
                            tokio::spawn(Self::execute_task(application_id, task, operators))
                        })
                        .collect();
                    let results = future::join_all(handles)
                        .await
                        .into_iter()
                        .map(|result| {
                            result.unwrap_or_else(|error| {
                                Err(anyhow::anyhow!("task panicked: {error}"))
                            })
                        })
                        .collect();
                    // Tasks are assumed idempotent: whatever is not submitted here is
                    // recomputed by the next call to `nextActions`.
                    let (outcomes, errors) = split_batch_results(results, independent);
                    // `None` sorts before any timestamp, so keeping the maximum keeps the
                    // latest retry the batch asked for: a task failing on every attempt
                    // cannot shorten the delay protecting the operator.
                    let mut retry_at = None;
                    for error in errors {
                        error!(%application_id, %error, "Error executing task");
                        retry_at = retry_at.max(Some(Timestamp::now().saturating_add(retry_delay)));
                    }
                    for outcome in outcomes {
                        if let Err(timestamp) = Self::submit_task_outcome(
                            &chain_client,
                            application_id,
                            &outcome,
                            retry_delay,
                        )
                        .await
                        {
                            retry_at = retry_at.max(Some(timestamp));
                            if !independent {
                                break;
                            }
                        }
                    }
                    if batch_sender
                        .send(BatchResult {
                            application_id,
                            retry_at,
                        })
                        .is_err()
                    {
                        error!("Batch receiver dropped for {application_id}");
                    }
                });
            }
        }
    }

    async fn execute_task(
        application_id: ApplicationId,
        task: Task,
        operators: OperatorMap,
    ) -> Result<TaskOutcome, anyhow::Error> {
        let Task {
            id,
            operator,
            input,
        } = task;
        let binary_path = operators
            .get(&operator)
            .ok_or_else(|| anyhow::anyhow!("unsupported operator: {operator}"))?;
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
            id,
            operator,
            output: String::from_utf8_lossy(&output.stdout).into(),
        };
        debug!("Done executing task for {application_id}");
        Ok(outcome)
    }

    // Keeping `&mut self` avoids borrowing `TaskProcessor` through `&self` across `.await`,
    // which would make the spawned future require `TaskProcessor: Sync`.
    #[expect(clippy::needless_pass_by_ref_mut)]
    async fn query_actions(
        &mut self,
        application_id: ApplicationId,
        cursor: Option<String>,
        now: Timestamp,
    ) -> Result<ProcessorActions, anyhow::Error> {
        let query = format!(
            "query {{ nextActions(cursor: {}, now: {}) }}",
            cursor.to_value(),
            now.to_value(),
        );
        let bytes = serde_json::to_vec(&json!({"query": query}))?;
        let query = linera_execution::Query::User {
            application_id,
            bytes,
        };
        let (
            linera_execution::QueryOutcome {
                response,
                operations: _,
            },
            _,
        ) = self.chain_client.query_application(query, None).await?;
        let linera_execution::QueryResponse::User(response) = response else {
            anyhow::bail!("cannot get a system response for a user query");
        };
        let mut response: serde_json::Value = serde_json::from_slice(&response)?;
        let actions: ProcessorActions =
            serde_json::from_value(response["data"]["nextActions"].take())?;
        Ok(actions)
    }

    /// Submits a task outcome on-chain. On success returns `Ok(())`. On failure, logs the
    /// error and returns `Err(retry_at)` with the timestamp at which to retry.
    async fn submit_task_outcome(
        chain_client: &ChainClient<Env>,
        application_id: ApplicationId,
        task_outcome: &TaskOutcome,
        retry_delay: TimeDelta,
    ) -> Result<(), Timestamp> {
        info!("Submitting task outcome for {application_id}: {task_outcome:?}");
        let retry_with_delay = || Timestamp::now().saturating_add(retry_delay);
        let query = task_outcome_query(task_outcome);
        let bytes = serde_json::to_vec(&json!({"query": query})).map_err(|error| {
            error!(%application_id, %error, "Error serializing task outcome query");
            retry_with_delay()
        })?;
        let query = linera_execution::Query::User {
            application_id,
            bytes,
        };
        let (
            linera_execution::QueryOutcome {
                response: _,
                operations,
            },
            _,
        ) = chain_client
            .query_application(query, None)
            .await
            .map_err(|error| {
                error!(%application_id, %error, "Error querying application");
                retry_with_delay()
            })?;
        if !operations.is_empty() {
            match chain_client
                .execute_operations(operations, vec![])
                .await
                .map_err(|error| {
                    error!(%application_id, %error, "Error executing operations");
                    retry_with_delay()
                })? {
                ClientOutcome::Committed(_) => {}
                ClientOutcome::WaitForTimeout(timeout) => {
                    error!(%application_id, "Not the round leader, retrying after {}", timeout.timestamp);
                    return Err(timeout.timestamp);
                }
                ClientOutcome::Conflict(_) => {
                    debug!(%application_id, "Block conflict, retrying immediately");
                    return Err(Timestamp::now());
                }
            }
        }
        Ok(())
    }
}

/// Splits the results of a finished batch into the outcomes to submit and the errors to
/// report.
///
/// If the outcomes are not `independent`, the ones ordered after the first failure are
/// dropped and recomputed on the next attempt, so that the application sees no gap in the
/// sequence it matches them against.
fn split_batch_results(
    results: Vec<Result<TaskOutcome, anyhow::Error>>,
    independent: bool,
) -> (Vec<TaskOutcome>, Vec<anyhow::Error>) {
    let mut outcomes = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => {
                errors.push(error);
                if !independent {
                    break;
                }
            }
        }
    }
    (outcomes, errors)
}

/// Builds the GraphQL query submitting `task_outcome` to its application.
fn task_outcome_query(task_outcome: &TaskOutcome) -> String {
    format!(
        "query {{ processTaskOutcome(outcome: {}) }}",
        task_outcome.to_value()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(id: Option<&str>, output: &str) -> TaskOutcome {
        TaskOutcome {
            id: id.map(str::to_string),
            operator: "echo".to_string(),
            output: output.to_string(),
        }
    }

    #[test]
    fn test_split_batch_results_stops_at_first_failure_when_ordered() {
        let results = vec![
            Ok(outcome(None, "first")),
            Err(anyhow::anyhow!("boom")),
            Ok(outcome(None, "third")),
        ];
        let (outcomes, errors) = split_batch_results(results, false);
        assert_eq!(
            outcomes.into_iter().map(|o| o.output).collect::<Vec<_>>(),
            vec!["first"]
        );
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_split_batch_results_keeps_siblings_when_independent() {
        let results = vec![
            Err(anyhow::anyhow!("boom")),
            Ok(outcome(Some("2"), "second")),
            Err(anyhow::anyhow!("bang")),
            Ok(outcome(Some("4"), "fourth")),
        ];
        let (outcomes, errors) = split_batch_results(results, true);
        assert_eq!(
            outcomes.into_iter().map(|o| o.output).collect::<Vec<_>>(),
            vec!["second", "fourth"]
        );
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn test_task_outcome_query() {
        assert_eq!(
            task_outcome_query(&outcome(None, "hello")),
            r#"query { processTaskOutcome(outcome: {operator: "echo", output: "hello"}) }"#
        );
        assert_eq!(
            task_outcome_query(&outcome(Some("42"), "hello")),
            r#"query { processTaskOutcome(outcome: {id: "42", operator: "echo", output: "hello"}) }"#
        );
    }
}
