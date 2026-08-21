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
use futures::{stream::StreamExt, FutureExt};
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
use tracing::{debug, error, info, warn};

use crate::controller::Update;

#[cfg(with_metrics)]
pub(crate) mod metrics {
    use linera_base::prometheus_util::register_int_counter;
    use prometheus::IntCounter;

    linera_base::declare_metrics! {
        /// Task groups that have outlived [`TaskProcessorConfig::slow_group_threshold`].
        ///
        /// The group is still running when this is incremented and may well succeed: a batch
        /// catching up on a backlog takes far longer than a steady-state one. What makes it worth
        /// alerting on is that nothing else reports it — the process stays healthy and its chain
        /// simply stops advancing, so a group that never returns is otherwise silent.
        pub static SLOW_TASK_GROUPS: IntCounter = register_int_counter(
            "slow_task_groups_total",
            "Number of task groups that outlived the slow-group threshold"
        );
    }
}

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

/// Timing limits applied to operator tasks.
#[derive(Clone, Copy, Debug)]
pub struct TaskProcessorConfig {
    /// How long to wait before retrying a task group that failed.
    pub retry_delay: TimeDelta,
    /// How long a task group may run before it is reported as slow.
    ///
    /// Reported only: the group is never interrupted. An operator that is merely slow cannot be
    /// told apart from one that will never return, and killing it would abort work that was about
    /// to succeed — a batch catching up on a backlog legitimately runs far longer than a
    /// steady-state one. Since a task that is cut short is retried, a limit set below what the
    /// work needs never completes it, it only repeats it.
    pub slow_group_threshold: TimeDelta,
}

/// Message sent from a background task group to the main loop on completion.
struct GroupResult {
    application_id: ApplicationId,
    /// The group's id, as returned by [`group_tasks`].
    group: Option<String>,
    /// If set, the group failed and should be retried at this timestamp.
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
    result_sender: mpsc::UnboundedSender<GroupResult>,
    result_receiver: mpsc::UnboundedReceiver<GroupResult>,
    update_receiver: mpsc::UnboundedReceiver<Update>,
    deadlines: BinaryHeap<Deadline>,
    operators: OperatorMap,
    config: TaskProcessorConfig,
    /// The groups currently running, so that a second copy is never started while one is in
    /// flight. Keyed by group rather than by application: a group that is slow or stuck must not
    /// keep its siblings from being polled.
    in_flight_groups: BTreeSet<(ApplicationId, Option<String>)>,
}

impl<Env: linera_core::Environment> TaskProcessor<Env> {
    /// Creates a new task processor.
    pub fn new(
        chain_id: ChainId,
        application_ids: Vec<ApplicationId>,
        chain_client: ChainClient<Env>,
        cancellation_token: CancellationToken,
        operators: OperatorMap,
        config: TaskProcessorConfig,
        update_receiver: Option<mpsc::UnboundedReceiver<Update>>,
    ) -> Self {
        let notifications = chain_client.subscribe().expect("client subscription");
        let (result_sender, result_receiver) = mpsc::unbounded_channel();
        let update_receiver = update_receiver.unwrap_or_else(|| mpsc::unbounded_channel().1);
        Self {
            chain_id,
            application_ids,
            cursors: BTreeMap::new(),
            chain_client,
            cancellation_token,
            notifications,
            result_sender,
            result_receiver,
            update_receiver,
            deadlines: BinaryHeap::new(),
            operators,
            config,
            in_flight_groups: BTreeSet::new(),
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
                Some(result) = self.result_receiver.recv() => {
                    self.in_flight_groups.remove(&(result.application_id, result.group));
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
        self.in_flight_groups
            .retain(|(app_id, _)| new_app_set.contains(app_id));

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
            debug!("Processing actions for {application_id}");
            let now = Timestamp::now();
            let app_cursor = self.cursors.get(&application_id).cloned();
            let actions = match self.query_actions(application_id, app_cursor, now).await {
                Ok(actions) => actions,
                Err(error) => {
                    error!(%application_id, %error, "Error reading application actions");
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
            // Start each group that is not already running. Groups are independent: their
            // outcomes commute, so one that is slow, stuck or failing must neither delay the
            // others nor keep this application from being polled again for their sake.
            for (group, tasks) in group_tasks(actions.execute_tasks) {
                if !self
                    .in_flight_groups
                    .insert((application_id, group.clone()))
                {
                    debug!(%application_id, ?group, "Skipping group: tasks already in flight");
                    continue;
                }
                let chain_client = self.chain_client.clone();
                let result_sender = self.result_sender.clone();
                let config = self.config;
                let operators = self.operators.clone();
                tokio::spawn(async move {
                    // Run the group on its own task so that a panic inside it is reported and
                    // turned into a retry, rather than losing the result message and leaving the
                    // group marked in flight forever.
                    let handle = tokio::spawn(Self::process_group(
                        application_id,
                        group.clone(),
                        tasks,
                        chain_client,
                        operators,
                        config,
                    ));
                    let retry_at = await_group(application_id, &group, handle, config).await;
                    if result_sender
                        .send(GroupResult {
                            application_id,
                            group,
                            retry_at,
                        })
                        .is_err()
                    {
                        error!(%application_id, "Result receiver dropped");
                    }
                });
            }
        }
    }

    /// Runs the tasks of one group, submitting their outcomes in order and stopping at the
    /// first failure: the outcomes of a group are matched by position, so the application must
    /// never see a gap in the sequence.
    ///
    /// Only the submissions are ordered. They contend for the chain's proposal lock, so
    /// running a task only once its predecessor is committed would make every query wait
    /// behind the block production of unrelated groups.
    ///
    /// Tasks are assumed idempotent, so whatever is left unsubmitted is recomputed by the next
    /// call to `nextActions`. Returns the timestamp at which to retry the group, if it failed.
    async fn process_group(
        application_id: ApplicationId,
        group: Option<String>,
        tasks: Vec<Task>,
        chain_client: ChainClient<Env>,
        operators: OperatorMap,
        config: TaskProcessorConfig,
    ) -> Option<Timestamp> {
        let mut handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            handles.push(tokio::spawn(execute_task(
                application_id,
                task,
                operators.clone(),
            )));
        }
        for handle in handles {
            let outcome = match handle.await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => {
                    error!(%application_id, ?group, %error, "Error executing task");
                    return Some(Timestamp::now().saturating_add(config.retry_delay));
                }
                Err(error) => {
                    error!(%application_id, ?group, %error, "Task panicked");
                    return Some(Timestamp::now().saturating_add(config.retry_delay));
                }
            };
            if let Err(timestamp) = Self::submit_task_outcome(
                &chain_client,
                application_id,
                &outcome,
                config.retry_delay,
            )
            .await
            {
                return Some(timestamp);
            }
        }
        None
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
        // An outcome's id is the group it belongs to.
        let group = &task_outcome.id;
        let retry_with_delay = || Timestamp::now().saturating_add(retry_delay);
        let query = task_outcome_query(task_outcome);
        let bytes = serde_json::to_vec(&json!({"query": query})).map_err(|error| {
            error!(%application_id, ?group, %error, "Error serializing task outcome query");
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
                error!(%application_id, ?group, %error, "Error querying application");
                retry_with_delay()
            })?;
        if !operations.is_empty() {
            match chain_client
                .execute_operations(operations, vec![])
                .await
                .map_err(|error| {
                    error!(%application_id, ?group, %error, "Error executing operations");
                    retry_with_delay()
                })? {
                ClientOutcome::Committed(_) => {}
                ClientOutcome::WaitForTimeout(timeout) => {
                    error!(%application_id, ?group, "Not the round leader, retrying after {}", timeout.timestamp);
                    return Err(timeout.timestamp);
                }
                ClientOutcome::Conflict(_) => {
                    debug!(%application_id, ?group, "Block conflict, retrying immediately");
                    return Err(Timestamp::now());
                }
            }
        }
        Ok(())
    }
}

/// Runs one task's operator binary to completion.
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

/// Awaits a task group, reporting it once if it outlives `slow_group_threshold`.
///
/// Reporting only — the group keeps running afterwards. Cutting it short would abort work that may
/// be about to succeed, and since the task would then be retried, a threshold below what the work
/// needs would repeat it forever instead of ever completing it.
async fn await_group(
    application_id: ApplicationId,
    group: &Option<String>,
    mut handle: tokio::task::JoinHandle<Option<Timestamp>>,
    config: TaskProcessorConfig,
) -> Option<Timestamp> {
    let on_panic = |error| {
        error!(%application_id, ?group, %error, "Task group panicked");
        Some(Timestamp::now().saturating_add(config.retry_delay))
    };
    let threshold = config.slow_group_threshold.as_duration();
    match tokio::time::timeout(threshold, &mut handle).await {
        Ok(result) => result.unwrap_or_else(on_panic),
        Err(_) => {
            warn!(
                %application_id, ?group,
                "Task group still running after {threshold:?}; leaving it to finish"
            );
            #[cfg(with_metrics)]
            metrics::SLOW_TASK_GROUPS.inc();
            handle.await.unwrap_or_else(on_panic)
        }
    }
}

/// Groups the tasks of a batch by id, keeping their relative order.
///
/// Tasks sharing an id, and all the tasks without one, can only be told apart by position, so
/// they belong to the same group. A distinctly identified task is a group of its own.
fn group_tasks(tasks: Vec<Task>) -> Vec<(Option<String>, Vec<Task>)> {
    let mut groups = BTreeMap::<Option<String>, Vec<Task>>::new();
    for task in tasks {
        groups.entry(task.id.clone()).or_default().push(task);
    }
    groups.into_iter().collect()
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

    fn task(id: Option<&str>, input: &str) -> Task {
        Task {
            id: id.map(str::to_string),
            operator: "echo".to_string(),
            input: input.to_string(),
        }
    }

    /// The inputs of each group, keyed by the group's id.
    fn inputs(groups: Vec<(Option<String>, Vec<Task>)>) -> Vec<(Option<String>, Vec<String>)> {
        groups
            .into_iter()
            .map(|(group, tasks)| (group, tasks.into_iter().map(|task| task.input).collect()))
            .collect()
    }

    fn group(id: Option<&str>, inputs: &[&str]) -> (Option<String>, Vec<String>) {
        (
            id.map(str::to_string),
            inputs.iter().copied().map(str::to_string).collect(),
        )
    }

    #[test]
    fn test_group_tasks_keeps_distinctly_identified_tasks_apart() {
        let tasks = vec![task(Some("1"), "first"), task(Some("2"), "second")];
        assert_eq!(
            inputs(group_tasks(tasks)),
            vec![group(Some("1"), &["first"]), group(Some("2"), &["second"])]
        );
    }

    #[test]
    fn test_group_tasks_gathers_the_unidentified_ones() {
        let tasks = vec![
            task(None, "first"),
            task(Some("1"), "second"),
            task(None, "third"),
        ];
        assert_eq!(
            inputs(group_tasks(tasks)),
            vec![
                group(None, &["first", "third"]),
                group(Some("1"), &["second"])
            ]
        );
    }

    #[test]
    fn test_group_tasks_gathers_the_ones_sharing_an_id() {
        let tasks = vec![
            task(Some("dup"), "first"),
            task(Some("other"), "second"),
            task(Some("dup"), "third"),
        ];
        assert_eq!(
            inputs(group_tasks(tasks)),
            vec![
                group(Some("dup"), &["first", "third"]),
                group(Some("other"), &["second"])
            ]
        );
    }

    /// Writes an executable shell script and returns an operator map pointing at it.
    fn operator_running(script: &str, dir: &tempfile::TempDir) -> OperatorMap {
        use std::os::unix::fs::PermissionsExt as _;

        let path = dir.path().join("operator");
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        Arc::new(BTreeMap::from([("op".to_string(), path)]))
    }

    /// A syntactically valid application id. `CryptoHash`'s `FromStr` is hex over 32 bytes and,
    /// unlike `test_hash`, is not gated behind `with_testing`.
    fn app_id() -> ApplicationId {
        ApplicationId::new("00".repeat(32).parse().unwrap())
    }

    fn timed_task() -> Task {
        Task {
            id: None,
            operator: "op".to_string(),
            input: String::new(),
        }
    }

    #[tokio::test]
    async fn execute_task_returns_the_operator_output() {
        let dir = tempfile::tempdir().unwrap();
        let operators = operator_running("echo done", &dir);
        let outcome = execute_task(app_id(), timed_task(), operators)
            .await
            .unwrap();
        assert_eq!(outcome.output.trim(), "done");
    }

    #[tokio::test]
    async fn a_group_that_outlives_the_threshold_is_reported_but_still_awaited() {
        let expected = Some(Timestamp::from(4_242));
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            expected
        });
        let config = TaskProcessorConfig {
            retry_delay: TimeDelta::from_secs(5),
            // Far below what the group takes, so the slow path is the one exercised.
            slow_group_threshold: TimeDelta::from_millis(10),
        };
        // The point of the assertion: crossing the threshold reports the group, it does not cut
        // it short, so the result it was about to produce still comes back.
        let retry_at = await_group(app_id(), &Some("group".to_string()), handle, config).await;
        assert_eq!(retry_at, expected);
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
