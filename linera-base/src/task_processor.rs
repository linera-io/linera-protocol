// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types related to the task processor features in the node service.

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

use crate::data_types::Timestamp;

/// The off-chain actions requested by the service of an on-chain application.
///
/// On-chain applications should be ready to respond to GraphQL queries of the form:
/// ```ignore
/// query {
///   nextActions(lastRequestedCallback: Timestamp, now: Timestamp!): ProcessorActions!
/// }
///
/// query {
///   processTaskOutcome(outcome: TaskOutcome!)
/// }
/// ```
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ProcessorActions {
    /// The application is requesting to be called back no later than the given timestamp.
    pub request_callback: Option<Timestamp>,
    /// An optional cursor for the task processor to store and pass to the application
    /// upon the next query for actions.
    pub set_cursor: Option<String>,
    /// The application is requesting the execution of the given tasks.
    ///
    /// If every task carries an [`id`](Task::id), the outcomes are independent: each one is
    /// submitted as soon as its task succeeds, and a task that fails is retried without
    /// holding back its siblings. Otherwise the outcomes are submitted in the order of this
    /// vector and submission stops at the first failure, so that an application matching
    /// them by position never sees a gap.
    pub execute_tasks: Vec<Task>,
}

scalar!(ProcessorActions);

/// An off-chain task requested by an on-chain application.
#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    /// An opaque, application-defined identifier, echoed back in the [`TaskOutcome`].
    ///
    /// Applications that set it match outcomes by identity rather than by position; see
    /// [`ProcessorActions::execute_tasks`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The operator handling the task.
    pub operator: String,
    /// The input argument in JSON.
    pub input: String,
}

/// The result of executing an off-chain operator.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskOutcome {
    /// The identifier of the [`Task`] this outcome belongs to, if it had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The operator handling the task.
    pub operator: String,
    /// The JSON output.
    pub output: String,
}

scalar!(TaskOutcome);
