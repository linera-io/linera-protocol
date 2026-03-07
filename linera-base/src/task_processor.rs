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
///   nextActions(now: Timestamp!): ProcessorActions!
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
    /// The application is requesting the execution of the given tasks.
    pub execute_tasks: Vec<Task>,
}

scalar!(ProcessorActions);

/// An off-chain task requested by an on-chain application.
#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
    /// The operator handling the task.
    pub operator: String,
    /// The input argument in JSON.
    pub input: String,
}

/// The result of executing an off-chain operator.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskOutcome {
    /// The operator handling the task.
    pub operator: String,
    /// The JSON output.
    pub output: String,
}

scalar!(TaskOutcome);
