// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Timeout wrapper for the validator RPCs the benchmark issues.
//!
//! Every blocking call goes through [`timed`] so a hung validator surfaces as a
//! recorded `timeout` error and the run keeps making progress instead of
//! freezing on a single unresponsive request.

use std::{future::Future, time::Duration};

use tokio::time::timeout;

use super::preflight::categorize;

/// Run an RPC future under `dur`. Returns the value on success, or an error
/// category string: `timeout` on elapse, otherwise the categorized RPC error.
pub(super) async fn timed<T, E>(
    dur: Duration,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, String>
where
    E: std::fmt::Display,
{
    // Box the inner future: the validator RPC futures are large, and keeping them
    // on the heap keeps every `timed(...)` call site small (clippy::large_futures).
    match timeout(dur, Box::pin(fut)).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(categorize(&error.to_string()).to_string()),
        Err(_elapsed) => Err("timeout".to_string()),
    }
}
