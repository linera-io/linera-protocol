// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Command functionality used for spanning child processes.

use anyhow::Context;
use async_trait::async_trait;
use std::process::Stdio;
use anyhow::ensure;
use tracing::debug;

/// Extension trait for [`tokio::process::Command`].
#[async_trait]
pub trait CommandExt: std::fmt::Debug {
    /// Similar to [`tokio::process::Command::spawn`] but sets `kill_on_drop` to `true`.
    /// Errors are tagged with a description of the command.
    fn spawn_into(&mut self) -> anyhow::Result<tokio::process::Child>;

    /// Similar to [`tokio::process::Command::output`] but does not capture `stderr` and
    /// returns the `stdout` as a string. Errors are tagged with a description of the
    /// command.
    async fn spawn_and_wait_for_stdout(&mut self) -> anyhow::Result<String>;

    /// Spawns and waits for process to finish executing.
    /// Will not wait for stdout, use `spawn_and_wait_for_stdout` for that
    async fn spawn_and_wait(&mut self) -> anyhow::Result<()>;

    /// Description used for error reporting.
    fn description(&self) -> String {
        format!("While executing {:?}", self)
    }
}

#[async_trait]
impl CommandExt for tokio::process::Command {
    fn spawn_into(&mut self) -> anyhow::Result<tokio::process::Child> {
        self.kill_on_drop(true);
        debug!("Spawning {:?}", self);
        let child = tokio::process::Command::spawn(self).with_context(|| self.description())?;
        Ok(child)
    }

    async fn spawn_and_wait_for_stdout(&mut self) -> anyhow::Result<String> {
        debug!("Spawning and waiting for {:?}", self);
        self.stdout(Stdio::piped());
        self.stderr(Stdio::inherit());
        self.kill_on_drop(true);

        let child = self.spawn().with_context(|| self.description())?;
        let output = child
            .wait_with_output()
            .await
            .with_context(|| self.description())?;
        ensure!(
            output.status.success(),
            "{}: got non-zero error code {}",
            self.description(),
            output.status
        );
        String::from_utf8(output.stdout).with_context(|| self.description())
    }

    async fn spawn_and_wait(&mut self) -> anyhow::Result<()> {
        debug!("Spawning and waiting for {:?}", self);
        self.kill_on_drop(true);

        let mut child = self.spawn().with_context(|| self.description())?;
        let status = child.wait().await.with_context(|| self.description())?;
        ensure!(
            status.success(),
            "{}: got non-zero error code {}",
            self.description(),
            status
        );

        Ok(())
    }
}
