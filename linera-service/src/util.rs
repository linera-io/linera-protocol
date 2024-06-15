// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::path::{Path, PathBuf};
use std::{num::ParseIntError, time::Duration};

use anyhow::{bail, Context as _, Result};
use async_graphql::http::GraphiQLSource;
use axum::response::{self, IntoResponse};
use futures::future;
use http::Uri;
#[cfg(test)]
use linera_base::command::parse_version_message;
use linera_base::data_types::{TimeDelta, Timestamp};
pub use linera_client::util::*;
use linera_core::{
    data_types::RoundTimeout,
    node::NotificationStream,
    worker::Reason,
};
use tokio::signal::unix;
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Extension trait for [`tokio::process::Child`].
pub trait ChildExt: std::fmt::Debug {
    fn ensure_is_running(&mut self) -> Result<()>;
}

impl ChildExt for tokio::process::Child {
    fn ensure_is_running(&mut self) -> Result<()> {
        if let Some(status) = self.try_wait().context("try_wait child process")? {
            bail!(
                "Child process {:?} already exited with status: {}",
                self,
                status
            );
        }
        debug!("Child process {:?} is running as expected.", self);
        Ok(())
    }
}

/// Listens for shutdown signals, and notifies the [`CancellationToken`] if one is
/// received.
pub async fn listen_for_shutdown_signals(shutdown_sender: CancellationToken) {
    let _shutdown_guard = shutdown_sender.drop_guard();

    let mut sigint =
        unix::signal(unix::SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
    let mut sigterm =
        unix::signal(unix::SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
    let mut sighup =
        unix::signal(unix::SignalKind::hangup()).expect("Failed to set up SIGHUP handler");

    tokio::select! {
        _ = sigint.recv() => debug!("Received SIGINT"),
        _ = sigterm.recv() => debug!("Received SIGTERM"),
        _ = sighup.recv() => debug!("Received SIGHUP"),
    }
}

#[cfg(with_testing)]
use {
    std::io::Write,
    tempfile::{tempdir, TempDir},
};

/// Returns an HTML response constructing the GraphiQL web page for the given URI.
pub(crate) async fn graphiql(uri: Uri) -> impl IntoResponse {
    let source = GraphiQLSource::build()
        .endpoint(uri.path())
        .subscription_endpoint("/ws")
        .finish();
    response::Html(source)
}

pub fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_millis(s.parse()?))
}

pub fn parse_millis_delta(s: &str) -> Result<TimeDelta, ParseIntError> {
    Ok(TimeDelta::from_millis(s.parse()?))
}

#[test]
fn test_parse_version_message() {
    let s = "something\n . . . version12\nother things";
    assert_eq!(parse_version_message(s), "version12");

    let s = "something\n . . . version12other things";
    assert_eq!(parse_version_message(s), "things");

    let s = "something . . . version12 other things";
    assert_eq!(parse_version_message(s), "");

    let s = "";
    assert_eq!(parse_version_message(s), "");
}

#[test]
fn test_ignore() {
    let readme = r#"
first line
```bash
some bash
```
second line
```bash
some other bash
```
third line
```bash,ignore
this will be ignored
```
    "#;
    let cursor = std::io::Cursor::new(readme);
    let parsed = QuotedBashAndGraphQlScript::read_bash_and_gql_quotes(cursor, None).unwrap();
    let expected = vec!["some bash\n".to_string(), "some other bash\n".to_string()];
    assert_eq!(parsed, expected)
}
