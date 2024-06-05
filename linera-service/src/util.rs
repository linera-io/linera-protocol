// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::path::{Path, PathBuf};
use std::{num::ParseIntError, time::Duration};

use anyhow::{bail, Context as _, Result};
use async_graphql::http::GraphiQLSource;
use axum::response::{self, IntoResponse};
use http::Uri;
#[cfg(test)]
use linera_base::command::parse_version_message;
use linera_base::data_types::TimeDelta;
use tokio::signal::unix;
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
    let mut sigint =
        unix::signal(unix::SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
    let mut sigterm =
        unix::signal(unix::SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
    let mut sigpipe =
        unix::signal(unix::SignalKind::pipe()).expect("Failed to set up SIGPIPE handler");
    let mut sighup =
        unix::signal(unix::SignalKind::hangup()).expect("Failed to set up SIGHUP handler");

    tokio::select! {
        _ = sigint.recv() => (),
        _ = sigterm.recv() => (),
        _ = sigpipe.recv() => (),
        _ = sighup.recv() => (),
    }

    shutdown_sender.cancel();
}

#[cfg(with_testing)]
use {
    std::io::Write,
    tempfile::{tempdir, TempDir},
};

#[cfg(with_testing)]
pub struct QuotedBashAndGraphQlScript {
    tmp_dir: TempDir,
    path: PathBuf,
}

#[cfg(with_testing)]
impl QuotedBashAndGraphQlScript {
    pub fn from_markdown<P: AsRef<Path>>(
        source_path: P,
        pause_after_gql_mutations: Option<Duration>,
    ) -> Result<Self, std::io::Error> {
        let file = std::io::BufReader::new(fs_err::File::open(source_path.as_ref())?);
        let tmp_dir = tempdir()?;
        let quotes = Self::read_bash_and_gql_quotes(file, pause_after_gql_mutations)?;

        let path = tmp_dir.path().join("test.sh");

        let mut test_script = fs_err::File::create(&path)?;
        for quote in quotes {
            writeln!(&mut test_script, "{}", quote)?;
        }

        Ok(Self { tmp_dir, path })
    }

    pub fn tmp_dir(&self) -> &Path {
        self.tmp_dir.as_ref()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(clippy::while_let_on_iterator)]
    fn read_bash_and_gql_quotes(
        reader: impl std::io::BufRead,
        pause_after_gql_mutations: Option<Duration>,
    ) -> std::io::Result<Vec<String>> {
        let mut result = Vec::new();
        let mut lines = reader.lines();

        while let Some(line) = lines.next() {
            let line = line?;

            if line.starts_with("```bash") {
                if line.ends_with("ignore") {
                    continue;
                } else {
                    let mut quote = String::new();
                    while let Some(line) = lines.next() {
                        let line = line?;
                        if line.starts_with("```") {
                            break;
                        }
                        quote += &line;
                        quote += "\n";

                        if line.contains("linera service") {
                            quote += "sleep 3";
                            quote += "\n";
                        }
                    }
                    result.push(quote);
                }
            } else if let Some(uri) = line.strip_prefix("```gql,uri=") {
                let mut quote = String::new();
                while let Some(line) = lines.next() {
                    let line = line?;
                    if line.starts_with("```") {
                        break;
                    }
                    quote += &line;
                    quote += "\n";
                }
                let json = serde_json::to_string(&quote).unwrap();
                let command = format!(
                    "curl -w '\\n' -g -X POST -H \"Content-Type: application/json\" -d '{{ \"query\": {json} }}' {uri} \
                     | tee /dev/stderr \
                     | jq -e .data \n"
                );
                result.push(command);

                if let Some(pause) = pause_after_gql_mutations {
                    // Hack: let's add a pause after mutations.
                    if quote.starts_with("mutation") {
                        result.push(format!("\nsleep {}\n", pause.as_secs()));
                    }
                }
            }
        }

        Ok(result)
    }
}

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
