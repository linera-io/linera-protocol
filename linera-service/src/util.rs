// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Context as _, Result};
use async_graphql::http::GraphiQLSource;
use axum::response::{self, IntoResponse};
use http::Uri;
#[cfg(test)]
use linera_base::command::parse_version_message;
#[cfg(any(test, feature = "test"))]
use std::path::{Path, PathBuf};
use std::{num::ParseIntError, time::Duration};
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

#[cfg(any(test, feature = "test"))]
use {
    std::io::Write,
    tempfile::{tempdir, TempDir},
};

#[cfg(any(test, feature = "test"))]
pub struct QuotedBashAndGraphQlScript {
    tmp_dir: TempDir,
    path: PathBuf,
}

#[cfg(any(test, feature = "test"))]
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
