// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{BufRead, BufReader, Write},
    num::ParseIntError,
    path::Path,
    time::Duration,
};

use anyhow::{bail, Context as _, Result};
use async_graphql::http::GraphiQLSource;
use axum::response::{self, IntoResponse};
use http::Uri;
#[cfg(test)]
use linera_base::command::parse_version_message;
use linera_base::data_types::TimeDelta;
pub use linera_client::util::*;
use tracing::debug;

// Exported for readme e2e tests.
pub static DEFAULT_PAUSE_AFTER_LINERA_SERVICE_SECS: &str = "3";
pub static DEFAULT_PAUSE_AFTER_GQL_MUTATIONS_SECS: &str = "3";

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

pub fn read_json<T: serde::de::DeserializeOwned>(path: impl Into<std::path::PathBuf>) -> Result<T> {
    Ok(serde_json::from_reader(fs_err::File::open(path)?)?)
}

#[cfg(with_testing)]
#[macro_export]
macro_rules! test_name {
    () => {
        stdext::function_name!()
            .strip_suffix("::{{closure}}")
            .expect("should be called from the body of a test")
    };
}

pub struct Markdown<B> {
    buffer: B,
}

impl Markdown<BufReader<fs_err::File>> {
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let buffer = BufReader::new(fs_err::File::open(path.as_ref())?);
        Ok(Self { buffer })
    }
}

impl<B> Markdown<B>
where
    B: BufRead,
{
    #[expect(clippy::while_let_on_iterator)]
    pub fn extract_bash_script_to(
        self,
        mut output: impl Write,
        pause_after_linera_service: Option<Duration>,
        pause_after_gql_mutations: Option<Duration>,
    ) -> std::io::Result<()> {
        let mut lines = self.buffer.lines();

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

                        if let Some(pause) = pause_after_linera_service {
                            if line.contains("linera service") {
                                quote += &format!("sleep {}\n", pause.as_secs());
                            }
                        }
                    }
                    writeln!(output, "{}", quote)?;
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

                writeln!(output, "QUERY=\"{}\"", quote.replace('"', "\\\""))?;
                writeln!(
                    output,
                    "JSON_QUERY=$( jq -n --arg q \"$QUERY\" '{{\"query\": $q}}' )"
                )?;
                writeln!(
                    output,
                    "QUERY_RESULT=$( \
                     curl -w '\\n' -g -X POST \
                       -H \"Content-Type: application/json\" \
                       -d \"$JSON_QUERY\" {uri} \
                     | tee /dev/stderr \
                     | jq -e .data \
                     )"
                )?;

                if let Some(pause) = pause_after_gql_mutations {
                    // Hack: let's add a pause after mutations.
                    if quote.starts_with("mutation") {
                        writeln!(output, "sleep {}\n", pause.as_secs())?;
                    }
                }
            }
        }

        output.flush()?;
        Ok(())
    }
}

/// Returns an HTML response constructing the GraphiQL web page for the given URI.
pub(crate) async fn graphiql(uri: Uri) -> impl IntoResponse {
    let source = GraphiQLSource::build()
        .endpoint(uri.path())
        .subscription_endpoint("/ws")
        .finish()
        .replace("@17", "@18")
        .replace(
            "ReactDOM.render(",
            "ReactDOM.createRoot(document.getElementById(\"graphiql\")).render(",
        );
    response::Html(source)
}

pub fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_millis(s.parse()?))
}

pub fn parse_millis_delta(s: &str) -> Result<TimeDelta, ParseIntError> {
    Ok(TimeDelta::from_millis(s.parse()?))
}

/// Checks the condition five times with increasing delays. Returns true if it is met.
#[cfg(with_testing)]
pub async fn eventually<F>(condition: impl Fn() -> F) -> bool
where
    F: std::future::Future<Output = bool>,
{
    for i in 0..5 {
        linera_base::time::timer::sleep(std::time::Duration::from_secs(i)).await;
        if condition().await {
            return true;
        }
    }
    false
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
    let buffer = std::io::Cursor::new(readme);
    let markdown = Markdown { buffer };
    let mut script = Vec::new();
    markdown
        .extract_bash_script_to(&mut script, None, None)
        .unwrap();
    let expected = "some bash\n\nsome other bash\n\n";
    assert_eq!(String::from_utf8_lossy(&script), expected);
}
