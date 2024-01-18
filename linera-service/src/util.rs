// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, ensure, Context as _, Result};
use async_graphql::http::GraphiQLSource;
use async_trait::async_trait;
use axum::response::{self, IntoResponse};
use http::Uri;
use std::{
    num::ParseIntError,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;
use tracing::{debug, error};

/// Attempts to resolve the path and test the version of the given binary against our
/// package version.
///
/// This is meant for binaries of the Linera repository. We use the current running binary
/// to locate the parent directory where to look for the given name.
pub async fn resolve_binary(name: &'static str, package: &'static str) -> Result<PathBuf> {
    let current_binary = std::env::current_exe()?;
    resolve_binary_in_same_directory_as(&current_binary, name, package).await
}

pub fn current_binary_parent() -> Result<PathBuf> {
    let current_binary = std::env::current_exe()?;
    binary_parent(&current_binary)
}

pub fn binary_parent(current_binary: &Path) -> Result<PathBuf> {
    let mut current_binary_parent = current_binary
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize '{}'", current_binary.display()))?;
    current_binary_parent.pop();

    #[cfg(any(test, feature = "test"))]
    // Test binaries are typically in target/debug/deps while crate binaries are in target/debug
    // (same thing for target/release).
    let current_binary_parent = if current_binary_parent.ends_with("target/debug/deps")
        || current_binary_parent.ends_with("target/release/deps")
    {
        PathBuf::from(current_binary_parent.parent().unwrap())
    } else {
        current_binary_parent
    };

    Ok(current_binary_parent)
}

/// Same as [`resolve_binary`] but gives the option to specify a binary path to use as
/// reference. The path may be relative or absolute but it must point to a valid file on
/// disk.
pub async fn resolve_binary_in_same_directory_as<P: AsRef<Path>>(
    current_binary: P,
    name: &'static str,
    package: &'static str,
) -> Result<PathBuf> {
    let current_binary = current_binary.as_ref();
    debug!(
        "Resolving binary {name} based on the current binary path: {}",
        current_binary.display()
    );

    let current_binary_parent =
        binary_parent(current_binary).expect("Fetching binary directory should not fail");

    let binary = current_binary_parent.join(name);
    let version = env!("CARGO_PKG_VERSION");
    if !binary.exists() {
        error!(
            "Cannot find a binary {name} in the directory {}. \
             Consider using `cargo install {package}` or `cargo build -p {package}`",
            current_binary_parent.display()
        );
        bail!("Failed to resolve binary {name}");
    }

    // Quick version check.
    debug!("Checking the version of {}", binary.display());
    let version_message = Command::new(&binary)
        .arg("--version")
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to execute and retrieve version from the binary {name} in directory {}",
                current_binary_parent.display()
            )
        })?
        .stdout;
    let found_version = String::from_utf8_lossy(&version_message)
        .trim()
        .split(' ')
        .last()
        .with_context(|| {
            format!(
                "Passing --version to the binary {name} in directory {} returned an empty result",
                current_binary_parent.display()
            )
        })?
        .to_string();
    if version != found_version {
        error!("The binary {name} in directory {} should have version {version} (found {found_version}). \
                Consider using `cargo install {package} --version '{version}'` or `cargo build -p {package}`",
               current_binary_parent.display()
        );
        bail!("Incorrect version for binary {name}");
    }
    debug!("{} has version {version}", binary.display());

    Ok(binary)
}

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
pub struct QuotedBashScript {
    tmp_dir: TempDir,
    path: PathBuf,
}

#[cfg(any(test, feature = "test"))]
impl QuotedBashScript {
    pub fn from_markdown<P: AsRef<Path>>(
        source_path: P,
        pause_after_gql_mutations: Option<Duration>,
    ) -> Result<Self, std::io::Error> {
        let file = std::io::BufReader::new(fs_err::File::open(source_path.as_ref())?);
        let tmp_dir = tempdir()?;
        let quotes = Self::read_bash_quotes(file, pause_after_gql_mutations)?;

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
    fn read_bash_quotes(
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
