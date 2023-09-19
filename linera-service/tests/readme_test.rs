// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "aws", feature = "rocksdb", feature = "scylladb"))]

mod common;

use common::INTEGRATION_TEST_GUARD;
use std::{io::Write, process::ExitStatus};
use tempfile::tempdir;
use tokio::process::Command;

async fn run_test_command(storage: &str) -> std::io::Result<ExitStatus> {
    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);
    let quote = quotes.pop().unwrap();

    let test_file = dir.path().join("test.sh");
    let mut test_script = std::fs::File::create(test_file)?;
    write!(&mut test_script, "{}", quote)?;

    Command::new("bash")
        .current_dir("..") // root of the repo
        .arg("-e")
        .arg("-x")
        .arg(dir.path().join("test.sh"))
        .arg(storage)
        .status()
        .await
}

#[allow(clippy::while_let_on_iterator)]
fn get_bash_quotes(reader: impl std::io::BufRead) -> std::io::Result<Vec<String>> {
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
        }
    }

    Ok(result)
}

#[cfg(feature = "rocksdb")]
#[test_log::test(tokio::test)]
async fn test_rocks_db_examples_in_readme() -> std::io::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let status = run_test_command("ROCKSDB").await?;
    assert!(status.success());
    Ok(())
}

#[cfg(feature = "aws")]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_examples_in_readme() -> anyhow::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let status = run_test_command("DYNAMODB").await?;
    assert!(status.success());
    Ok(())
}

#[cfg(feature = "scylladb")]
#[test_log::test(tokio::test)]
async fn test_scylla_db_examples_in_readme() -> anyhow::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let status = run_test_command("SCYLLADB").await?;
    assert!(status.success());
    Ok(())
}
