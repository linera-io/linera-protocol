// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "rocksdb")]

mod common;

use common::INTEGRATION_TEST_GUARD;
use std::io::Write;
use tempfile::tempdir;
use tokio::process::Command;

#[test_log::test(tokio::test)]
async fn test_examples_in_readme() -> std::io::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;

    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);
    let quote = quotes.pop().unwrap();

    let test_file = dir.path().join("test.sh");
    let mut test_script = std::fs::File::create(test_file)?;
    write!(&mut test_script, "{}", quote)?;

    let status = Command::new("bash")
        // Run from the root of the repo.
        .current_dir("..")
        // Increase log verbosity to verify that services can write to stderr.
        .env("RUST_LOG", "linera_service=debug")
        .arg("-e")
        .arg("-x")
        .arg(dir.path().join("test.sh"))
        .status()
        .await?;

    assert!(status.success());
    Ok(())
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
