// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use std::{io::Write, process::Command};
use tempfile::tempdir;

#[test]
#[ignore]
fn test_examples_in_readme() -> std::io::Result<()> {
    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);

    let mut test_script = std::fs::File::create(dir.path().join("test.sh"))?;
    write!(&mut test_script, "{}", quotes.pop().unwrap())?;

    let status = Command::new("bash")
        .current_dir("..") // root of the repo
        .env("DEST", dir.path())
        .arg("-e")
        .arg("-x")
        .arg(dir.path().join("test.sh"))
        .status()?;
    assert!(status.success());
    Ok(())
}

#[allow(clippy::while_let_on_iterator)]
fn get_bash_quotes<R>(reader: R) -> std::io::Result<Vec<String>>
where
    R: std::io::BufRead,
{
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
