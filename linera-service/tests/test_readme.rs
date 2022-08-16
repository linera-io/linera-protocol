// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//use linera_storage::LocalStackTestContext;
use std::{io::Write, process::Command};
use tempfile::tempdir;

#[test]
fn test_examples_in_readme() -> std::io::Result<()> {
    let dir = tempdir().unwrap();
    let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
    let mut quotes = get_bash_quotes(file)?;
    // Check that we have the expected number of examples starting with "```bash".
    assert_eq!(quotes.len(), 1);
    let quote = quotes.pop().unwrap();

    let mut test_script = std::fs::File::create(dir.path().join("test.sh"))?;
    write!(&mut test_script, "{}", quote)?;

    let status = Command::new("bash")
        .current_dir("..") // root of the repo
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

// const ROCKSDB_STORAGE: &str = "--storage rocksdb:server_\"$I\"_\"$J\".db";
// const S3_STORAGE: &str = "--storage s3:localstack:server-\"$I\"";

// #[test]
// #[ignore]
// fn test_examples_in_readme_s3() -> std::io::Result<()> {
//     let _localstack_guard = LocalStackTestContext::new();
//     let dir = tempdir().unwrap();
//     let file = std::io::BufReader::new(std::fs::File::open("../README.md")?);
//     let mut quotes = get_bash_quotes(file)?;
//     // Check that we have the expected number of examples starting with "```bash".
//     assert_eq!(quotes.len(), 1);
//     let quote = quotes.pop().unwrap();
//     assert_eq!(quote.matches(ROCKSDB_STORAGE).count(), 3);
//     let quote = quote.replace(ROCKSDB_STORAGE, S3_STORAGE);

//     let mut test_script = std::fs::File::create(dir.path().join("test.sh"))?;
//     write!(&mut test_script, "{}", quote)?;

//     let status = Command::new("bash")
//         .current_dir("..") // root of the repo
//         .arg("-e")
//         .arg("-x")
//         .arg(dir.path().join("test.sh"))
//         .status()?;
//     assert!(status.success());
//     Ok(())
// }
