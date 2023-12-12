// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "rocksdb")]

mod common;

use common::INTEGRATION_TEST_GUARD;
use linera_service::util::QuotedBashScript;
use tokio::{process::Command, sync::Mutex};

#[test_case::test_case(".." ; "main")]
#[test_case::test_case("../examples/fungible" ; "fungible")]
#[test_case::test_case("../examples/crowd-funding" ; "crowd funding")]
#[test_case::test_case("../examples/social" ; "social")]
#[test_log::test(tokio::test)]
async fn test_script_in_readme(path: &str) -> std::io::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .await;
    let script = QuotedBashScript::from_markdown(format!("{path}/README.md"))?;
    let status = Command::new("bash")
        // Run from the root of the repo.
        .current_dir("..")
        // Increase log verbosity to verify that services can write to stderr.
        .env("RUST_LOG", "linera_service=debug")
        .arg("-e")
        .arg("-x")
        .arg(script.path())
        .status()
        .await?;

    assert!(status.success());
    Ok(())
}
