// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "rocksdb")]

mod common;

use common::INTEGRATION_TEST_GUARD;
use linera_service::util::QuotedBashScript;
use tokio::process::Command;

#[test_log::test(tokio::test)]
async fn test_examples_in_readme() -> std::io::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    let script = QuotedBashScript::from_markdown("../README.md")?;
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
