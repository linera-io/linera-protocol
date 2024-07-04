// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage_service")]

mod common;

use common::INTEGRATION_TEST_GUARD;
use linera_service::{test_name, util::QuotedBashAndGraphQlScript};
use tokio::{process::Command, time::Duration};

#[test_case::test_case(".." ; "main")]
#[test_case::test_case("../examples/amm" ; "amm")]
#[test_case::test_case("../examples/counter" ; "counter")]
#[test_case::test_case("../examples/crowd-funding" ; "crowd funding")]
#[test_case::test_case("../examples/fungible" ; "fungible")]
#[test_case::test_case("../examples/gen-nft" ; "gen-nft")]
#[test_case::test_case("../examples/hex-game" ; "hex-game")]
#[test_case::test_case("../examples/native-fungible" ; "native-fungible")]
#[test_case::test_case("../examples/non-fungible" ; "non-fungible")]
#[test_case::test_case("../examples/matching-engine" ; "matching engine")]
#[test_case::test_case("../examples/meta-counter" ; "meta counter")]
#[test_case::test_case("../examples/social" ; "social")]
#[test_log::test(tokio::test)]
async fn test_script_in_readme_with_storage_service(path: &str) -> std::io::Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting test {} for path {}", test_name!(), path);

    let script = QuotedBashAndGraphQlScript::from_markdown(
        format!("{path}/README.md"),
        Some(Duration::from_secs(3)),
    )?;
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
