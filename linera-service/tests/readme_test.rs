// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

mod common;

use std::{env, path::PathBuf};

use common::INTEGRATION_TEST_GUARD;
use linera_client::{
    client_options::{
        DEFAULT_PAUSE_AFTER_GQL_MUTATIONS_SECS, DEFAULT_PAUSE_AFTER_LINERA_SERVICE_SECS,
    },
    util::parse_secs,
};
use linera_service::{test_name, util::Markdown};
use tempfile::tempdir;
use tokio::process::Command;

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

    let file = Markdown::new(PathBuf::from(path).join("README.md"))?;
    let tmp_dir = tempdir()?;
    let path = tmp_dir.path().join("test.sh");
    let mut script = fs_err::File::create(&path)?;
    let pause_after_linera_service = parse_secs(DEFAULT_PAUSE_AFTER_LINERA_SERVICE_SECS).unwrap();
    let pause_after_gql_mutations = parse_secs(DEFAULT_PAUSE_AFTER_GQL_MUTATIONS_SECS).unwrap();
    file.extract_bash_script_to(
        &mut script,
        Some(pause_after_linera_service),
        Some(pause_after_gql_mutations),
    )?;

    let mut command = Command::new("bash");
    command
        // Run from the root of the repo.
        .current_dir("..")
        .arg("-e")
        .arg("-x")
        .arg(script.path());

    if env::var_os("RUST_LOG").is_none() {
        // Increase log verbosity to verify that services can write to stderr.
        command.env("RUST_LOG", "linera_execution::wasm=debug");
    }

    let status = command.status().await?;

    assert!(status.success());
    Ok(())
}
