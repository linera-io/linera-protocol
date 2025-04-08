// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, process::Command};

use anyhow::Result;
use linera_base::command::resolve_binary;
use linera_service::cli_wrappers::{local_net::PathProvider, ClientWrapper, Network, OnClientDrop};

mod common;

#[test_log::test(tokio::test)]
async fn test_project_new() -> Result<()> {
    let _rustflags_override = common::override_disable_warnings_as_errors();
    let path_provider = PathProvider::create_temporary_directory()?;
    let id = 0;
    let client = ClientWrapper::new(
        path_provider,
        Network::Grpc,
        None,
        id,
        OnClientDrop::LeakChains,
    );
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let linera_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should not be at the root");
    let tmp_dir = client.project_new("init-test", linera_root).await?;
    let project_dir = tmp_dir.path().join("init-test");
    client
        .build_application(project_dir.as_path(), "init-test", false)
        .await?;

    let mut child = Command::new("cargo")
        .args(["fmt", "--check"])
        .current_dir(project_dir.as_path())
        .spawn()?;
    assert!(child.wait()?.success());

    let mut child = Command::new("cargo")
        .arg("test")
        .current_dir(project_dir.as_path())
        .spawn()?;
    assert!(child.wait()?.success());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_project_test() -> Result<()> {
    let path_provider = PathProvider::create_temporary_directory()?;
    let id = 0;
    let client = ClientWrapper::new(
        path_provider,
        Network::Grpc,
        None,
        id,
        OnClientDrop::LeakChains,
    );
    client
        .project_test(&ClientWrapper::example_path("counter")?)
        .await?;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_resolve_binary() -> Result<()> {
    resolve_binary("linera", env!("CARGO_PKG_NAME")).await?;
    resolve_binary("linera-proxy", env!("CARGO_PKG_NAME")).await?;
    assert!(resolve_binary("linera-spaceship", env!("CARGO_PKG_NAME"))
        .await
        .is_err());

    Ok(())
}
