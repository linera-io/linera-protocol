// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::Result;
use linera_base::command::CommandExt;
use tokio::process::Command;

pub async fn get_github_root() -> Result<PathBuf> {
    let github_root = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .spawn_and_wait_for_stdout()
        .await?;
    Ok(PathBuf::from(
        github_root
            .strip_suffix('\n')
            .expect("Stripping suffix should not fail")
            .to_string(),
    ))
}
