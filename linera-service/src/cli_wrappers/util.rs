// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use pathdiff::diff_paths;
use tokio::process::Command;

use crate::util::CommandExt;

pub async fn get_github_root_relative_to(current_dir: &String) -> Result<String, anyhow::Error> {
    let github_root = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .spawn_and_wait_for_stdout()
        .await?;
    let github_root = github_root
        .strip_suffix('\n')
        .expect("Stripping suffix should not fail");
    let github_root =
        diff_paths(github_root, current_dir).expect("Getting relative path should not fail");
    let github_root = github_root.to_str().expect("Getting str should not fail");

    if github_root.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(github_root.to_string())
    }
}

pub async fn get_github_root_relative_to_current_dir() -> Result<String, anyhow::Error> {
    let current_dir = get_current_dir().await?;
    Ok(get_github_root_relative_to(&current_dir).await?)
}

pub async fn get_current_dir() -> Result<String, anyhow::Error> {
    let current_dir = Command::new("pwd").spawn_and_wait_for_stdout().await?;
    Ok(current_dir
        .strip_suffix('\n')
        .expect("Stripping suffix should not fail")
        .to_string())
}
