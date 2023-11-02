// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use pathdiff::diff_paths;
use tokio::process::Command;

use crate::util::CommandExt;

use super::util::{get_current_dir, get_github_root_relative_to};

pub struct DockerWrapper;

impl DockerWrapper {
    pub async fn build(bin_path: &str) -> Result<String, anyhow::Error> {
        let image_name = "linera-test:latest";
        let current_dir = get_current_dir().await?;
        let github_root = get_github_root_relative_to(&current_dir).await?;

        let bin_path =
            diff_paths(bin_path, current_dir).expect("Getting relative path should not fail");
        let bin_path = bin_path.to_str().expect("Getting str should not fail");

        let binaries_arg = format!("binaries={bin_path}");
        let binaries_arg = binaries_arg.as_str();

        let dockerfile_path = format!("{}/docker/Dockerfile", github_root);
        let dockerfile_path = dockerfile_path.as_str();

        Ok(Command::new("docker")
            .arg("build")
            .args(["-f", dockerfile_path])
            .args(["--build-arg", binaries_arg])
            .arg(github_root)
            .args(["-t", image_name])
            .spawn_and_wait_for_stdout()
            .await?)
    }
}
