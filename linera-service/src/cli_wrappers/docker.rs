// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::CommandExt;
use anyhow::{Context, Result};
use pathdiff::diff_paths;
use std::path::PathBuf;
use tokio::process::Command;

pub struct DockerImage {
    name: String,
}

impl DockerImage {
    pub async fn new(name: String, bin_path: &PathBuf, github_root: &PathBuf) -> Result<Self> {
        let docker_image = Self { name };
        docker_image.build(bin_path, github_root).await?;
        Ok(docker_image)
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    async fn build(&self, bin_path: &PathBuf, github_root: &PathBuf) -> Result<()> {
        let bin_path = diff_paths(bin_path, github_root).context("Getting relative path failed")?;
        let binaries_arg = format!(
            "binaries={}",
            bin_path.to_str().context("Getting str failed")?
        );

        Command::new("docker")
            .current_dir(github_root)
            .arg("build")
            .args(["-f", "docker/Dockerfile"])
            .args(["--build-arg", &binaries_arg])
            .arg(".")
            .args(["-t", &self.name])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }
}
