// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use anyhow::{Context, Result};
use linera_base::command::{current_binary_parent, CommandExt};
use pathdiff::diff_paths;
use tokio::process::Command;

pub struct DockerImage {
    name: String,
}

impl DockerImage {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub async fn build(name: &String, binaries: &BuildArg, github_root: &PathBuf) -> Result<Self> {
        let build_arg = match binaries {
            BuildArg::Directory(bin_path) => {
                // Get the binaries from the specified path
                let bin_path =
                    diff_paths(bin_path, github_root).context("Getting relative path failed")?;
                let bin_path_str = bin_path.to_str().context("Getting str failed")?;
                format!("binaries={bin_path_str}")
            }
            BuildArg::ParentDirectory => {
                // Get the binaries from current_binary_parent
                let parent_path = current_binary_parent()
                    .expect("Fetching current binaries path should not fail");
                let bin_path =
                    diff_paths(parent_path, github_root).context("Getting relative path failed")?;
                let bin_path_str = bin_path.to_str().context("Getting str failed")?;
                format!("binaries={bin_path_str}")
            }
            BuildArg::Build => {
                // Build inside the Docker container
                let arch = std::env::consts::ARCH;
                // Translate architecture for Docker build arg
                let docker_arch = match arch {
                    "arm" => "aarch",
                    _ => arch,
                };
                format!("target={}-unknown-linux-gnu", docker_arch)
            }
        };

        let docker_image = Self { name: name.clone() };
        let mut command = Command::new("docker");
        command
            .current_dir(github_root)
            .arg("build")
            .args(["-f", "docker/Dockerfile"])
            .args(["--build-arg", &build_arg]);

        #[cfg(not(with_testing))]
        command
            .args([
                "--build-arg",
                &format!(
                    "git_commit={}",
                    linera_version::VersionInfo::get()?.git_commit
                ),
            ])
            .args([
                "--build-arg",
                &format!(
                    "build_date={}",
                    // Same format as $(TZ=UTC date)
                    chrono::Utc::now().format("%a %b %d %T UTC %Y")
                ),
            ]);

        command.arg(".").args(["-t", name]).spawn_and_wait().await?;
        Ok(docker_image)
    }
}

/// Which binaries to use in the Docker container.
#[derive(Clone)]
pub enum BuildArg {
    /// Build the binaries within the container.
    Build,
    /// Look for the binaries in the parent directory of the current binary.
    ParentDirectory,
    /// Look for the binaries in the specified path.
    Directory(PathBuf),
}

impl From<Option<Option<PathBuf>>> for BuildArg {
    fn from(arg: Option<Option<PathBuf>>) -> Self {
        match arg {
            None => BuildArg::Build,
            Some(None) => BuildArg::ParentDirectory,
            Some(Some(path)) => BuildArg::Directory(path),
        }
    }
}
