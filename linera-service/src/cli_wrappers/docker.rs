// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use pathdiff::diff_paths;
use std::path::PathBuf;
use tokio::process::Command;

use crate::util::{current_binary_parent, CommandExt};

pub struct DockerImage {
    name: String,
}

impl DockerImage {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub async fn build(
        name: String,
        binaries: &Option<Option<PathBuf>>,
        github_root: &PathBuf,
    ) -> Result<Self> {
        let docker_image = Self { name: name.clone() };
        let mut command = Command::new("docker");
        command
            .current_dir(github_root)
            .arg("build")
            .args(["-f", "docker/Dockerfile"]);

        if let Some(binaries) = binaries {
            let bin_path = if let Some(bin_path) = binaries {
                // If binaries is set, but with a directory path arg, we'll get the binaries
                // from that directory path
                diff_paths(bin_path, github_root).context("Getting relative path failed")?
            } else {
                // If binaries is set, but with no directory path arg, we'll get the binaries
                // from current_binary_parent
                diff_paths(
                    current_binary_parent()
                        .expect("Fetching current binaries path should not fail"),
                    github_root,
                )
                .context("Getting relative path failed")?
            };

            let binaries_arg = format!(
                "binaries={}",
                bin_path.to_str().context("Getting str failed")?
            );

            command.args(["--build-arg", &binaries_arg]);
        } else {
            // If binaries is None, we'll do the build inside the Docker container
            let arch = std::env::consts::ARCH;

            // Translate architecture for Docker build arg
            let docker_arch = match arch {
                "arm" => "aarch",
                _ => arch,
            };

            let target_arg = format!("target={}-unknown-linux-gnu", docker_arch);
            command.args(["--build-arg", &target_arg]);
        }

        #[cfg(not(any(test, feature = "test")))]
        command
            .args([
                "--build-arg",
                &format!("git_commit={}", linera_base::VersionInfo::get()?.git_commit),
            ])
            .args([
                "--build-arg",
                &format!(
                    "build_date={}",
                    // Same format as $(TZ=UTC date)
                    chrono::Utc::now().format("%a %b %d %T UTC %Y").to_string()
                ),
            ]);

        command
            .arg(".")
            .args(["-t", &name])
            .spawn_and_wait()
            .await?;

        Ok(docker_image)
    }
}
