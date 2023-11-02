// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::process::Command;

use crate::util::CommandExt;

use super::util::get_github_root_relative_to_current_dir;

pub struct HelmWrapper;

impl HelmWrapper {
    pub async fn uninstall(release_name: &str) -> Result<String, anyhow::Error> {
        Ok(Command::new("helm")
            .arg("uninstall")
            .arg(release_name)
            .arg("--wait")
            .spawn_and_wait_for_stdout()
            .await?)
    }

    pub async fn install(release_name: &str) -> Result<String, anyhow::Error> {
        let github_root = get_github_root_relative_to_current_dir().await?;
        let execution_dir = format!("{}/kubernetes/linera-validator", &github_root);

        Ok(Command::new("helm")
            .arg("install")
            .arg(release_name)
            .arg(&execution_dir)
            .args(["--values", &format!("{}/values-local.yaml", &execution_dir)])
            .arg("--wait")
            .args(["--set", "installCRDs=true"])
            .spawn_and_wait_for_stdout()
            .await?)
    }
}
