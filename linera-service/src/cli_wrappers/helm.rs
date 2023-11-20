// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::util::CommandExt;
use anyhow::{Context, Result};
use pathdiff::diff_paths;
use std::path::{Path, PathBuf};
use tokio::process::Command;

pub struct HelmRelease {
    name: String,
}

impl HelmRelease {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub async fn install(
        &self,
        configs_dir: &PathBuf,
        server_config_id: usize,
        github_root: &Path,
    ) -> Result<()> {
        let execution_dir = format!("{}/kubernetes/linera-validator", github_root.display());

        let configs_dir = diff_paths(configs_dir, execution_dir.clone())
            .context("Getting relative path failed")?;
        let configs_dir = configs_dir.to_str().expect("Getting str failed");

        Command::new("helm")
            .current_dir(&execution_dir)
            .arg("install")
            .arg(&self.name)
            .arg(".")
            .args(["--values", "values-local.yaml"])
            .arg("--wait")
            .args(["--set", "installCRDs=true"])
            .args([
                "--set",
                &format!("validator.serverConfig={configs_dir}/server_{server_config_id}.json"),
            ])
            .args([
                "--set",
                &format!("validator.genesisConfig={configs_dir}/genesis.json"),
            ])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }
}
